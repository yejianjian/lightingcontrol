"""OPC DA 现场长稳测试工具。

用途：把 DaTabSettings/DaMainWindow 使用的同一个 OpcDaClientEngine 连到现场 OPC DA 服务器，
按指定时长持续观察：
  * 连接状态是否始终 connected（断线/重连次数，任何一次掉线都判定失败）；
  * OPC 服务器状态（GetStatus）与主动读取是否持续成功；
  * 订阅（IOPCAsyncIO2）回调是否持续到达；老服务器降级轮询时计数同样可见；
  * 可选：周期写一个标量点位并读回，验证控制链路在长时间运行后仍然可用。

用法：
    python tools/da_soak_test.py --progid Matrikon.OPC.Simulation.1 --duration-minutes 30
    python tools/da_soak_test.py --host 192.168.1.10 --progid Kepware.KEPServerEX.V6 --duration-minutes 75
    python tools/da_soak_test.py --duration-minutes 2 --sample-seconds 2 --write-tag "Bucket Brigade.Boolean"

退出码：0=PASS，1=FAIL，2=初始连接失败。
"""
import argparse
import asyncio
import datetime
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from opcda.engine import OpcDaClientEngine  # noqa: E402
from utils.logger import global_logger  # noqa: E402


def _now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class DaSoakRunner:
    def __init__(self, args):
        self.args = args
        self.engine = OpcDaClientEngine(
            host=args.host,
            prog_id=args.progid,
            update_rate_ms=args.update_rate_ms,
            browse_max_depth=args.depth,
        )
        # 无 Qt 环境：由引擎心跳主动泵 COM 消息（若在 Qt 里用同一引擎应置 False）
        self.engine.pump_events_enabled = True
        self.engine.heartbeat_interval_s = args.heartbeat_seconds
        self.engine.reconnect_base_delay = 5.0
        self.engine.reconnect_max_delay = 30.0

        self.started = None
        self.duration_s = max(1.0, args.duration_minutes * 60.0)
        self.transitions = []
        self.connection_lost = []
        self.samples = []
        self.callback_count = 0
        self.callback_items = set()
        self.read_failures = 0
        self.write_failures = 0
        self.write_ok = 0
        self._finished = False
        self.final_state = None

    # ---- callbacks -------------------------------------------------------------------
    def _on_state(self, state, detail):
        if self._finished:
            return
        self.transitions.append({
            "at": _now(),
            "elapsed_s": round(time.monotonic() - self.started, 1) if self.started else 0,
            "state": state,
            "reason": (detail or {}).get("reason", ""),
        })

    def _on_lost(self):
        if self._finished:
            return
        self.connection_lost.append({
            "at": _now(),
            "elapsed_s": round(time.monotonic() - self.started, 1) if self.started else 0,
            "state": self.engine.state,
        })

    def _on_data(self, node_id, value, timestamp):
        self.callback_count += 1
        self.callback_items.add(node_id)

    # ---- run -------------------------------------------------------------------------
    async def _connect(self):
        deadline = time.monotonic() + self.args.connect_timeout
        last = None
        while time.monotonic() < deadline:
            try:
                await self.engine.connect()
                await self.engine.bootstrap()
                return True
            except Exception as exc:
                last = exc
                print("[retry] initial connect failed: {!r}; retrying in 5s".format(exc))
                await asyncio.sleep(5)
        print("[fatal] cannot connect within {:.0f}s: {!r}".format(self.args.connect_timeout, last))
        return False

    async def run(self):
        self.started = time.monotonic()
        self.engine.on_state_change = self._on_state
        self.engine.on_connection_lost = self._on_lost
        self.engine.set_subscription_callback(self._on_data)

        print("[soak] endpoint=opcda://{}/{} duration={}min update_rate={}ms".format(
            self.args.host, self.args.progid, self.args.duration_minutes, self.args.update_rate_ms))
        if not await self._connect():
            return 2

        await self.engine.reconnect_forever()
        print("[soak] connected: {} nodes, subscription={}, prog_id={}".format(
            len(self.engine.nodes), getattr(self.engine, "subscription", None), self.args.progid))

        await self._observe()
        self.final_state = self.engine.state
        self.subscription_mode = getattr(self.engine, "subscription", None)
        self._finished = True
        try:
            await self.engine.disconnect()
        except Exception as exc:
            print("[soak] disconnect warning: {!r}".format(exc))

        report = self._report()
        self._write(report)
        return 0 if report["verdict"]["passed"] else 1

    async def _observe(self):
        deadline = self.started + self.duration_s
        last_status = 0.0
        last_callbacks = 0
        while time.monotonic() < deadline:
            await asyncio.sleep(min(self.args.sample_seconds, max(0.5, deadline - time.monotonic())))
            elapsed = time.monotonic() - self.started

            connected = self.engine.connected
            read_ok = None
            read_ms = None
            status = {}
            client = self.engine.client
            if connected and client is not None:
                ts = time.monotonic()
                try:
                    status = client.get_server_status() or {}
                    read_ok = True
                    read_ms = round((time.monotonic() - ts) * 1000, 1)
                except Exception as exc:
                    read_ok = False
                    self.read_failures += 1
                    print("[warn] get_server_status failed: {!r}".format(exc))

            if self.args.write_tag and connected:
                try:
                    value = True if self.args.write_value.lower() in ("1", "true", "on") else False
                    ok = await self.engine.write_node_value(self.args.write_tag, value)
                    if ok:
                        self.write_ok += 1
                        back = client.read_items([self.args.write_tag], source=2) if client else []
                        if back and bool(back[0].value) != value:
                            self.write_failures += 1
                            print("[warn] write readback mismatch: {!r} != {}".format(
                                getattr(back[0], "value", None), value))
                    else:
                        self.write_failures += 1
                except Exception as exc:
                    self.write_failures += 1
                    print("[warn] write failed: {!r}".format(exc))

            self.samples.append({
                "at": _now(),
                "elapsed_s": round(elapsed, 1),
                "connected": connected,
                "state": self.engine.state,
                "server_state": status.get("state"),
                "status_ok": read_ok,
                "status_latency_ms": read_ms,
                "callbacks": self.callback_count,
                "nodes": len(self.engine.nodes),
            })

            if elapsed - last_status >= 300:
                last_status = elapsed
                print("[status] +{:.1f}min state={} nodes={} callbacks={} (+{} since last) "
                      "read_fail={} write_ok={}".format(
                          elapsed / 60.0, self.engine.state, len(self.engine.nodes),
                          self.callback_count, self.callback_count - last_callbacks,
                          self.read_failures, self.write_ok))
                last_callbacks = self.callback_count

    def _report(self):
        duration = self.duration_s
        bad_states = [t for t in self.transitions
                      if t["state"] in ("reconnecting", "disconnected")]
        reasons = []
        if self.connection_lost or bad_states:
            reasons.append("链路在测试期间掉线/重连: connection_lost={} transitions={}".format(
                len(self.connection_lost), [t["state"] for t in bad_states]))
        if self.read_failures:
            reasons.append("GetStatus 读取失败 {} 次".format(self.read_failures))
        if self.write_failures:
            reasons.append("写点位失败 {} 次".format(self.write_failures))
        if self.final_state != "connected":
            reasons.append("观察结束时引擎未处于 connected（实际 {}）".format(self.final_state))
        # 订阅模式下长时间零回调是异常（轮询模式下回调由引擎每周期产生，同样应增长）
        if duration >= self.args.min_callbacks_window and self.callback_count == 0:
            reasons.append("{} 秒内订阅/轮询回调为 0，数据面可能未工作".format(
                int(self.args.min_callbacks_window)))
        return {
            "generated_at": _now(),
            "endpoint": "opcda://{}/{}".format(self.args.host, self.args.progid),
            "host": self.args.host,
            "prog_id": self.args.progid,
            "duration_minutes": self.args.duration_minutes,
            "update_rate_ms": self.args.update_rate_ms,
            "nodes": len(self.engine.nodes),
            "subscription": getattr(self, "subscription_mode", None),
            "callbacks": self.callback_count,
            "callback_unique_items": len(self.callback_items),
            "write_ok": self.write_ok,
            "write_failures": self.write_failures,
            "read_failures": self.read_failures,
            "transitions": self.transitions,
            "connection_lost": self.connection_lost,
            "samples": self.samples,
            "verdict": {"passed": not reasons, "reasons": reasons},
        }

    def _write(self, report):
        path = self.args.report
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        v = report["verdict"]
        print("")
        print("=" * 72)
        print("OPC DA 长稳测试结果: {}".format("PASS" if v["passed"] else "FAIL"))
        print("  endpoint       : {}".format(report["endpoint"]))
        print("  duration       : {} min".format(report["duration_minutes"]))
        print("  nodes          : {}".format(report["nodes"]))
        print("  subscription   : {}".format(report["subscription"]))
        print("  callbacks      : {} (unique items {})".format(
            report["callbacks"], report["callback_unique_items"]))
        print("  connection lost: {}".format(len(report["connection_lost"])))
        print("  read failures  : {}".format(report["read_failures"]))
        print("  write          : ok={} fail={}".format(report["write_ok"], report["write_failures"]))
        if v["reasons"]:
            print("  reasons:")
            for reason in v["reasons"]:
                print("    - {}".format(reason))
        print("  report         : {}".format(os.path.abspath(path)))
        print("=" * 72)


def main(argv=None):
    p = argparse.ArgumentParser(description="OPC DA 长稳测试（直接 COM/DCOM 连接）")
    p.add_argument("--host", default="localhost", help="DCOM 主机（本机 localhost）")
    p.add_argument("--progid", default="Matrikon.OPC.Simulation.1", help="OPC DA ProgID")
    p.add_argument("--duration-minutes", type=float, default=75.0)
    p.add_argument("--sample-seconds", type=float, default=10.0)
    p.add_argument("--update-rate-ms", type=int, default=500)
    p.add_argument("--heartbeat-seconds", type=float, default=1.0)
    p.add_argument("--depth", type=int, default=3)
    p.add_argument("--connect-timeout", type=float, default=60.0)
    p.add_argument("--write-tag", default="", help="可选：周期写入的标量点位 ItemID（如 Bucket Brigade.Boolean）")
    p.add_argument("--write-value", default="true")
    p.add_argument("--min-callbacks-window", type=float, default=30.0,
                   help="超过该秒数仍无任何订阅/轮询回调则判定失败")
    p.add_argument("--report", default=os.path.join("logs", "da_soak_report.json"))
    args = p.parse_args(argv)

    runner = DaSoakRunner(args)
    try:
        code = asyncio.run(runner.run())
    except KeyboardInterrupt:
        print("[soak] interrupted")
        code = 130
    sys.exit(code)


if __name__ == "__main__":
    main()

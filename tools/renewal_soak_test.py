"""现场可用的 OPC UA 安全通道续约长稳测试（自动化测试程序）。

用途
----
把灯光控制软件真正使用的 OpcClientEngine 连到现场 OPC UA 端点（例如
本机 DA→UA 网关 opc.tcp://127.0.0.1:48401），持续观察：

* 安全通道是否按寿命 50% 自动续约（引擎 renewal_count / last_renewal_monotonic）；
* 跨过 token 寿命边界（常见为 1 小时）时链路是否始终 connected（无感、不掉线）；
* 每次续约后数据面是否仍然可用（主动 Read + 订阅推送计数）；
* 万一掉线，记录状态迁移与恢复耗时，并在最终报告里判定失败。

默认时长 75 分钟，正好跨过"连接满 1 小时整点断线"的现场故障点。也可以先用
--duration-minutes 5 做冒烟，再用 --duration-minutes 75 做整点回归。

退出码：0 = 通过；1 = 失败（掉线/续约次数不足/读取失败）；2 = 初始连接失败。

用法
----
    # 用仓库 data/lighting_config.json 里的连接参数（密码为 DPAPI 加密，需同一 Windows 用户运行）
    python tools/renewal_soak_test.py --config data/lighting_config.json --duration-minutes 75

    # 现场 dist 目录的配置也可以直接读
    python tools/renewal_soak_test.py --config dist/data/lighting_config.json

    # 手工指定端点/账号
    python tools/renewal_soak_test.py --host 127.0.0.1 --port 48401 --user admin --password 123456

注意：不要在生产网关上打开写操作；本程序默认只读。
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

from opc.client_engine import OpcClientEngine  # noqa: E402
from utils.logger import global_logger  # noqa: E402

SERVER_STATE_NODE_ID = "i=2259"


def _now_iso():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_connection_from_config(path):
    """从 lighting_config.json 读取连接参数；密码用 utils.security 解密。"""
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    conn = cfg.get("connection", {}) or {}
    password = conn.get("password", "") or ""
    if password:
        try:
            from utils.security import decrypt_secret
            password = decrypt_secret(password)
        except Exception as e:
            print(f"[warn] 密码解密失败，将按空密码连接: {e}")
            password = ""
    return {
        "host": str(conn.get("host") or "127.0.0.1"),
        "port": int(conn.get("port") or 48401),
        "user": conn.get("user", "") or "",
        "password": password,
        "ns_filter": conn.get("ns_filter", "ns=2;") or "ns=2;",
    }


class SoakRunner:
    def __init__(self, args, conn):
        self.args = args
        self.conn = conn
        self.engine = OpcClientEngine(
            host=conn["host"],
            port=conn["port"],
            username=conn["user"],
            password=conn["password"],
            namespace_filter=conn["ns_filter"],
            browse_max_depth=args.depth,
        )
        self.started_at = None
        self.duration_s = max(1.0, args.duration_minutes * 60.0)
        self.transitions = []
        self.connection_lost_events = []
        self.samples = []
        self.notification_count = 0
        self.read_failures = 0
        self.renewal_times = []
        self._last_renewal_count = 0
        self._state_at_start = None
        self._test_finished = False   # 观察窗口结束后主动 disconnect 的状态迁移不计入失败
        self.final_state = None

    @property
    def renewal_count(self):
        return getattr(self.engine, "renewal_count", 0)

    # ---- engine callbacks -------------------------------------------------------------
    def _on_state_change(self, state, detail):
        if self._test_finished:
            return
        self.transitions.append({
            "at": _now_iso(),
            "elapsed_s": round(time.monotonic() - self.started_at, 1) if self.started_at else 0,
            "state": state,
            "reason": (detail or {}).get("reason", ""),
        })

    def _on_connection_lost(self):
        self.connection_lost_events.append({
            "at": _now_iso(),
            "elapsed_s": round(time.monotonic() - self.started_at, 1) if self.started_at else 0,
            "engine_state": self.engine.state,
        })

    def _on_sub_data(self, node_id, value, timestamp):
        self.notification_count += 1

    # ---- main flow --------------------------------------------------------------------
    async def _connect_with_deadline(self):
        deadline = time.monotonic() + self.args.connect_timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                await self.engine.connect()
                self.engine.set_subscription_callback(self._on_sub_data)
                await self.engine.bootstrap()
                return True
            except Exception as e:
                last_error = e
                print(f"[retry] 初始连接失败（{e!r}），5s 后重试...")
                await asyncio.sleep(5)
        print(f"[fatal] {self.args.connect_timeout}s 内无法完成初始连接: {last_error!r}")
        return False

    async def run(self):
        self.started_at = time.monotonic()
        self.engine.on_state_change = self._on_state_change
        self.engine.on_connection_lost = self._on_connection_lost
        # 长稳测试同时验证"真掉线后能恢复"，但掉线本身会被判为失败
        self.engine.reconnect_base_delay = 5.0
        self.engine.reconnect_max_delay = 30.0

        print(f"[soak] endpoint=opc.tcp://{self.conn['host']}:{self.conn['port']}/ "
              f"duration={self.args.duration_minutes}min user={self.conn['user'] or '<anonymous>'}")
        if not await self._connect_with_deadline():
            return 2

        await self.engine.reconnect_forever()
        self._state_at_start = self.engine.state
        lifetime_ms = self.engine._channel_lifetime_ms()
        lifetime_s = lifetime_ms / 1000.0
        print(f"[soak] connected. channel lifetime={lifetime_ms}ms "
              f"(renewal expected every ~{lifetime_s * 0.5:.0f}s)")

        await self._observe(lifetime_s)
        self.final_state = self.engine.state
        self._test_finished = True
        await self.engine.disconnect()
        report = self._build_report(lifetime_ms)
        self._write_report(report)
        return 0 if report["verdict"]["passed"] else 1

    async def _observe(self, lifetime_s):
        deadline = self.started_at + self.duration_s
        next_renewal_report = self._last_renewal_count
        last_status_print = 0.0
        while time.monotonic() < deadline:
            await asyncio.sleep(min(self.args.sample_seconds, max(0.5, deadline - time.monotonic())))
            elapsed = time.monotonic() - self.started_at

            reconnect_now = self.engine.state == OpcClientEngine.STATE_RECONNECTING
            read_ok = None
            read_latency_ms = None
            if self.engine.connected and self.engine.client is not None:
                ts0 = time.monotonic()
                try:
                    node = self.engine.client.get_node(SERVER_STATE_NODE_ID)
                    await asyncio.wait_for(node.read_value(), timeout=5.0)
                    read_ok = True
                    read_latency_ms = round((time.monotonic() - ts0) * 1000, 1)
                except Exception as e:
                    read_ok = False
                    self.read_failures += 1
                    print(f"[warn] ServerState 读取失败: {e!r}")

            if self.engine.renewal_count > next_renewal_report:
                for _ in range(self.engine.renewal_count - next_renewal_report):
                    self.renewal_times.append({
                        "at": _now_iso(),
                        "elapsed_s": round(elapsed, 1),
                    })
                next_renewal_report = self.engine.renewal_count
                print(f"[renew] #{self.engine.renewal_count} ok at +{elapsed:.0f}s "
                      f"(channel lifetime={lifetime_s:.0f}s)")

            self.samples.append({
                "at": _now_iso(),
                "elapsed_s": round(elapsed, 1),
                "connected": bool(self.engine.connected),
                "state": self.engine.state,
                "read_ok": read_ok,
                "read_latency_ms": read_latency_ms,
                "renewal_count": self.engine.renewal_count,
                "notifications": self.notification_count,
            })

            if elapsed - last_status_print >= 300:
                last_status_print = elapsed
                print(f"[status] +{elapsed / 60:.0f}min state={self.engine.state} "
                      f"renewals={self.engine.renewal_count} reads_failed={self.read_failures} "
                      f"notifications={self.notification_count}")

            if reconnect_now:
                # 自动重连会继续跑，但"无感续约"已经失败，继续观察记录恢复过程
                continue

    def _build_report(self, lifetime_ms):
        duration_s = self.duration_s
        lifetime_s = lifetime_ms / 1000.0
        # 距上次续约/建链每 0.5 个寿命续约一次
        if duration_s >= lifetime_s * 0.6:
            expected_renewals = int((duration_s - lifetime_s * 0.5) / (lifetime_s * 0.5)) + 1
        else:
            expected_renewals = 0
        state_failures = [
            t for t in self.transitions
            if t["state"] in (OpcClientEngine.STATE_RECONNECTING, OpcClientEngine.STATE_DISCONNECTED)
        ]
        reasons = []
        if self.connection_lost_events or state_failures:
            reasons.append(
                f"测试期间发生断线/重连：{len(self.connection_lost_events)} 次 connection_lost，"
                f"状态迁移={[t['state'] for t in state_failures]}"
            )
        # 允许 1 次误差：连接建立耗时会挤掉最后一个续约窗口，且不同服务端允许的抖动不同
        required_renewals = max(0, expected_renewals - 1)
        if self.renewal_count < required_renewals:
            reasons.append(
                f"续约次数不足：实际 {self.renewal_count} 次，按通道寿命 {lifetime_s:.0f}s "
                f"与测试时长 {duration_s / 60:.0f}min 预期至少 {required_renewals} 次"
            )
        if self.read_failures:
            reasons.append(f"ServerState 主动读取失败 {self.read_failures} 次")
        if self.final_state != OpcClientEngine.STATE_CONNECTED:
            reasons.append(f"观察窗口结束时引擎未处于 connected（实际 {self.final_state}）")

        return {
            "generated_at": _now_iso(),
            "endpoint": f"opc.tcp://{self.conn['host']}:{self.conn['port']}/",
            "duration_minutes": self.args.duration_minutes,
            "channel_lifetime_ms": lifetime_ms,
            "renewal_count": self.engine.renewal_count,
            "expected_renewals": expected_renewals,
            "renewal_times": self.renewal_times,
            "notifications": self.notification_count,
            "read_failures": self.read_failures,
            "state_transitions": self.transitions,
            "connection_lost_events": self.connection_lost_events,
            "samples": self.samples,
            "verdict": {"passed": not reasons, "reasons": reasons},
        }

    def _write_report(self, report):
        path = self.args.report
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        verdict = report["verdict"]
        print("")
        print("=" * 72)
        print(f"续约长稳测试结果: {'PASS' if verdict['passed'] else 'FAIL'}")
        print(f"  endpoint        : {report['endpoint']}")
        print(f"  测试时长        : {report['duration_minutes']} min")
        print(f"  通道寿命        : {report['channel_lifetime_ms']} ms")
        print(f"  成功续约        : {report['renewal_count']} 次（预期 >= {max(0, report['expected_renewals'] - 1)}）")
        print(f"  断线/重连       : {len(report['connection_lost_events'])} 次")
        print(f"  主动读取失败    : {report['read_failures']} 次")
        print(f"  订阅推送计数    : {report['notifications']}")
        if verdict["reasons"]:
            print("  失败原因:")
            for reason in verdict["reasons"]:
                print(f"    - {reason}")
        print(f"  报告文件        : {os.path.abspath(path)}")
        print("=" * 72)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="OPC UA 安全通道续约长稳测试")
    parser.add_argument("--config", default="", help="lighting_config.json 路径，自动读取 host/port/账号")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--ns-filter", default=None)
    parser.add_argument("--depth", type=int, default=3, help="节点遍历最大深度")
    parser.add_argument("--duration-minutes", type=float, default=75.0,
                        help="测试时长（分钟），默认 75 覆盖 1 小时整点故障")
    parser.add_argument("--sample-seconds", type=float, default=10.0)
    parser.add_argument("--connect-timeout", type=float, default=60.0)
    parser.add_argument("--report", default=os.path.join("logs", "renewal_soak_report.json"))
    args = parser.parse_args(argv)

    conn = {
        "host": "127.0.0.1", "port": 48401, "user": "", "password": "", "ns_filter": "ns=2;",
    }
    if args.config:
        conn.update(_load_connection_from_config(args.config))
    if args.host is not None:
        conn["host"] = args.host
    if args.port is not None:
        conn["port"] = args.port
    if args.user is not None:
        conn["user"] = args.user
    if args.password is not None:
        conn["password"] = args.password
    if args.ns_filter is not None:
        conn["ns_filter"] = args.ns_filter
    return args, conn


def main(argv=None):
    args, conn = parse_args(argv)
    try:
        code = asyncio.run(SoakRunner(args, conn).run())
    except KeyboardInterrupt:
        print("[soak] interrupted by user")
        code = 130
    sys.exit(code)


if __name__ == "__main__":
    main()

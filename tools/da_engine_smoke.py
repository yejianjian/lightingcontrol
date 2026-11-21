"""OPC DA 引擎真实服务器联调冒烟工具。

与 tools/da_selftest.py（T1 的低层客户端自检）不同，本脚本验证**引擎适配层**
端到端行为：连接 -> 浏览 -> AddItems -> 读初值 -> 订阅推送 -> 批量写 ->
模拟断线自动重连（复用点位表）-> 断开。

依赖本机已安装并运行的 OPC DA 服务器（默认 Matrikon.OPC.Simulation.1）。
客户端模块缺失或服务器不可用时打印 SKIP 并以退出码 3 结束，便于 CI/批处理跳过。

用法：
    python tools/da_engine_smoke.py
    python tools/da_engine_smoke.py --host 192.168.1.50 --progid Kepware.KEPServerEX.V6
    python tools/da_engine_smoke.py --simulate-reconnect --wait 5
"""

import argparse
import asyncio
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from opcda.engine import OpcDaClientEngine  # noqa: E402
from utils.logger import global_logger  # noqa: E402


SKIP_EXIT_CODE = 3


def parse_args():
    parser = argparse.ArgumentParser(description="OPC DA engine integration smoke test")
    parser.add_argument("--host", default="localhost",
                        help="DCOM host of the OPC DA server (default: localhost)")
    parser.add_argument("--progid", default="Matrikon.OPC.Simulation.1",
                        help="OPC Server ProgID (default: Matrikon.OPC.Simulation.1)")
    parser.add_argument("--rate", type=int, default=500, help="update rate in ms")
    parser.add_argument("--depth", type=int, default=3, help="browse max depth")
    parser.add_argument("--wait", type=float, default=4.0,
                        help="seconds to collect subscription pushes")
    parser.add_argument("--heartbeat", type=float, default=0.25,
                        help="engine heartbeat interval in seconds (small for smoke)")
    parser.add_argument("--simulate-reconnect", action="store_true",
                        help="simulate a connection loss and verify auto-reconnect reuses nodes")
    parser.add_argument("--write", action="store_true",
                        help="attempt a write on a writable node")
    return parser.parse_args()


async def wait_until(predicate, timeout, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


async def run(args):
    try:
        from opcda.client import list_servers  # noqa: F401
    except Exception as exc:
        print("SKIP: opcda.client not available ({}: {})".format(type(exc).__name__, exc))
        return SKIP_EXIT_CODE

    print("== OPC DA engine smoke ==")
    print("host={} progid={} rate={}ms depth={}".format(
        args.host, args.progid, args.rate, args.depth))

    try:
        servers = list_servers(args.host) or []
        print("servers on {}: {}".format(args.host, servers[:20]))
    except Exception as exc:
        print("list_servers failed (non-fatal): {}: {}".format(type(exc).__name__, exc))
        servers = []

    engine = OpcDaClientEngine(
        host=args.host, prog_id=args.progid,
        update_rate_ms=args.rate, browse_max_depth=args.depth,
    )
    # 冒烟脚本没有 Qt 事件循环泵消息，靠引擎心跳泵（pump_events_enabled 默认 True）
    engine.heartbeat_interval_s = max(0.05, args.heartbeat)
    engine.reconnect_base_delay = 1.0
    engine.reconnect_max_delay = 5.0

    transitions = []
    pushes = []
    recovered = []
    lost = []

    def on_state(state, detail):
        transitions.append(state)
        print("[state] {} ({})".format(state, detail.get("reason")))

    engine.on_state_change = on_state
    engine.on_nodes_recovered = lambda: recovered.append(time.monotonic())
    engine.on_connection_lost = lambda: lost.append(time.monotonic())
    engine.set_subscription_callback(
        lambda node_id, value, timestamp: pushes.append((node_id, value, timestamp))
    )

    try:
        started = time.monotonic()
        await engine.connect()
        await engine.bootstrap()
        print("connected in {:.2f}s; state={}; subscription={}".format(
            time.monotonic() - started, engine.state, engine.subscription))

        node_count = len(engine.nodes)
        print("nodes: {}".format(node_count))
        if not node_count:
            print("FAIL: browse returned no nodes")
            return 1

        sample = list(engine.nodes.values())[:8]
        for node in sample:
            print("  node_id={!r} name={!r} type={} value={!r} ts={!r} writable={}".format(
                node.get("node_id"), node.get("name"), node.get("type"),
                node.get("value"), node.get("timestamp"), node.get("writable")))

        # 让订阅推送/轮询跑一会儿
        print("collecting pushes for {:.1f}s ...".format(args.wait))
        await asyncio.sleep(args.wait)
        unique = {item[0] for item in pushes}
        print("pushes: {} events / {} distinct nodes (mode={})".format(
            len(pushes), len(unique), engine.subscription))
        if engine.subscription == "async" and not pushes:
            print("WARN: async subscription produced no pushes (server may have no changing tags)")

        # 批量写：优先 Boolean 点位，确保写入的值类型与点位匹配。
        # 注意排除 Matrikon 的 "Write Error.*"（故意返回错误的测试点）与只写点。
        if args.write:
            def _write_rank(node):
                item_id = str(node.get("node_id", ""))
                bad = ("Write Error" in item_id) or item_id.startswith("Write Only.")
                return (1 if bad else 0, 0 if str(node.get("type")) == "Boolean" else 1,
                        item_id)

            writable = [
                node for node in engine.nodes.values()
                if node.get("writable")
                and not ("Write Error" in str(node.get("node_id", "")))
                and not str(node.get("node_id", "")).startswith("Write Only.")
            ]
            writable.sort(key=_write_rank)
            if not writable:
                print("write test skipped: no writable non-error node found")
            else:
                targets = [node["node_id"] for node in writable[:5]]
                types = [node.get("type") for node in writable[:5]]
                success, fail = await engine.write_values_batch(targets, True)
                print("batch write -> success={} fail={} (targets={}, types={})".format(
                    success, fail, targets, types))

        # 模拟断线 + 自动重连（复用点位表）
        if args.simulate_reconnect:
            nodes_before = set(engine.nodes.keys())
            generation_before = engine._generation
            engine._auto_reconnect = True
            lost_before = len(lost)
            print("simulating connection loss ...")
            engine._handle_connection_lost("smoke: simulated drop", engine._generation)
            recovered_ok = await wait_until(
                lambda: engine.connected and engine.total_reconnects >= 1, timeout=20.0
            )
            print("reconnect ok={} recovered_callbacks={} lost_callbacks={} total_reconnects={}".format(
                recovered_ok, len(recovered), len(lost) - lost_before,
                engine.total_reconnects))
            if not recovered_ok:
                print("FAIL: engine did not reconnect")
                return 1
            if set(engine.nodes.keys()) != nodes_before:
                print("FAIL: node table changed across reconnect")
                return 1
            if engine._generation <= generation_before:
                print("FAIL: generation did not advance")
                return 1
            print("reconnect reused the cached node table ({} items)".format(len(nodes_before)))

        print("state sequence: {}".format(" -> ".join(
            entry for entry in transitions)))
        return 0
    except Exception as exc:
        global_logger.error("smoke failed: {}".format(exc), exc_info=True)
        print("FAIL: {}: {}".format(type(exc).__name__, exc))
        return 1
    finally:
        try:
            engine.stop_reconnect()
            await engine.disconnect()
        except Exception as exc:
            print("disconnect failed (non-fatal): {}: {}".format(type(exc).__name__, exc))


def main():
    args = parse_args()
    # asyncio.run 在 3.8+ 可用，且避免 3.12 对 get_event_loop() 的 DeprecationWarning
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())

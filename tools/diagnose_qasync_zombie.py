"""复现"qasync + QProgressDialog.setValue() 重入导致后台任务僵尸化"的诊断脚本。

用法（先起一个短寿命服务器，再分别带/不带对话框各跑一次）::

    python tools/_short_lifetime_server.py 48501
    set CHECK_PORT=48501
    python tools/diagnose_qasync_zombie.py --dialog
    python tools/diagnose_qasync_zombie.py

修复后：带对话框也应看到 [RENEW] 正常续约（24s 内 renewal_count 约 7，0 次断线）；
修复前：带对话框 renewal_count=0、续约任务停在 pending 且没有任何 [RENEW]/[HEARTBEAT]。
"""
import asyncio, importlib.util, os, sys, time
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRATCH = os.path.join(REPO, "tools", ".diagnose_tmp")
os.makedirs(os.path.join(SCRATCH, "logs"), exist_ok=True)
for f in ("client_cert.der", "client_key.pem", "server_cert_0.der"):
    p = os.path.join(SCRATCH, f)
    if os.path.exists(p):
        os.remove(p)
logp = os.path.join(SCRATCH, "logs", "app.log")
if os.path.exists(logp):
    os.remove(logp)

USE_DIALOG = "--dialog" in sys.argv
PORT = int(os.environ.get("CHECK_PORT", "48501"))
WAIT_S = float(os.environ.get("CHECK_WAIT", "24"))

sys.path.insert(0, REPO)
sys.frozen = True
sys._MEIPASS = REPO
sys.executable = os.path.join(SCRATCH, "LightingControl_v1.4.0.exe")

from PyQt5.QtWidgets import QApplication, QWidget, QProgressDialog
from PyQt5.QtCore import Qt, QTimer
from qasync import QEventLoop

import opc.client_engine as mod
print("loaded module from", mod.__file__, flush=True)

app = QApplication([])
loop = QEventLoop(app)
asyncio.set_event_loop(loop)
win = QWidget()
win.show()

engine = mod.OpcClientEngine(host="127.0.0.1", port=PORT, namespace_filter="ns=2;", browse_max_depth=2)

def describe(tag):
    t = engine._renew_task
    m = engine._monitor_task
    def d(x):
        return "None" if x is None else ("done" if x.done() else ("cancelled" if x.cancelled() else "pending"))
    print(tag, "state=", engine.state, "renew_task=", d(t), "monitor_task=", d(m),
          "renewal_count=", engine.renewal_count, flush=True)

async def connect_flow():
    progress = None
    if USE_DIALOG:
        progress = QProgressDialog("正在连接服务器...", "取消", 0, 100, win)
        progress.setWindowTitle("连接中")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.setValue(10)
        progress.setLabelText("connecting...")
    try:
        await engine.connect()
        describe("after connect()")
        if progress is not None:
            progress.setValue(40)   # <-- Qt 内部会 processEvents()，重入调度后台任务
            describe("after progress.setValue(40)")
        engine.set_subscription_callback(lambda *a: None)
        await engine.bootstrap()
        describe("after bootstrap()")
        await asyncio.sleep(WAIT_S)
        describe("after wait")
    finally:
        if progress is not None:
            progress.close()
    try:
        await engine.disconnect()
    except Exception as e:
        print("disconnect err", e, flush=True)
    loop.stop()

def trigger():
    print("trigger use_dialog=", USE_DIALOG, flush=True)
    asyncio.create_task(connect_flow())

QTimer.singleShot(500, trigger)
QTimer.singleShot(int((WAIT_S + 20) * 1000), loop.stop)
with loop:
    loop.run_forever()
print("renewal_count final =", engine.renewal_count, flush=True)
print("---- log tail ----", flush=True)
text = open(logp, encoding="utf-8", errors="replace").read()
print("\n".join(text.splitlines()[-25:]), flush=True)
print("RENEW_STARTED_COUNT=", text.count("renewal supervisor started"), flush=True)
print("RENEW_OK_COUNT=", text.count("renewed successfully"), flush=True)

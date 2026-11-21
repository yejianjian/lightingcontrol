"""冻结产物冒烟测试：在隔离目录启动打包好的单文件 exe 并校验启动日志。

用法::

    python tools/smoke_frozen_build.py dist\\LightingControl_v1.3.0.exe

为什么必须有这一步：`python -m compileall` / 直接 import 无法覆盖**运行时**才走的
路径（例如 `_MEIPASS` 资源定位、可写目录选择、qasync 与 Qt 的初始化顺序）。
本脚本就是靠它抓出了 `main_window.py` 里一处残留的旧函数名 —— 源码模式完全正常，
但打包后启动即 `NameError` 崩溃。

校验点：
1. MainWindow 初始化完成、qasync 事件循环启动、系统托盘创建成功；
2. `ui/style.qss` 能从 `_MEIPASS` 读到（日志里不应出现"样式文件不存在"）；
3. `logs/`、`data/` 落在 **exe 同级目录**（而不是退出即删的 `_MEIPASS`）。
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

CHECKS = [
    ("MainWindow initialized", "MainWindow init complete"),
    ("Event loop running", "qasync event loop running"),
    ("System tray icon initialized", "system tray created"),
]
STARTUP_TIMEOUT = 60.0

# MainWindow 会把实际尺寸写进日志；用它校验布局没有把窗口撑出屏幕。
# 现场最小分辨率按 1024x768 计（再扣掉任务栏/标题栏）。
MAX_MIN_WIDTH = 1024
MAX_MIN_HEIGHT = 768 - 45
GEOMETRY_RE = re.compile(r"MainWindow initialized\. size=(\d+)x(\d+), minimum=(\d+)x(\d+)")


def main(exe_path, keep_sandbox=False):
    exe_path = os.path.abspath(exe_path)
    if not os.path.exists(exe_path):
        print(f"FAIL: not found: {exe_path}")
        return 2

    sandbox = tempfile.mkdtemp(prefix="lc_smoke_")
    os.makedirs(os.path.join(sandbox, "data"), exist_ok=True)
    shutil.copy2(exe_path, sandbox)

    # 带上仓库里的配置，顺带验证 JSON 读写路径
    repo_cfg = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "data", "lighting_config.json")
    if os.path.exists(repo_cfg):
        shutil.copy2(repo_cfg, os.path.join(sandbox, "data", "lighting_config.json"))

    print(f"exe     : {exe_path}")
    print(f"sandbox : {sandbox}")

    log_path = os.path.join(sandbox, "logs", "app.log")
    proc = subprocess.Popen([os.path.join(sandbox, os.path.basename(exe_path))], cwd=sandbox)
    log_text = ""
    try:
        deadline = time.monotonic() + STARTUP_TIMEOUT
        while time.monotonic() < deadline:
            if os.path.exists(log_path):
                log_text = _read(log_path)
                if "Event loop running" in log_text:
                    break
            if proc.poll() is not None:
                print(f"!! process exited early with code {proc.returncode}")
                break
            time.sleep(0.5)
        # GUI 程序不会自行退出，留点时间写日志后终止
        time.sleep(2.0)
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)
        log_text = _read(log_path) or log_text

    failures = 0
    print("-" * 62)
    for needle, label in CHECKS:
        ok = needle in log_text
        failures += 0 if ok else 1
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
    style_ok = "样式文件不存在" not in log_text
    failures += 0 if style_ok else 1
    print(f"  [{'PASS' if style_ok else 'FAIL'}] ui/style.qss readable from _MEIPASS")
    logs_ok = os.path.exists(log_path)
    failures += 0 if logs_ok else 1
    print(f"  [{'PASS' if logs_ok else 'FAIL'}] logs/ written next to the exe (not _MEIPASS)")

    # 布局校验：窗口最小尺寸不得超出 1024x768 的可用区域，否则用户全屏也看不全
    match = GEOMETRY_RE.search(log_text)
    if match is None:
        failures += 1
        print("  [FAIL] window geometry line not found in the log")
    else:
        size_w, size_h, min_w, min_h = (int(g) for g in match.groups())
        layout_ok = min_w <= MAX_MIN_WIDTH and min_h <= MAX_MIN_HEIGHT
        failures += 0 if layout_ok else 1
        print(f"  [{'PASS' if layout_ok else 'FAIL'}] layout fits {MAX_MIN_WIDTH}x"
              f"{MAX_MIN_HEIGHT}+ (window {size_w}x{size_h}, minimum {min_w}x{min_h})")
    print("-" * 62)
    print("app.log:")
    print(log_text or "(no log produced)")

    if failures:
        print(f"SMOKE TEST FAILED ({failures} check(s))")
        print(f"sandbox kept for inspection: {sandbox}")
        return 1

    print("SMOKE TEST OK")
    if keep_sandbox:
        print(f"sandbox kept: {sandbox}")
    else:
        shutil.rmtree(sandbox, ignore_errors=True)
    return 0


def _read(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return ""


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1], keep_sandbox="--keep" in sys.argv))

"""日志基础设施。

重要设计约束：本应用的 Qt 事件循环、asyncio 事件循环、OPC 订阅回调与所有本地 I/O
**共用一个线程**（qasync）。因此日志在任何 handler 上做的同步格式化/写入都会直接
阻塞心跳与发布处理——一旦超过 ``READ_TIMEOUT``，看门狗会把"线程被日志卡住"误判成
断线并触发重连。

asyncua 恰好会在 INFO/DEBUG 级别打印**整个报文**（例如每次 Publish 响应都会
``logger.info("Publish callback called with result: %s")``，1100 个点位的批量变化是
300KB+ 的单条记录）。所以在把第三方库日志接入本应用 handler 之前，必须先把这类
噪声过滤掉。
"""

import logging
import logging.handlers
import sys
import os

# 单条日志消息上限：超长消息（第三方库整报文 dump）截断后再送进 handler，
# 避免格式化/写盘/GUI 渲染造成秒级阻塞。
MAX_LOG_MESSAGE_CHARS = 2000

# 需要被过滤掉的第三方库噪声消息特征（小写匹配）
_NOISY_MESSAGE_MARKERS = (
    "publish callback called with result",
    "sending:",
    "received:",
    "request header:",
    "response header:",
)


def _truncate(text, limit=MAX_LOG_MESSAGE_CHARS):
    text = str(text)
    if len(text) <= limit:
        return text
    return f"{text[:limit]}…[truncated {len(text) - limit} chars]"


class LibraryNoiseFilter(logging.Filter):
    """过滤第三方库（asyncua）的高频大报文日志。

    只针对 ``asyncua.*`` logger 生效，本应用的日志不受影响。这里做两道拦截：

    1. **级别闸门**：只放行 WARNING 以上。asyncua 在 INFO 级别会把每一次
       ``browse`` / ``create_monitored_items`` / ``create_subscription`` 都打一遍
       （1100 个点位就是上千行），足以把现场日志淹没。注意不能只给父 logger
       ``asyncua`` 设级别 —— 子 logger 会各自解析级别，必须用 handler 过滤器兜底。
    2. **长度截断**：Publish 响应可能包含整批点位，单条记录可达几百 KB。

    需要深挖链路问题时调用 :func:`set_verbose_library_logging` 临时放开。
    """

    def __init__(self):
        super().__init__()
        self.allow_verbose = False

    def filter(self, record):
        name = record.name or ""
        is_library = name == "asyncua" or name.startswith("asyncua.")
        if not is_library:
            return True
        if not self.allow_verbose and record.levelno < logging.WARNING:
            return False
        try:
            message = record.getMessage()
        except Exception:
            return True
        if len(message) > MAX_LOG_MESSAGE_CHARS:
            record.msg = _truncate(message)
            record.args = ()
            return True
        lowered = message.lower()
        for marker in _NOISY_MESSAGE_MARKERS:
            if lowered.startswith(marker) or f" {marker}" in lowered:
                return False
        return True


# 已挂到各 handler 上的过滤器实例，供运行时切换
_LIBRARY_FILTERS = []


def set_verbose_library_logging(enabled):
    """临时放开第三方库的 INFO/DEBUG 日志（排障用，勿在 24/7 现场长期开启）。"""
    for flt in _LIBRARY_FILTERS:
        flt.allow_verbose = bool(enabled)
    logging.getLogger('asyncua').setLevel(
        logging.DEBUG if enabled else logging.WARNING
    )


def _new_library_filter():
    """创建并登记一个噪声过滤器（登记后即可被 set_verbose_library_logging 切换）。"""
    flt = LibraryNoiseFilter()
    _LIBRARY_FILTERS.append(flt)
    return flt


LOG_DIR_ENV = 'LC_LOG_DIR'


def _application_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def resolve_log_dir():
    """解析日志目录：环境变量 > 应用目录/logs > 系统临时目录。

    打包成 exe 后若安装在 Program Files 等只读目录，直接创建 RotatingFileHandler
    会抛 PermissionError，让整个程序连启动都失败（日志只是辅助功能，绝不该成为
    启动阻断点）。这里按优先级回退，最差退化为"仅控制台"。
    """
    override = os.environ.get(LOG_DIR_ENV)
    candidates = []
    if override:
        candidates.append(override)
    candidates.append(os.path.join(_application_dir(), 'logs'))
    try:
        import tempfile
        candidates.append(os.path.join(tempfile.gettempdir(), 'LightingControl', 'logs'))
    except Exception:
        pass
    for candidate in candidates:
        try:
            os.makedirs(candidate, exist_ok=True)
        except OSError:
            continue
        if os.access(candidate, os.W_OK):
            return candidate
    return None


def _make_file_handler(log_dir):
    """在 log_dir 下创建滚动文件 handler（失败时抛 OSError 由调用方回退）。"""
    log_file = os.path.join(log_dir, 'app.log')
    handler = logging.handlers.RotatingFileHandler(
        log_file, encoding='utf-8', maxBytes=5*1024*1024, backupCount=3
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - [%(levelname)s] - %(filename)s:%(lineno)d - %(message)s'))
    handler.addFilter(_new_library_filter())
    return handler


def setup_logger():
    log_dir = resolve_log_dir()

    logger = logging.getLogger('LightingControl')
    logger.setLevel(logging.INFO)
    # 本 logger 自带完整 handler，关闭向 root 传播，避免每条记录被输出两次
    logger.propagate = False

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
        console_handler.setFormatter(console_formatter)
        console_handler.addFilter(_new_library_filter())
        logger.addHandler(console_handler)

        # 使用 RotatingFileHandler 防止日志文件无限增长（最大5MB × 3备份）。
        # 任何 I/O 失败都只降级为"无文件日志"，不允许影响程序启动。
        # 注意：os.access() 在部分 ACL/沙箱环境下会误报可写，因此真正的判定标准是
        # 能否成功打开文件；失败就依次尝试系统临时目录。
        candidates = []
        if log_dir:
            candidates.append(log_dir)
        try:
            import tempfile
            candidates.append(os.path.join(tempfile.gettempdir(), 'LightingControl', 'logs'))
        except Exception:
            pass
        file_handler = None
        for attempt in candidates:
            try:
                try:
                    os.makedirs(attempt, exist_ok=True)
                except OSError:
                    pass
                file_handler = _make_file_handler(attempt)
                break
            except OSError as err:
                try:
                    sys.stderr.write(
                        "[logger] cannot open log file in {!r} ({}); trying fallbacks.\n".format(attempt, err))
                except Exception:
                    pass
        if file_handler is not None:
            logger.addHandler(file_handler)

    # 让框架级诊断（asyncio 的 "Task was destroyed but it is pending" 等）也能落盘：
    # 否则 24/7 运行中最关键的一类线索在 console=False 的打包版本里会彻底消失。
    _install_root_handler(logger)

    # 将 asyncua 等第三方库的日志接入同一文件/控制台，便于排查续约、连接问题
    attach_library_loggers(logger)
    return logger


def _install_root_handler(app_logger):
    """给 root logger 挂一个轻量 handler，只收第三方/标准库的 WARNING 以上记录。"""
    root = logging.getLogger()
    if getattr(root, "_lightingcontrol_bridge", False):
        return
    try:
        handler = logging.StreamHandler(sys.stderr if sys.stderr else sys.stdout)
        handler.setLevel(logging.WARNING)
        handler.setFormatter(logging.Formatter('%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'))
        handler.addFilter(_new_library_filter())
        root.addHandler(handler)
        if root.level == logging.NOTSET or root.level > logging.WARNING:
            root.setLevel(logging.WARNING)
        root._lightingcontrol_bridge = True
    except Exception:
        pass


def attach_library_loggers(logger):
    """将第三方库(asyncua)的日志分流到本应用的文件与控制台 handler。

    asyncua 的续约失败等信息原本只输出到它自己的 logger，而该 logger 在
    本应用中没有 handler，导致 app.log 里完全看不到真实原因。

    注意：这里**只接 WARNING 以上**的第三方日志。asyncua 的 INFO/DEBUG 会把完整
    报文（Publish 响应、Browse 响应）打成单条几百 KB 的记录，既拖慢 GUI 线程又会在
    一两小时内吃掉整个滚动日志。需要深挖时用日志页的 DEBUG 选项会临时放开，
    但依然受 ``LibraryNoiseFilter`` 与长度截断保护。
    """
    if not logger.handlers:
        return
    handlers = list(logger.handlers)
    lib_logger = logging.getLogger('asyncua')
    # 只收 WARNING 以上。asyncua 在 INFO 级别会把每一次 browse / create_monitored_items /
    # create_subscription 都打一遍（1100 个点位是上千行），现场日志会被彻底淹没。
    # 连接与续约的关键信息本应用已经用自己的日志（[STATE]/[RENEW]/[RECONNECT]）记录，
    # 因此也**不要**把 asyncua.client 单独调低：那会让它替本应用打出无前缀的裸日志
    # （现场看到的是裸 "browse"，而不是可检索的 [RENEW] 记录），反而更难排查。
    lib_logger.setLevel(logging.WARNING)
    for h in handlers:
        if h not in lib_logger.handlers:
            lib_logger.addHandler(h)
    # asyncua.* 子 logger 会向 'asyncua' 汇聚，关闭向上传播避免重复输出
    lib_logger.propagate = False


global_logger = setup_logger()


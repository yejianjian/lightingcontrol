# -*- coding: utf-8 -*-
"""OPC DA（COM/DCOM）低层客户端。

本模块是 OPC DA 协议栈与上层引擎之间唯一的接缝：上层只依赖这里的几个方法，
而 COM 细节（comtypes 接口、VARIANT 转换、out 数组内存释放、订阅回调的消息泵、
STA 套间、DCOM 远程创建）全部封装在内部。

设计约束与要点：

1. **单线程、STA**：客户端在其 connect() 的线程中创建所有 COM 对象，后续所有
   调用必须来自同一线程（Qt/qasync 主线程）。COM 以 STA 初始化；订阅回调由服务器
   投递到该线程的消息队列，因此调用方必须周期性调用 pump_events()（Qt 主线程
   自身会泵消息，无 Qt 的测试/引擎心跳需显式调用）。
2. **唯一组**：所有点位挂到同一个 OPC 组（按 update_rate_ms 创建，更新周期变化时
   用 IOPCGroupStateMgt::SetState 原地修改，不重建组，避免订阅中断）。
3. **批量**：AddItems / SyncRead / SyncWrite 一次传数组（内部按每批 1000 个分块），
   1000+ 点位不会逐点往返。
4. **内存**：服务器返回的数组/字符串统一用 CoTaskMemFree 释放，VIANT 用
   VariantClear 释放；connect/disconnect/失败路径都不泄漏 COM 对象。
5. **兼容**：服务器缺少 IOPCAsyncIO2 时 start_subscription 抛
   OpcDaError('async unsupported: ...')，由引擎降级为 SyncRead 轮询；缺少
   IOPCCommon/IOPCGroupStateMgt 时静默降级。
"""

from __future__ import annotations

import ctypes
import logging
import socket
import threading
import time
from dataclasses import dataclass
from typing import Optional

import comtypes
import comtypes.client
from comtypes import COMError, IUnknown
from comtypes.automation import VARIANT
from comtypes.connectionpoints import IConnectionPointContainer

from . import com

logger = logging.getLogger(__name__)

# 单次 COM 调用携带的最大点位数（OPC 服务器/代理对单次调用规模有隐含上限，
# 分块后仍是一次调用传数组，大点位表不会逐点往返）
DEFAULT_BATCH_SIZE = 1000
# 订阅使用的默认组更新周期
DEFAULT_UPDATE_RATE_MS = 500
# 组名序号（同一服务器上多客户端/重连时避免组名冲突）
_group_name_counter = [0]


class OpcDaError(Exception):
    """OPC DA 操作失败。

    :ivar code: HRESULT（统一为无符号 32 位；None 表示非 COM 错误）
    :ivar errors: 批量操作时的逐项错误列表 [(item_id, hresult, error_text), ...]
    """

    def __init__(self, message, code=None, errors=None):
        self.code = com.as_unsigned(code) if code is not None else None
        self.errors = list(errors or [])
        text = str(message)
        if self.code not in (None, 0):
            text = '%s（%s）' % (text, com.format_hresult(self.code))
        super(OpcDaError, self).__init__(text)


@dataclass
class DaItem:
    """一个已挂载（AddItems 成功）的 OPC DA 点位。"""

    item_id: str          # 完整 OPC ItemID（作为 UI 的 node_id 使用）
    name: str             # 叶子名（UI 显示名）
    path: str             # 分支路径，如 "Random"；根级为空串
    canonical_vt: int = 0  # VARENUM（VT_*）；0 表示未知
    access: int = 0        # 1=read 2=write 3=read/write
    server_handle: int = 0
    client_handle: int = 0


@dataclass
class DaValue:
    """一次读/订阅回调得到的点位值。"""

    item_id: str
    value: object = None
    quality: int = 0
    timestamp: Optional[str] = None
    error: int = 0

    @property
    def is_good(self):
        return com.is_quality_good(self.quality) and not self.error


# --------------------------------------------------------------------------------------
# 模块级工具函数
# --------------------------------------------------------------------------------------


def pump_events():
    """派发当前线程已排队的 Windows 消息，让 COM 订阅回调得以执行（非阻塞）。"""
    com.pump_message_events()


def vtype_to_type_name(vt):
    """VARENUM → 类型名（契约要求：Boolean/Int16/Int32/UInt16/.../String/Unknown）。"""
    return com.vtype_to_type_name(vt)


def list_servers(host='localhost'):
    """枚举可用的 OPC DA 服务器 ProgID（本机与远程统一，契约见 opcda_API_CONTRACT.md）。

    枚举顺序：
    1. **优先** OPC Server List / OPCEnum（com.list_da_servers）：本机 clsctx=None，
       远程 clsctx=CLSCTX_REMOTE_SERVER + machine=host，与 connect 的 DCOM 写法一致；
    2. 第一步为空（或失败）时回退注册表 Component Categories 扫描：本机用
       com.list_local_da_servers()（64/32 位视图），远程用
       com.list_remote_da_servers(host)（Remote Registry，需目标机启用服务/权限）。

    任何失败都返回 []，绝不抛异常（UI 的刷新按钮可以直接调用；远程枚举可能在
    DCOM/注册表上阻塞数秒，UI 必须放到工作线程执行）。
    """
    try:
        local = _is_local_host(host)
        remote_host = None if local else str(host).strip()
        names = set()
        try:
            names.update(com.list_da_servers(remote_host) or [])
        except Exception:
            # 本机 64 位进程可能缺少 OPC.ServerList 的 64 位注册（只注册了 32 位
            # 视图），远程主机也可能未安装 OPCEnum——都属预期，回退注册表扫描。
            logger.debug('OPC Server List 枚举失败（host=%r），回退注册表扫描',
                         host, exc_info=True)
        if not names:
            try:
                if local:
                    names.update(com.list_local_da_servers() or [])
                else:
                    names.update(com.list_remote_da_servers(remote_host) or [])
            except Exception:
                logger.debug('注册表扫描枚举 OPC DA 服务器失败（host=%r）',
                             host, exc_info=True)
        return sorted({str(name) for name in names if name})
    except Exception:  # pragma: no cover - 枚举结果绝不能把异常抛给 UI
        logger.exception('枚举 OPC DA 服务器失败（host=%r）', host)
        return []


# 明确视为"本机"的写法：空值、localhost、回环、未指定地址
_LOCAL_HOST_ALIASES = frozenset(
    ('', '.', 'localhost', '127.0.0.1', '::1', '[::1]', '0.0.0.0'))


def _local_ip_addresses():
    """收集本机所有网卡地址（多网卡 / LAN IP 都算本机），失败返回空集合。"""
    addresses = set()
    try:
        hostname = socket.gethostname()
    except Exception:  # pragma: no cover - 极端环境取不到主机名
        return addresses
    try:
        # getaddrinfo 会返回本机所有网卡/协议的地址，多网卡机器不会漏
        for info in socket.getaddrinfo(hostname, None):
            sockaddr = info[4] if len(info) > 4 else None
            if sockaddr and sockaddr[0]:
                addresses.add(str(sockaddr[0]).split('%', 1)[0].lower())
    except Exception:  # pragma: no cover - 离线/解析失败
        pass
    try:
        addresses.add(str(socket.gethostbyname(hostname)).lower())
    except Exception:  # pragma: no cover
        pass
    return addresses


def _is_local_host(host):
    """判断主机名/IP 是否指向本机（None/空/localhost/回环/主机名/任一网卡 IP）。"""
    if host is None:
        return True
    text = str(host).strip().lower()
    if text in _LOCAL_HOST_ALIASES:
        return True
    candidate = text
    if candidate.startswith('[') and candidate.endswith(']'):
        candidate = candidate[1:-1]
    candidate = candidate.split('%', 1)[0]  # 去掉 IPv6 zone id（fe80::1%12）
    if candidate.startswith('127.'):
        return True
    names = set()
    try:
        hostname = socket.gethostname()
        names.add(hostname.lower())
        fqdn = socket.getfqdn()
        if fqdn:
            names.add(fqdn.lower())
    except Exception:  # pragma: no cover
        pass
    addresses = _local_ip_addresses()
    if candidate in names or candidate in addresses:
        return True
    # 用户填的是别名/域名：解析后只要落在本机任一网卡或回环上就按本机处理
    try:
        for info in socket.getaddrinfo(candidate, None):
            sockaddr = info[4] if len(info) > 4 else None
            if not sockaddr or not sockaddr[0]:
                continue
            ip = str(sockaddr[0]).split('%', 1)[0].lower()
            if ip in addresses or ip.startswith('127.') or ip == '::1':
                return True
    except Exception:
        pass
    return False


def _com_error_code(exc):
    """从 comtypes.COMError / OSError 中提取 HRESULT（无符号）。

    ctypes 在 HRESULT 失败时抛 OSError 的形式是
    args=(winerror, message, None, hresult, None)，因此要从第 4 项取 HRESULT，
    否则会把 WM 的 winerror（如 22）当成 HRESULT 上报。
    """
    args = getattr(exc, 'args', ())
    if len(args) >= 5 and isinstance(args[3], int) and args[3] != 0:
        return com.as_unsigned(args[3])
    if isinstance(exc, COMError):
        if args and isinstance(args[0], int):
            return com.as_unsigned(args[0])
    if args and isinstance(args[0], int) and len(args) < 4:
        return com.as_unsigned(args[0])
    winerror = getattr(exc, 'winerror', None)
    if winerror is not None:
        return com.as_unsigned(winerror)
    return None


def _new_group_name():
    _group_name_counter[0] += 1
    return 'lightingcontrol_da_%d_%d_%d' % (
        __import__('os').getpid(), int(time.time()) % 100000, _group_name_counter[0])


# --------------------------------------------------------------------------------------
# 订阅回调（COM 对象，由服务器在客户端 STA 消息泵中调用）
# --------------------------------------------------------------------------------------


class _DataCallbackSink(comtypes.COMObject):
    """IOPCDataCallback 的客户端实现。

    comtypes 要求方法名带接口前缀（IOPCDataCallback_OnDataChange），并按
    _com_interfaces_ 声明生成 vtable。回调内不得抛异常（异常会穿过 COM 边界），
    因此统一捕获并记录日志。
    """

    _com_interfaces_ = [com.IOPCDataCallback]

    def __init__(self, client):
        super(_DataCallbackSink, self).__init__()
        self._client = client

    def IOPCDataCallback_OnDataChange(self, transid, hgroup, master_quality,
                                      master_error, count, client_items, values,
                                      qualities, timestamps, errors):
        try:
            self._client._handle_data_change(
                transid, hgroup, count, client_items, values,
                qualities, timestamps, errors)
        except Exception:  # pragma: no cover - 回调内绝不外抛
            logger.exception('处理 OnDataChange 回调失败')
        return 0

    def IOPCDataCallback_OnReadComplete(self, transid, hgroup, master_quality,
                                        master_error, count, client_items, values,
                                        qualities, timestamps, errors):
        return 0

    def IOPCDataCallback_OnWriteComplete(self, transid, hgroup, master_error,
                                         count, client_handles, errors):
        return 0

    def IOPCDataCallback_OnCancelComplete(self, transid, hgroup):
        return 0


# --------------------------------------------------------------------------------------
# 客户端主体
# --------------------------------------------------------------------------------------


class OpcDaClient(object):
    """直接连接 OPC DA 服务器（COM/DCOM）的同步客户端。

    典型用法::

        client = OpcDaClient('Matrikon.OPC.Simulation.1')
        client.connect()
        items = client.browse(max_depth=3)
        client.add_items([item.item_id for item in items])
        values = client.read_items()
        client.set_data_change_callback(on_data)
        client.start_subscription(500)
        while running:
            client.pump_events()
            time.sleep(0.05)
        client.stop_subscription()
        client.disconnect()

    所有 COM 调用都在 connect() 所在线程执行（STA）。
    """

    def __init__(self, prog_id, host='localhost', locale=0x409):
        self._prog_id = str(prog_id or '').strip()
        if not self._prog_id:
            raise ValueError('prog_id 不能为空')
        self._host = str(host or 'localhost').strip() or 'localhost'
        self._locale = int(locale or 0x409)

        # COM 对象（连接期间有效）
        self._unknown = None            # 服务器 IUnknown（CreateObject 返回）
        self._server = None             # IOPCServer
        self._server_common = None      # IOPCCommon（可选）
        self._group_unknown = None      # 组 IUnknown
        self._group_handle = 0          # 服务器返回的组句柄（RemoveGroup 用）
        self._group_client_handle = 1   # AddGroup 时传入的客户端组句柄（回调 hGroup 用它）
        self._group_rate_ms = 0
        self._item_mgt = None           # IOPCItemMgt
        self._sync_io = None            # IOPCSyncIO
        self._group_state = None        # IOPCGroupStateMgt（可选）
        self._async_io2 = None          # IOPCAsyncIO2（订阅期间）
        self._connection_point_container = None
        self._connection_point = None
        self._sink = None
        self._advise_cookie = 0

        # 点位表：保持 AddItems 的顺序（read_items(None) 按此顺序返回）
        self._items = {}                # item_id -> DaItem
        self._client_handles = {}       # hClient -> item_id
        self._next_client_handle = 1
        self._last_add_errors = []

        # 状态
        self._connected = False
        self._thread_id = None
        self._subscription_active = False
        self._data_change_callback = None
        self._trans_id = 0

        # 订阅统计（引擎可据此判断订阅链路是否存活）
        self.callback_event_count = 0
        self.callback_item_count = 0
        self.last_callback_at = 0.0
        self.last_callback_items = 0
        self.subscription_warnings = []

    # ---------------------------------------------------------------- 基本属性

    @property
    def prog_id(self):
        return self._prog_id

    @property
    def host(self):
        return self._host

    @property
    def locale(self):
        return self._locale

    @property
    def connected(self):
        return self.is_connected()

    @property
    def update_rate_ms(self):
        return self._group_rate_ms

    @property
    def subscription_active(self):
        return self._subscription_active

    @property
    def last_add_errors(self):
        """最近一次 add_items 的逐项错误 [(item_id, hresult, error_text), ...]。"""
        return list(self._last_add_errors)

    def is_connected(self):
        """是否已成功连接（不发起 COM 调用）。"""
        return bool(self._connected and self._server is not None)

    # ---------------------------------------------------------------- 连接 / 断开

    def connect(self, timeout=15.0):
        """创建 COM 服务器对象并完成 IOPCServer/QI。

        :param timeout: 预留参数（COM 调用本身没有可中断的超时；远程 DCOM 的
            建链超时由 DCOM 配置决定），当前仅记录在实例上供上层参考。
        失败抛 OpcDaError；重复调用会先 disconnect()。
        """
        if self.is_connected():
            self.disconnect()
        self._thread_id = threading.get_ident()
        try:
            com.ensure_com_initialized()
        except RuntimeError as exc:
            raise OpcDaError(str(exc), com.RPC_E_CHANGED_MODE)

        local = _is_local_host(self._host)
        machine = None if local else self._host
        clsctx = None if local else com.CLSCTX_REMOTE_SERVER
        if not local:
            # 远程 DCOM：尽力把客户端默认安全级别设为 Connect+Impersonate
            com.try_init_dcom_security()

        unknown = None
        server = None
        try:
            unknown = comtypes.client.CreateObject(
                self._prog_id, clsctx=clsctx, machine=machine, interface=IUnknown)
        except Exception as exc:  # COMError / OSError
            code = _com_error_code(exc)
            self._release_com_objects()
            com.uninitialize_com()
            self._thread_id = None
            raise OpcDaError(
                '创建 OPC DA 服务器对象失败：ProgID=%r，主机=%r' % (self._prog_id, self._host),
                code)
        try:
            server = unknown.QueryInterface(com.IOPCServer)
        except Exception as exc:
            code = _com_error_code(exc) or com.E_NOINTERFACE
            self._release_com_objects()
            com.uninitialize_com()
            self._thread_id = None
            raise OpcDaError(
                '已创建服务器对象，但查询 IOPCServer 接口失败：%r' % self._prog_id, code)

        self._unknown = unknown
        self._server = server

        # IOPCCommon 是可选的（Matrikon 就不支持），失败忽略
        try:
            self._server_common = unknown.QueryInterface(com.IOPCCommon)
        except Exception:
            self._server_common = None
        if self._server_common is not None:
            try:
                self._server_common.SetLocaleID(self._locale)
            except Exception:
                pass
            try:
                self._server_common.SetClientName('LightingControlDA')
            except Exception:
                pass

        # 用一次 GetStatus 验证服务器真的可用（创建成功但进程已死的场景）
        try:
            self._read_server_status()
        except OpcDaError:
            self._release_com_objects()
            com.uninitialize_com()
            self._thread_id = None
            raise

        self._connected = True
        self.connect_timeout = float(timeout or 0)
        logger.info('OPC DA 已连接：%s（主机 %s，locale=0x%X）',
                    self._prog_id, self._host, self._locale)

    def _release_com_objects(self):
        """按依赖顺序释放所有 COM 接口（失败忽略，保证幂等）。"""
        self._sink = None
        self._connection_point = None
        self._connection_point_container = None
        self._advise_cookie = 0
        self._async_io2 = None
        self._item_mgt = None
        self._sync_io = None
        self._group_state = None
        self._group_unknown = None
        self._server_common = None
        self._server = None
        self._unknown = None
        self._connected = False
        self._group_handle = 0
        self._group_client_handle = 1
        self._group_rate_ms = 0
        self._subscription_active = False
        self._items.clear()
        self._client_handles.clear()
        self._last_add_errors = []
        self._last_callback_items = 0

    def disconnect(self):
        """断开连接（幂等；任何异常都吞掉，不向调用方抛出）。"""
        thread_id = self._thread_id
        try:
            self.stop_subscription()
        except Exception:  # pragma: no cover
            logger.exception('停止订阅时发生异常（已忽略）')
        if self._group_unknown is not None and self._server is not None and self._group_handle:
            try:
                self._server.RemoveGroup(int(self._group_handle), 1)
            except Exception:
                logger.debug('RemoveGroup 失败（忽略）', exc_info=True)
        self._release_com_objects()
        self._last_add_errors = []
        # 只在与 connect 相同的线程上配对 CoUninitialize
        if thread_id is not None and thread_id == threading.get_ident():
            com.uninitialize_com()
        self._thread_id = None
        logger.info('OPC DA 已断开：%s', self._prog_id)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()
        return False

    def _check_ready(self):
        if not self.is_connected():
            raise OpcDaError('尚未连接 OPC DA 服务器，请先调用 connect()',
                             com.CO_E_OBJNOTCONNECTED)
        if self._thread_id is not None and threading.get_ident() != self._thread_id:
            raise OpcDaError(
                'OPC DA 客户端只能在创建它的线程中调用（COM 为 STA 套间）',
                com.RPC_E_WRONG_THREAD)

    # ---------------------------------------------------------------- 服务器状态

    def _read_server_status(self):
        """读取并释放 OPCSERVERSTATUS，返回 dict。"""
        status_ptr = ctypes.POINTER(com.OPCSERVERSTATUS)()
        try:
            self._server.GetStatus(ctypes.byref(status_ptr))
        except Exception as exc:
            raise OpcDaError('读取 OPC 服务器状态失败', _com_error_code(exc))
        if not status_ptr:
            raise OpcDaError('读取 OPC 服务器状态失败：服务器返回空指针',
                             com.E_POINTER)
        try:
            status = status_ptr.contents
            vendor = status.szVendorInfo or ''
            version = '%d.%d.%d' % (status.wMajorVersion, status.wMinorVersion,
                                    status.wBuildNumber)
            return {
                'state': int(status.dwServerState),
                'state_text': com.server_state_text(status.dwServerState),
                'vendor': vendor,
                'version': version,
                'group_count': int(status.dwGroupCount),
                'bandwidth': int(status.dwBandWidth),
                'start_time': com.filetime_to_local_string(status.ftStartTime),
                'current_time': com.filetime_to_local_string(status.ftCurrentTime),
                'last_update_time': com.filetime_to_local_string(status.ftLastUpdateTime),
            }
        finally:
            try:
                com.free_cotaskmem(com.get_status_vendor_pointer(status))
            finally:
                com.free_cotaskmem(status_ptr)

    def get_server_status(self):
        """返回服务器状态 dict（state/state_text/vendor/version/group_count/start_time/current_time）。

        连接已断开或服务器无响应时抛 OpcDaError（引擎用它做心跳/断线检测）。
        """
        self._check_ready()
        return self._read_server_status()

    # ---------------------------------------------------------------- 浏览

    def browse(self, max_depth=3, max_items=20000):
        """层次浏览服务器地址空间，返回 DaItem 列表（canonical_vt/access 为 0）。

        层次命名空间用 ChangeBrowsePosition(DOWN/UP) 递归，叶子用 GetItemID 取
        完整 ItemID；FLAT 命名空间（或层次浏览无结果）用 BrowseOPCItemIDs(FLAT)
        回退。item_id 一定是可直接传给 add_items 的完整 ItemID。

        :param max_depth: 分支最大下钻层数（根为第 0 层；Matrikon 的叶子在第 2 层）
        :param max_items: 最多返回的点位数，防止病态服务器把 UI 拖死
        """
        self._check_ready()
        max_depth = max(0, int(max_depth if max_depth is not None else 3))
        max_items = max(1, int(max_items or 20000))
        try:
            browser = self._server.QueryInterface(com.IOPCBrowseServerAddressSpace)
        except Exception as exc:
            raise OpcDaError('服务器不支持地址空间浏览（IOPCBrowseServerAddressSpace）',
                             _com_error_code(exc) or com.E_NOINTERFACE)

        items = []
        try:
            namespace = com.OPC_NS_HIERARCHICAL
            try:
                namespace_type = ctypes.c_int()
                browser.QueryOrganization(ctypes.byref(namespace_type))
                if namespace_type.value in (com.OPC_NS_HIERARCHICAL, com.OPC_NS_FLAT):
                    namespace = namespace_type.value
            except Exception:
                logger.debug('QueryOrganization 失败，按层次命名空间处理', exc_info=True)

            if namespace == com.OPC_NS_HIERARCHICAL:
                try:
                    items = self._browse_hierarchical(browser, max_depth, max_items)
                except Exception as exc:
                    logger.warning('层次浏览失败，回退 FLAT 浏览：%s', exc)
                    items = []
            if not items:
                items = self._browse_flat(browser, max_items)
        finally:
            # 浏览位置是服务器端状态，必须复位到根，否则影响后续浏览/其他客户端
            try:
                browser.ChangeBrowsePosition(com.OPC_BROWSE_TO, '')
            except Exception:
                pass
        return items

    def _browse_hierarchical(self, browser, max_depth, max_items):
        result = []
        seen = set()
        try:
            browser.ChangeBrowsePosition(com.OPC_BROWSE_TO, '')
        except Exception as exc:
            raise OpcDaError('无法定位到地址空间根，浏览失败', _com_error_code(exc))

        def walk(level, path_parts):
            if len(result) >= max_items:
                return
            leaves = self._browse_enum(browser, com.OPC_LEAF)
            for leaf in leaves:
                if len(result) >= max_items:
                    return
                item_id = self._get_item_id(browser, leaf) or leaf
                if item_id in seen:
                    continue
                seen.add(item_id)
                result.append(self._make_browse_item(item_id, leaf,
                                                    '/'.join(path_parts)))
            if level >= max_depth:
                return
            branches = self._browse_enum(browser, com.OPC_BRANCH)
            for branch in branches:
                if len(result) >= max_items:
                    return
                try:
                    browser.ChangeBrowsePosition(com.OPC_BROWSE_DOWN, branch)
                except Exception:
                    logger.debug('进入分支失败：%r', branch, exc_info=True)
                    continue
                try:
                    walk(level + 1, path_parts + [branch])
                finally:
                    try:
                        browser.ChangeBrowsePosition(com.OPC_BROWSE_UP, '')
                    except Exception:
                        pass

        walk(0, [])
        return result

    def _browse_flat(self, browser, max_items):
        try:
            browser.ChangeBrowsePosition(com.OPC_BROWSE_TO, '')
        except Exception:
            pass
        names = self._browse_enum(browser, com.OPC_FLAT)
        result = []
        seen = set()
        for name in names:
            if len(result) >= max_items:
                break
            item_id = self._get_item_id(browser, name) or name
            if item_id in seen:
                continue
            seen.add(item_id)
            result.append(self._make_browse_item(item_id, name, ''))
        return result

    def _make_browse_item(self, item_id, leaf, browse_path):
        path, name = com.split_item_id(item_id)
        if not name:
            name = leaf
        if not path and browse_path:
            # 服务器 ItemID 本身不含层次（少见），退回浏览路径用于 UI 分组
            path = browse_path
        return DaItem(item_id=item_id, name=name, path=path)

    def _browse_enum(self, browser, filter_type):
        """BrowseOPCItemIDs + IEnumString 迭代。"""
        enum_ptr = ctypes.POINTER(com.IEnumString)()
        try:
            browser.BrowseOPCItemIDs(filter_type, '', 0,
                                     com.OPC_READABLE | com.OPC_WRITEABLE,
                                     ctypes.byref(enum_ptr))
        except Exception as exc:
            raise OpcDaError('浏览地址空间失败（filter=%d）' % filter_type,
                             _com_error_code(exc))
        if not enum_ptr:
            return []  # S_FALSE：成功但无数据
        return self._enum_strings(enum_ptr)

    def _get_item_id(self, browser, leaf):
        """把浏览得到的叶子名转换成完整 ItemID（服务器分配字符串，需释放）。"""
        value_ptr = ctypes.c_wchar_p()
        try:
            browser.GetItemID(leaf, ctypes.byref(value_ptr))
            return value_ptr.value
        except Exception:
            logger.debug('GetItemID(%r) 失败，退回浏览名', leaf, exc_info=True)
            return None
        finally:
            if value_ptr.value:
                com.free_cotaskmem(ctypes.cast(value_ptr, ctypes.c_void_p))

    @staticmethod
    def _enum_strings(enum):
        result = []
        buffer_size = 64
        buffer = (ctypes.c_wchar_p * buffer_size)()
        raw_pointers = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_void_p))
        fetched = ctypes.c_ulong()
        while True:
            for index in range(buffer_size):
                buffer[index] = None
            fetched.value = 0
            try:
                enum.Next(buffer_size, buffer, ctypes.byref(fetched))
            except Exception:
                logger.debug('IEnumString.Next 失败，停止枚举', exc_info=True)
                break
            count = int(fetched.value)
            if count <= 0:
                break
            for index in range(count):
                value = buffer[index]
                if value:
                    result.append(value)
            for index in range(count):
                com.free_cotaskmem(raw_pointers[index])
        return result

    # ---------------------------------------------------------------- 组管理

    def _ensure_group(self, update_rate_ms):
        rate = int(update_rate_ms or DEFAULT_UPDATE_RATE_MS)
        if rate <= 0:
            rate = DEFAULT_UPDATE_RATE_MS
        if self._group_unknown is None:
            self._create_group(rate)
            return
        if rate != self._group_rate_ms:
            self._set_group_rate(rate)

    def _create_group(self, rate):
        handle = com.OPCHANDLE()
        revised = com.DWORD()
        group_unknown = ctypes.POINTER(IUnknown)()
        name = _new_group_name()
        try:
            self._server.AddGroup(name, 1, rate, 1, None, None, self._locale,
                                  ctypes.byref(handle), ctypes.byref(revised),
                                  ctypes.byref(com.IID_IOPCItemMgt),
                                  ctypes.byref(group_unknown))
        except Exception as exc:
            raise OpcDaError('创建 OPC 组失败（更新周期 %dms）' % rate,
                             _com_error_code(exc))
        if not group_unknown:
            raise OpcDaError('创建 OPC 组失败：服务器未返回组对象', com.E_POINTER)
        self._group_unknown = group_unknown
        self._group_handle = int(handle.value)
        self._group_client_handle = 1
        self._group_rate_ms = int(revised.value) or rate
        try:
            self._item_mgt = group_unknown.QueryInterface(com.IOPCItemMgt)
        except Exception as exc:
            self._group_unknown = None
            self._server.RemoveGroup(self._group_handle, 1)
            self._group_handle = 0
            raise OpcDaError('查询 IOPCItemMgt 接口失败', _com_error_code(exc))
        try:
            self._sync_io = group_unknown.QueryInterface(com.IOPCSyncIO)
        except Exception:
            self._sync_io = None
            logger.warning('服务器组不支持 IOPCSyncIO，读写功能不可用')
        try:
            self._group_state = group_unknown.QueryInterface(com.IOPCGroupStateMgt)
        except Exception:
            self._group_state = None
            logger.debug('服务器组不支持 IOPCGroupStateMgt，更新周期将不可调整')

    def _ensure_sync_io(self):
        if self._sync_io is not None:
            return self._sync_io
        if self._group_unknown is None:
            raise OpcDaError('尚未创建 OPC 组，请先 add_items()', com.CO_E_OBJNOTCONNECTED)
        try:
            self._sync_io = self._group_unknown.QueryInterface(com.IOPCSyncIO)
        except Exception as exc:
            raise OpcDaError('服务器不支持同步读写（IOPCSyncIO）',
                             _com_error_code(exc) or com.E_NOINTERFACE)
        return self._sync_io

    def _set_group_rate(self, rate):
        """用 SetState 原地修改组更新周期（不重建组，订阅不中断）。"""
        if self._group_state is None:
            logger.warning('服务器不支持 IOPCGroupStateMgt，无法把更新周期调整为 %dms', rate)
            return
        requested = com.DWORD(rate)
        revised = com.DWORD()
        try:
            self._group_state.SetState(ctypes.byref(requested), ctypes.byref(revised),
                                       None, None, None, None, None)
        except Exception:
            logger.warning('调整组更新周期为 %dms 失败，保持 %dms',
                           rate, self._group_rate_ms, exc_info=True)
            return
        new_rate = int(revised.value) or rate
        self._group_rate_ms = new_rate
        logger.debug('组更新周期已调整为 %dms（请求 %dms）', new_rate, rate)

    # ---------------------------------------------------------------- 添加点位

    def add_items(self, item_ids, update_rate_ms=DEFAULT_UPDATE_RATE_MS):
        """批量 Adding 点位到内部唯一组，返回成功添加的 DaItem 列表。

        已挂载的点位不会重复 AddItems（直接返回现有 DaItem）。失败的点位不返回，
        逐项错误记录在 self.last_add_errors 中；若全部失败则抛带错误列表的
        OpcDaError。
        """
        self._check_ready()
        ordered = []
        seen = set()
        for raw_id in item_ids or []:
            item_id = str(raw_id)
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            ordered.append(item_id)
        if not ordered:
            self._ensure_group(update_rate_ms)
            return []

        self._ensure_group(update_rate_ms)
        pending = [item_id for item_id in ordered if item_id not in self._items]
        errors = []
        for start in range(0, len(pending), DEFAULT_BATCH_SIZE):
            chunk = pending[start:start + DEFAULT_BATCH_SIZE]
            batch_errors = self._add_items_batch(chunk)
            errors.extend(batch_errors)
        self._last_add_errors = errors
        if errors:
            logger.warning('%d 个点位添加失败，例如：%s', len(errors), errors[:5])

        result = [self._copy_item(self._items[item_id])
                  for item_id in ordered if item_id in self._items]
        if not result and errors:
            raise OpcDaError('所有点位添加失败（共 %d 个）' % len(errors),
                             errors[0][1], errors=errors)
        return result

    def _add_items_batch(self, item_ids):
        """一次 AddItems 调用完成一批点位；返回失败列表。"""
        count = len(item_ids)
        definitions = (com.OPCITEMDEF * count)()
        client_handles = []
        for index, item_id in enumerate(item_ids):
            handle = self._next_client_handle
            self._next_client_handle += 1
            client_handles.append(handle)
            definition = definitions[index]
            definition.szAccessPath = None
            definition.szItemID = item_id
            definition.bActive = 1
            definition.hClient = handle
            definition.dwBlobSize = 0
            definition.pBlob = None
            definition.vtRequestedDataType = 0
            definition.wReserved = 0

        results = ctypes.POINTER(com.OPCITEMRESULT)()
        call_errors = ctypes.POINTER(com.HRESULT)()
        try:
            self._item_mgt.AddItems(count, definitions,
                                    ctypes.byref(results), ctypes.byref(call_errors))
        except Exception as exc:
            raise OpcDaError('批量添加点位失败（%d 个）' % count, _com_error_code(exc))
        errors = []
        try:
            for index, item_id in enumerate(item_ids):
                code = call_errors[index] if call_errors else 0
                if com.succeeded(code) and results:
                    result = results[index]
                    path, name = com.split_item_id(item_id)
                    item = DaItem(
                        item_id=item_id,
                        name=name,
                        path=path,
                        canonical_vt=int(result.vtCanonicalDataType),
                        access=int(result.dwAccessRights),
                        server_handle=int(result.hServer),
                        client_handle=client_handles[index],
                    )
                    self._items[item_id] = item
                    self._client_handles[item.client_handle] = item_id
                    if result.dwBlobSize and result.pBlob:
                        com.free_cotaskmem(result.pBlob)
                else:
                    errors.append((item_id, com.as_unsigned(code) or 0,
                                   com.hresult_text(code)))
        finally:
            com.free_cotaskmem(results)
            com.free_cotaskmem(call_errors)
        return errors

    def remove_items(self, item_ids):
        """从内部唯一组 RemoveItems 一批点位（手动添加点位删除时使用）。

        :return: [(item_id, hresult, error_text)]，成功项 hresult=0。
                 未挂载的 ItemID 直接跳过（不视为错误）。
        """
        self._check_ready()
        # 不要用默认周期覆盖订阅中已经协商好的更新周期
        self._ensure_group(self._group_rate_ms or DEFAULT_UPDATE_RATE_MS)
        removals = []
        for raw_id in item_ids or []:
            item_id = str(raw_id)
            if item_id in self._items:
                removals.append(item_id)
        if not removals:
            return []
        count = len(removals)
        handles = (com.OPCHANDLE * count)()
        for index, item_id in enumerate(removals):
            handles[index] = self._items[item_id].server_handle
        errors_ptr = ctypes.POINTER(com.HRESULT)()
        try:
            self._item_mgt.RemoveItems(count, handles, ctypes.byref(errors_ptr))
        except Exception as exc:
            raise OpcDaError('批量移除点位失败（%d 个）' % count, _com_error_code(exc))
        results = []
        try:
            for index, item_id in enumerate(removals):
                code = errors_ptr[index] if errors_ptr else 0
                if com.succeeded(code):
                    self._items.pop(item_id, None)
                    # 同步清理 hClient -> ItemID 映射，避免旧句柄污染后续回调
                    for client_handle, mapped_id in list(self._client_handles.items()):
                        if mapped_id == item_id:
                            self._client_handles.pop(client_handle, None)
                    results.append((item_id, 0, ''))
                else:
                    results.append((item_id, com.as_unsigned(code) or 0,
                                    com.hresult_text(code)))
        finally:
            com.free_cotaskmem(errors_ptr)
        return results

    @staticmethod
    def _copy_item(item):
        return DaItem(item_id=item.item_id, name=item.name, path=item.path,
                      canonical_vt=item.canonical_vt, access=item.access,
                      server_handle=item.server_handle,
                      client_handle=item.client_handle)

    def mounted_items(self):
        """返回当前已挂载点位的 DaItem 副本列表（AddItems 顺序）。"""
        return [self._copy_item(item) for item in self._items.values()]

    # ---------------------------------------------------------------- 读取

    def read_items(self, item_ids=None, source=com.OPC_DS_CACHE):
        """批量同步读取点位值。

        :param item_ids: 为空表示读取全部已挂载点位（按 AddItems 顺序）
        :param source: 1=Cache，2=Device
        :return: list[DaValue]，顺序与请求一致；未挂载的 ItemID 返回
                 error=OPC_E_UNKNOWNITEMID 的条目
        """
        self._check_ready()
        source = int(source)
        if source not in (com.OPC_DS_CACHE, com.OPC_DS_DEVICE):
            raise ValueError('source 必须是 1（Cache）或 2（Device）')
        sync_io = self._ensure_sync_io()
        if item_ids is None:
            requested = list(self._items.keys())
        else:
            requested = [str(item_id) for item_id in item_ids]
        if not requested:
            return []

        output = [None] * len(requested)
        pending = []
        for index, item_id in enumerate(requested):
            if item_id in self._items:
                pending.append((index, item_id))
            else:
                output[index] = DaValue(item_id=item_id, value=None, quality=0,
                                        timestamp=None,
                                        error=com.OPC_E_UNKNOWNITEMID)

        for start in range(0, len(pending), DEFAULT_BATCH_SIZE):
            chunk = pending[start:start + DEFAULT_BATCH_SIZE]
            values = self._read_chunk(sync_io, chunk, source)
            for position, value in zip(chunk, values):
                output[position[0]] = value
        return [value if value is not None else
                DaValue(item_id=requested[index], value=None, quality=0,
                        timestamp=None, error=com.OPC_E_UNKNOWNITEMID)
                for index, value in enumerate(output)]

    def _read_chunk(self, sync_io, chunk, source):
        count = len(chunk)
        handles = (com.OPCHANDLE * count)()
        for index, (_, item_id) in enumerate(chunk):
            handles[index] = self._items[item_id].server_handle
        states = ctypes.POINTER(com.OPCITEMSTATE)()
        errors = ctypes.POINTER(com.HRESULT)()
        try:
            sync_io.Read(int(source), count, handles,
                         ctypes.byref(states), ctypes.byref(errors))
        except Exception as exc:
            raise OpcDaError('同步读取失败（%d 个点位）' % count,
                             _com_error_code(exc))
        values = []
        try:
            for index, (_, item_id) in enumerate(chunk):
                code = com.as_unsigned(errors[index]) if errors else 0
                if code:
                    values.append(DaValue(item_id=item_id, value=None, quality=0,
                                          timestamp=None, error=code))
                    continue
                if not states:
                    values.append(DaValue(item_id=item_id, value=None, quality=0,
                                          timestamp=None, error=com.E_POINTER))
                    continue
                state = states[index]
                values.append(DaValue(
                    item_id=item_id,
                    value=com.variant_to_python(state.vData),
                    quality=int(state.wQuality),
                    timestamp=com.filetime_to_local_string(state.ftTimeStamp),
                    error=0,
                ))
        finally:
            # 释放每个 OPCITEMSTATE.vData 里的 BSTR/SAFEARRAY，再释放整个数组
            if states:
                for index in range(count):
                    com.variant_clear(states[index].vData)
            com.free_cotaskmem(states)
            com.free_cotaskmem(errors)
        return values

    # ---------------------------------------------------------------- 写入

    def write_items(self, values):
        """批量同步写入点位值。

        :param values: {item_id: value} 映射
        :return: [(item_id, hresult, error_text), ...]，顺序与传入映射一致；
                 hresult==0 表示写入成功
        :raises OpcDaError: 值类型无法安全转换（含数组类型点位）时直接失败，
            不会尝试构造可能崩溃的 SAFEARRAY；未挂载/只读点位作为逐项错误返回
        """
        self._check_ready()
        mapping = list(values.items()) if values else []
        if not mapping:
            return []
        sync_io = self._ensure_sync_io()

        # 未挂载的点位自动挂载一次（引擎在重连后可能先写后订阅）
        missing = [item_id for item_id, _ in mapping if item_id not in self._items]
        if missing:
            try:
                self.add_items(missing, self._group_rate_ms or DEFAULT_UPDATE_RATE_MS)
            except OpcDaError:
                logger.debug('写入前自动挂载部分点位失败', exc_info=True)

        results = [None] * len(mapping)
        valid = []
        for index, (item_id, value) in enumerate(mapping):
            item_id = str(item_id)
            item = self._items.get(item_id)
            if item is None:
                results[index] = (item_id, com.OPC_E_UNKNOWNITEMID,
                                  com.hresult_text(com.OPC_E_UNKNOWNITEMID))
                continue
            if item.access and not (item.access & com.OPC_WRITEABLE):
                results[index] = (item_id, com.OPC_E_BADRIGHTS,
                                  '点位不可写（access=%d）' % item.access)
                continue
            # 数组/无法安全构造 VARIANT 的类型必须显式中断：实测写 SAFEARRAY
            # 会在 comtypes 里触发解释器级崩溃，绝不能让 UI 线程走到那一步。
            if item.canonical_vt & com.VT_ARRAY:
                raise OpcDaError(
                    'unsupported data type for write: 数组类型点位暂不支持写值'
                    '（item_id=%s, canonical_vt=0x%04X）' % (item_id, item.canonical_vt))
            try:
                variant = com.python_to_variant(value, item.canonical_vt)
            except (ValueError, TypeError) as exc:
                raise OpcDaError(
                    'unsupported data type for write: %s'
                    '（item_id=%s, canonical_vt=0x%04X）' % (
                        exc, item_id, item.canonical_vt))
            except Exception as exc:  # pragma: no cover - 防御性
                raise OpcDaError(
                    'unsupported data type for write: %s（item_id=%s）' % (exc, item_id))
            valid.append((index, item_id, item, variant))

        if not valid:
            return [entry for entry in results if entry is not None]

        count = len(valid)
        handles = (com.OPCHANDLE * count)()
        variants = (VARIANT * count)()
        array_ready = False
        try:
            for position, (_, _, item, variant) in enumerate(valid):
                handles[position] = item.server_handle
                variants[position] = variant
            array_ready = True
            for start in range(0, count, DEFAULT_BATCH_SIZE):
                chunk_len = min(DEFAULT_BATCH_SIZE, count - start)
                chunk_handles = ctypes.cast(
                    ctypes.byref(handles, start * ctypes.sizeof(com.OPCHANDLE)),
                    ctypes.POINTER(com.OPCHANDLE))
                chunk_values = ctypes.cast(
                    ctypes.byref(variants, start * ctypes.sizeof(VARIANT)),
                    ctypes.POINTER(VARIANT))
                errors = ctypes.POINTER(com.HRESULT)()
                try:
                    try:
                        sync_io.Write(chunk_len, chunk_handles, chunk_values,
                                      ctypes.byref(errors))
                    except Exception as exc:
                        raise OpcDaError('批量写入失败（%d 个点位）' % chunk_len,
                                         _com_error_code(exc))
                    for offset in range(chunk_len):
                        code = com.as_unsigned(errors[offset]) if errors else 0
                        mapping_index, item_id, _, _ = valid[start + offset]
                        results[mapping_index] = (
                            item_id, code, '' if code == 0 else com.hresult_text(code))
                finally:
                    com.free_cotaskmem(errors)
        finally:
            if array_ready:
                # 数组元素接管了 VARIANT 里的 BSTR/SAFEARRAY，统一在这里释放
                for position in range(count):
                    com.variant_clear(variants[position])
            else:
                for _, _, _, variant in valid:
                    com.variant_clear(variant)
        return [entry for entry in results if entry is not None]

    # ---------------------------------------------------------------- 订阅

    def set_data_change_callback(self, callback):
        """设置数据变化回调 callback(list[DaValue])（在调用线程的消息泵中触发）。"""
        if callback is not None and not callable(callback):
            raise TypeError('callback 必须可调用或为 None')
        self._data_change_callback = callback

    def start_subscription(self, update_rate_ms=DEFAULT_UPDATE_RATE_MS):
        """启动数据变化订阅（IOPCAsyncIO2 + IOPCDataCallback）。"""
        self._check_ready()
        self._ensure_group(update_rate_ms)
        if self._subscription_active:
            return
        if self._group_unknown is None:
            raise OpcDaError('尚未创建 OPC 组，无法订阅', com.CO_E_OBJNOTCONNECTED)

        try:
            async_io2 = self._group_unknown.QueryInterface(com.IOPCAsyncIO2)
        except Exception:
            raise OpcDaError(
                'async unsupported: 服务器不支持 IOPCAsyncIO2 异步订阅接口',
                com.E_NOINTERFACE)
        self._async_io2 = async_io2

        try:
            container = self._group_unknown.QueryInterface(IConnectionPointContainer)
            connection_point = container.FindConnectionPoint(
                ctypes.byref(com.IID_IOPCDataCallback))
        except Exception as exc:
            self._async_io2 = None
            raise OpcDaError('服务器不支持数据变化回调连接点', _com_error_code(exc))
        if not connection_point:
            self._async_io2 = None
            raise OpcDaError('服务器未返回数据变化回调连接点', com.E_POINTER)

        sink = _DataCallbackSink(self)
        try:
            cookie = connection_point.Advise(sink)
        except Exception as exc:
            self._async_io2 = None
            raise OpcDaError('订阅回调 Advise 失败', _com_error_code(exc))

        self._connection_point_container = container
        self._connection_point = connection_point
        self._sink = sink
        self._advise_cookie = int(cookie or 0)
        self.subscription_warnings = []

        try:
            async_io2.SetEnable(1)
        except Exception as exc:
            self.subscription_warnings.append('SetEnable(True) 失败：%s' % (exc,))

        # 触发一次全量初值回调（Cache 源，不打扰现场设备）；失败仅告警
        self._trans_id += 1
        cancel_id = com.DWORD()
        try:
            async_io2.Refresh2(com.OPC_DS_CACHE, self._trans_id,
                               ctypes.byref(cancel_id))
        except Exception as exc:
            self.subscription_warnings.append('Refresh2 失败：%s' % (exc,))
            logger.warning('Refresh2 触发初值回调失败（订阅仍继续）：%s', exc)

        self._subscription_active = True
        logger.info('OPC DA 订阅已启动：%d 个点位，周期 %dms',
                    len(self._items), self._group_rate_ms)

    def stop_subscription(self):
        """停止数据变化订阅（幂等；失败忽略）。"""
        if not self._subscription_active and self._connection_point is None:
            return
        # 先关闭异步使能：部分服务器在 Unadvise 之后再 SetEnable 会返回 E_FAIL
        if self._async_io2 is not None:
            try:
                self._async_io2.SetEnable(0)
            except Exception:
                logger.debug('SetEnable(False) 失败（忽略）', exc_info=True)
        if self._connection_point is not None and self._advise_cookie:
            try:
                self._connection_point.Unadvise(self._advise_cookie)
            except Exception:
                logger.debug('Unadvise 失败（忽略）', exc_info=True)
        self._advise_cookie = 0
        self._sink = None
        self._connection_point = None
        self._connection_point_container = None
        self._async_io2 = None
        self._subscription_active = False
        logger.info('OPC DA 订阅已停止：%s', self._prog_id)

    def _handle_data_change(self, transid, hgroup, count, client_items, values,
                            qualities, timestamps, errors):
        """OnDataChange 的客户端处理：hClient 映射回 ItemID 并转 DaValue。"""
        try:
            group_handle = int(hgroup)
        except (TypeError, ValueError):
            group_handle = 0
        # OPC 规范里 hGroup 是 AddGroup 传入的客户端组句柄，但个别服务器回传服务器
        # 组句柄；两者都接受，其余（旧连接/其他组）直接忽略。
        if group_handle and group_handle not in (self._group_client_handle,
                                                 self._group_handle):
            logger.debug('忽略其他组的数据变化回调：hGroup=%s', group_handle)
            return
        count = int(count or 0)
        if count <= 0 or not client_items or not values:
            return
        data = []
        for index in range(count):
            handle = int(client_items[index])
            item_id = self._client_handles.get(handle)
            if item_id is None:
                continue
            code = com.as_unsigned(errors[index]) if errors else 0
            try:
                value = None if code else com.variant_to_python(values[index])
            except Exception:  # pragma: no cover - 防御性
                value = None
            data.append(DaValue(
                item_id=item_id,
                value=value,
                quality=int(qualities[index]) if qualities else 0,
                timestamp=(com.filetime_to_local_string(timestamps[index])
                           if timestamps else None),
                error=code,
            ))
        if not data:
            return
        self.callback_event_count += 1
        self.callback_item_count += len(data)
        self.last_callback_at = time.time()
        self.last_callback_items = len(data)
        callback = self._data_change_callback
        if callback is not None:
            try:
                callback(data)
            except Exception:  # pragma: no cover - 用户回调异常不污染 COM
                logger.exception('数据变化回调执行失败')

    # ---------------------------------------------------------------- 错误字符串

    def get_error_string(self, code):
        """把 HRESULT 转成可读文本（优先服务器 GetErrorString，回退本地映射）。"""
        unsigned = com.as_unsigned(code)
        if unsigned is None:
            return '未知错误'
        if unsigned == 0:
            return '成功（0x00000000）'
        if self._server is not None:
            value_ptr = ctypes.c_wchar_p()
            try:
                self._server.GetErrorString(com.as_signed(unsigned), self._locale,
                                            ctypes.byref(value_ptr))
                text = value_ptr.value
                if text:
                    return text.strip()
            except Exception:
                pass
            finally:
                if value_ptr.value:
                    com.free_cotaskmem(ctypes.cast(value_ptr, ctypes.c_void_p))
        if self._server_common is not None:
            value_ptr = ctypes.c_wchar_p()
            try:
                self._server_common.GetErrorString(com.as_signed(unsigned),
                                                   ctypes.byref(value_ptr))
                text = value_ptr.value
                if text:
                    return text.strip()
            except Exception:
                pass
            finally:
                if value_ptr.value:
                    com.free_cotaskmem(ctypes.cast(value_ptr, ctypes.c_void_p))
        return com.hresult_text(unsigned)

    # ---------------------------------------------------------------- 消息泵

    def pump_events(self):
        """派发本线程的 Windows 消息，让订阅回调得以执行（非阻塞）。"""
        com.pump_message_events()

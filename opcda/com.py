# -*- coding: utf-8 -*-
"""OPC DA 低层 COM 定义与类型转换工具。

本模块是 opcda.client 的实现基础：手工声明（不使用 comtypes.client.GetModule
运行时生成类型库包装）OPC DA 2.05a 所需的 ctypes 结构体、COM 接口、IID、枚举
常量，以及 VARIANT / FILETIME / HRESULT / 质量码等数据在 COM 与 Python 之间的
转换函数。

为什么手写接口声明：
1. 单文件打包（PyInstaller）时不需要 comtypes 在运行期把类型库转成
   comtypes.gen.* 模块 —— 那一步在冻结环境里依赖注册表与可写目录，很容易失败；
2. out 数组（OPCITEMSTATE/OPCITEMRESULT/IEnumString 等）的分配与释放可以精确控制，
   保证 CoTaskMemFree / VariantClear 成对出现，不泄漏服务器分配的内存；
3. 只声明真正用到的接口，服务器缺少可选接口时 QI 失败可以优雅降级。

接口 IID 与 vtable 顺序已用本机 OPC Core Components 的 opcproxy.dll /
opccomn_ps.dll 类型库逐一核对（尤其注意 IOPCAsyncIO2 的方法顺序是
Read/Write/Refresh2/Cancel2/SetEnable/GetEnable，IOPCGroupStateMgt.GetState
是 8 个 out 参数）。
"""

from __future__ import annotations

import ctypes
import datetime
import decimal
import threading

import comtypes
import comtypes.client
from comtypes import COMMETHOD, GUID, HRESULT, IUnknown
from comtypes.automation import (
    VARIANT,
    VT_ARRAY,
    VT_BOOL,
    VT_BSTR,
    VT_BYREF,
    VT_CY,
    VT_DATE,
    VT_EMPTY,
    VT_I1,
    VT_I2,
    VT_I4,
    VT_I8,
    VT_INT,
    VT_NULL,
    VT_R4,
    VT_R8,
    VT_UI1,
    VT_UI2,
    VT_UI4,
    VT_UI8,
    VT_UINT,
)

# --------------------------------------------------------------------------------------
# 基础 ctypes 类型与 OPC DA 常量
# --------------------------------------------------------------------------------------

OPCHANDLE = ctypes.c_ulong
VARTYPE = ctypes.c_ushort
DWORD = ctypes.c_ulong
BOOL = ctypes.c_int
WORD = ctypes.c_ushort
LONG = ctypes.c_long
FLOAT = ctypes.c_float

# OPCBROWSEDIRECTION
OPC_BROWSE_UP = 1
OPC_BROWSE_DOWN = 2
OPC_BROWSE_TO = 3

# OPCBROWSETYPE
OPC_BRANCH = 1
OPC_LEAF = 2
OPC_FLAT = 3

# OPCNAMESPACETYPE
OPC_NS_HIERARCHICAL = 1
OPC_NS_FLAT = 2

# 访问权限
OPC_READABLE = 1
OPC_WRITEABLE = 2

# 同步读取数据源
OPC_DS_CACHE = 1
OPC_DS_DEVICE = 2

# OPCGROUPENUMTYPE（服务器枚举组时用，本客户端暂未使用）
OPC_ENUM_PRIVATE_CONNECTIONS = 1
OPC_ENUM_PUBLIC_CONNECTIONS = 2
OPC_ENUM_ALL_CONNECTIONS = 3

# 服务器状态（OPC_STATUS_*，注意需与 OPC_STATUS_MASK 相与）
OPC_STATUS_RUNNING = 1
OPC_STATUS_FAILED = 2
OPC_STATUS_NOCONFIG = 3
OPC_STATUS_SUSPENDED = 4
OPC_STATUS_TEST = 5
OPC_STATUS_COMM_FAULT = 6
OPC_STATUS_MASK = 0xFC

# 质量码（OPC_QUALITY_*）
OPC_QUALITY_BAD = 0x00
OPC_QUALITY_UNCERTAIN = 0x40
OPC_QUALITY_GOOD = 0xC0
OPC_QUALITY_MASK = 0xC0
OPC_QUALITY_CONFIG_ERROR = 0x04
OPC_QUALITY_NOT_CONNECTED = 0x08
OPC_QUALITY_DEVICE_FAILURE = 0x0C
OPC_QUALITY_SENSOR_FAILURE = 0x10
OPC_QUALITY_LAST_KNOWN = 0x14
OPC_QUALITY_COMM_FAILURE = 0x18
OPC_QUALITY_OUT_OF_SERVICE = 0x1C
OPC_QUALITY_WAITING_FOR_INITIAL_DATA = 0x20
OPC_QUALITY_LAST_USABLE = 0x44
OPC_QUALITY_SENSOR_CAL = 0x50
OPC_QUALITY_EGU_EXCEEDED = 0x54
OPC_QUALITY_SUB_NORMAL = 0x58
OPC_QUALITY_LOCAL_OVERRIDE = 0xD8

# COM 创建上下文
CLSCTX_LOCAL_SERVER = comtypes.CLSCTX_LOCAL_SERVER
CLSCTX_REMOTE_SERVER = comtypes.CLSCTX_REMOTE_SERVER

# 常用 HRESULT（统一以无符号 32 位整数保存/显示）
S_OK = 0x00000000
S_FALSE = 0x00000001
E_NOTIMPL = 0x80004001
E_NOINTERFACE = 0x80004002
E_POINTER = 0x80004003
E_FAIL = 0x80004005
E_UNEXPECTED = 0x8000FFFF
E_INVALIDARG = 0x80070057
E_ACCESSDENIED = 0x80070005
# OLE 连接点错误（FindConnectionPoint/Advise 失败）——属"服务器不支持回调订阅"，
# 可安全降级为 SyncRead 轮询，不代表链路故障。
CONNECT_E_ADVISELIMIT = 0x80040201
CONNECT_E_NOCONNECTION = 0x80040202
CONNECT_E_CANNOTCONNECT = 0x80040203
REGDB_E_CLASSNOTREG = 0x80040154
CO_E_NOTINITIALIZED = 0x800401F0
CO_E_OBJNOTCONNECTED = 0x800401FD
RPC_E_CHANGED_MODE = 0x80010106
RPC_E_DISCONNECTED = 0x80010108
RPC_E_WRONG_THREAD = 0x8001010E
RPC_E_TOO_LATE = 0x80010119
RPC_S_SERVER_UNAVAILABLE = 0x800706BA
RPC_S_CALL_FAILED = 0x800706BE
RPC_S_CALL_FAILED_DNE = 0x800706BF
RPC_X_NULL_REF_POINTER = 0x800706F4
CO_E_CLASSSTRING = 0x800401F3
CLASS_E_CLASSNOTAVAILABLE = 0x80040111

# OPC 自定义错误码（opcerrs.h）
OPC_E_INVALIDHANDLE = 0xC0040001
OPC_E_BADTYPE = 0xC0040002
OPC_E_PUBLIC = 0xC0040003
OPC_E_BADRIGHTS = 0xC0040004
OPC_E_UNKNOWNITEMID = 0xC0040005
OPC_E_INVALIDITEMID = 0xC0040006
OPC_E_INVALIDFILTER = 0xC0040007
OPC_E_UNKNOWNPATH = 0xC0040008
OPC_E_RANGE = 0xC0040009
OPC_E_DUPLICATENAME = 0xC004000A
OPC_S_UNKNOWN_LCID = 0x0004000B
OPC_E_UNKNOWNLCID = 0xC004000C
OPC_E_INVALIDCONFIGFILE = 0xC004000D
OPC_E_NOTFOUND = 0xC004000E
OPC_E_INVALID_PICTURE = 0xC004000F
OPC_E_INVALIDDATATYPE = 0xC0040010
OPC_E_WRITEONLY = 0xC0040011

# --------------------------------------------------------------------------------------
# ctypes 结构体（字段布局与 OPC DA 2.05a IDL 一致）
# --------------------------------------------------------------------------------------


class FILETIME(ctypes.Structure):
    """Win32 FILETIME（UTC，自 1601-01-01 起的 100ns 数）。"""

    _fields_ = [
        ('dwLowDateTime', DWORD),
        ('dwHighDateTime', DWORD),
    ]


class OPCSERVERSTATUS(ctypes.Structure):
    _fields_ = [
        ('ftStartTime', FILETIME),
        ('ftCurrentTime', FILETIME),
        ('ftLastUpdateTime', FILETIME),
        ('dwServerState', LONG),
        ('dwGroupCount', DWORD),
        ('dwBandWidth', DWORD),
        ('wMajorVersion', WORD),
        ('wMinorVersion', WORD),
        ('wBuildNumber', WORD),
        ('wReserved', WORD),
        ('szVendorInfo', ctypes.c_wchar_p),
    ]


class OPCITEMDEF(ctypes.Structure):
    _fields_ = [
        ('szAccessPath', ctypes.c_wchar_p),
        ('szItemID', ctypes.c_wchar_p),
        ('bActive', BOOL),
        ('hClient', OPCHANDLE),
        ('dwBlobSize', DWORD),
        ('pBlob', ctypes.POINTER(ctypes.c_byte)),
        ('vtRequestedDataType', VARTYPE),
        ('wReserved', WORD),
    ]


class OPCITEMRESULT(ctypes.Structure):
    _fields_ = [
        ('hServer', OPCHANDLE),
        ('vtCanonicalDataType', VARTYPE),
        ('wReserved', WORD),
        ('dwAccessRights', DWORD),
        ('dwBlobSize', DWORD),
        ('pBlob', ctypes.POINTER(ctypes.c_byte)),
    ]


class OPCITEMSTATE(ctypes.Structure):
    _fields_ = [
        ('hClient', OPCHANDLE),
        ('ftTimeStamp', FILETIME),
        ('wQuality', WORD),
        ('wReserved', WORD),
        ('vData', VARIANT),
    ]


class COSERVERINFO(ctypes.Structure):
    """远程 DCOM 创建对象时使用的 COSERVERINFO。"""

    _fields_ = [
        ('dwReserved1', DWORD),
        ('pwszName', ctypes.c_wchar_p),
        ('pAuthInfo', ctypes.c_void_p),
        ('dwReserved2', DWORD),
    ]


# --------------------------------------------------------------------------------------
# IID（与 OPC Core Components 的代理/存根类型库核对过）
# --------------------------------------------------------------------------------------

IID_IOPCServer = GUID('{39C13A4D-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCServerPublicGroups = GUID('{39C13A4E-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCBrowseServerAddressSpace = GUID('{39C13A4F-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCGroupStateMgt = GUID('{39C13A50-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCPublicGroupStateMgt = GUID('{39C13A51-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCSyncIO = GUID('{39C13A52-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCAsyncIO = GUID('{39C13A53-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCItemMgt = GUID('{39C13A54-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCDataCallback = GUID('{39C13A70-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCAsyncIO2 = GUID('{39C13A71-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCItemProperties = GUID('{39C13A72-011E-11D0-9675-0020AFD8ADB3}')
IID_IOPCCommon = GUID('{F31DFDE2-07B6-11D2-B2D8-0060083BA1FB}')
IID_IOPCShutdown = GUID('{F31DFDE1-07B6-11D2-B2D8-0060083BA1FB}')
IID_IEnumString = GUID('{00000101-0000-0000-C000-000000000046}')

# OPC Server List（OPCEnum / opc_comn）：本机与远程统一的 DA 服务器枚举入口
CLSID_OPC_SERVER_LIST = GUID('{13486D51-4821-11D2-A494-3CB306C10000}')
IID_IOPCServerList = GUID('{13486D50-4821-11D2-A494-3CB306C10000}')
# OPC 规范里的 IEnumCLSID 实际就是标准 IEnumGUID（EnumClassesOfCategories 的出参接口）
IID_IEnumCLSID = GUID('{0002E000-0000-0000-C000-000000000046}')

# OPC DA 1.0 / 2.0 / 3.0 的组件类别（用于注册表扫描枚举本机 DA 服务器）
CATID_OPC_DA_10 = '{63D5F430-CFE4-11D1-B642-00A0A0194E1F}'
CATID_OPC_DA_20 = '{63D5F432-CFE4-11D1-B642-00A0A0194E1F}'
CATID_OPC_DA_30 = '{CC603642-66D7-48F1-B69A-B625E73652D7}'
DA_CATEGORY_IDS = (CATID_OPC_DA_10, CATID_OPC_DA_20, CATID_OPC_DA_30)

# --------------------------------------------------------------------------------------
# COM 接口定义
# --------------------------------------------------------------------------------------


class IOPCServer(IUnknown):
    _iid_ = IID_IOPCServer
    _methods_ = [
        COMMETHOD([], HRESULT, 'AddGroup',
                  (['in'], ctypes.c_wchar_p, 'szName'),
                  (['in'], BOOL, 'bActive'),
                  (['in'], DWORD, 'dwRequestedUpdateRate'),
                  (['in'], OPCHANDLE, 'hClientGroup'),
                  (['in'], ctypes.POINTER(LONG), 'pTimeBias'),
                  (['in'], ctypes.POINTER(FLOAT), 'pPercentDeadband'),
                  (['in'], DWORD, 'dwLCID'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServerGroup'),
                  (['in'], ctypes.POINTER(DWORD), 'pRevisedUpdateRate'),
                  (['in'], ctypes.POINTER(GUID), 'riid'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(IUnknown)), 'ppUnk')),
        COMMETHOD([], HRESULT, 'GetErrorString',
                  (['in'], HRESULT, 'dwError'),
                  (['in'], DWORD, 'dwLocale'),
                  (['in'], ctypes.POINTER(ctypes.c_wchar_p), 'ppString')),
        COMMETHOD([], HRESULT, 'GetGroupByName',
                  (['in'], ctypes.c_wchar_p, 'szName'),
                  (['in'], ctypes.POINTER(GUID), 'riid'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(IUnknown)), 'ppUnk')),
        COMMETHOD([], HRESULT, 'GetStatus',
                  (['in'], ctypes.POINTER(ctypes.POINTER(OPCSERVERSTATUS)), 'ppServerStatus')),
        COMMETHOD([], HRESULT, 'RemoveGroup',
                  (['in'], OPCHANDLE, 'hServerGroup'),
                  (['in'], BOOL, 'bForce')),
        COMMETHOD([], HRESULT, 'CreateGroupEnumerator',
                  (['in'], DWORD, 'dwScope'),
                  (['in'], ctypes.POINTER(GUID), 'riid'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(IUnknown)), 'ppUnk')),
    ]


class IOPCItemMgt(IUnknown):
    _iid_ = IID_IOPCItemMgt
    _methods_ = [
        COMMETHOD([], HRESULT, 'AddItems',
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCITEMDEF), 'pItemArray'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(OPCITEMRESULT)), 'ppItemResults'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'ValidateItems',
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCITEMDEF), 'pItemArray'),
                  (['in'], BOOL, 'bBlobUpdate'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(OPCITEMRESULT)), 'ppValidationResults'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'RemoveItems',
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServer'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'SetActiveState',
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServer'),
                  (['in'], BOOL, 'bActive'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'SetClientHandles',
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServer'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phClient'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'SetDatatypes',
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServer'),
                  (['in'], ctypes.POINTER(VARTYPE), 'pRequestedDatatypes'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'CreateEnumerator',
                  (['in'], ctypes.POINTER(GUID), 'riid'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(IUnknown)), 'ppUnk')),
    ]


class IOPCSyncIO(IUnknown):
    _iid_ = IID_IOPCSyncIO
    _methods_ = [
        COMMETHOD([], HRESULT, 'Read',
                  (['in'], DWORD, 'dwSource'),
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServer'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(OPCITEMSTATE)), 'ppItemValues'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'Write',
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServer'),
                  (['in'], ctypes.POINTER(VARIANT), 'pItemValues'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
    ]


class IOPCGroupStateMgt(IUnknown):
    _iid_ = IID_IOPCGroupStateMgt
    _methods_ = [
        COMMETHOD([], HRESULT, 'GetState',
                  (['in'], ctypes.POINTER(DWORD), 'pUpdateRate'),
                  (['in'], ctypes.POINTER(BOOL), 'pActive'),
                  (['in'], ctypes.POINTER(ctypes.c_wchar_p), 'ppName'),
                  (['in'], ctypes.POINTER(LONG), 'pTimeBias'),
                  (['in'], ctypes.POINTER(FLOAT), 'pPercentDeadband'),
                  (['in'], ctypes.POINTER(DWORD), 'pLCID'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phClientGroup'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServerGroup')),
        COMMETHOD([], HRESULT, 'SetState',
                  (['in'], ctypes.POINTER(DWORD), 'pRequestedUpdateRate'),
                  (['in'], ctypes.POINTER(DWORD), 'pRevisedUpdateRate'),
                  (['in'], ctypes.POINTER(BOOL), 'pActive'),
                  (['in'], ctypes.POINTER(LONG), 'pTimeBias'),
                  (['in'], ctypes.POINTER(FLOAT), 'pPercentDeadband'),
                  (['in'], ctypes.POINTER(DWORD), 'pLCID'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phClientGroup')),
        COMMETHOD([], HRESULT, 'SetName',
                  (['in'], ctypes.c_wchar_p, 'szName')),
        COMMETHOD([], HRESULT, 'CloneGroup',
                  (['in'], ctypes.c_wchar_p, 'szName'),
                  (['in'], ctypes.POINTER(GUID), 'riid'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(IUnknown)), 'ppUnk')),
    ]


class IOPCCommon(IUnknown):
    _iid_ = IID_IOPCCommon
    _methods_ = [
        COMMETHOD([], HRESULT, 'SetLocaleID', (['in'], DWORD, 'dwLcid')),
        COMMETHOD([], HRESULT, 'GetLocaleID', (['in'], ctypes.POINTER(DWORD), 'pdwLcid')),
        COMMETHOD([], HRESULT, 'QueryAvailableLocaleIDs',
                  (['in'], ctypes.POINTER(DWORD), 'pdwCount'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(DWORD)), 'pdwLcid')),
        COMMETHOD([], HRESULT, 'GetErrorString',
                  (['in'], HRESULT, 'dwError'),
                  (['in'], ctypes.POINTER(ctypes.c_wchar_p), 'ppString')),
        COMMETHOD([], HRESULT, 'SetClientName',
                  (['in'], ctypes.c_wchar_p, 'szName')),
    ]


class IEnumString(IUnknown):
    _iid_ = IID_IEnumString
    _methods_ = [
        COMMETHOD([], HRESULT, 'Next',
                  (['in'], ctypes.c_ulong, 'celt'),
                  (['in'], ctypes.POINTER(ctypes.c_wchar_p), 'rgelt'),
                  (['in'], ctypes.POINTER(ctypes.c_ulong), 'pceltFetched')),
        COMMETHOD([], HRESULT, 'Skip', (['in'], ctypes.c_ulong, 'celt')),
        COMMETHOD([], HRESULT, 'Reset'),
        COMMETHOD([], HRESULT, 'Clone',
                  (['in'], ctypes.POINTER(ctypes.POINTER(IUnknown)), 'ppenum')),
    ]


class IOPCBrowseServerAddressSpace(IUnknown):
    _iid_ = IID_IOPCBrowseServerAddressSpace
    _methods_ = [
        COMMETHOD([], HRESULT, 'QueryOrganization',
                  (['in'], ctypes.POINTER(ctypes.c_int), 'pNameSpaceType')),
        COMMETHOD([], HRESULT, 'ChangeBrowsePosition',
                  (['in'], ctypes.c_int, 'dwBrowseDirection'),
                  (['in'], ctypes.c_wchar_p, 'szString')),
        COMMETHOD([], HRESULT, 'BrowseOPCItemIDs',
                  (['in'], ctypes.c_int, 'dwBrowseFilterType'),
                  (['in'], ctypes.c_wchar_p, 'szFilterCriteria'),
                  (['in'], VARTYPE, 'vtDataTypeFilter'),
                  (['in'], DWORD, 'dwAccessRightsFilter'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(IEnumString)), 'ppIEnumString')),
        COMMETHOD([], HRESULT, 'GetItemID',
                  (['in'], ctypes.c_wchar_p, 'szItemDataID'),
                  (['in'], ctypes.POINTER(ctypes.c_wchar_p), 'szItemID')),
        COMMETHOD([], HRESULT, 'BrowseAccessPaths',
                  (['in'], ctypes.c_wchar_p, 'szItemID'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(IEnumString)), 'ppIEnumString')),
    ]


class IOPCAsyncIO2(IUnknown):
    """异步 I/O 2.0。注意 vtable 顺序：Read/Write/Refresh2/Cancel2/SetEnable/GetEnable。"""

    _iid_ = IID_IOPCAsyncIO2
    _methods_ = [
        COMMETHOD([], HRESULT, 'Read',
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServer'),
                  (['in'], DWORD, 'dwTransactionID'),
                  (['in'], ctypes.POINTER(DWORD), 'pdwCancelID'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'Write',
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phServer'),
                  (['in'], ctypes.POINTER(VARIANT), 'pItemValues'),
                  (['in'], DWORD, 'dwTransactionID'),
                  (['in'], ctypes.POINTER(DWORD), 'pdwCancelID'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'Refresh2',
                  (['in'], DWORD, 'dwSource'),
                  (['in'], DWORD, 'dwTransactionID'),
                  (['in'], ctypes.POINTER(DWORD), 'pdwCancelID')),
        COMMETHOD([], HRESULT, 'Cancel2', (['in'], DWORD, 'dwCancelID')),
        COMMETHOD([], HRESULT, 'SetEnable', (['in'], BOOL, 'bEnable')),
        COMMETHOD([], HRESULT, 'GetEnable', (['in'], ctypes.POINTER(BOOL), 'pbEnable')),
    ]


class IOPCDataCallback(IUnknown):
    """订阅回调接口（客户端实现，服务器调用）。"""

    _iid_ = IID_IOPCDataCallback
    _methods_ = [
        COMMETHOD([], HRESULT, 'OnDataChange',
                  (['in'], DWORD, 'dwTransid'),
                  (['in'], OPCHANDLE, 'hGroup'),
                  (['in'], HRESULT, 'hrMasterquality'),
                  (['in'], HRESULT, 'hrMastererror'),
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phClientItems'),
                  (['in'], ctypes.POINTER(VARIANT), 'pvValues'),
                  (['in'], ctypes.POINTER(WORD), 'pwQualities'),
                  (['in'], ctypes.POINTER(FILETIME), 'pftTimeStamps'),
                  (['in'], ctypes.POINTER(HRESULT), 'pErrors')),
        COMMETHOD([], HRESULT, 'OnReadComplete',
                  (['in'], DWORD, 'dwTransid'),
                  (['in'], OPCHANDLE, 'hGroup'),
                  (['in'], HRESULT, 'hrMasterquality'),
                  (['in'], HRESULT, 'hrMastererror'),
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'phClientItems'),
                  (['in'], ctypes.POINTER(VARIANT), 'pvValues'),
                  (['in'], ctypes.POINTER(WORD), 'pwQualities'),
                  (['in'], ctypes.POINTER(FILETIME), 'pftTimeStamps'),
                  (['in'], ctypes.POINTER(HRESULT), 'pErrors')),
        COMMETHOD([], HRESULT, 'OnWriteComplete',
                  (['in'], DWORD, 'dwTransid'),
                  (['in'], OPCHANDLE, 'hGroup'),
                  (['in'], HRESULT, 'hrMasterErr'),
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(OPCHANDLE), 'pClienthandles'),
                  (['in'], ctypes.POINTER(HRESULT), 'pErrors')),
        COMMETHOD([], HRESULT, 'OnCancelComplete',
                  (['in'], DWORD, 'dwTransid'),
                  (['in'], OPCHANDLE, 'hGroup')),
    ]


class IOPCItemProperties(IUnknown):
    """点位属性接口（可选，本客户端暂不依赖，保留定义便于后续扩展）。"""

    _iid_ = IID_IOPCItemProperties
    _methods_ = [
        COMMETHOD([], HRESULT, 'QueryAvailableProperties',
                  (['in'], ctypes.c_wchar_p, 'szItemID'),
                  (['in'], ctypes.POINTER(DWORD), 'pdwCount'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(DWORD)), 'ppPropertyIDs'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(ctypes.c_wchar_p)), 'ppDescriptions'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(VARTYPE)), 'ppvtDataTypes')),
        COMMETHOD([], HRESULT, 'GetItemProperties',
                  (['in'], ctypes.c_wchar_p, 'szItemID'),
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(DWORD), 'pdwPropertyIDs'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(VARIANT)), 'ppvData'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
        COMMETHOD([], HRESULT, 'LookupItemIDs',
                  (['in'], ctypes.c_wchar_p, 'szItemID'),
                  (['in'], DWORD, 'dwCount'),
                  (['in'], ctypes.POINTER(DWORD), 'pdwPropertyIDs'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(ctypes.c_wchar_p)), 'ppszNewItemIDs'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(HRESULT)), 'ppErrors')),
    ]


class IEnumCLSID(IUnknown):
    """CLSID 枚举器（OPC 规范名 IEnumCLSID，IID 即标准 IEnumGUID）。

    IOPCServerList.EnumClassesOfCategories 的出参。出参沿用本模块既有写法：
    ['in'] + 调用方 byref(指针)，由调用方显式释放/持有，避免 ctypes 自动转换
    丢掉 CLSID 数组的指针。
    """

    _iid_ = IID_IEnumCLSID
    _methods_ = [
        COMMETHOD([], HRESULT, 'Next',
                  (['in'], ctypes.c_ulong, 'celt'),
                  (['in'], ctypes.POINTER(GUID), 'rgelt'),
                  (['in'], ctypes.POINTER(ctypes.c_ulong), 'pceltFetched')),
        COMMETHOD([], HRESULT, 'Skip', (['in'], ctypes.c_ulong, 'celt')),
        COMMETHOD([], HRESULT, 'Reset'),
        COMMETHOD([], HRESULT, 'Clone',
                  (['in'], ctypes.POINTER(ctypes.POINTER(IUnknown)), 'ppenum')),
    ]


class IOPCServerList(IUnknown):
    """OPC Server List（OPCEnum / opc_comn）接口：按组件类别枚举已注册的 DA 服务器。

    vtable 顺序与参数已用本机 OPC Core Components 的 OpcEnum.exe 类型库核对：
    EnumClassesOfCategories / GetClassDetails / CLSIDFromProgID，IID 与
    OpcServerList coclass（CLSID {13486D51-4821-11D2-A494-3CB306C10000}）一致。
    LPWSTR 出参用 c_wchar_p 指针接收（与 GetItemID 相同的写法），调用方负责
    CoTaskMemFree 释放，避免服务器分配的内存泄漏。

    注意：OpcEnum 未安装、远程 DCOM 未放行，或进程位数与注册表视图不匹配
    （例如 64 位进程只有 32 位 OPC.ServerList 注册）时创建会失败并抛异常，
    由 client.list_servers 捕获后快速回退注册表扫描，不会卡住 UI。
    """

    _iid_ = IID_IOPCServerList
    _methods_ = [
        COMMETHOD([], HRESULT, 'EnumClassesOfCategories',
                  (['in'], ctypes.c_ulong, 'cImplemented'),
                  (['in'], ctypes.POINTER(GUID), 'rgcatidImpl'),
                  (['in'], ctypes.c_ulong, 'cRequired'),
                  (['in'], ctypes.POINTER(GUID), 'rgcatidReq'),
                  (['in'], ctypes.POINTER(ctypes.POINTER(IEnumCLSID)), 'ppenumClsid')),
        COMMETHOD([], HRESULT, 'GetClassDetails',
                  (['in'], ctypes.POINTER(GUID), 'clsid'),
                  (['in'], ctypes.POINTER(ctypes.c_wchar_p), 'ppszProgID'),
                  (['in'], ctypes.POINTER(ctypes.c_wchar_p), 'ppszUserType')),
        COMMETHOD([], HRESULT, 'CLSIDFromProgID',
                  (['in'], ctypes.c_wchar_p, 'szProgID'),
                  (['in'], ctypes.POINTER(GUID), 'clsid')),
    ]


# --------------------------------------------------------------------------------------
# 内存管理
# --------------------------------------------------------------------------------------

try:  # pragma: no cover - 非 Windows 环境仅用于导入/单测，不执行 COM
    _ole32 = ctypes.WinDLL('ole32')
    _ole32.CoTaskMemFree.argtypes = [ctypes.c_void_p]
    _ole32.CoTaskMemFree.restype = None
except (AttributeError, OSError):  # pragma: no cover
    _ole32 = None

try:  # pragma: no cover
    _oleaut32 = ctypes.WinDLL('oleaut32')
    _oleaut32.VariantClear.argtypes = [ctypes.POINTER(VARIANT)]
    _oleaut32.VariantClear.restype = ctypes.c_long
except (AttributeError, OSError):  # pragma: no cover
    _oleaut32 = None


def free_cotaskmem(ptr):
    """释放服务器通过 CoTaskMemAlloc 分配的指针（NULL/None 安全，绝不抛异常）。"""
    if ptr is None or _ole32 is None:
        return
    try:
        addr = ptr if isinstance(ptr, int) else ctypes.cast(ptr, ctypes.c_void_p).value
        if addr:
            _ole32.CoTaskMemFree(addr)
    except Exception:  # pragma: no cover - 释放失败不影响主流程
        pass


def free_bstr(ptr):
    """释放服务器分配的输出字符串指针（LPWSTR* 指向的 BSTR）。"""
    free_cotaskmem(ptr)


def variant_clear(var):
    """对 VARIANT 调用 VariantClear，释放其持有的 BSTR/SAFEARRAY 等资源。"""
    if var is None or _oleaut32 is None:
        return
    try:
        _oleaut32.VariantClear(ctypes.byref(var))
    except Exception:  # pragma: no cover
        pass


def get_status_vendor_pointer(status_struct):
    """取 OPCSERVERSTATUS.szVendorInfo 的原始指针。

    ctypes 会把 c_wchar_p 字段自动转换成 Python str，丢失指针，因此按字段偏移
    重新读取原始指针，供 CoTaskMemFree 释放服务器分配的厂商字符串。
    """
    try:
        offset = OPCSERVERSTATUS.szVendorInfo.offset
        return ctypes.cast(ctypes.byref(status_struct, offset),
                           ctypes.POINTER(ctypes.c_void_p))[0]
    except Exception:  # pragma: no cover
        return None


# --------------------------------------------------------------------------------------
# HRESULT 处理
# --------------------------------------------------------------------------------------


def as_unsigned(code):
    """把任意符号形式的 HRESULT 归一为无符号 32 位整数。"""
    if code is None:
        return None
    return int(code) & 0xFFFFFFFF


def as_signed(code):
    """把 HRESULT 转成有符号 32 位整数（comtypes/ctypes 内部使用的形式）。"""
    value = as_unsigned(code)
    if value is None:
        return None
    return value - 0x100000000 if value >= 0x80000000 else value


def hresult_hex(code):
    """格式化为 0xRRRRRRRR（None 显示为 0x00000000）。"""
    return '0x%08X' % (as_unsigned(code) or 0)


def succeeded(code):
    """HRESULT 是否成功（S_OK 与 S_FALSE 都算成功）。"""
    if code is None:
        return True
    return as_signed(code) >= 0


# --------------------------------------------------------------------------------------
# 错误分类：区分"点位级业务错误"与"链路/传输故障"
#
# 现场教训：AddItems/Read 的异常文本里含"点位"二字，旧实现用关键字判断"部分点位
# 无效"，会把 RPC/DCOM 链路崩溃也当成坏点忽略，随后在已断开的 COM 对象上把状态
# 推到 connected（假连接）。必须按 HRESULT 码严格区分。
# --------------------------------------------------------------------------------------

# 只影响单个 ItemID 的业务错误（坏点/权限/类型/范围），不代表链路已死
_ITEM_LEVEL_ERRORS = frozenset((
    OPC_E_INVALIDHANDLE, OPC_E_BADTYPE, OPC_E_BADRIGHTS, OPC_E_UNKNOWNITEMID,
    OPC_E_INVALIDITEMID, OPC_E_UNKNOWNPATH, OPC_E_RANGE, OPC_E_DUPLICATENAME,
    OPC_E_NOTFOUND, OPC_E_INVALIDDATATYPE, OPC_E_WRITEONLY,
))

# 服务器不支持异步订阅接口/回调连接点时允许降级为轮询（不是链路故障）
_ASYNC_UNSUPPORTED_ERRORS = frozenset((
    E_NOINTERFACE, E_NOTIMPL, E_POINTER,
    CONNECT_E_ADVISELIMIT, CONNECT_E_NOCONNECTION, CONNECT_E_CANNOTCONNECT,
))


def is_item_level_error(code):
    """该 HRESULT 是否为点位级业务错误（只影响单个点位，不触发整链重连）。"""
    unsigned = as_unsigned(code)
    return unsigned in _ITEM_LEVEL_ERRORS


def is_async_unsupported_error(code):
    """该 HRESULT 是否表示服务器不支持 IOPCAsyncIO2（可安全降级为轮询）。"""
    unsigned = as_unsigned(code)
    return unsigned in _ASYNC_UNSUPPORTED_ERRORS


# 写操作的"点位级"拒绝：除 OPC 点位错误外，服务器还可能用 E_ACCESSDENIED /
# E_INVALIDARG 表示"该点位不可写/值不合法"，同样不应触发整链重连。
_WRITE_ITEM_LEVEL_ERRORS = _ITEM_LEVEL_ERRORS | frozenset((E_ACCESSDENIED, E_INVALIDARG))


def is_item_level_write_error(code):
    """写结果数组里的 HRESULT 是否属于点位级拒绝（只读/类型/坏点）。"""
    unsigned = as_unsigned(code)
    return unsigned in _WRITE_ITEM_LEVEL_ERRORS


def is_link_level_error(code):
    """该 HRESULT 是否属于链路/传输故障（需要上抛并触发重连）。

    保守策略：任何失败 HRESULT 只要不是明确的点位级业务错误，都按链路故障处理；
    code 为 None（拿不到 HRESULT，例如 RPC 层直接抛的 OSError）同样按链路故障处理。
    """
    if code is None:
        return True
    unsigned = as_unsigned(code)
    if unsigned is None:
        return True
    if is_item_level_error(unsigned):
        return False
    return as_signed(unsigned) < 0


_HRESULT_TEXT = {
    S_OK: '成功',
    S_FALSE: '成功但无数据（S_FALSE）',
    E_NOTIMPL: '未实现（E_NOTIMPL）',
    E_NOINTERFACE: '服务器不支持该接口（E_NOINTERFACE）',
    E_POINTER: '无效指针（E_POINTER）',
    E_FAIL: '未指定的错误（E_FAIL）',
    E_UNEXPECTED: '意外失败（E_UNEXPECTED）',
    E_INVALIDARG: '参数无效（E_INVALIDARG）',
    E_ACCESSDENIED: '拒绝访问（E_ACCESSDENIED）：请检查 DCOM 权限',
    REGDB_E_CLASSNOTREG: 'ProgID 未注册或服务器未安装（REGDB_E_CLASSNOTREG）',
    CLASS_E_CLASSNOTAVAILABLE: '服务器类不可用（CLASS_E_CLASSNOTAVAILABLE）',
    CO_E_CLASSSTRING: 'ProgID 字符串无效（CO_E_CLASSSTRING）',
    CO_E_NOTINITIALIZED: 'COM 未初始化（CO_E_NOTINITIALIZED）',
    CO_E_OBJNOTCONNECTED: 'COM 对象已断开（CO_E_OBJNOTCONNECTED）',
    RPC_E_CHANGED_MODE: 'COM 套间模型冲突（RPC_E_CHANGED_MODE）：请使用 STA 线程',
    RPC_E_DISCONNECTED: '与服务器的连接已断开（RPC_E_DISCONNECTED）',
    RPC_E_WRONG_THREAD: '跨线程调用 COM 对象（RPC_E_WRONG_THREAD）',
    RPC_E_TOO_LATE: 'COM 安全初始化过晚（RPC_E_TOO_LATE）',
    RPC_S_SERVER_UNAVAILABLE: 'RPC 服务器不可用（RPC_S_SERVER_UNAVAILABLE）：主机/服务器未启动',
    RPC_S_CALL_FAILED: 'RPC 调用失败（RPC_S_CALL_FAILED）：连接被中断',
    RPC_S_CALL_FAILED_DNE: 'RPC 调用失败且未执行（RPC_S_CALL_FAILED_DNE）',
    RPC_X_NULL_REF_POINTER: '传入了空引用指针（RPC_X_NULL_REF_POINTER）',
    OPC_E_INVALIDHANDLE: '无效的点位句柄（OPC_E_INVALIDHANDLE）',
    OPC_E_BADTYPE: '数据类型不匹配（OPC_E_BADTYPE）',
    OPC_E_PUBLIC: '公共组上不允许该操作（OPC_E_PUBLIC）',
    OPC_E_BADRIGHTS: '点位访问权限不允许（OPC_E_BADRIGHTS）',
    OPC_E_UNKNOWNITEMID: '未知的 ItemID（OPC_E_UNKNOWNITEMID）',
    OPC_E_INVALIDITEMID: 'ItemID 格式非法（OPC_E_INVALIDITEMID）',
    OPC_E_INVALIDFILTER: '浏览过滤器非法（OPC_E_INVALIDFILTER）',
    OPC_E_UNKNOWNPATH: '未知的分支路径（OPC_E_UNKNOWNPATH）',
    OPC_E_RANGE: '数值越界（OPC_E_RANGE）',
    OPC_E_DUPLICATENAME: '名称重复（OPC_E_DUPLICATENAME）',
    OPC_S_UNKNOWN_LCID: '未知的 LocaleID（OPC_S_UNKNOWN_LCID）',
    OPC_E_UNKNOWNLCID: '未知的 LocaleID（OPC_E_UNKNOWNLCID）',
    OPC_E_INVALIDCONFIGFILE: '配置文件非法（OPC_E_INVALIDCONFIGFILE）',
    OPC_E_NOTFOUND: '未找到（OPC_E_NOTFOUND）',
    OPC_E_INVALID_PICTURE: '无效图片（OPC_E_INVALID_PICTURE）',
    OPC_E_INVALIDDATATYPE: '无效的 VARTYPE（OPC_E_INVALIDDATATYPE）',
    OPC_E_WRITEONLY: '点位只写，不能读取（OPC_E_WRITEONLY）',
}

try:  # pragma: no cover
    _kernel32 = ctypes.WinDLL('kernel32')
    _kernel32.FormatMessageW.argtypes = [
        DWORD, ctypes.c_void_p, DWORD, DWORD, ctypes.c_wchar_p, DWORD, ctypes.c_void_p]
    _kernel32.FormatMessageW.restype = DWORD
except (AttributeError, OSError):  # pragma: no cover
    _kernel32 = None


def _format_win32_message(win32_code):
    if _kernel32 is None:
        return ''
    try:
        buf = ctypes.create_unicode_buffer(512)
        flags = 0x00001000 | 0x00000200  # FORMAT_MESSAGE_FROM_SYSTEM | IGNORE_INSERTS
        count = _kernel32.FormatMessageW(flags, None, int(win32_code) & 0xFFFF,
                                         0, buf, len(buf), None)
        if count:
            return buf.value.strip().replace('\r', ' ').replace('\n', ' ')
    except Exception:  # pragma: no cover
        pass
    return ''


def hresult_text(code):
    """把 HRESULT 转成中文可读文本（未知码回退为十六进制 + 系统消息）。"""
    unsigned = as_unsigned(code)
    if unsigned is None:
        return '未知错误（无 HRESULT）'
    text = _HRESULT_TEXT.get(unsigned)
    if text:
        return text
    if (unsigned & 0xFFFF0000) == 0x80070000:
        win32 = unsigned & 0xFFFF
        sys_text = _format_win32_message(win32)
        if sys_text:
            return '系统错误 %d（0x%04X）：%s' % (win32, win32, sys_text)
        return '系统错误 %d（0x%04X）' % (win32, win32)
    return '未知 HRESULT %s' % hresult_hex(unsigned)


def format_hresult(code):
    """返回 0x80040154 ProgID 未注册... 形式，用于日志/异常消息。"""
    return '%s %s' % (hresult_hex(code), hresult_text(code))


# --------------------------------------------------------------------------------------
# VARTYPE / VARIANT 转换
# --------------------------------------------------------------------------------------

_VT_NAMES = {
    VT_EMPTY: 'Unknown',
    VT_NULL: 'Null',
    VT_I2: 'Int16',
    VT_I4: 'Int32',
    VT_R4: 'Float',
    VT_R8: 'Double',
    VT_CY: 'Currency',
    VT_DATE: 'DateTime',
    VT_BSTR: 'String',
    VT_BOOL: 'Boolean',
    VT_I1: 'Int8',
    VT_UI1: 'UInt8',
    VT_UI2: 'UInt16',
    VT_UI4: 'UInt32',
    VT_I8: 'Int64',
    VT_UI8: 'UInt64',
    VT_INT: 'Int32',
    VT_UINT: 'UInt32',
}

# 整数 VARTYPE 的取值区间，写值前校验，避免 ctypes 静默截断写坏点位
_INT_RANGES = {
    VT_I1: (-128, 127),
    VT_I2: (-32768, 32767),
    VT_I4: (-2147483648, 2147483647),
    VT_I8: (-9223372036854775808, 9223372036854775807),
    VT_INT: (-2147483648, 2147483647),
    VT_UI1: (0, 255),
    VT_UI2: (0, 65535),
    VT_UI4: (0, 4294967295),
    VT_UI8: (0, 18446744073709551615),
    VT_UINT: (0, 4294967295),
}

_VT_UNION_FIELDS = {
    VT_I1: 'VT_I1', VT_I2: 'VT_I2', VT_I4: 'VT_I4', VT_I8: 'VT_I8',
    VT_INT: 'VT_INT', VT_UI1: 'VT_UI1', VT_UI2: 'VT_UI2', VT_UI4: 'VT_UI4',
    VT_UI8: 'VT_UI8', VT_UINT: 'VT_UINT', VT_R4: 'VT_R4', VT_R8: 'VT_R8',
}

_VT_TYPEMASK = 0x0FFF


def vtype_base(vt):
    """去掉 VT_ARRAY / VT_BYREF 标志后的基础 VARENUM。"""
    if vt is None:
        return 0
    return int(vt) & _VT_TYPEMASK


def vtype_to_type_name(vt):
    """VARENUM → 类型名（Boolean/Int16/Int32/UInt16/UInt32/Int64/UInt64/Float/Double/String/Unknown）。"""
    if vt is None:
        return 'Unknown'
    vt = int(vt)
    base = vtype_base(vt)
    name = _VT_NAMES.get(base, 'Unknown')
    if vt & VT_ARRAY:
        return 'Array[%s]' % name
    if vt & VT_BYREF and name != 'Unknown':
        return '%s&' % name
    return name


def quality_text(quality):
    """OPC 质量码 → 简短文本（Good/Uncertain/Bad + 子状态）。"""
    if quality is None:
        return 'Bad'
    q = int(quality) & 0xFFFF
    detail = {
        OPC_QUALITY_BAD: 'Bad',
        OPC_QUALITY_CONFIG_ERROR: 'Bad/ConfigError',
        OPC_QUALITY_NOT_CONNECTED: 'Bad/NotConnected',
        OPC_QUALITY_DEVICE_FAILURE: 'Bad/DeviceFailure',
        OPC_QUALITY_SENSOR_FAILURE: 'Bad/SensorFailure',
        OPC_QUALITY_LAST_KNOWN: 'Bad/LastKnown',
        OPC_QUALITY_COMM_FAILURE: 'Bad/CommFailure',
        OPC_QUALITY_OUT_OF_SERVICE: 'Bad/OutOfService',
        OPC_QUALITY_WAITING_FOR_INITIAL_DATA: 'Bad/WaitingForInitialData',
        OPC_QUALITY_UNCERTAIN: 'Uncertain',
        OPC_QUALITY_LAST_USABLE: 'Uncertain/LastUsable',
        OPC_QUALITY_SENSOR_CAL: 'Uncertain/SensorCal',
        OPC_QUALITY_EGU_EXCEEDED: 'Uncertain/EGUExceeded',
        OPC_QUALITY_SUB_NORMAL: 'Uncertain/SubNormal',
        OPC_QUALITY_GOOD: 'Good',
        OPC_QUALITY_LOCAL_OVERRIDE: 'Good/LocalOverride',
    }
    if q in detail:
        return detail[q]
    base = q & OPC_QUALITY_MASK
    if base == OPC_QUALITY_GOOD:
        return 'Good'
    if base == OPC_QUALITY_UNCERTAIN:
        return 'Uncertain'
    return 'Bad'


def is_quality_good(quality):
    """质量是否 Good（0xC0 位段）。"""
    if quality is None:
        return False
    return (int(quality) & OPC_QUALITY_MASK) == OPC_QUALITY_GOOD


_SERVER_STATE_TEXT = {
    OPC_STATUS_RUNNING: '运行中',
    OPC_STATUS_FAILED: '故障',
    OPC_STATUS_NOCONFIG: '无配置',
    OPC_STATUS_SUSPENDED: '已暂停',
    OPC_STATUS_TEST: '测试模式',
    OPC_STATUS_COMM_FAULT: '通信故障',
}

SERVER_STATE_TEXT = _SERVER_STATE_TEXT


def server_state_text(state):
    """OPC 服务器状态码 → 中文文本。"""
    if state is None:
        return '未知'
    raw = int(state)
    text = _SERVER_STATE_TEXT.get(raw)
    if text is None:
        # 个别服务器会在状态值里带厂商私有位；再按 OPC_STATUS_MASK 掩码兜底
        text = _SERVER_STATE_TEXT.get(raw & OPC_STATUS_MASK)
    return text or ('未知状态 %d' % raw)


# --------------------------------------------------------------------------------------
# FILETIME → 本地时间字符串
# --------------------------------------------------------------------------------------

_FILETIME_EPOCH = datetime.datetime(1601, 1, 1, tzinfo=datetime.timezone.utc)


def filetime_to_datetime(ft):
    """FILETIME → 带 UTC 时区的 datetime；0/非法值返回 None。"""
    if ft is None:
        return None
    try:
        low = int(getattr(ft, 'dwLowDateTime', 0))
        high = int(getattr(ft, 'dwHighDateTime', 0))
    except (TypeError, ValueError):
        return None
    value = (high << 32) | low
    if value <= 0:
        return None
    try:
        return _FILETIME_EPOCH + datetime.timedelta(microseconds=value // 10)
    except (OverflowError, ValueError):
        return None


def filetime_to_local_string(ft):
    """FILETIME → 本地时间 "%Y-%m-%d %H:%M:%S"；0/非法值返回 None。"""
    dt = filetime_to_datetime(ft)
    if dt is None:
        return None
    try:
        local = dt.astimezone()
    except Exception:  # pragma: no cover
        local = dt
    return local.strftime('%Y-%m-%d %H:%M:%S')


# --------------------------------------------------------------------------------------
# VARIANT ↔ Python
# --------------------------------------------------------------------------------------


def _normalize_scalar(value):
    """把 comtypes 可能返回的 Decimal/datetime/ctypes 值归一为 UI 友好的 Python 值。"""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, decimal.Decimal):
        try:
            return float(value)
        except Exception:  # pragma: no cover
            return str(value)
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    try:
        return value.item()  # numpy / ctypes 标量
    except Exception:
        return value


def variant_to_python(var):
    """COM VARIANT → Python 值（bool/int/float/str/None；数组转为 list）。"""
    if var is None:
        return None
    try:
        vt = int(var.vt)
    except Exception:  # pragma: no cover
        return None
    base = vtype_base(vt)
    try:
        value = var.value
    except Exception:  # pragma: no cover - 极少数服务器返回未支持的 VARIANT
        return None
    if value is None:
        return None
    if base == VT_BOOL:
        try:
            return bool(value)
        except Exception:  # pragma: no cover
            return None
    if vt & VT_ARRAY:
        try:
            return [_normalize_scalar(item) for item in value]
        except TypeError:  # pragma: no cover
            return _normalize_scalar(value)
    if isinstance(value, (tuple, list)):
        return [_normalize_scalar(item) for item in value]
    return _normalize_scalar(value)


def _to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ('1', 'true', 'yes', 'on'):
            return True
        if text in ('0', 'false', 'no', 'off', ''):
            return False
        raise ValueError('无法把 %r 转换为 Boolean' % (value,))
    raise ValueError('无法把 %r 转换为 Boolean' % (value,))


def _to_int(value, vt):
    if isinstance(value, bool):
        number = int(value)
    elif isinstance(value, int):
        number = value
    elif isinstance(value, float):
        number = int(value)
    elif isinstance(value, str):
        number = int(float(value.strip()))
    else:
        raise ValueError('无法把 %r 转换为整数' % (value,))
    low, high = _INT_RANGES[vt]
    if number < low or number > high:
        raise ValueError('数值 %r 超出 %s 范围 [%d, %d]' % (
            value, vtype_to_type_name(vt), low, high))
    return number


def _to_float(value):
    if isinstance(value, (int, float, bool)):
        return float(value)
    if isinstance(value, str):
        return float(value.strip())
    raise ValueError('无法把 %r 转换为浮点数' % (value,))


def _to_text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode('utf-8', 'replace')
    return str(value)


def _to_datetime(value):
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, str):
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d'):
            try:
                return datetime.datetime.strptime(value.strip(), fmt)
            except ValueError:
                continue
    raise ValueError('无法把 %r 转换为日期时间' % (value,))


def python_to_variant(value, canonical_vt=0):
    """Python 值 → VARIANT（按 canonical_vt 强制类型；未知类型按 Python 值自动推断）。

    返回的 VARIANT 若带有 VT_BSTR 等内容，调用方在 COM 调用结束后必须调用
    variant_clear 释放，避免 BSTR 泄漏。
    """
    vt = int(canonical_vt or 0)
    if vt & VT_ARRAY:
        # 数组写值暂不支持：comtypes 构造 SAFEARRAY(VARIANT) 在 64 位进程里可能
        # 触发访问违例（实测写 VT_ARRAY|VT_BSTR 点位会崩），这里显式拒绝，
        # 由 write_items 作为单项错误上报，避免把整个进程带崩。
        raise ValueError('暂不支持向数组类型点位写值（canonical_vt=0x%04X）' % vt)
    base = vt if vt else VT_EMPTY
    if base in (VT_EMPTY, VT_NULL):
        return VARIANT(value)
    var = VARIANT()
    if base == VT_BOOL:
        var.vt = VT_BOOL
        var._.VT_BOOL = -1 if _to_bool(value) else 0
        return var
    if base in _VT_UNION_FIELDS:
        field = _VT_UNION_FIELDS[base]
        var.vt = base
        if base in (VT_R4, VT_R8):
            setattr(var._, field, _to_float(value))
        else:
            setattr(var._, field, _to_int(value, base))
        return var
    if base == VT_BSTR:
        var.vt = VT_BSTR
        var._.bstrVal = _to_text(value)
        return var
    if base == VT_DATE:
        return VARIANT(_to_datetime(value))
    if base == VT_CY:
        try:
            return VARIANT(decimal.Decimal(str(value)))
        except Exception:
            raise ValueError('无法把 %r 转换为 Currency' % (value,))
    return VARIANT(value)


# --------------------------------------------------------------------------------------
# ItemID / 路径拆分
# --------------------------------------------------------------------------------------


def split_item_id(item_id):
    """把完整 ItemID 拆成 (path, leaf_name)。

    OPC 服务器的 ItemID 层次分隔符通常是点号，也有服务器用斜杠。取最后一个分隔符
    切分，path 中的分隔符统一为斜杠（供 UI 树分组使用）。根级点位返回 ("", item_id)。
    """
    item_id = '' if item_id is None else str(item_id)
    index = max(item_id.rfind('.'), item_id.rfind('/'), item_id.rfind('\\'))
    if index <= 0 or index >= len(item_id) - 1:
        return '', item_id
    leaf = item_id[index + 1:]
    path = item_id[:index].replace('\\', '/').replace('.', '/')
    parts = [part for part in path.split('/') if part]
    return '/'.join(parts), leaf


# --------------------------------------------------------------------------------------
# 本机 OPC DA 服务器枚举（注册表 Component Categories 扫描）
# --------------------------------------------------------------------------------------

try:  # pragma: no cover - 仅 Windows 有意义
    import winreg
except ImportError:  # pragma: no cover
    winreg = None


def _scan_registry_view(root_key, view_flag):
    r"""扫描 HKCR\CLSID\{...}\Implemented Categories 中实现 DA 类别的类。"""
    progids = []
    access = winreg.KEY_READ | view_flag
    try:
        root = winreg.OpenKey(root_key, 'CLSID', 0, access)
    except OSError:
        return progids
    try:
        index = 0
        while True:
            try:
                clsid = winreg.EnumKey(root, index)
            except OSError:
                break
            index += 1
            if not clsid.startswith('{'):
                continue
            try:
                categories_key = winreg.OpenKey(root, clsid + '\\Implemented Categories',
                                               0, access)
            except OSError:
                continue
            matched = False
            try:
                cat_index = 0
                while True:
                    try:
                        category = winreg.EnumKey(categories_key, cat_index)
                    except OSError:
                        break
                    cat_index += 1
                    if category.upper() in DA_CATEGORY_IDS:
                        matched = True
                        break
            finally:
                winreg.CloseKey(categories_key)
            if not matched:
                continue
            for sub_key in ('ProgID', 'VersionIndependentProgID'):
                try:
                    key = winreg.OpenKey(root, clsid + '\\' + sub_key, 0, access)
                except OSError:
                    continue
                try:
                    value = winreg.QueryValueEx(key, '')[0]
                except OSError:
                    value = None
                finally:
                    winreg.CloseKey(key)
                if value:
                    progids.append(str(value))
                    break
    finally:
        winreg.CloseKey(root)
    return progids


def list_local_da_servers():
    """本机已注册的 OPC DA 服务器 ProgID 列表（64/32 位注册表视图都扫）。

    任何失败都返回已扫描到的结果（不抛异常）。不依赖 OPC Core Components /
    OPC.Automation，只读注册表，因此打包后也能工作。
    """
    if winreg is None:
        return []
    found = set()
    for view_flag in (0, getattr(winreg, 'KEY_WOW64_64KEY', 0),
                      getattr(winreg, 'KEY_WOW64_32KEY', 0)):
        try:
            for progid in _scan_registry_view(winreg.HKEY_CLASSES_ROOT, view_flag):
                found.add(progid)
        except Exception:  # pragma: no cover - 注册表异常绝不外抛
            continue
    return sorted(found)


# --------------------------------------------------------------------------------------
# OPC 服务器枚举（OPC Server List / OPCEnum，本机与远程统一）
# --------------------------------------------------------------------------------------

# 明确视为"本机"的主机写法（client 已判定为本机时会传 None；这里兼容直接调用）
_LOCAL_SERVERLIST_HOSTS = frozenset(
    ('', '.', 'localhost', '127.0.0.1', '::1', '[::1]', '0.0.0.0'))


def _is_local_serverlist_host(host):
    """仅用于选择本机/远程的 COM 创建方式（不做 DNS 解析，避免枚举路径阻塞）。"""
    if host is None:
        return True
    return str(host).strip().lower() in _LOCAL_SERVERLIST_HOSTS


def _create_opc_server_list(host=None):
    """创建 OPC Server List 对象并 QI IOPCServerList。

    本机 clsctx=None；远程 clsctx=CLSCTX_REMOTE_SERVER + machine=host，
    与 client.connect 里创建 OPC 服务器对象的方式一致（含 DCOM 安全初始化）。
    """
    local = _is_local_serverlist_host(host)
    machine = None if local else str(host).strip()
    clsctx = None if local else CLSCTX_REMOTE_SERVER
    if not local:
        # 远程 DCOM：尽力把客户端默认安全级别设为 Connect+Impersonate
        try_init_dcom_security()
    unknown = comtypes.client.CreateObject(
        CLSID_OPC_SERVER_LIST, clsctx=clsctx, machine=machine, interface=IUnknown)
    try:
        return unknown.QueryInterface(IOPCServerList)
    finally:
        del unknown  # 释放 CoCreateInstance 得到的初始接口，只保留 QI 后的引用


def _server_list_class_progid(server_list, clsid):
    """用 GetClassDetails 取单个 CLSID 的 ProgID（并释放服务器分配的字符串）。"""
    progid_ptr = ctypes.c_wchar_p()
    usertype_ptr = ctypes.c_wchar_p()
    try:
        server_list.GetClassDetails(ctypes.byref(clsid),
                                    ctypes.byref(progid_ptr),
                                    ctypes.byref(usertype_ptr))
        return progid_ptr.value or ''
    finally:
        if progid_ptr.value:
            free_cotaskmem(ctypes.cast(progid_ptr, ctypes.c_void_p))
        if usertype_ptr.value:
            free_cotaskmem(ctypes.cast(usertype_ptr, ctypes.c_void_p))


def _enum_da_servers_from_server_list(server_list):
    """EnumClassesOfCategories(DA 类别) → IEnumCLSID.Next → GetClassDetails(ProgID)。

    过滤空 ProgID 并按去重后的列表排序返回。枚举过程中枚举器失效时保留已取得的
    结果（不抛异常），其余的 COM 错误（创建/首个调用）向上抛给调用方兜底。
    """
    category_array = (GUID * len(DA_CATEGORY_IDS))(
        *[GUID(category) for category in DA_CATEGORY_IDS])
    enum_ptr = ctypes.POINTER(IEnumCLSID)()
    server_list.EnumClassesOfCategories(
        len(DA_CATEGORY_IDS), category_array, 0, None, ctypes.byref(enum_ptr))
    if not enum_ptr:
        return []
    found = set()
    fetched = ctypes.c_ulong()
    try:
        while True:
            clsid = GUID()
            fetched.value = 0
            try:
                enum_ptr.Next(1, ctypes.byref(clsid), ctypes.byref(fetched))
            except Exception:  # pragma: no cover - 枚举器提前失效：保留已有结果
                break
            if int(fetched.value) <= 0:
                break
            progid = _server_list_class_progid(server_list, clsid)
            if progid:
                found.add(progid)
    finally:
        del enum_ptr  # 先释放枚举器，再由调用方决定是否 CoUninitialize
    return sorted(found)


def list_da_servers(host=None):
    """通过 OPC Server List（OPCEnum）枚举 OPC DA 服务器 ProgID。

    :param host: None/空/localhost 表示本机；其它值按远程 DCOM 处理。
    :return: 去重排序后的 ProgID 列表；COM 不可用（如 64 位进程缺少 32 位
        OPC.ServerList 注册）时抛异常，由上层 client.list_servers 回退到注册表扫描。
    """
    initialized_here = False
    server_list = None
    try:
        if not com_initialized():
            ensure_com_initialized()
            initialized_here = True
        server_list = _create_opc_server_list(host)
        return _enum_da_servers_from_server_list(server_list)
    finally:
        server_list = None  # 先释放 COM 接口，再与 CoInitializeEx 配对 CoUninitialize
        if initialized_here:
            uninitialize_com()


def list_remote_da_servers(host):
    """远程主机注册表 Component Categories 扫描（OPCEnum 不可用时的兜底）。

    winreg.ConnectRegistry(host, HKEY_CLASSES_ROOT) 后用与本地相同的
    _scan_registry_view 扫描 32/64 位视图（能读才读）；远程注册表服务未启用、
    权限不足或主机不可达时返回 []，绝不抛异常。
    """
    if winreg is None or not host:
        return []
    text = str(host).strip()
    if not text:
        return []
    remote_root = None
    try:
        remote_root = winreg.ConnectRegistry(text, winreg.HKEY_CLASSES_ROOT)
    except Exception:  # pragma: no cover - 远程注册表不可用属预期情况
        return []
    found = set()
    try:
        for view_flag in (0, getattr(winreg, 'KEY_WOW64_64KEY', 0),
                          getattr(winreg, 'KEY_WOW64_32KEY', 0)):
            try:
                for progid in _scan_registry_view(remote_root, view_flag):
                    found.add(progid)
            except Exception:  # pragma: no cover - 远程视图不支持时跳过
                continue
    finally:
        try:
            winreg.CloseKey(remote_root)
        except Exception:  # pragma: no cover
            pass
    return sorted(found)


# --------------------------------------------------------------------------------------
# 消息泵（订阅回调必须靠它派发）
# --------------------------------------------------------------------------------------

try:  # pragma: no cover
    _user32 = ctypes.WinDLL('user32', use_last_error=True)
except (AttributeError, OSError):  # pragma: no cover
    _user32 = None

PM_NOREMOVE = 0x0000
PM_REMOVE = 0x0001
WM_QUIT = 0x0012


class POINT(ctypes.Structure):
    _fields_ = [('x', LONG), ('y', LONG)]


class MSG(ctypes.Structure):
    _fields_ = [
        ('hwnd', ctypes.c_void_p),
        ('message', ctypes.c_uint),
        ('wParam', ctypes.c_size_t),
        ('lParam', ctypes.c_ssize_t),
        ('time', DWORD),
        ('pt', POINT),
        ('lPrivate', DWORD),
    ]


if _user32 is not None:  # pragma: no cover
    _user32.PeekMessageW.argtypes = [
        ctypes.POINTER(MSG), ctypes.c_void_p, ctypes.c_uint, ctypes.c_uint, ctypes.c_uint]
    _user32.PeekMessageW.restype = ctypes.c_int
    _user32.TranslateMessage.argtypes = [ctypes.POINTER(MSG)]
    _user32.TranslateMessage.restype = ctypes.c_int
    _user32.DispatchMessageW.argtypes = [ctypes.POINTER(MSG)]
    _user32.DispatchMessageW.restype = ctypes.c_ssize_t


def pump_message_events(max_messages=0):
    """派发当前线程所有已排队的 Windows 消息（非阻塞）。

    COM 订阅回调（IOPCDataCallback）由服务器通过 DCOM 送达客户端 STA 消息队列，
    只有调用 PeekMessage/DispatchMessage 才会真正执行。Qt/qasync 主线程本身会泵
    消息，无 Qt 的自动化测试/引擎心跳必须主动调用本函数。

    :param max_messages: 大于 0 时最多派发这么多条消息（防止极端情况下饿死调用方）。
    :return: 实际派发的消息条数。
    """
    if _user32 is None:
        return 0
    processed = 0
    msg = MSG()
    while _user32.PeekMessageW(ctypes.byref(msg), None, 0, 0, PM_REMOVE):
        _user32.TranslateMessage(ctypes.byref(msg))
        _user32.DispatchMessageW(ctypes.byref(msg))
        processed += 1
        if max_messages and processed >= max_messages:
            break
    return processed


# --------------------------------------------------------------------------------------
# COM 套间初始化
# --------------------------------------------------------------------------------------

# 记录当前线程是否由本模块成功调用过 CoInitializeEx；断开时必须配对 CoUninitialize。
_apartment_state = threading.local()


def ensure_com_initialized():
    """确保当前线程以 STA（Apartment Threaded）初始化 COM。

    - 已初始化（S_FALSE，例如 comtypes 导入时初始化过）：视为成功，仍记录配对的
      CoUninitialize；
    - 已以 MTA 初始化（RPC_E_CHANGED_MODE）：抛 RuntimeError，因为 OPC 订阅回调
      与 Qt 都要求 STA。
    """
    if getattr(_apartment_state, 'initialized', False):
        return
    if _ole32 is None:  # pragma: no cover
        raise RuntimeError('当前平台不支持 COM')
    result = None
    try:
        co_init_ex = ctypes.WinDLL('ole32').CoInitializeEx
        co_init_ex.argtypes = [ctypes.c_void_p, DWORD]
        co_init_ex.restype = ctypes.c_long
        result = co_init_ex(None, comtypes.COINIT_APARTMENTTHREADED)
    except Exception as exc:  # pragma: no cover
        raise RuntimeError('COM 初始化失败：%s' % (exc,))
    signed = as_signed(result)
    if signed < 0:
        if as_unsigned(signed) == RPC_E_CHANGED_MODE:
            raise RuntimeError(
                '当前线程已按 MTA 初始化 COM，OPC DA 客户端必须在 STA 线程中使用')
        raise RuntimeError('COM 初始化失败：%s' % format_hresult(result))
    _apartment_state.initialized = True


def uninitialize_com():
    """与 ensure_com_initialized 配对；未初始化时静默返回。"""
    if not getattr(_apartment_state, 'initialized', False):
        return
    _apartment_state.initialized = False
    try:
        comtypes.CoUninitialize()
    except Exception:  # pragma: no cover
        pass


def com_initialized():
    """当前线程是否已由本模块完成 COM 初始化。"""
    return bool(getattr(_apartment_state, 'initialized', False))


# 远程 DCOM 安全初始化（进程级，只在首次远程连接时尝试）。
# CoInitializeSecurity 是进程级一次性调用；UI 线程与"刷新服务器列表"工作线程可能
# 并发首次触发，必须加锁，否则两个线程同时调用会得到 RPC_E_TOO_LATE/失败。
_security_initialized = [False]
_security_lock = threading.Lock()


def try_init_dcom_security():
    """尽力设置 DCOM 客户端默认安全级别（Connect + Impersonate）。

    本机连接不需要；远程 DCOM 时如果不设置，某些服务器会拒绝（E_ACCESSDENIED）。
    进程内一旦有别的组件先设置过，会返回 RPC_E_TOO_LATE，属正常情况，忽略即可。
    """
    with _security_lock:
        if _security_initialized[0]:
            return True
        _security_initialized[0] = True
        if _ole32 is None:  # pragma: no cover
            return False
        try:
            func = _ole32.CoInitializeSecurity
            func.argtypes = [ctypes.c_void_p, ctypes.c_long, ctypes.c_void_p,
                             ctypes.c_void_p, DWORD, DWORD, ctypes.c_void_p,
                             DWORD, ctypes.c_void_p]
            func.restype = ctypes.c_long
            # RPC_C_AUTHN_LEVEL_CONNECT=2, RPC_C_IMP_LEVEL_IMPERSONATE=3, EOAC_NONE=0
            result = func(None, -1, None, None, 2, 3, None, 0, None)
            return as_signed(result) >= 0 or as_unsigned(result) == RPC_E_TOO_LATE
        except Exception:  # pragma: no cover
            return False


__all__ = [
    'OPC_BROWSE_UP', 'OPC_BROWSE_DOWN', 'OPC_BROWSE_TO',
    'OPC_BRANCH', 'OPC_LEAF', 'OPC_FLAT',
    'OPC_NS_HIERARCHICAL', 'OPC_NS_FLAT',
    'OPC_READABLE', 'OPC_WRITEABLE',
    'OPC_DS_CACHE', 'OPC_DS_DEVICE',
    'OPC_QUALITY_GOOD', 'OPC_QUALITY_BAD', 'OPC_QUALITY_UNCERTAIN', 'OPC_QUALITY_MASK',
    'OPC_STATUS_RUNNING', 'OPC_STATUS_FAILED', 'OPC_STATUS_MASK',
    'FILETIME', 'OPCSERVERSTATUS', 'OPCITEMDEF', 'OPCITEMRESULT', 'OPCITEMSTATE',
    'COSERVERINFO',
    'IOPCServer', 'IOPCItemMgt', 'IOPCSyncIO', 'IOPCBrowseServerAddressSpace',
    'IEnumString', 'IOPCCommon', 'IOPCGroupStateMgt', 'IOPCAsyncIO2',
    'IOPCDataCallback', 'IOPCItemProperties', 'IOPCServerList', 'IEnumCLSID',
    'IID_IOPCServer', 'IID_IOPCItemMgt', 'IID_IOPCSyncIO',
    'IID_IOPCBrowseServerAddressSpace', 'IID_IOPCGroupStateMgt',
    'IID_IOPCAsyncIO2', 'IID_IOPCDataCallback', 'IID_IOPCCommon', 'IID_IEnumString',
    'CLSID_OPC_SERVER_LIST', 'IID_IOPCServerList', 'IID_IEnumCLSID',
    'free_cotaskmem', 'free_bstr', 'variant_clear', 'get_status_vendor_pointer',
    'as_unsigned', 'as_signed', 'hresult_hex', 'succeeded', 'hresult_text', 'format_hresult',
    'vtype_base', 'vtype_to_type_name', 'quality_text', 'is_quality_good',
    'server_state_text', 'filetime_to_datetime', 'filetime_to_local_string',
    'variant_to_python', 'python_to_variant', 'split_item_id',
    'list_local_da_servers', 'list_da_servers', 'list_remote_da_servers',
    'pump_message_events',
    'ensure_com_initialized', 'uninitialize_com', 'com_initialized',
    'try_init_dcom_security', 'CLSCTX_LOCAL_SERVER', 'CLSCTX_REMOTE_SERVER',
]

# -*- coding: utf-8 -*-
"""opcda 低层客户端单元测试（不依赖 OPC 服务器 / COM）。

覆盖：
- VARTYPE → 类型名映射（契约要求）
- HRESULT 归一化/格式化/本地错误文本
- 质量码、服务器状态文本
- FILETIME 转换
- Python ↔ VARIANT 双向转换（含数组拒绝、整数越界拒绝）
- ItemID 路径拆分
- OpcDaError 结构与错误码归一
- DaItem/DaValue 默认值与 is_good
- list_servers 枚举顺序（OPC Server List/OPCEnum → 注册表兜底）与本机判定
- 模块公开 API 与接口声明齐全

运行：python -m unittest tests.test_da_client_unit -v
"""

import datetime
import decimal
import os
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from comtypes.automation import VARIANT

from opcda import com
from opcda import client as da_client
from opcda.client import DaItem, DaValue, OpcDaError, list_servers, pump_events


class TestVartypeName(unittest.TestCase):
    def test_contract_mapping(self):
        expected = {
            2: 'Int16', 3: 'Int32', 4: 'Float', 5: 'Double', 6: 'Currency',
            7: 'DateTime', 8: 'String', 11: 'Boolean',
            16: 'Int8', 17: 'UInt8', 18: 'UInt16', 19: 'UInt32',
            20: 'Int64', 21: 'UInt64', 22: 'Int32', 23: 'UInt32',
        }
        for vt, name in expected.items():
            self.assertEqual(com.vtype_to_type_name(vt), name, 'vt=0x%04X' % vt)

    def test_unknown_and_empty(self):
        self.assertEqual(com.vtype_to_type_name(0), 'Unknown')
        self.assertEqual(com.vtype_to_type_name(None), 'Unknown')
        self.assertEqual(com.vtype_to_type_name(0x0FFF), 'Unknown')

    def test_array_and_byref_flags(self):
        self.assertEqual(com.vtype_to_type_name(com.VT_ARRAY | 5), 'Array[Double]')
        self.assertEqual(com.vtype_to_type_name(com.VT_ARRAY | 8), 'Array[String]')
        self.assertEqual(com.vtype_to_type_name(com.VT_BYREF | 5), 'Double&')

    def test_vtype_base(self):
        self.assertEqual(com.vtype_base(com.VT_ARRAY | 5), 5)
        self.assertEqual(com.vtype_base(com.VT_BYREF | 8), 8)
        self.assertEqual(com.vtype_base(None), 0)

    def test_client_reexport(self):
        self.assertEqual(da_client.vtype_to_type_name(2), 'Int16')
        self.assertEqual(da_client.vtype_to_type_name(com.VT_ARRAY | com.VT_R8),
                         'Array[Double]')


class TestHresult(unittest.TestCase):
    def test_unsigned_signed_conversion(self):
        self.assertEqual(com.as_unsigned(-2147024809), 0x80070057)
        self.assertEqual(com.as_unsigned(0x80070057), 0x80070057)
        self.assertEqual(com.as_signed(0x80070057), -2147024809)
        self.assertEqual(com.as_signed(0), 0)
        self.assertEqual(com.as_signed(1), 1)
        self.assertIsNone(com.as_unsigned(None))

    def test_hex_format(self):
        self.assertEqual(com.hresult_hex(0x80070057), '0x80070057')
        self.assertEqual(com.hresult_hex(-2147024809), '0x80070057')
        self.assertEqual(com.hresult_hex(None), '0x00000000')

    def test_succeeded(self):
        self.assertTrue(com.succeeded(0))
        self.assertTrue(com.succeeded(1))       # S_FALSE 也是成功
        self.assertTrue(com.succeeded(None))
        self.assertFalse(com.succeeded(0x80004005))
        self.assertFalse(com.succeeded(-2147024809))

    def test_known_opc_errors(self):
        text = com.hresult_text(com.OPC_E_UNKNOWNITEMID)
        self.assertIn('OPC_E_UNKNOWNITEMID', text)
        text = com.hresult_text(com.OPC_E_BADRIGHTS)
        self.assertIn('OPC_E_BADRIGHTS', text)
        text = com.hresult_text(com.REGDB_E_CLASSNOTREG)
        self.assertIn('REGDB_E_CLASSNOTREG', text)
        text = com.hresult_text(com.E_ACCESSDENIED)
        self.assertIn('E_ACCESSDENIED', text)
        self.assertIn('DCOM', text)

    def test_unknown_hresult_fallback(self):
        text = com.hresult_text(0xDEADBEEF)
        self.assertIn('0xDEADBEEF', text)
        self.assertTrue(text)

    def test_win32_facility_fallback(self):
        # 0x80070005 在表里，换一个未列出的 Win32 错误码验证系统消息回退
        text = com.hresult_text(0x80070002)
        self.assertTrue(text)
        self.assertIn('0x0002', text)

    def test_format_hresult(self):
        text = com.format_hresult(com.REGDB_E_CLASSNOTREG)
        self.assertIn('0x80040154', text)
        self.assertIn('REGDB_E_CLASSNOTREG', text)


class TestQualityAndState(unittest.TestCase):
    def test_quality_good(self):
        self.assertEqual(com.quality_text(0x00C0), 'Good')
        self.assertEqual(com.quality_text(0x00D8), 'Good/LocalOverride')
        self.assertTrue(com.is_quality_good(0x00C0))
        self.assertTrue(com.is_quality_good(0x00D8))
        self.assertFalse(com.is_quality_good(0x0000))
        self.assertFalse(com.is_quality_good(0x0040))
        self.assertFalse(com.is_quality_good(None))

    def test_quality_bad_uncertain(self):
        self.assertEqual(com.quality_text(0x0000), 'Bad')
        self.assertEqual(com.quality_text(0x0018), 'Bad/CommFailure')
        self.assertEqual(com.quality_text(0x0040), 'Uncertain')
        self.assertEqual(com.quality_text(0x0044), 'Uncertain/LastUsable')
        self.assertFalse(com.is_quality_good(0x0018))

    def test_server_state_text(self):
        self.assertEqual(com.server_state_text(1), '运行中')
        self.assertEqual(com.server_state_text(2), '故障')
        self.assertEqual(com.server_state_text(4), '已暂停')
        self.assertEqual(com.server_state_text(6), '通信故障')
        self.assertEqual(com.server_state_text(5), '测试模式')
        self.assertEqual(com.server_state_text(3), '无配置')
        self.assertIn('未知', com.server_state_text(99))
        self.assertEqual(com.server_state_text(None), '未知')


class TestFiletime(unittest.TestCase):
    def test_zero_returns_none(self):
        ft = com.FILETIME(0, 0)
        self.assertIsNone(com.filetime_to_datetime(ft))
        self.assertIsNone(com.filetime_to_local_string(ft))
        self.assertIsNone(com.filetime_to_datetime(None))

    def test_epoch_value(self):
        # 1970-01-01T00:00:00Z = 116444736000000000 * 100ns
        value = 116444736000000000
        ft = com.FILETIME(value & 0xFFFFFFFF, value >> 32)
        dt = com.filetime_to_datetime(ft)
        self.assertIsNotNone(dt)
        self.assertEqual(dt.astimezone(datetime.timezone.utc),
                         datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc))
        text = com.filetime_to_local_string(ft)
        self.assertEqual(len(text), 19)
        self.assertEqual(text[4], '-')
        self.assertEqual(text[13], ':')

    def test_known_local_time(self):
        # 2026-10-23 12:34:56Z
        target = datetime.datetime(2026, 10, 23, 12, 34, 56,
                                   tzinfo=datetime.timezone.utc)
        delta = target - datetime.datetime(1601, 1, 1, tzinfo=datetime.timezone.utc)
        value = int(delta.total_seconds()) * 10 ** 7
        ft = com.FILETIME(value & 0xFFFFFFFF, value >> 32)
        dt = com.filetime_to_datetime(ft)
        self.assertEqual(dt, target)


class TestVariantConversion(unittest.TestCase):
    def test_scalar_roundtrip(self):
        cases = [
            (com.VT_BOOL, True, True),
            (com.VT_BOOL, False, False),
            (com.VT_I1, -5, -5),
            (com.VT_I2, -1234, -1234),
            (com.VT_I4, 123456, 123456),
            (com.VT_I8, 2 ** 40, 2 ** 40),
            (com.VT_UI1, 200, 200),
            (com.VT_UI2, 60000, 60000),
            (com.VT_UI4, 4000000000, 4000000000),
            (com.VT_UI8, 2 ** 63, 2 ** 63),
            (com.VT_R4, 1.5, 1.5),
            (com.VT_R8, 3.25, 3.25),
            (com.VT_BSTR, 'hello 中文', 'hello 中文'),
        ]
        for vt, raw, expected in cases:
            variant = com.python_to_variant(raw, vt)
            self.assertEqual(int(variant.vt), vt, 'vt=0x%04X' % vt)
            self.assertEqual(com.variant_to_python(variant), expected,
                             'vt=0x%04X' % vt)
            com.variant_clear(variant)

    def test_bool_from_int(self):
        variant = com.python_to_variant(0, com.VT_BOOL)
        self.assertFalse(com.variant_to_python(variant))
        variant = com.python_to_variant(5, com.VT_BOOL)
        self.assertTrue(com.variant_to_python(variant))
        com.variant_clear(variant)

    def test_unknown_vt_infers_from_python(self):
        for raw in (True, 5, 2.5, 'text'):
            variant = com.python_to_variant(raw, 0)
            self.assertEqual(com.variant_to_python(variant), raw)
            com.variant_clear(variant)

    def test_none_and_empty(self):
        self.assertIsNone(com.variant_to_python(VARIANT()))
        self.assertIsNone(com.variant_to_python(None))
        variant = com.python_to_variant(None, 0)
        self.assertIsNone(com.variant_to_python(variant))
        com.variant_clear(variant)

    def test_int_range_rejected(self):
        with self.assertRaises(ValueError):
            com.python_to_variant(40000, com.VT_I2)
        with self.assertRaises(ValueError):
            com.python_to_variant(-1, com.VT_UI1)
        with self.assertRaises(ValueError):
            com.python_to_variant('not-a-number', com.VT_I4)

    def test_array_write_rejected(self):
        with self.assertRaises(ValueError):
            com.python_to_variant([1, 2, 3], com.VT_ARRAY | com.VT_R8)
        with self.assertRaises(ValueError):
            com.python_to_variant('abc', com.VT_ARRAY | com.VT_BSTR)

    def test_date_roundtrip(self):
        value = datetime.datetime(2026, 10, 23, 12, 34, 56)
        variant = com.python_to_variant(value, com.VT_DATE)
        self.assertEqual(int(variant.vt), com.VT_DATE)
        # 客户端把日期值归一为本地时间字符串，便于 UI 直接展示
        self.assertEqual(com.variant_to_python(variant), '2026-10-23 12:34:56')
        com.variant_clear(variant)

    def test_currency(self):
        variant = com.python_to_variant(1.5, com.VT_CY)
        self.assertEqual(int(variant.vt), com.VT_CY)
        self.assertAlmostEqual(float(com.variant_to_python(variant)), 1.5, places=4)
        com.variant_clear(variant)

    def test_variant_to_python_decimal_normalized(self):
        # CY 读回是 Decimal，客户端统一归一为 float
        variant = VARIANT(decimal.Decimal('2.25'))
        self.assertEqual(com.variant_to_python(variant), 2.25)


class TestSplitItemId(unittest.TestCase):
    def test_dot_path(self):
        self.assertEqual(com.split_item_id('Random.Int1'), ('Random', 'Int1'))
        self.assertEqual(com.split_item_id('Bucket Brigade.Real4'),
                         ('Bucket Brigade', 'Real4'))
        self.assertEqual(com.split_item_id('Device.Channel.Tag'),
                         ('Device/Channel', 'Tag'))

    def test_root_level(self):
        self.assertEqual(com.split_item_id('Tag1'), ('', 'Tag1'))
        self.assertEqual(com.split_item_id('#MonitorACLFile'),
                         ('', '#MonitorACLFile'))
        self.assertEqual(com.split_item_id(''), ('', ''))
        self.assertEqual(com.split_item_id(None), ('', ''))

    def test_slash_path(self):
        self.assertEqual(com.split_item_id('a/b/c'), ('a/b', 'c'))
        self.assertEqual(com.split_item_id('a\\b\\c'), ('a/b', 'c'))


class TestOpcDaError(unittest.TestCase):
    def test_code_normalization(self):
        exc = OpcDaError('失败', -2147024809)
        self.assertEqual(exc.code, 0x80070057)
        self.assertIn('0x80070057', str(exc))
        self.assertIn('失败', str(exc))

    def test_without_code(self):
        exc = OpcDaError('普通错误')
        self.assertIsNone(exc.code)
        self.assertEqual(str(exc), '普通错误')
        self.assertEqual(exc.errors, [])

    def test_with_per_item_errors(self):
        errors = [('A.B', com.OPC_E_UNKNOWNITEMID, '未知')]
        exc = OpcDaError('批量失败', com.OPC_E_UNKNOWNITEMID, errors=errors)
        self.assertEqual(exc.errors, errors)
        self.assertEqual(exc.code, com.OPC_E_UNKNOWNITEMID)


class TestDataclasses(unittest.TestCase):
    def test_da_item_defaults(self):
        item = DaItem(item_id='Random.Int1', name='Int1', path='Random')
        self.assertEqual(item.canonical_vt, 0)
        self.assertEqual(item.access, 0)
        self.assertEqual(item.server_handle, 0)
        self.assertEqual(item.client_handle, 0)

    def test_da_value_defaults_and_good(self):
        value = DaValue(item_id='Random.Int1')
        self.assertIsNone(value.value)
        self.assertEqual(value.quality, 0)
        self.assertIsNone(value.timestamp)
        self.assertEqual(value.error, 0)
        self.assertFalse(value.is_good)
        good = DaValue(item_id='Random.Int1', value=1, quality=0x00C0)
        self.assertTrue(good.is_good)
        bad = DaValue(item_id='Random.Int1', value=1, quality=0x0000)
        self.assertFalse(bad.is_good)


class TestServerEnumerationWithoutCom(unittest.TestCase):
    """服务器枚举逻辑单测：patch 掉真实 DCOM/注册表，离线可跑。"""

    def test_local_falls_back_to_registry_when_opc_list_empty(self):
        # 本机：OPC Server List 为空 → 回退 com.list_local_da_servers()，去重排序
        with patch.object(da_client.com, 'list_da_servers', return_value=[]), \
                patch.object(da_client.com, 'list_local_da_servers',
                             return_value=['A.B.1']):
            self.assertEqual(list_servers('localhost'), ['A.B.1'])

    def test_local_opc_list_result_is_sorted_and_deduplicated(self):
        with patch.object(da_client.com, 'list_da_servers',
                          return_value=['B.C.2', 'A.B.1', 'A.B.1']):
            self.assertEqual(list_servers('localhost'), ['A.B.1', 'B.C.2'])

    def test_remote_uses_opc_server_list(self):
        # 远程：优先 OPC Server List，成功时不碰本机注册表扫描
        with patch.object(da_client, '_is_local_host', return_value=False), \
                patch.object(da_client.com, 'list_da_servers',
                             return_value=['Remote.Server.1']) as enum, \
                patch.object(da_client.com, 'list_local_da_servers') as local_scan:
            self.assertEqual(list_servers('remotehost'), ['Remote.Server.1'])
            enum.assert_called_once_with('remotehost')
            local_scan.assert_not_called()

    def test_remote_falls_back_to_remote_registry(self):
        # 远程：OPC Server List 为空 → com.list_remote_da_servers()
        with patch.object(da_client, '_is_local_host', return_value=False), \
                patch.object(da_client.com, 'list_da_servers', return_value=[]), \
                patch.object(da_client.com, 'list_remote_da_servers',
                             return_value=['Remote.Registry.1']) as remote_scan:
            self.assertEqual(list_servers('remotehost'), ['Remote.Registry.1'])
            remote_scan.assert_called_once_with('remotehost')

    def test_remote_all_enumeration_failures_return_empty(self):
        # 远程：OPC 枚举抛异常 + 远程注册表兜底抛异常 → []，绝不向外抛异常
        with patch.object(da_client, '_is_local_host', return_value=False), \
                patch.object(da_client.com, 'list_da_servers',
                             side_effect=OSError('DCOM 不可用')), \
                patch.object(da_client.com, 'list_remote_da_servers',
                             side_effect=OSError('Remote Registry 未启用')):
            self.assertEqual(list_servers('remotehost'), [])

    def test_remote_empty_results_return_empty(self):
        with patch.object(da_client, '_is_local_host', return_value=False), \
                patch.object(da_client.com, 'list_da_servers', return_value=[]), \
                patch.object(da_client.com, 'list_remote_da_servers', return_value=[]):
            self.assertEqual(list_servers('remotehost'), [])

    def test_is_local_host_detects_loopback_and_local_ip(self):
        self.assertTrue(da_client._is_local_host('127.0.0.1'))
        self.assertTrue(da_client._is_local_host('localhost'))
        self.assertTrue(da_client._is_local_host(socket.gethostname()))
        local_ip = socket.gethostbyname(socket.gethostname())
        self.assertTrue(da_client._is_local_host(local_ip))
        self.assertFalse(da_client._is_local_host('192.0.2.10'))

    def test_is_local_host_detects_all_lan_ips(self):
        # 多网卡/LAN IP 不能误判为远程（原实现只比较单个解析 IP）
        try:
            infos = socket.getaddrinfo(socket.gethostname(), None)
        except OSError:
            self.skipTest('本机地址解析不可用')
        lan_ips = {str(info[4][0]) for info in infos if info[4] and info[4][0]}
        for ip in lan_ips:
            self.assertTrue(da_client._is_local_host(ip), ip)

    def test_localhost_returns_list_of_str(self):
        servers = list_servers('localhost')
        self.assertIsInstance(servers, list)
        for progid in servers:
            self.assertIsInstance(progid, str)

    def test_local_registry_scan_shape(self):
        servers = com.list_local_da_servers()
        self.assertIsInstance(servers, list)
        self.assertEqual(servers, sorted(set(servers)))


class TestErrorClassification(unittest.TestCase):
    """链路故障 vs 点位级业务错误的判定（防止假连接/误重连）。"""

    def test_item_level_codes(self):
        for code in (com.OPC_E_UNKNOWNITEMID, com.OPC_E_BADRIGHTS, com.OPC_E_BADTYPE,
                     com.OPC_E_INVALIDITEMID, com.OPC_E_WRITEONLY):
            self.assertTrue(com.is_item_level_error(code), hex(code))
            self.assertFalse(com.is_link_level_error(code), hex(code))

    def test_link_level_codes(self):
        for code in (com.RPC_S_CALL_FAILED, com.RPC_E_DISCONNECTED,
                     com.E_ACCESSDENIED, com.E_FAIL, com.RPC_S_SERVER_UNAVAILABLE):
            self.assertTrue(com.is_link_level_error(code), hex(code))
        # 拿不到 HRESULT 的异常（RPC 层 OSError 等）保守按链路故障处理
        self.assertTrue(com.is_link_level_error(None))

    def test_write_denied_is_item_level_for_write_results(self):
        self.assertTrue(com.is_item_level_write_error(com.E_ACCESSDENIED))
        self.assertTrue(com.is_item_level_write_error(com.OPC_E_BADRIGHTS))
        self.assertFalse(com.is_item_level_write_error(com.RPC_S_CALL_FAILED))

    def test_local_fqdn_is_recognized_without_dns(self):
        """本机 FQDN 用 Windows API（非 DNS）识别；远程同短名域名仍判为远程。"""
        try:
            import ctypes
            buffer = ctypes.create_unicode_buffer(256)
            size = ctypes.c_ulong(len(buffer))
            ok = ctypes.windll.kernel32.GetComputerNameExW(3, buffer, ctypes.byref(size))
        except Exception:
            self.skipTest("非 Windows 环境")
        if not ok or not buffer.value:
            self.skipTest("无法读取本机 FQDN")
        fqdn = str(buffer.value).strip().lower()
        self.assertTrue(da_client._is_local_host(fqdn))
        if "." in fqdn:
            self.assertFalse(da_client._is_local_host(fqdn.split(".", 1)[0] + ".other.example"))

    def test_local_host_does_not_match_same_short_name_remote_fqdn(self):
        """远程主机短名与本机同名时不能被误判为本机（忽略 DCOM machine 参数）。"""
        short = socket.gethostname().split(".", 1)[0].strip().lower()
        if not short:
            self.skipTest("无法获取本机短主机名")
        self.assertFalse(da_client._is_local_host(short + ".site2.local"))
        self.assertFalse(da_client._is_local_host(short + ".other.domain.example"))

    def test_async_unsupported_codes(self):
        for code in (com.E_NOINTERFACE, com.E_NOTIMPL,
                     com.CONNECT_E_NOCONNECTION, com.CONNECT_E_ADVISELIMIT):
            self.assertTrue(com.is_async_unsupported_error(code), hex(code))
        self.assertFalse(com.is_async_unsupported_error(com.RPC_E_DISCONNECTED))


class TestVariantOwnership(unittest.TestCase):
    """写字符串点位时的 VARIANT 所有权转移（double-free 回归）。"""

    def test_move_variant_into_slot_neutralizes_source(self):
        variant = com.python_to_variant("hello", com.VT_BSTR)
        self.assertEqual(variant.vt, com.VT_BSTR)
        slots = (VARIANT * 1)()
        da_client._move_variant_into_slot(slots, 0, variant)
        # 源 VARIANT 已置空：其析构不会再对同一 BSTR 调 VariantClear
        self.assertEqual(variant.vt, com.VT_EMPTY)
        self.assertEqual(slots[0].vt, com.VT_BSTR)
        com.variant_clear(slots[0])
        self.assertEqual(slots[0].vt, com.VT_EMPTY)

    def test_python_to_variant_bstr_roundtrip(self):
        variant = com.python_to_variant("灯控", com.VT_BSTR)
        try:
            self.assertEqual(com.variant_to_python(variant), "灯控")
        finally:
            com.variant_clear(variant)


class TestModuleApi(unittest.TestCase):
    REQUIRED_CLIENT_NAMES = (
        'OpcDaClient', 'OpcDaError', 'DaItem', 'DaValue',
        'list_servers', 'pump_events', 'vtype_to_type_name',
    )
    REQUIRED_METHODS = (
        'connect', 'disconnect', 'is_connected', 'get_server_status',
        'browse', 'add_items', 'read_items', 'write_items',
        'set_data_change_callback', 'start_subscription', 'stop_subscription',
        'get_error_string', 'pump_events',
    )

    def test_client_module_exports(self):
        for name in self.REQUIRED_CLIENT_NAMES:
            self.assertTrue(hasattr(da_client, name), name)

    def test_client_methods_exist(self):
        for name in self.REQUIRED_METHODS:
            self.assertTrue(callable(getattr(da_client.OpcDaClient, name, None)), name)

    def test_com_interfaces_declared(self):
        for name in ('IOPCServer', 'IOPCItemMgt', 'IOPCSyncIO',
                     'IOPCBrowseServerAddressSpace', 'IEnumString',
                     'IOPCCommon', 'IOPCGroupStateMgt', 'IOPCAsyncIO2',
                     'IOPCDataCallback', 'IOPCServerList', 'IEnumCLSID'):
            interface = getattr(com, name, None)
            self.assertIsNotNone(interface, name)
            self.assertTrue(getattr(interface, '_iid_', None), name)

    def test_module_level_pump_events_is_callable(self):
        self.assertIsNone(pump_events())

    def test_client_constructor_validation(self):
        with self.assertRaises(ValueError):
            da_client.OpcDaClient('')
        instance = da_client.OpcDaClient('Matrikon.OPC.Simulation.1')
        self.assertEqual(instance.prog_id, 'Matrikon.OPC.Simulation.1')
        self.assertEqual(instance.host, 'localhost')
        self.assertFalse(instance.is_connected())
        self.assertEqual(instance.mounted_items(), [])
        self.assertEqual(instance.last_add_errors, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)

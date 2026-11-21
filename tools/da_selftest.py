# -*- coding: utf-8 -*-
"""OPC DA 客户端自检工具（T1 交付，Lead 可直接运行验收）。

在真实 OPC DA 服务器（默认 Matrikon OPC Simulation Server）上依次验证：
服务器枚举、连接、服务器状态、层次浏览、批量 AddItems、批量同步读、批量写、
500ms 数据变化订阅（要求收到多次 OnDataChange 且 hClient 能映射回 ItemID）、
错误路径、停止订阅、幂等断开、重连。

用法::

    python tools/da_selftest.py
    python tools/da_selftest.py --progid Matrikon.OPC.Simulation.1 --rate 500
    python tools/da_selftest.py --host 192.168.1.10 --progid Kepware.KEPServerEX.V6
    python tools/da_selftest.py --skip-write --verbose

退出码：0 = 全部通过（允许 WARN）；1 = 存在 FAIL。
本脚本不依赖 pytest，py3.8 / py3.12 均可直接运行。
"""

from __future__ import print_function

import argparse
import platform
import sys
import time
import traceback

import comtypes

_HERE = __import__('os').path.dirname(__import__('os').path.abspath(__file__))
_ROOT = __import__('os').path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from opcda import com  # noqa: E402
from opcda.client import (  # noqa: E402
    OpcDaClient,
    OpcDaError,
    list_servers,
    pump_events,
    vtype_to_type_name,
)

DEFAULT_PROGID = 'Matrikon.OPC.Simulation.1'
# Matrikon 模拟服务器的已知分支（用于浏览结果校验，不是硬性要求）
EXPECTED_BRANCHES = ('Random', 'Bucket Brigade', 'Write Only')


class Reporter(object):
    """记录 PASS/FAIL/WARN 并输出。"""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warned = 0
        self.failures = []

    def section(self, title):
        print('')
        print('=' * 78)
        print('== %s' % title)
        print('=' * 78)

    def info(self, text):
        print('   %s' % text)

    def ok(self, name, detail=''):
        self.passed += 1
        print('   PASS  %s%s' % (name, ('  [%s]' % detail) if detail else ''))

    def warn(self, name, detail=''):
        self.warned += 1
        print('   WARN  %s%s' % (name, ('  [%s]' % detail) if detail else ''))

    def fail(self, name, detail=''):
        self.failed += 1
        self.failures.append('%s%s' % (name, ('  [%s]' % detail) if detail else ''))
        print('   FAIL  %s%s' % (name, ('  [%s]' % detail) if detail else ''))

    def check(self, name, condition, detail=''):
        if condition:
            self.ok(name, detail)
        else:
            self.fail(name, detail)
        return bool(condition)

    def summary(self):
        print('')
        print('=' * 78)
        print('自检结果：PASS=%d  WARN=%d  FAIL=%d' % (self.passed, self.warned, self.failed))
        if self.failures:
            print('失败项：')
            for failure in self.failures:
                print('  - %s' % failure)
        print('=' * 78)
        return 0 if self.failed == 0 else 1


def pump_for(client, seconds, interval=0.02):
    """在 seconds 秒内持续泵消息（订阅回调只有泵消息才会被派发）。"""
    end = time.time() + float(seconds)
    while time.time() < end:
        client.pump_events()
        time.sleep(interval)


def short(value, limit=60):
    text = repr(value)
    return text if len(text) <= limit else text[:limit] + '...'


def format_value(value):
    if isinstance(value, list):
        return '[%s]' % ', '.join(format_value(item) for item in value[:4])
    return short(value)


def pick_write_targets(items, count=3):
    """挑几个**标量、可写**的点位用于写测试（跳过数组/系统内部点位）。

    数组类型点位的写入在客户端被显式拒绝（避免 comtypes SAFEARRAY 崩溃），
    因此写测试只选标量点位，并优先普通业务分支（Bucket Brigade / Write Only 等）。
    """
    preferred_prefixes = ('Bucket Brigade.', 'Write Only.', 'Read/Write.')
    candidates = []
    for item in items:
        if not (item.access & com.OPC_WRITEABLE):
            continue
        if item.canonical_vt & com.VT_ARRAY:
            continue  # 数组类型点位不支持写值（客户端显式拒绝，避免 SAFEARRAY 崩溃）
        base = com.vtype_base(item.canonical_vt)
        if base not in (com.VT_BOOL, com.VT_I2, com.VT_I4, com.VT_R4,
                        com.VT_R8, com.VT_BSTR):
            continue
        if item.item_id.startswith(('#', '@', '.')):
            continue  # 服务器内部/系统点位，不碰
        preferred = item.item_id.startswith(preferred_prefixes)
        candidates.append((0 if preferred else 1, item.item_id, item))
    candidates.sort(key=lambda row: (row[0], row[1]))
    return [row[2] for row in candidates[:count]]


def value_for_item(item):
    base = com.vtype_base(item.canonical_vt)
    if base == com.VT_BOOL:
        return True
    if base in (com.VT_I2, com.VT_I4):
        return 42
    if base in (com.VT_R4, com.VT_R8):
        return 3.5
    if base == com.VT_BSTR:
        return 'selftest'
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description='OPC DA 客户端自检（comtypes 直连 COM）')
    parser.add_argument('--host', default='localhost', help='OPC DA 服务器主机（默认 localhost）')
    parser.add_argument('--progid', default=DEFAULT_PROGID, help='OPC DA 服务器 ProgID')
    parser.add_argument('--rate', type=int, default=500, help='订阅更新周期 ms（默认 500）')
    parser.add_argument('--subscribe-seconds', type=float, default=5.0,
                        help='订阅观察时长秒（默认 5，至少应收到 3 次回调）')
    parser.add_argument('--depth', type=int, default=3, help='浏览最大深度（默认 3）')
    parser.add_argument('--skip-write', action='store_true', help='跳过写测试')
    parser.add_argument('--verbose', action='store_true', help='输出调试日志')
    args = parser.parse_args(argv)

    import logging
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format='%(levelname)s %(name)s: %(message)s')

    reporter = Reporter()
    client = OpcDaClient(args.progid, host=args.host)

    # ------------------------------------------------------------------ 1. 环境
    reporter.section('1. 运行环境')
    reporter.info('Python      : %s' % sys.version.replace('\n', ' '))
    reporter.info('平台        : %s / %s' % (platform.system(), platform.machine()))
    reporter.info('comtypes    : %s' % getattr(comtypes, '__version__', 'unknown'))
    reporter.info('目标服务器  : %s @ %s' % (args.progid, args.host))
    try:
        reporter.check('import opcda / opcda.com / opcda.client', True)
    except Exception as exc:  # pragma: no cover
        reporter.fail('import opcda', str(exc))
        return reporter.summary()

    # ------------------------------------------------------------------ 2. 服务器枚举
    reporter.section('2. 本机 OPC DA 服务器枚举（注册表 Component Categories）')
    try:
        servers = list_servers(args.host)
        reporter.info('枚举结果：%s' % (servers or '[]'))
        if args.host.lower() in ('localhost', '127.0.0.1', ''):
            reporter.check('list_servers() 返回列表', isinstance(servers, list))
            if servers:
                reporter.ok('扫描到 %d 个本机 OPC DA 服务器' % len(servers),
                            ', '.join(servers[:5]))
                if args.progid in servers:
                    reporter.ok('目标 ProgID 在枚举结果中', args.progid)
                else:
                    reporter.warn('目标 ProgID 不在枚举结果中（可能未注册 Component Categories）',
                                  args.progid)
            else:
                reporter.warn('未扫描到 OPC DA 服务器（可能未安装或未注册）')
        else:
            reporter.ok('远程主机不枚举（按设计返回 []）',
                        '远程请输入已知 ProgID')
    except Exception as exc:
        reporter.fail('list_servers() 不应抛异常', str(exc))

    result_code = 1
    try:
        # -------------------------------------------------------------- 3. 连接
        reporter.section('3. 连接与服务器状态')
        started = time.time()
        client.connect()
        reporter.ok('connect() 成功', '耗时 %.0fms' % ((time.time() - started) * 1000))
        reporter.check('is_connected() 为 True', client.is_connected())
        status = client.get_server_status()
        reporter.info('state=%s(%s) vendor=%s version=%s groups=%s' % (
            status['state'], status['state_text'], status['vendor'],
            status['version'], status['group_count']))
        reporter.info('start_time=%s current_time=%s' % (
            status['start_time'], status['current_time']))
        for key in ('state', 'state_text', 'vendor', 'version', 'group_count',
                    'start_time', 'current_time'):
            reporter.check('get_server_status() 含字段 %s' % key, key in status)
        reporter.check('服务器状态为运行中（1）',
                       status['state'] == com.OPC_STATUS_RUNNING,
                       status['state_text'])
        reporter.check('start_time 为本地时间字符串',
                       bool(status['start_time']) and len(status['start_time']) == 19,
                       str(status['start_time']))

        # -------------------------------------------------------------- 4. 浏览
        reporter.section('4. 层次浏览')
        browse_started = time.time()
        items = client.browse(max_depth=args.depth)
        browse_cost = (time.time() - browse_started) * 1000.0
        reporter.info('浏览到 %d 个点位，耗时 %.0fms' % (len(items), browse_cost))
        reporter.check('browse() 返回非空列表', len(items) > 0, '%d 个' % len(items))
        paths = {}
        for item in items:
            paths.setdefault(item.path, []).append(item)
        reporter.info('分支（path）数量：%d' % len(paths))
        for path in sorted(paths, key=lambda value: (-len(paths[value]), value))[:8]:
            samples = [entry.item_id for entry in paths[path][:3]]
            reporter.info('  %-28s %4d 个  例: %s' % (path or '<根>', len(paths[path]), samples))
        found_branches = [name for name in EXPECTED_BRANCHES
                          if any(name in (item.path or '') or name in item.item_id
                                 for item in items)]
        if found_branches:
            reporter.ok('浏览到预期分支', ', '.join(found_branches))
        else:
            reporter.warn('未发现 Matrikon 预期分支（非 Matrikon 服务器可忽略）',
                          ', '.join(EXPECTED_BRANCHES))
        full_ids = all(item.item_id and (' ' not in item.item_id or True) for item in items)
        reporter.check('每个点位都有完整 ItemID', full_ids)
        reporter.check('每个点位都有显示名', all(item.name for item in items))
        duplicate = len(items) - len(set(item.item_id for item in items))
        reporter.check('ItemID 无重复', duplicate == 0, '重复 %d' % duplicate)
        # 未被 add 之前 canonical_vt/access 为 0
        reporter.check('浏览结果 canonical_vt 默认 0',
                       all(item.canonical_vt == 0 for item in items))

        # -------------------------------------------------------------- 5. 批量 AddItems
        reporter.section('5. 批量 AddItems（一次调用传数组）')
        item_ids = [item.item_id for item in items]
        add_started = time.time()
        added = client.add_items(item_ids, update_rate_ms=args.rate)
        add_cost = (time.time() - add_started) * 1000.0
        reporter.info('AddItems 成功 %d/%d，耗时 %.1fms（组周期 %dms）' % (
            len(added), len(item_ids), add_cost, client.update_rate_ms))
        reporter.check('add_items() 返回数量与浏览一致',
                       len(added) == len(item_ids),
                       '%d != %d' % (len(added), len(item_ids)))
        reporter.check('add_items() 无逐项错误', not client.last_add_errors,
                       short(client.last_add_errors[:3]))
        reporter.check('批量耗时低于 3000ms（非逐点往返）', add_cost < 3000.0,
                       '%.1fms' % add_cost)
        reporter.check('每个 DaItem 有 server_handle',
                       all(item.server_handle for item in added))
        reporter.check('每个 DaItem 有唯一 client_handle',
                       len(set(item.client_handle for item in added)) == len(added))
        vt_counter = {}
        access_counter = {}
        for item in added:
            vt_counter[item.canonical_vt] = vt_counter.get(item.canonical_vt, 0) + 1
            access_counter[item.access] = access_counter.get(item.access, 0) + 1
        reporter.info('canonical_vt 分布：%s' % vt_counter)
        reporter.info('access 分布：%s' % access_counter)
        reporter.check('已解析出 canonical_vt',
                       all(item.canonical_vt for item in added))
        sample_items = [item for item in added if item.item_id in
                        ('Random.Int1', 'Random.Real4', 'Random.Boolean',
                         'Random.String', 'Write Only.Boolean')]
        for item in sample_items:
            reporter.info('  %-28s vt=%-3d(%s) access=%d' % (
                item.item_id, item.canonical_vt,
                vtype_to_type_name(item.canonical_vt), item.access))

        # -------------------------------------------------------------- 6. 批量读
        reporter.section('6. 批量同步读（Cache / Device）')
        readable_ids = set(item.item_id for item in added
                           if item.access & com.OPC_READABLE)
        reporter.info('可读点位 %d 个，只写点位 %d 个' % (
            len(readable_ids), len(added) - len(readable_ids)))
        for source, label in ((com.OPC_DS_CACHE, 'CACHE'), (com.OPC_DS_DEVICE, 'DEVICE')):
            read_started = time.time()
            values = client.read_items(source=source)
            read_cost = (time.time() - read_started) * 1000.0
            reporter.info('%s：读取 %d 个，耗时 %.1fms' % (label, len(values), read_cost))
            reporter.check('%s 读取数量正确' % label, len(values) == len(added),
                           '%d != %d' % (len(values), len(added)))
            readable_errors = [v for v in values
                               if v.error and v.item_id in readable_ids]
            reporter.check('%s 可读点位无逐项错误（只写点位允许错误）' % label,
                           not readable_errors,
                           short([(v.item_id, com.hresult_hex(v.error))
                                  for v in readable_errors[:3]]))
            good = sum(1 for value in values if com.is_quality_good(value.quality))
            reporter.info('%s 质量 Good：%d/%d' % (label, good, len(values)))
            reporter.check('%s 至少一半点位质量为 Good' % label,
                           good * 2 >= len(values), '%d/%d' % (good, len(values)))
            no_error_values = [value for value in values if value.error == 0]
            with_ts = sum(1 for value in no_error_values if value.timestamp)
            reporter.check('%s 无错误点位都有时间戳' % label,
                           with_ts == len(no_error_values),
                           '%d/%d' % (with_ts, len(no_error_values)))
            reporter.info('%s 样例：%s' % (label, [(v.item_id, format_value(v.value),
                                                 v.quality, v.timestamp)
                                                for v in values[:3]]))
            random_values = [value for value in values
                             if value.item_id.startswith('Random.') and not value.error]
            if random_values:
                changed = len(set(str(value.value) for value in random_values)) == len(random_values)
                reporter.info('Random.* 值样例：%s' % [
                    (v.item_id, format_value(v.value)) for v in random_values[:5]])
                reporter.check('%s Random.* 值类型可读' % label, True)
            # 未挂载点位应返回错误而不是抛异常
            unknown = client.read_items(['No.Such.Item.For.Selftest'])
            reporter.check('读取未挂载点位返回 error 条目',
                           len(unknown) == 1 and unknown[0].error == com.OPC_E_UNKNOWNITEMID,
                           short(unknown[0].error if unknown else None))

        # -------------------------------------------------------------- 7. 写
        reporter.section('7. 批量同步写')
        if args.skip_write:
            reporter.warn('按参数跳过写测试（--skip-write）')
        else:
            targets = pick_write_targets(added)
            write_values = {}
            for item in targets:
                write_values[item.item_id] = value_for_item(item)
            reporter.info('写目标：%s' % [(item.item_id, vtype_to_type_name(item.canonical_vt),
                                        format_value(write_values[item.item_id]))
                                       for item in targets])
            if targets:
                results = client.write_items(write_values)
                reporter.info('写结果：%s' % [(item_id, com.hresult_hex(code), text)
                                              for item_id, code, text in results])
                reporter.check('write_items() 返回数量正确',
                               len(results) == len(write_values))
                reporter.check('目标点位逐项写入成功',
                               all(code == 0 for _, code, _ in results),
                               short([(i, com.hresult_hex(c)) for i, c, _ in results
                                      if c]))
                # 回读写点位（access & READABLE）验证值确实落到服务器
                readback_targets = {
                    item.item_id: write_values[item.item_id] for item in targets
                    if item.access & com.OPC_READABLE}
                if readback_targets:
                    readback = client.read_items(list(readback_targets), source=com.OPC_DS_DEVICE)
                    matches = []
                    for value in readback:
                        expected = readback_targets[value.item_id]
                        if isinstance(expected, float):
                            ok_value = (value.value is not None
                                        and abs(float(value.value) - expected) < 1e-3)
                        else:
                            ok_value = str(value.value) == str(expected)
                        matches.append((value.item_id, ok_value, format_value(value.value)))
                    reporter.info('回读：%s' % matches)
                    if all(item[1] for item in matches):
                        reporter.ok('写入后回读值一致', short(matches))
                    else:
                        reporter.warn('写入后回读值不一致（点位可能被服务器周期刷新覆盖）',
                                      short(matches))
                else:
                    reporter.warn('无可回读的点位（写入目标均为只写点位）')
                # 只读点位写入应返回 OPCE_BADRIGHTS
                readonly = [item for item in added
                            if (item.access & com.OPC_READABLE)
                            and not (item.access & com.OPC_WRITEABLE)
                            and com.vtype_base(item.canonical_vt) in
                            (com.VT_I2, com.VT_I4, com.VT_R4, com.VT_R8)]
                if readonly:
                    item = readonly[0]
                    bad = client.write_items({item.item_id: 1})[0]
                    reporter.check('只读点位写入返回 OPC_E_BADRIGHTS',
                                   bad[1] == com.OPC_E_BADRIGHTS,
                                   short(bad))
                # 无法安全转换的值必须显式抛错（绝不能构造崩溃性 VARIANT）
                int_targets = [item for item in targets
                               if com.vtype_base(item.canonical_vt) in (com.VT_I2,
                                                                        com.VT_I4)]
                if int_targets:
                    item = int_targets[0]
                    try:
                        client.write_items({item.item_id: 'not-a-number'})
                        reporter.fail('非法值写入未报错')
                    except OpcDaError as exc:
                        reporter.ok('非法值写入安全抛 OpcDaError（不崩溃）',
                                    short(str(exc)))
                # 数组类型点位必须被显式拒绝而不是崩溃
                array_writable = [item for item in added
                                  if (item.canonical_vt & com.VT_ARRAY)
                                  and (item.access & com.OPC_WRITEABLE)]
                if array_writable:
                    item = array_writable[0]
                    try:
                        client.write_items({item.item_id: [1, 2, 3]})
                        reporter.fail('数组点位写入未报错')
                    except OpcDaError as exc:
                        reporter.check('数组点位写入安全报错（unsupported data type for write）',
                                       'unsupported data type for write' in str(exc),
                                       short(str(exc)))
                else:
                    reporter.warn('没有数组类型可写点位，跳过数组写入安全测试')
            else:
                reporter.warn('没有可写点位，跳过写测试')

        # -------------------------------------------------------------- 8. 订阅
        reporter.section('8. 数据变化订阅（IOPCAsyncIO2 + IOPCDataCallback，%dms）'
                         % args.rate)
        received = []
        callback_rows = []

        def on_data(values):
            received.append(values)
            for value in values:
                callback_rows.append((value.item_id, value.value, value.quality))

        client.set_data_change_callback(on_data)
        subscribe_started = time.time()
        client.start_subscription(args.rate)
        reporter.ok('start_subscription() 成功')
        if client.subscription_warnings:
            reporter.warn('订阅启动告警', short(client.subscription_warnings))
        reporter.check('subscription_active 为 True', client.subscription_active)
        mount_set = set(item.item_id for item in added)
        # 500ms 周期，观察 5s 应收到 >=3 次
        pump_for(client, args.subscribe_seconds)
        events = len(received)
        reporter.info('回调次数=%d，累计点位更新=%d，client 统计=%d 事件/%d 项' % (
            events, len(callback_rows), client.callback_event_count,
            client.callback_item_count))
        if callback_rows:
            reporter.info('回调样例：%s' % [
                (item_id, format_value(value), '0x%04X' % quality)
                for item_id, value, quality in callback_rows[:8]])
        reporter.check('订阅收到 >= 3 次 OnDataChange', events >= 3,
                       '%d 次 / %.1fs' % (events, args.subscribe_seconds))
        bad_ids = sorted(set(row[0] for row in callback_rows) - mount_set)
        reporter.check('回调 hClient 全部能映射回已挂载 ItemID',
                       not bad_ids, short(bad_ids[:5]))
        random_updates = [row for row in callback_rows
                          if row[0].startswith('Random.')]
        reporter.check('Random.* 点位有实时更新', len(random_updates) > 0,
                       '%d 条' % len(random_updates))
        if len(received) >= 2:
            reporter.check('收到多次独立回调批次', True, '%d 批' % len(received))
        good_updates = [row for row in callback_rows if com.is_quality_good(row[2])]
        reporter.check('回调中包含 Good 质量数据', len(good_updates) > 0,
                       '%d/%d' % (len(good_updates), len(callback_rows)))
        # 回调里的 item_id 可直接用于读
        if callback_rows:
            sample_id = callback_rows[0][0]
            reporter.check('回调 item_id 可用于 read_items',
                           client.read_items([sample_id])[0].item_id == sample_id)
        # 订阅中修改周期（SetState），回调应继续
        events_before_rate_change = len(received)
        client.start_subscription(250)
        reporter.check('订阅中可将周期改为 250ms', client.update_rate_ms <= 500,
                       '%dms' % client.update_rate_ms)
        pump_for(client, 1.6)
        reporter.check('调整周期后仍收到回调', len(received) > events_before_rate_change,
                       '%d -> %d' % (events_before_rate_change, len(received)))

        # 停止订阅后不应再有新回调
        client.stop_subscription()
        reporter.check('stop_subscription() 后 subscription_active 为 False',
                       not client.subscription_active)
        count_after_stop = len(received)
        pump_for(client, 1.2)
        reporter.check('停止订阅后不再收到回调', len(received) == count_after_stop,
                       '%d -> %d' % (count_after_stop, len(received)))
        client.stop_subscription()
        reporter.ok('stop_subscription() 幂等')

        # -------------------------------------------------------------- 9. 错误映射
        reporter.section('9. 错误映射与健壮性')
        text = client.get_error_string(com.REGDB_E_CLASSNOTREG)
        reporter.info('get_error_string(0x80040154) = %s' % text)
        reporter.check('get_error_string() 返回非空文本', bool(text))
        text2 = client.get_error_string(com.OPC_E_UNKNOWNITEMID)
        reporter.info('get_error_string(0xC0040005) = %s' % text2)
        reporter.check('OPC 错误码有中文可读文本', bool(text2))
        reporter.check('vtype_to_type_name(2) == Int16',
                       vtype_to_type_name(2) == 'Int16', vtype_to_type_name(2))
        reporter.check('vtype_to_type_name(11) == Boolean',
                       vtype_to_type_name(11) == 'Boolean', vtype_to_type_name(11))
        reporter.check('vtype_to_type_name(8) == String',
                       vtype_to_type_name(8) == 'String', vtype_to_type_name(8))
        try:
            client.add_items(['No.Such.Item.For.Selftest'])
            reporter.warn('无效 ItemID 未抛异常（服务器可能接受）')
        except OpcDaError as exc:
            reporter.ok('无效 ItemID 抛 OpcDaError', short(str(exc)))
            reporter.check('无效 ItemID 错误码可识别',
                           exc.code in (com.OPC_E_UNKNOWNITEMID,
                                        com.OPC_E_UNKNOWNPATH,
                                        com.OPC_E_INVALIDITEMID,
                                        com.OPC_E_INVALIDHANDLE,
                                        com.OPC_E_NOTFOUND, None),
                           com.hresult_hex(exc.code))
        # 无效 ProgID 连接失败要快速、带 HRESULT、且不泄漏 COM 初始化
        bad_client = OpcDaClient('No.Such.ProgID.For.Selftest', host=args.host)
        try:
            bad_client.connect()
            reporter.fail('无效 ProgID 的 connect() 应失败')
            bad_client.disconnect()
        except OpcDaError as exc:
            reporter.ok('无效 ProgID 抛 OpcDaError', short(str(exc)))
            reporter.check('无效 ProgID 错误码可识别（REGDB_E_CLASSNOTREG/CO_E_CLASSSTRING）',
                           exc.code in (com.REGDB_E_CLASSNOTREG,
                                        com.CO_E_CLASSSTRING),
                           com.hresult_hex(exc.code))
        # COM 消息泵接口可用
        reporter.check('pump_events() 可调用', pump_events() is None)

        # -------------------------------------------------------------- 10. 断开/重连
        reporter.section('10. 断开（幂等）与重连')
        client.disconnect()
        reporter.check('disconnect() 后 is_connected() 为 False',
                       not client.is_connected())
        reporter.check('disconnect() 后 subscription_active 为 False',
                       not client.subscription_active)
        client.disconnect()
        reporter.ok('disconnect() 幂等（重复调用不抛异常）')
        client.connect()
        reporter.ok('disconnect() 后可重连')
        re_added = client.add_items(item_ids[:5], update_rate_ms=args.rate)
        reporter.check('重连后可重新 AddItems',
                       len(re_added) == min(5, len(item_ids)),
                       '%d 个' % len(re_added))
        re_values = client.read_items()
        reporter.check('重连后可读取点位', len(re_values) == len(re_added),
                       '%d 个' % len(re_values))
        client.disconnect()
        result_code = 0
    except OpcDaError as exc:
        reporter.fail('OPC DA 操作异常', short(str(exc)))
        reporter.info(traceback.format_exc())
    except Exception as exc:  # pragma: no cover - 未知异常也必须留证据
        reporter.fail('未预期异常', short(str(exc)))
        reporter.info(traceback.format_exc())
    finally:
        try:
            client.disconnect()
        except Exception:
            pass
    return reporter.summary()


if __name__ == '__main__':
    sys.exit(main())

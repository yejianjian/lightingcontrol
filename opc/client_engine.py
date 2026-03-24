import asyncio
import os
import socket
import ipaddress
import datetime
import time
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization, hashes
from cryptography import x509
from cryptography.x509.oid import NameOID
from asyncua import Client
from asyncua.crypto import security_policies
from asyncua import ua
from utils.logger import global_logger

class SubHandler:
    def __init__(self, callback):
        self.callback = callback
        self.last_data_time = time.time()  # 订阅健康追踪：最后收到数据的时间
        self.call_count = 0                # 总回调计数

    def datachange_notification(self, node, val, data):
        try:
            self.last_data_time = time.time()
            self.call_count += 1

            timestamp = None
            if hasattr(data, 'monitored_item') and hasattr(data.monitored_item, 'Value'):
                timestamp = data.monitored_item.Value.SourceTimestamp
                
            if timestamp:
                # OPC UA 规范返回的是 UTC 时间，需要赋予 UTC 时区后转为系统本地时区
                local_ts = timestamp.replace(tzinfo=datetime.timezone.utc).astimezone()
                ts_str = local_ts.strftime("%Y-%m-%d %H:%M:%S")
            else:
                ts_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
            self.callback(node.nodeid.to_string(), val, ts_str)
        except Exception as e:
            from utils.logger import global_logger
            global_logger.error(f"datachange_notification failed: {e}", exc_info=True)

def generate_client_cert(cert_file="client_cert.der", key_file="client_key.pem"):
    hostname = socket.gethostname()
    if os.path.exists(cert_file) and os.path.exists(key_file):
        return cert_file, key_file, hostname

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.DOMAIN_COMPONENT, hostname),
        x509.NameAttribute(NameOID.COMMON_NAME, u"LightingControlClient"),
    ])
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    ).not_valid_after(
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=3650)
    ).add_extension(
        x509.SubjectAlternativeName([
            x509.UniformResourceIdentifier(u"urn:example.org:FreeOpcUa:opcua-asyncio"),
            x509.DNSName(hostname),
            x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1"))
        ]), critical=False,
    ).add_extension(
        x509.KeyUsage(
            digital_signature=True, content_commitment=True, key_encipherment=True,
            data_encipherment=True, key_agreement=False, key_cert_sign=False,
            crl_sign=False, encipher_only=False, decipher_only=False
        ), critical=True,
    ).add_extension(
        x509.ExtendedKeyUsage([
            x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
            x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
        ]), critical=True,
    ).sign(key, hashes.SHA256())

    with open(key_file, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.DER))
    
    return cert_file, key_file, hostname

class OpcClientEngine:
    def __init__(self, host="localhost", port=48401, username="", password="", namespace_filter="ns=2;", browse_max_depth=5):
        self.host = host
        self.port = port
        self.url = f"opc.tcp://{host}:{port}/"
        self.username = username
        self.password = password
        self.namespace_filter = namespace_filter  # 业务节点命名空间前缀，可配置
        self.browse_max_depth = browse_max_depth  # 节点树遍历最大深度
        self.client = None
        self.nodes = {}
        self.subscription = None
        self._sub_handler = None  # SubHandler 引用，用于健康检测
        self._sub_callback = None  # 订阅回调引用，用于自动重建
        self.connected = False
        self.on_connection_lost = None  # 断线回调钩子
        self._monitor_task = None
        self._consecutive_write_failures = 0  # 连续写入失败计数
        self._max_write_failures = 3         # 连续失败阈值，超过触发重连
        
    async def connect(self):
        cert_file, key_file, hostname = generate_client_cert()
        self.client = Client(url=self.url)
        self.client.application_uri = "urn:example.org:FreeOpcUa:opcua-asyncio"
        
        # 不再强行覆写 secure_channel_timeout 和 session_timeout，
        # 让服务器通过 RevisedLifetime 决定实际值。
        # asyncua 默认 3600000ms(1h)，服务器通常也接受此值。
        # asyncua 内部的 _renew_channel_loop 会在 75% 时间点(~45min)自动续约。
        
        # 绑定 asyncua 原生的 connection_lost_callback（异步回调）
        # 当 asyncua 内部的看门狗或续约 Task 检测到连接丢失时，会调用此回调
        self.client.connection_lost_callback = self._on_asyncua_connection_lost
        
        if self.username:
            self.client.set_user(self.username)
            self.client.set_password(self.password)

        server_cert_path = "server_cert_0.der"
        if os.path.exists(server_cert_path):
            await self.client.set_security_string(f"Basic256Sha256,SignAndEncrypt,{cert_file},{key_file},{server_cert_path}")
        else:
            await self.client.set_security_string(f"Basic256Sha256,SignAndEncrypt,{cert_file},{key_file}")

        # Hook to bypass URI strict check
        orig_create_session = self.client.uaclient.create_session
        async def custom_create_session(params):
            params.ServerUri = f"urn:{hostname}:neuopc"
            params.EndpointUrl = self.url
            return await orig_create_session(params)
        self.client.uaclient.create_session = custom_create_session

        try:
            await self.client.connect()
            self.connected = True
            global_logger.info(f"Connected. SecureChannel timeout={self.client.secure_channel_timeout}ms, Session timeout={self.client.session_timeout}ms")
            # 启动后台心跳检测任务
            self._start_monitor()
            return True
        except Exception as e:
            self.connected = False
            # 确保即使连接失败也清理 client 引用
            self.client = None
            raise e

    def _start_monitor(self):
        """启动后台连接监视协程"""
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
        self._monitor_task = asyncio.create_task(self._monitor_connection())

    async def _on_asyncua_connection_lost(self, exc):
        """asyncua 内部检测到的连接丢失回调（异步）
        
        当 asyncua 的 _monitor_server_loop 看门狗检测到服务器不可达时触发。
        这是续约失败后的最终防线。
        """
        global_logger.error(f"asyncua internal connection lost detected: {exc}")
        if self.connected:
            self.connected = False
            if self.on_connection_lost and callable(self.on_connection_lost):
                try:
                    self.on_connection_lost()
                except Exception as cb_err:
                    global_logger.error(f"on_connection_lost callback error: {cb_err}")

    async def _monitor_connection(self):
        """心跳检测：定期读取服务器状态节点，检测断线"""
        try:
            while self.connected:
                await asyncio.sleep(15)
                if not self.connected:
                    break
                try:
                    # 检查 asyncua 内部的续约 Task 是否已崩溃退出
                    if self.client and hasattr(self.client, '_renew_channel_task'):
                        task = self.client._renew_channel_task
                        if task and task.done():
                            exc = task.exception() if not task.cancelled() else None
                            global_logger.error(f"asyncua _renew_channel_task has died! Exception: {exc}")
                            raise Exception(f"SecureChannel renew task crashed: {exc}")
                    
                    if self.client and hasattr(self.client, '_monitor_server_task'):
                        task = self.client._monitor_server_task
                        if task and task.done():
                            exc = task.exception() if not task.cancelled() else None
                            global_logger.error(f"asyncua _monitor_server_task has died! Exception: {exc}")
                            raise Exception(f"Server monitor task crashed: {exc}")

                    # 读取服务器状态节点（标准 OPC UA 节点）作为心跳并加入超时保护
                    server_state = self.client.get_node("i=2259")
                    await asyncio.wait_for(server_state.read_value(), timeout=5.0)

                    # 额外校验订阅的健康度 (距离上次收到推送是否超过45秒)
                    if self._sub_handler and getattr(self._sub_handler, 'last_data_time', None) and (time.time() - self._sub_handler.last_data_time) > 45.0:
                        global_logger.error("Subscription heartbeat timeout (>45s without push data), treating as connection lost.")
                        raise Exception("Subscription dead")
                except (asyncio.CancelledError, GeneratorExit):
                    break
                except Exception as e:
                    global_logger.error(f"Connection monitor detected failure: {e}")
                    self.connected = False
                    if self.on_connection_lost and callable(self.on_connection_lost):
                        try:
                            self.on_connection_lost()
                        except Exception as cb_err:
                            global_logger.error(f"on_connection_lost callback error: {cb_err}")
                    break
        except asyncio.CancelledError:
            pass
        finally:
            global_logger.debug("Connection monitor task exited.")
            
    async def disconnect(self):
        global_logger.info("[disconnect] Starting disconnect sequence...")
        # 先标记为不连通，防止监控任务进入下一次循环
        self.connected = False

        # 停止心跳监视
        if self._monitor_task:
            global_logger.info("[disconnect] Cancelling _monitor_task...")
            if not self._monitor_task.done():
                self._monitor_task.cancel()
            # 放弃等待 _monitor_task 退出，防止在特殊情况下因为 qasync 事件循环问题造成死锁
            self._monitor_task = None
            global_logger.info("[disconnect] _monitor_task cancelled.")

        # 先取消订阅（加超时保护，防止在死连接上挂起）
        if self.subscription:
            global_logger.info("[disconnect] Deleting subscription...")
            try:
                await asyncio.wait_for(self.subscription.delete(), timeout=3.0)
                global_logger.info("[disconnect] Subscription deleted successfully.")
            except asyncio.TimeoutError:
                global_logger.warning("Subscription delete timed out (connection already dead), skipping.")
            except Exception as e:
                global_logger.warning(f"Error deleting subscription: {e}")
            finally:
                self.subscription = None

        # 清理 Node 对象引用，防止内存泄漏
        self.nodes.clear()

        # 断开客户端连接（加超时保护，防止在死连接上挂起）
        if self.client:
            global_logger.info("[disconnect] Disconnecting asyncua client...")
            try:
                await asyncio.wait_for(self.client.disconnect(), timeout=5.0)
                global_logger.info("[disconnect] asyncua client disconnected successfully.")
            except asyncio.TimeoutError:
                global_logger.warning("Client disconnect timed out (connection already dead), force closing socket.")
                # 超时后强制关闭底层 socket
                try:
                    self.client.disconnect_socket()
                except Exception:
                    pass
            except Exception as e:
                global_logger.warning(f"Error disconnecting client: {e}")
                # 异常后也尝试强制关闭 socket
                try:
                    self.client.disconnect_socket()
                except Exception:
                    pass
            finally:
                self.client = None
        global_logger.info("[disconnect] Disconnect sequence complete.")

    async def get_all_nodes(self):
        if not self.connected:
            return []

        objects_node = self.client.nodes.objects
        discovered_nodes = []
        await self._browse_recursive(objects_node, discovered_nodes, self.browse_max_depth)
        self.nodes = {n['node_id']: n for n in discovered_nodes}
        # 返回给外层的数据剥离 node_obj（底层 asyncua 引用），
        # node_obj 仅保留在 self.nodes 内部缓存中供订阅使用
        return [{k: v for k, v in n.items() if k != 'node_obj'} for n in discovered_nodes]

    async def _browse_recursive(self, node, discovered_nodes, depth_remaining):
        """递归遍历 OPC UA 节点树，收集业务变量节点"""
        if depth_remaining <= 0:
            return
        try:
            children = await node.get_children()
            for child in children:
                try:
                    node_class = await child.read_node_class()
                    if node_class == ua.NodeClass.Variable:
                        node_idx = child.nodeid.to_string()

                        # 仅关注业务控制命名空间的节点，过滤系统节点
                        if self.namespace_filter and not node_idx.startswith(self.namespace_filter):
                            continue

                        name = (await child.read_display_name()).Text
                        try:
                            val_obj = await child.read_data_value()
                            val = val_obj.Value.Value if val_obj.Value else None
                            timestamp = val_obj.SourceTimestamp.strftime("%Y-%m-%d %H:%M:%S") if val_obj.SourceTimestamp else ""
                            vtype = val_obj.Value.VariantType.name if val_obj.Value else None
                        except Exception:
                            val = None
                            timestamp = ""
                            vtype = None

                        dtype = vtype if vtype else (type(val).__name__ if val is not None else "Unknown")

                        if name != "ServerStatus":
                            discovered_nodes.append({
                                "name": name,
                                "node_id": node_idx,
                                "type": dtype,
                                "value": val,
                                "timestamp": timestamp,
                                "node_obj": child
                            })
                    elif node_class == ua.NodeClass.Object:
                        await self._browse_recursive(child, discovered_nodes, depth_remaining - 1)
                except Exception as child_err:
                    global_logger.warning(f"Error reading child {child}: {child_err}")
        except Exception as e:
            global_logger.warning(f"Error browsing node {node}: {e}")

    async def start_subscription(self, callback):
        if not self.connected or not self.nodes: return
        node_objs = [n['node_obj'] for n in self.nodes.values()]
        
        # 将标准的心跳时间节点 i=2258 (CurrentTime) 加入订阅, 以确保持续触发 datachange_notification 来做健康度判定
        try:
            server_time_node = self.client.get_node("i=2258")
            node_objs.append(server_time_node)
        except Exception as e:
            global_logger.warning(f"Could not add server current time to subscription: {e}")
        self._sub_callback = callback  # 保存回调引用，以便自动重建订阅
        handler = SubHandler(callback)
        self._sub_handler = handler  # 保存 handler 引用，用于健康检测
        # Create a subscription with a 500ms publishing interval
        try:
            self.subscription = await self.client.create_subscription(500, handler)
            # To avoid overloading the server, we might need to batch them
            batch_size = 300
            for i in range(0, len(node_objs), batch_size):
                batch = node_objs[i:i+batch_size]
                try:
                    await self.subscription.subscribe_data_change(batch)
                    global_logger.info(f"Successfully subscribed data changes for a batch of {len(batch)} nodes.")
                except Exception as sub_e:
                    global_logger.error(f"Batch subscribe failed: {sub_e}")
        except Exception as e:
            global_logger.error(f"Failed to create start_subscription: {e}", exc_info=True)

    def _get_variant_type(self, node_id, value):
        """获取写入值的 Variant 类型"""
        variant_type = ua.VariantType.Boolean
        if isinstance(value, bool):
            variant_type = ua.VariantType.Boolean
        elif isinstance(value, int):
            variant_type = ua.VariantType.Int16
        elif isinstance(value, float):
            variant_type = ua.VariantType.Float

        if node_id in self.nodes:
            cached_type = self.nodes[node_id].get('type', '')
            if 'Int' in cached_type:
                variant_type = getattr(ua.VariantType, cached_type, ua.VariantType.Int16)
                value = int(value)
            elif 'Float' in cached_type or 'Double' in cached_type or 'Real' in cached_type:
                variant_type = getattr(ua.VariantType, cached_type, ua.VariantType.Double)
                value = float(value)
            elif 'String' in cached_type:
                variant_type = ua.VariantType.String
                value = str(value)
            elif 'Boolean' in cached_type:
                variant_type = ua.VariantType.Boolean
                value = bool(value)

        return variant_type, value

    async def write_node_value(self, node_id, value, display_name=None):
        """
        向 OPC 服务器下发控制指令
        """
        if not self.connected:
            global_logger.warning("Attempted to write while not connected.")
            return False

        target_display = display_name if display_name else node_id
        ts_start = time.time()
        try:
            node = self.client.get_node(node_id)

            variant_type, value = self._get_variant_type(node_id, value)
            data_value = ua.DataValue(ua.Variant(value, variant_type))

            await asyncio.wait_for(node.write_value(data_value), timeout=5.0)

            elapsed_ms = (time.time() - ts_start) * 1000
            global_logger.info(f"[WRITE] {target_display} = {value} | {elapsed_ms:.1f}ms")
            self._consecutive_write_failures = 0  # 成功时重置计数器
            return True

        except asyncio.TimeoutError:
            elapsed_ms = (time.time() - ts_start) * 1000
            global_logger.error(f"Timeout (5.0s) while writing to node {target_display} | elapsed={elapsed_ms:.1f}ms")
            self._on_write_failure()
            return False
        except Exception as e:
            elapsed_ms = (time.time() - ts_start) * 1000
            global_logger.error(f"Failed to write to node {target_display} | elapsed={elapsed_ms:.1f}ms | {e}")
            self._on_write_failure()
            return False

    async def write_values_batch(self, node_ids, value, display_names=None):
        """
        批量写入多个节点（方案B：使用 WriteList 减少 RTT）
        node_ids: 节点ID列表
        value: 写入的值（所有节点相同，如 True/False）
        display_names: 可选，对应节点的显示名列表
        返回: (成功数, 失败数)
        """
        if not self.connected:
            global_logger.warning("Attempted batch write while not connected.")
            return 0, len(node_ids)

        if not node_ids:
            return 0, 0

        ts_start = time.time()
        display_names = display_names or [None] * len(node_ids)

        try:
            nodes = [self.client.get_node(nid) for nid in node_ids]

            # 预计算所有节点的 Variant 类型
            variant_types = []
            values = []
            for nid, dv in zip(node_ids, display_names):
                vt, val = self._get_variant_type(nid, value)
                variant_types.append(vt)
                values.append(val)

            data_values = [ua.DataValue(ua.Variant(v, vt)) for v, vt in zip(values, variant_types)]

            # 使用 write_values 批量写入，单次网络往返
            await asyncio.wait_for(
                self.client.write_values(nodes, data_values),
                timeout=30.0  # 批量写入超时设长一些
            )

            elapsed_ms = (time.time() - ts_start) * 1000
            global_logger.info(f"[BATCH_WRITE] {len(node_ids)} nodes = {value} | {elapsed_ms:.1f}ms | avg={elapsed_ms/len(node_ids):.2f}ms/node")
            return len(node_ids), 0

        except asyncio.TimeoutError:
            elapsed_ms = (time.time() - ts_start) * 1000
            global_logger.error(f"Batch write timeout | {len(node_ids)} nodes | elapsed={elapsed_ms:.1f}ms")
            self._on_write_failure()
            return 0, len(node_ids)
        except Exception as e:
            elapsed_ms = (time.time() - ts_start) * 1000
            global_logger.error(f"Batch write failed | {len(node_ids)} nodes | elapsed={elapsed_ms:.1f}ms | {e}")
            self._on_write_failure()
            return 0, len(node_ids)

    def _on_write_failure(self):
        """写入失败时累加计数，连续失败超过阈值时触发重连"""
        self._consecutive_write_failures += 1
        global_logger.warning(
            f"Write failure count: {self._consecutive_write_failures}/{self._max_write_failures}"
        )
        if self._consecutive_write_failures >= self._max_write_failures:
            global_logger.error(
                f"Consecutive write failures reached threshold ({self._max_write_failures}). "
                f"Connection appears stale. Triggering reconnection..."
            )
            self.connected = False
            self._consecutive_write_failures = 0
            if self.on_connection_lost and callable(self.on_connection_lost):
                try:
                    self.on_connection_lost()
                except Exception as cb_err:
                    global_logger.error(f"on_connection_lost callback error: {cb_err}")

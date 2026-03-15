import asyncio
import os
import socket
import ipaddress
import datetime
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

    def datachange_notification(self, node, val, data):
        try:
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
    def __init__(self, host="localhost", port=48401, username="", password="", namespace_filter="ns=2;"):
        self.host = host
        self.port = port
        self.url = f"opc.tcp://{host}:{port}/"
        self.username = username
        self.password = password
        self.namespace_filter = namespace_filter  # 业务节点命名空间前缀，可配置
        self.client = None
        self.nodes = {}
        self.subscription = None
        self.connected = False
        self.on_connection_lost = None  # 断线回调钩子
        self._monitor_task = None
        
    async def connect(self):
        cert_file, key_file, hostname = generate_client_cert()
        self.client = Client(url=self.url)
        self.client.application_uri = "urn:example.org:FreeOpcUa:opcua-asyncio"
        
        # [Phase 10] 修正 45 分钟断线规律问题
        # 1. 调长安全通道寿命至 24 小时 (单位:ms)，规避 1 小时周期下 75% 时间点的续约失败
        self.client.secure_channel_timeout = 86400000 
        
        # 2. 校正 Session 活跃超时时长，设为 30 分钟 (单位:ms)，增加弱网环境下的 Session 持久度
        self.client.session_timeout = 1800000 
        
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

    async def _monitor_connection(self):
        """心跳检测：定期读取服务器状态节点，检测断线"""
        try:
            while self.connected:
                await asyncio.sleep(20) # 原5秒，放宽至20秒以防狂刷日志
                if not self.connected:
                    break
                try:
                    # 读取服务器状态节点（标准 OPC UA 节点）作为心跳
                    server_state = self.client.get_node("i=2259")
                    await server_state.read_value()
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
        # 先标记为不连通，防止监控任务进入下一次循环
        self.connected = False

        # 停止心跳监视
        if self._monitor_task:
            if not self._monitor_task.done():
                self._monitor_task.cancel()
                try:
                    await asyncio.wait_for(self._monitor_task, timeout=2.0)
                except Exception:
                    pass
            self._monitor_task = None
            
        try:
            if self.subscription:
                await self.subscription.delete()
        except Exception as e:
            global_logger.warning(f"Error deleting subscription: {e}")
        finally:
            self.subscription = None

        try:
            if self.client:
                await self.client.disconnect()
        except Exception as e:
            global_logger.warning(f"Error disconnecting client: {e}")
        finally:
            self.client = None
            # 清理节点引用，协助 GC
            self.nodes.clear()

    async def get_all_nodes(self):
        if not self.connected:
            return []
        
        
        objects_node = self.client.nodes.objects
        
        discovered_nodes = []
        async def browse_recursive(node, max_depth=5):
            if max_depth <= 0: return
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
                            await browse_recursive(child, max_depth - 1)
                    except Exception as child_err:
                        global_logger.warning(f"Error reading child {child}: {child_err}")
            except Exception as e:
                global_logger.warning(f"Error browsing node {node}: {e}")
        
        await browse_recursive(objects_node)
        self.nodes = {n['node_id']: n for n in discovered_nodes}
        return discovered_nodes

    async def start_subscription(self, callback):
        if not self.connected or not self.nodes: return
        node_objs = [n['node_obj'] for n in self.nodes.values()]
        handler = SubHandler(callback)
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

    async def write_node_value(self, node_id, value, display_name=None):
        """
        向 OPC 服务器下发控制指令
        """
        if not self.connected:
            global_logger.warning("Attempted to write while not connected.")
            return False
            
        target_display = display_name if display_name else node_id
        try:
            node = self.client.get_node(node_id)
            
            # 自动推断写入 Variant 类型（根据当前工程大部分为布尔量）
            variant_type = ua.VariantType.Boolean
            # 注意: Python 中 bool 是 int 的子类，bool 检查必须在 int 之前，
            # 否则 True/False 会被 isinstance(value, int) 捕获导致类型错判
            if isinstance(value, bool):
                variant_type = ua.VariantType.Boolean
            elif isinstance(value, int):
                variant_type = ua.VariantType.Int16
            elif isinstance(value, float):
                variant_type = ua.VariantType.Float
                
            # 试图通过缓存里的真实节点类型做精确对齐
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

            data_value = ua.DataValue(ua.Variant(value, variant_type))
            
            # 添加了 asyncio.wait_for 5秒内超时以避免在网络被挂起时导致的整个协程假死不工作
            await asyncio.wait_for(node.write_value(data_value), timeout=5.0)
            
            global_logger.info(f"Successfully wrote {value} to node {target_display}")
            return True
            
        except asyncio.TimeoutError:
            global_logger.error(f"Timeout (5.0s) while writing to node {target_display}. The server might be unreachable or hanging.")
            return False
        except Exception as e:
            global_logger.error(f"Failed to write to node {target_display}: {e}", exc_info=True)
            return False

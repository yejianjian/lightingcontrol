import asyncio
import os
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization, hashes
from cryptography import x509
from cryptography.x509.oid import NameOID
import datetime
from asyncua import Client
from asyncua.crypto import security_policies

# 自动生成客户端证书和私钥
import socket
import ipaddress

def generate_client_cert(cert_file="client_cert.der", key_file="client_key.pem"):
    # 动态获取当前主机名用于证书DNS信息
    hostname = socket.gethostname()

    print("生成客户端自签名证书...")
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
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
        # Valid for 10 years
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=3650)
    ).add_extension(
        x509.SubjectAlternativeName([
            x509.UniformResourceIdentifier(u"urn:example.org:FreeOpcUa:opcua-asyncio"),
            x509.DNSName(hostname),
            x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1"))
        ]),
        critical=False,
    ).add_extension(
        x509.KeyUsage(
            digital_signature=True,
            content_commitment=True,  # nonRepudiation
            key_encipherment=True,
            data_encipherment=True,
            key_agreement=False,
            key_cert_sign=False,
            crl_sign=False,
            encipher_only=False,
            decipher_only=False
        ),
        critical=True,
    ).add_extension(
        x509.ExtendedKeyUsage([
            x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
            x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
        ]),
        critical=True,
    ).sign(key, hashes.SHA256())

    with open(key_file, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.DER))
    
    print("证书生成完毕。")
    return cert_file, key_file


async def main():
    # 目标 OPC UA 服务器地址 (neuopc 默认端口通常为 4840)
    hostname = socket.gethostname()
    url = "opc.tcp://localhost:48401/"
    
    cert_file, key_file = generate_client_cert()

    # 初始化客户端
    client = Client(url=url)
    client.application_uri = "urn:example.org:FreeOpcUa:opcua-asyncio"
    
    # NeuOPC如果需要指定用户名密码，暂时按截图设置 user=admin pass=123456
    client.set_user("admin")
    client.set_password("123456")

    # 配置安全策略 (尝试基础的安全策略与证书)
    # 强制将我们上面获取的 server_cert_0.der 传入，让客户端信任它
    server_cert_path = "server_cert_0.der"
    if os.path.exists(server_cert_path):
        security_string = f"Basic256Sha256,SignAndEncrypt,{cert_file},{key_file},{server_cert_path}"
    else:
        security_string = f"Basic256Sha256,SignAndEncrypt,{cert_file},{key_file}"
        
    await client.set_security_string(security_string)
    
    # 强制信任服务器证书（针对 neuopc 的自签名证书）
    # asyncua 默认并不会严格验证服务器证书链，除非启用了专门的验证逻辑
    # 我们此处捕获可能的连接加密异常

    # 直接拦截 UaClient 层的 create_session 以强制篡改最终在 TCP 上传输的参数
    orig_create_session = client.uaclient.create_session
    async def custom_create_session(params):
        params.ServerUri = f"urn:{hostname}:neuopc"
        params.EndpointUrl = url
        return await orig_create_session(params)
    client.uaclient.create_session = custom_create_session

    print(f"尝试连接到 {url} ...")
    try:
        async with client:
            print("连接成功！")
            
            # 读取根节点
            root = client.nodes.root
            print(f"Root node is: {root}")
            
            # 获取命名空间数组
            namespaces = await client.get_namespace_array()
            print("Namespaces:")
            for idx, ns in enumerate(namespaces):
                print(f"  {idx}: {ns}")
                
            # 简单测试获取一个节点 (如 Server Status)
            server_node = client.nodes.server
            state = await server_node.get_child(["0:ServerStatus", "0:State"])
            state_val = await state.read_value()
            print(f"Server state is: {state_val}")

            # 在此循环保持心跳
            print("按 Ctrl+C 退出测试。")
            while True:
                await asyncio.sleep(1)

    except Exception as e:
        print(f"连接失败或发生错误: {e}")

if __name__ == "__main__":
    asyncio.run(main())

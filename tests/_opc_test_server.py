"""集成测试用的一次性 OPC UA 服务器进程。

用法::

    python tests/_opc_test_server.py <port> <cert_der_without_ext>

使用 Basic256Sha256/SignAndEncrypt，只暴露两个可写布尔点位（模拟两路灯）。
测试通过终止该进程来模拟"服务器/网络消失"，再重新拉起以验证引擎的自动重连。
"""

import asyncio
import datetime
import ipaddress
import os
import sys

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from asyncua import Server, ua

APP_URI = "urn:example.org:FreeOpcUa:opcua-asyncio"


def make_cert(prefix):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u"opc-test-server")])
    san = [
        x509.UniformResourceIdentifier(APP_URI),
        x509.DNSName("localhost"),
        x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
    ]
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc))
        .not_valid_after(datetime.datetime(2099, 12, 31, tzinfo=datetime.timezone.utc))
        .add_extension(x509.SubjectAlternativeName(san), critical=False)
        .add_extension(x509.ExtendedKeyUsage([
            x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
            x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
        ]), critical=False)
        .sign(key, hashes.SHA256())
    )
    with open(prefix + ".der", "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.DER))
    with open(prefix + ".pem", "wb") as f:
        f.write(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ))
    return prefix + ".der", prefix + ".pem"


async def main(port, prefix):
    cert, key = make_cert(prefix)
    server = Server()
    await server.init()
    server.set_endpoint(f"opc.tcp://127.0.0.1:{port}/")
    server.set_server_name("LightingTestServer")
    # 显式放开权限：asyncua 各版本的默认权限规则不一致（1.1.5 会拒绝匿名写入），
    # 测试需要的是"能读写"而不是"能鉴权"。
    try:
        from asyncua.crypto.permission_rules import SimpleRoleRuleset
        server.set_security_policy(
            [ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt],
            permission_ruleset=SimpleRoleRuleset(),
        )
    except Exception as e:
        print(f"permission ruleset setup skipped: {e}")

    await server.load_certificate(cert)
    await server.load_private_key(key)

    idx = await server.register_namespace("http://test.lighting")
    obj = await server.nodes.objects.add_object(idx, "Lights")
    for i in (1, 2):
        var = await obj.add_variable(f"ns={idx};s=Light_{i}", f"Light {i}", False)
        await var.set_writable()

    async with server:
        sys.stdout.write("READY\n")
        sys.stdout.flush()
        if os.environ.get("OPC_TEST_SERVER_MIRROR"):
            # 把服务端日志镜像到文件，便于测试失败后排查服务端到底收到了什么
            import logging
            import logging.handlers
            mirror = os.environ["OPC_TEST_SERVER_MIRROR"]
            handler = logging.FileHandler(mirror, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
            root = logging.getLogger()
            root.setLevel(logging.INFO)
            root.addHandler(handler)
        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    port = int(sys.argv[1])
    prefix = sys.argv[2]
    os.makedirs(os.path.dirname(prefix), exist_ok=True)
    try:
        asyncio.run(main(port, prefix))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass

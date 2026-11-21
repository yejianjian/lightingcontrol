"""Local asyncua server granting a short secure-channel lifetime, for soak-tool validation."""
import asyncio, datetime, ipaddress, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from asyncua import Server, ua
from asyncua.common import connection as ua_connection
from asyncua.crypto.permission_rules import SimpleRoleRuleset, User, UserRole

LIFETIME_MS = int(os.environ.get("LIFETIME_MS", "6000"))
# 可选：设置 AUTH_USER/AUTH_PASSWORD 后服务器只接受用户名密码登录（拒绝匿名），
# 用于复现"网关要求鉴权 → 客户端匿名连接被 BadUserAccessDenied 拒绝"的现场场景。
AUTH_USER = os.environ.get("AUTH_USER", "")
AUTH_PASSWORD = os.environ.get("AUTH_PASSWORD", "")


class _PasswordUserManager:
    def __init__(self, username, password):
        self._username = username
        self._password = password

    def get_user(self, iserver, username=None, password=None, certificate=None):
        if username == self._username and password == self._password:
            return User(role=UserRole.User)
        return None
APP_URI = "urn:example.org:FreeOpcUa:opcua-asyncio"

def make_cert(prefix):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "short-lifetime-server")])
    san = [x509.UniformResourceIdentifier(APP_URI), x509.DNSName("localhost"),
           x509.IPAddress(ipaddress.IPv4Address("127.0.0.1"))]
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime(2000,1,1,tzinfo=datetime.timezone.utc))
            .not_valid_after(datetime.datetime(2099,12,31,tzinfo=datetime.timezone.utc))
            .add_extension(x509.SubjectAlternativeName(san), critical=False)
            .add_extension(x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                                                  x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .sign(key, hashes.SHA256()))
    open(prefix+".der","wb").write(cert.public_bytes(serialization.Encoding.DER))
    open(prefix+".pem","wb").write(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
    return prefix+".der", prefix+".pem"

orig = ua_connection.SecureConnection.open
def patched(conn, params, server):
    result = orig(conn, params, server)
    result.SecurityToken.RevisedLifetime = LIFETIME_MS
    if params.RequestType == ua.SecurityTokenRequestType.Renew:
        print("[server] RENEW", flush=True)
    return result
ua_connection.SecureConnection.open = patched

async def enforce(server):
    while True:
        await asyncio.sleep(0.3)
        now = datetime.datetime.now(datetime.timezone.utc)
        for proto in list(getattr(server.bserver, "clients", [])):
            try:
                conn = proto.processor._connection
                if not conn.is_open():
                    continue
                tok = conn.security_token
                if now > tok.CreatedAt + datetime.timedelta(milliseconds=tok.RevisedLifetime):
                    print("[server] TOKEN EXPIRED -> closing", flush=True)
                    proto.transport.close()
            except Exception:
                pass

async def main(port):
    cert, key = make_cert(os.path.join(os.path.dirname(os.path.abspath(__file__)), "short_server_cert"))
    user_manager = _PasswordUserManager(AUTH_USER, AUTH_PASSWORD) if AUTH_USER else None
    server = Server(user_manager=user_manager) if user_manager else Server()
    await server.init()
    server.set_endpoint(f"opc.tcp://127.0.0.1:{port}/")
    server.set_server_name("ShortLifetimeServer")
    server.set_security_policy([ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt],
                               permission_ruleset=SimpleRoleRuleset())
    if AUTH_USER:
        # 关闭匿名登录：匿名 ActivateSession 会被服务端以 BadUserAccessDenied 拒绝
        server.set_identity_tokens([ua.UserNameIdentityToken])
    await server.load_certificate(cert); await server.load_private_key(key)
    idx = await server.register_namespace("http://test.lighting")
    obj = await server.nodes.objects.add_object(idx, "Lights")
    for i in (1,2):
        var = await obj.add_variable(f"ns={idx};s=Light_{i}", f"Light {i}", False)
        await var.set_writable()
    async with server:
        print(f"[server] listening on {port}, lifetime={LIFETIME_MS}ms", flush=True)
        await enforce(server)

if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1])))

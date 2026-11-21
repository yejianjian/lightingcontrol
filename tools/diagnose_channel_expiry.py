"""Ad-hoc reproduction: gateway enforces a short secure-channel token lifetime.

Server = asyncua Basic256Sha256 server whose OpenSecureChannel response revises the
channel lifetime down to SHORT_MS (like neuopc / a real OPC UA gateway granting 1h),
and which closes the TCP transport if the client does not renew before the token
expires. Run with ./venv/Scripts/python.exe.
"""
import asyncio
import datetime
import ipaddress
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from asyncua import Server, ua
from asyncua.common import connection as ua_connection

SHORT_MS = 8000
APP_URI = "urn:example.org:FreeOpcUa:opcua-asyncio"

events = []


def make_cert(prefix):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "opc-repro-server")])
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


_orig_open = ua_connection.SecureConnection.open


def _patched_open(self, params, server):
    result = _orig_open(self, params, server)
    # Simulate a real gateway that caps the granted lifetime.
    result.SecurityToken.RevisedLifetime = SHORT_MS
    kind = "RENEW" if params.RequestType == ua.SecurityTokenRequestType.Renew else "ISSUE"
    events.append((time.monotonic(), "OPEN_" + kind))
    print(f"[server] {kind} channel opened, granted lifetime={SHORT_MS}ms", flush=True)
    return result


async def lifetime_enforcer(server):
    """Close the socket when a token has been in use past its lifetime (gateway behaviour)."""
    while True:
        await asyncio.sleep(0.5)
        now = datetime.datetime.now(datetime.timezone.utc)
        for proto in list(getattr(server.bserver, "clients", [])):
            try:
                conn = proto.processor._connection
                if not conn.is_open():
                    continue
                tok = conn.security_token
                deadline = tok.CreatedAt + datetime.timedelta(milliseconds=tok.RevisedLifetime)
                if now > deadline:
                    events.append((time.monotonic(), "EXPIRED"))
                    print(f"[server] token expired at {deadline}; closing connection", flush=True)
                    proto.transport.close()
            except Exception as e:
                print("[server] enforcer error:", e, flush=True)


async def main():
    port = 48611
    prefix = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server_cert")
    cert, key = make_cert(prefix)

    ua_connection.SecureConnection.open = _patched_open

    server = Server()
    await server.init()
    server.set_endpoint(f"opc.tcp://127.0.0.1:{port}/")
    server.set_server_name("ReproServer")
    from asyncua.crypto.permission_rules import SimpleRoleRuleset
    server.set_security_policy(
        [ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt],
        permission_ruleset=SimpleRoleRuleset(),
    )
    await server.load_certificate(cert)
    await server.load_private_key(key)
    idx = await server.register_namespace("http://test.lighting")
    obj = await server.nodes.objects.add_object(idx, "Lights")
    for i in (1, 2):
        var = await obj.add_variable(f"ns={idx};s=Light_{i}", f"Light {i}", False)
        await var.set_writable()

    from opc.client_engine import OpcClientEngine

    engine = OpcClientEngine(host="127.0.0.1", port=port, namespace_filter="ns=2;", browse_max_depth=2)
    states = []
    engine.on_state_change = lambda s, d: (states.append((time.monotonic(), s)), print(f"[engine] STATE -> {s} ({d.get('reason')})", flush=True))
    engine.on_connection_lost = lambda: print("[engine] on_connection_lost", flush=True)

    async with server:
        enforcer = asyncio.create_task(lifetime_enforcer(server))
        await asyncio.sleep(0.5)
        t0 = time.monotonic()
        try:
            await engine.connect()
            engine.set_subscription_callback(lambda *a: None)
            await engine.bootstrap()
            await engine.reconnect_forever()
            print("[test] connected; observing for 26s ...", flush=True)
            await asyncio.sleep(26)
        finally:
            enforcer.cancel()
            await engine.disconnect()

    print("\n==== RESULT ====")
    print("duration: %.1fs" % (time.monotonic() - t0))
    print("state transitions:", [(round(t - t0, 1), s) for t, s in states])
    print("server events:", [(round(t - t0, 1), e) for t, e in events])
    opens = [e for _, e in events if e == "OPEN_RENEW"]
    print("renewals:", len(opens), "| expiries:", sum(1 for _, e in events if e == "EXPIRED"))
    print("engine state:", engine.state)


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
from asyncua.client.client import Client
from cryptography import x509
from cryptography.hazmat.backends import default_backend

async def main():
    url = "opc.tcp://127.0.0.1:48401"
    client = Client(url=url)
    try:
        endpoints = await client.connect_and_get_server_endpoints()
        for i, ep in enumerate(endpoints):
            if ep.ServerCertificate:
                cert = x509.load_der_x509_certificate(ep.ServerCertificate, default_backend())
                print(f"Server Certificate {i}:")
                print(f"  Subject: {cert.subject}")
                print(f"  Issuer: {cert.issuer}")
                print(f"  Serial: {cert.serial_number}")
                print(f"  Not valid before: {cert.not_valid_before_utc}")
                print(f"  Not valid after: {cert.not_valid_after_utc}")
                
                try:
                    ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
                    print(f"  SAN: {ext.value}")
                except x509.ExtensionNotFound:
                    print("  No SAN extension found.")
                    
                with open(f"server_cert_{i}.der", "wb") as f:
                    f.write(ep.ServerCertificate)
                print(f"  Saved to server_cert_{i}.der\n")
    except Exception as e:
        print(f"Error getting endpoints: {e}")

if __name__ == "__main__":
    asyncio.run(main())

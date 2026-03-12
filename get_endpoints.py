import asyncio
from asyncua.client.client import Client

async def main():
    url = "opc.tcp://127.0.0.1:48401"
    client = Client(url=url)
    try:
        endpoints = await client.connect_and_get_server_endpoints()
        for i, ep in enumerate(endpoints):
            print(f"Endpoint {i}:")
            print(f"  URL: {ep.EndpointUrl}")
            print(f"  Server: {ep.Server.ApplicationUri}")
            print(f"  SecurityMode: {ep.SecurityMode}")
            print(f"  SecurityPolicyUri: {ep.SecurityPolicyUri}")
            cert = ep.ServerCertificate
            print(f"  Certificate length: {len(cert) if cert else 0} bytes")
    except Exception as e:
        print(f"Error getting endpoints: {e}")

if __name__ == "__main__":
    asyncio.run(main())

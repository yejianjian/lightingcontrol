import asyncio
import socket
import os
from asyncua import Client

async def main():
    hostname = socket.gethostname()
    url = f"opc.tcp://localhost:48401/"
    client = Client(url=url)
    client.set_user("admin")
    client.set_password("123456")
    client.application_uri = "urn:example.org:FreeOpcUa:opcua-asyncio"
    
    cert_file, key_file, server_cert_path = "client_cert.der", "client_key.pem", "server_cert_0.der"
    if os.path.exists(server_cert_path):
        security_string = f"Basic256Sha256,SignAndEncrypt,{cert_file},{key_file},{server_cert_path}"
    else:
        security_string = f"Basic256Sha256,SignAndEncrypt,{cert_file},{key_file}"
    await client.set_security_string(security_string)

    orig_create_session = client.uaclient.create_session
    async def custom_create_session(params):
        params.ServerUri = f"urn:{hostname}:neuopc"
        params.EndpointUrl = url
        return await orig_create_session(params)
    client.uaclient.create_session = custom_create_session

    try:
        await client.connect()
        print("Connected to OPC UA Server")
        
        objects = client.nodes.objects
        children = await objects.get_children()
        print("\nObjects children:")
        for child in children:
            name = (await child.read_display_name()).Text
            node_class = await child.read_node_class()
            print(f"  - {name} ({child.nodeid}, {node_class})")
            
        # 尝试遍历非 Server 的节点
        for child in children:
            name = (await child.read_display_name()).Text
            if name != "Server":
                print(f"\nBrowsing {name}:")
                sub_children = await child.get_children()
                for sub in sub_children:
                    sub_name = (await sub.read_display_name()).Text
                    print(f"    - {sub_name} ({sub.nodeid})")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
from asyncua import Client

async def test_leak():
    client = Client(url="opc.tcp://10.255.255.1:4840") # unreachable mock
    client.session_timeout = 1000
    try:
        await asyncio.wait_for(client.connect(), timeout=2.0)
    except Exception as e:
        print(f"Connection failed: {e}")
        
    print("Wait 5 seconds to see if anything is still running...")
    await asyncio.sleep(5)
    
    # Try properly disconnecting
    try:
        await client.disconnect()
        print("Disconnected clean")
    except Exception as e:
        print(f"Disconnect failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_leak())

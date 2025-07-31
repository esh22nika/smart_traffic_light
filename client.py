import random
import time
import xmlrpc.client

SERVER_IP = "192.168.0.231"  # Replace with your server's IP
PORT = 8000

proxy = xmlrpc.client.ServerProxy(f"http://{SERVER_IP}:{PORT}/", allow_none=True)

previous_pair = None

def signal_controller():
    global previous_pair
    pair_1 = {1, 2}
    pair_2 = {3, 4}

    if previous_pair == pair_1:
        active_pair = pair_2
    elif previous_pair == pair_2:
        active_pair = pair_1
    else:
        active_pair = random.choice([pair_1, pair_2])

    previous_pair = active_pair
    print(f"\n[Controller] Generated active pair: {list(active_pair)}")

    try:
        messages = proxy.signal_manipulator(list(active_pair))
        print("[Client] Server Response:")
        for msg in messages:
            print(msg)
    except Exception as e:
        print(f"[Client] Error communicating with server: {e}")

while True:
    signal_controller()
    time.sleep(10)
# t_signal.py
# Traffic client: Berkeley client + traffic sensing + VIP simulation
import time
import random
import threading
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

CONTROLLER_IP = "http://192.168.0.168:8000"  # controller endpoint
MY_PORT = 7000
MY_NAME = "t_signal"

# Initial skew: +45 minutes
local_skew = 45 * 60

signal_pairs = {
    "1": [1, 2],
    "2": [1, 2],
    "3": [3, 4],
    "4": [3, 4]
}

VIP_PROB = 0.35  # chance that the sensed vehicle is VIP (tweak for tests)

def traffic_detection_loop():
    proxy = ServerProxy(CONTROLLER_IP, allow_none=True)
    while True:
        sensed = str(random.choice([1, 2, 3, 4]))
        pair = signal_pairs[sensed]
        try:
            if random.random() < VIP_PROB:
                print(f"\n[{MY_NAME}] 🚨 VIP detected at {pair}. Notifying controller...")
                proxy.vip_arrival(pair)
            else:
                print(f"\n[{MY_NAME}] 🚦 Normal traffic at {sensed}. Requesting switch for {pair}")
                proxy.signal_controller(pair)
        except Exception as e:
            print(f"[{MY_NAME}] ❌ Error contacting controller: {e}")
        time.sleep(6)

# ---- Berkeley client methods ----
def get_clock_value(server_time):
    own_time = time.time() + local_skew
    diff = own_time - server_time
    print(f"[{MY_NAME}] ⏱ get_clock_value → diff={diff:+.2f}s")
    return diff

def set_time(new_time):
    global local_skew
    now = time.time()
    local_skew = new_time - now
    print(f"[{MY_NAME}] ⏳ set_time → new_skew={local_skew:+.2f}s (local={time.ctime(now)})")
    return "OK"

# ---- RPC server for Berkeley ----
if _name_ == "_main_":
    t = threading.Thread(target=traffic_detection_loop, daemon=True)
    t.start()

    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")
    print(f"[{MY_NAME}] 🖥 RPC server on :{MY_PORT}, initial skew={local_skew:+.2f}s")
    server.serve_forever()

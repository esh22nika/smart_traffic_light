# t_signal.py (updated for Berkeley clock sync as Client 1)
#client 1 task 4

import time
import random
import threading
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

# -------------------------
# CONFIGURATION
# -------------------------
CONTROLLER_IP = "http://172.16.182.18:8000"  # Controller RPC endpoint
MY_PORT = 7000                               # Port for t_signal's own RPC server
MY_NAME = "t_signal"

# Initial clock skew: +45 minutes (in seconds)
local_skew = 45 * 60

# Map sensed signal IDs to pairs
signal_pairs = {
    "1": [1, 2],
    "2": [1, 2],
    "3": [3, 4],
    "4": [3, 4]
}

# -------------------------
# TRAFFIC DETECTION LOGIC
# -------------------------
def traffic_detection_loop():
    proxy = ServerProxy(CONTROLLER_IP, allow_none=True)
    while True:
        sensed = str(random.choice([1, 2, 3, 4]))
        target_pair = signal_pairs[sensed]
        try:
            print(f"\n[{MY_NAME}] 🚦 Sensed traffic at signal {sensed}. Requesting switch for {target_pair}")
            proxy.signal_controller(target_pair)
        except Exception as e:
            print(f"[{MY_NAME}] ❌ Error contacting controller: {e}")
        time.sleep(6)


# -------------------------
# BERKELEY CLIENT METHODS
# -------------------------
def get_clock_value(server_time):
    """
    Step 2 & 3: Return own_time - server_time
    """
    own_time = time.time() + local_skew
    clock_value = own_time - server_time
    print(f"[{MY_NAME}] ⏱ get_clock_value → own_time={format_time(own_time)}, server_time={format_time(server_time)}, diff={clock_value:+.2f}s")
    return clock_value


def set_time(new_time):
    """
    Step 6 & 7: Set local clock to new_time
    """
    global local_skew
    current_actual_time = time.time()
    local_skew = new_time - current_actual_time
    print(f"[{MY_NAME}] ⏳ set_time → new_time={format_time(new_time)}, actual_time={format_time(current_actual_time)}, new_skew={local_skew:+.2f}s")
    return "OK"


def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


# -------------------------
# MAIN ENTRY POINT
# -------------------------
if __name__ == "__main__":
    # Start traffic detection in a separate thread
    traffic_thread = threading.Thread(target=traffic_detection_loop, daemon=True)
    traffic_thread.start()

    # Start RPC server for Berkeley sync
    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")
    print(f"[{MY_NAME}] 🖥 RPC server listening on port {MY_PORT} (initial skew = {local_skew:+.2f}s)")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{MY_NAME}] Shutting down...")

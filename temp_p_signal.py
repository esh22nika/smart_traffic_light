# p_signal.py (updated for Berkeley clock sync as Client 2)
#client 2 task 4
import time
import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

# -------------------------
# CONFIGURATION
# -------------------------
MY_PORT = 9000
MY_NAME = "p_signal"

# Initial clock skew: -45 minutes (in seconds)
local_skew = -45 * 60

# -------------------------
# EXISTING PEDESTRIAN ACK LOGIC
# -------------------------
def p_signal(target_pair):
    print(f"[{MY_NAME}] ✅ Acknowledged request for traffic pair {target_pair}")
    return "OK"

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
    # RPC server to handle both pedestrian acknowledgements and Berkeley sync
    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
    server.register_function(p_signal, "p_signal")
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")

    print(f"[{MY_NAME}] 🖥 RPC server listening on port {MY_PORT} (initial skew = {local_skew:+.2f}s)")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{MY_NAME}] Shutting down...")


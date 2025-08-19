# p_signal.py
# Pedestrian server: Berkeley client + ack
import time
from xmlrpc.server import SimpleXMLRPCServer

MY_PORT = 9000
MY_NAME = "p_signal"

# Initial skew: -45 minutes
local_skew = -45 * 60

def p_signal(target_pair):
    print(f"[{MY_NAME}] ✅ Ack for traffic pair {target_pair}")
    return "OK"

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

if _name_ == "_main_":
    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
    server.register_function(p_signal, "p_signal")
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")
    print(f"[{MY_NAME}] 🖥 RPC server on :{MY_PORT}, initial skew={local_skew:+.2f}s")
    server.serve_forever()

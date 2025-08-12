from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading

# -------------------------
# CONFIGURATION
# -------------------------
CLIENTS = {
    "t_signal": "http://172.16.182.10:7000",  # Client1
    "p_signal": "http://172.16.182.3:9000",   # Client2
}
PEDESTRIAN_IP = CLIENTS["p_signal"]
RESPONSE_TIMEOUT = 5  # seconds

server_skew = 0.0
state_lock = threading.Lock()

# -------------------------
# Timeout Transport
# -------------------------
class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn

# -------------------------
# Signal state
# -------------------------
signal_status = {
    1: "RED", 2: "RED", 3: "GREEN", 4: "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}

# -------------------------
# Clock helpers
# -------------------------
def get_server_time():
    return time.time() + server_skew

def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

# -------------------------
# Berkeley sync (once per cycle)
# -------------------------
def berkeley_cycle_once():
    global server_skew
    print("\n[Berkeley] === Starting Berkeley sync cycle ===")
    server_time = get_server_time()
    print(f"[Berkeley] Step 1 — Broadcasting server time: {format_time(server_time)}")

    clock_values = {"controller": 0.0}

    # Step 2 & 3
    for name, url in CLIENTS.items():
        try:
            proxy = ServerProxy(url, allow_none=True, transport=TimeoutTransport(RESPONSE_TIMEOUT))
            print(f"[Berkeley] Contacting {name} at {url}...")
            clock_value = float(proxy.get_clock_value(server_time))
            clock_values[name] = clock_value
            print(f"[Berkeley] {name} clock_value: {clock_value:+.2f}s")
        except Exception as e:
            print(f"[Berkeley] ❌ Failed to get clock value from {name}: {e}")

    if not clock_values:
        print("[Berkeley] ❌ No clock values collected.")
        return

    avg_offset = sum(clock_values.values()) / len(clock_values)
    print(f"[Berkeley] Step 4 — Average offset: {avg_offset:+.2f}s")

    new_clock_epoch = server_time + avg_offset

    print("\n[Berkeley] Step 5 — Adjustment table:")
    print(f"{'Process':<12}{'Old Clock':<22}{'Offset':<12}{'New Clock':<22}")
    print("-" * 70)
    print(f"{'controller':<12}{format_time(server_time):<22}{'0.00s':<12}{format_time(new_clock_epoch):<22}")
    for name in CLIENTS:
        if name in clock_values:
            old_time = server_time + clock_values[name]
            print(f"{name:<12}{format_time(old_time):<22}{clock_values[name]:+0.2f}s{format_time(new_clock_epoch):>12}")
        else:
            print(f"{name:<12}{'UNREACHABLE':<22}{'N/A':<12}{format_time(new_clock_epoch):<22}")

    # Step 6 & 7
    for name, url in CLIENTS.items():
        try:
            proxy = ServerProxy(url, allow_none=True, transport=TimeoutTransport(RESPONSE_TIMEOUT))
            proxy.set_time(new_clock_epoch)
            print(f"[Berkeley] ✅ Sent new time to {name}")
        except Exception as e:
            print(f"[Berkeley] ❌ Failed to send time to {name}: {e}")

    adjustment = new_clock_epoch - server_time
    server_skew += adjustment
    print(f"[Berkeley] Controller adjusted by {adjustment:+.2f}s (new skew={server_skew:+.2f}s)")
    print("[Berkeley] === Cycle complete ===\n")

# -------------------------
# Traffic control logic
# -------------------------
def signal_controller(target_pair):
    # Step 1: Sync clocks before changing signals
    berkeley_cycle_once()

    print(f"\n[Controller] 📥 Received request to switch traffic pair {target_pair}")

    # Step 2: Call p_signal for ack
    try:
        pedestrian_proxy = ServerProxy(PEDESTRIAN_IP, allow_none=True)
        response = pedestrian_proxy.p_signal(target_pair)
        if response != "OK":
            print("[Controller] ❌ p_signal returned unexpected response!")
            return False
        print("[Controller] ✅ p_signal acknowledged. Proceeding...")
    except Exception as e:
        print(f"[Controller] ❌ Error contacting p_signal: {e}")
        return False

    # Step 3: Pedestrian logic
    handle_pedestrian_signals(target_pair)

    # Step 4: Traffic logic
    handle_traffic_signals(target_pair)

    print("[Controller] 🔁 Completed full signal cycle.\n")
    return True

def handle_pedestrian_signals(target_pair):
    red_group = target_pair
    green_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    print(f"[Controller] ⚠️ Pedestrian signals {[f'P{x}' for x in red_group]} → Blinking RED (5s)")
    for _ in range(5):
        with state_lock:
            for sig in red_group:
                signal_status[f"P{sig}"] = "BLINKING RED"
        time.sleep(1)

    with state_lock:
        for sig in red_group:
            signal_status[f"P{sig}"] = "RED"
    print(f"[Controller] 🔴 Pedestrian {red_group} → RED")

    with state_lock:
        for sig in green_group:
            signal_status[f"P{sig}"] = "YELLOW"
    print(f"[Controller] 🟡 Pedestrian {green_group} → YELLOW")
    time.sleep(5)

    with state_lock:
        for sig in green_group:
            signal_status[f"P{sig}"] = "GREEN"
    print(f"[Controller] 🟢 Pedestrian {green_group} → GREEN")

def handle_traffic_signals(target_pair):
    red_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    with state_lock:
        for sig in red_group:
            signal_status[sig] = "YELLOW"
    print(f"[Controller] 🟡 Traffic {red_group} → YELLOW")
    time.sleep(2)

    with state_lock:
        for sig in red_group:
            signal_status[sig] = "RED"
    print(f"[Controller] 🔴 Traffic {red_group} → RED")

    with state_lock:
        for sig in target_pair:
            signal_status[sig] = "GREEN"
    print(f"[Controller] 🟢 Traffic {target_pair} → GREEN")

    with state_lock:
        print(f"[Controller] ✅ Final Signal Status: {signal_status}")

# -------------------------
# RPC server setup
# -------------------------
def ping():
    return "OK"

def get_signal_status():
    with state_lock:
        return signal_status

if __name__ == "__main__":
    server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)
    server.register_function(signal_controller, "signal_controller")
    server.register_function(ping, "ping")
    server.register_function(get_signal_status, "get_signal_status")

    print("[Controller] 🚦 RPC server running on port 8000")
    print("[Controller] ℹ️ Will run Berkeley sync before every signal change")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Controller] Shutting down...")

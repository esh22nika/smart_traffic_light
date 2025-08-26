# controller.py
# Master: signal logic + Berkeley sync + VIP/Deadlock handling
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading

# ---------------- CONFIG ----------------
CLIENTS = {
    "t_signal": "http://172.16.182.10:7000",  # change to your t_signal host:port
    "p_signal": "http://172.16.182.3:9000",   # change to your p_signal host:port
}
PEDESTRIAN_IP = CLIENTS["p_signal"]
RESPONSE_TIMEOUT = 5
VIP_CROSSING_TIME = 4  # seconds a VIP is assumed to take to cross

server_skew = 0.0
state_lock = threading.Lock()

# Track which pair (1-2 or 3-4) has pending VIPs
vip_pending = {"12": False, "34": False}

# ------------- Timeout transport ----------
class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout
    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn

# ------------- Signal state ---------------
signal_status = {
    1: "RED", 2: "RED", 3: "GREEN", 4: "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}

def _current_green_pair():
    """Return [1,2] or [3,4] depending on current traffic state."""
    with state_lock:
        g12 = (signal_status[1] == "GREEN" and signal_status[2] == "GREEN")
        return [1, 2] if g12 else [3, 4]

# ------------- Clock helpers --------------
def get_server_time():
    return time.time() + server_skew
def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

# ---------- Berkeley sync (once) ----------
class _T(TimeoutTransport): pass  # alias so we type less

def berkeley_cycle_once():
    global server_skew
    print("\n[Berkeley] === Starting Berkeley sync cycle ===")
    server_time = get_server_time()
    print(f"[Berkeley] Step 1 — Broadcasting server time: {format_time(server_time)}")

    clock_values = {"controller": 0.0}
    for name, url in CLIENTS.items():
        try:
            proxy = ServerProxy(url, allow_none=True, transport=_T(RESPONSE_TIMEOUT))
            print(f"[Berkeley] Contacting {name} at {url}...")
            cv = float(proxy.get_clock_value(server_time))
            clock_values[name] = cv
            print(f"[Berkeley] {name} clock_value: {cv:+.2f}s")
        except Exception as e:
            print(f"[Berkeley] ❌ Failed to get clock value from {name}: {e}")

    avg_offset = sum(clock_values.values()) / len(clock_values)
    new_epoch = server_time + avg_offset

    print("\n[Berkeley] Step 5 — Adjustment table:")
    print(f"{'Process':<12}{'Old Clock':<22}{'Offset':<12}{'New Clock':<22}")
    print("-"*70)
    print(f"{'controller':<12}{format_time(server_time):<22}{'0.00s':<12}{format_time(new_epoch):<22}")
    for name in CLIENTS:
        if name in clock_values:
            old_time = server_time + clock_values[name]
            print(f"{name:<12}{format_time(old_time):<22}{clock_values[name]:+0.2f}s{format_time(new_epoch):>12}")
        else:
            print(f"{name:<12}{'UNREACHABLE':<22}{'N/A':<12}{format_time(new_epoch):<22}")

    for name, url in CLIENTS.items():
        try:
            ServerProxy(url, allow_none=True, transport=_T(RESPONSE_TIMEOUT)).set_time(new_epoch)
            print(f"[Berkeley] ✅ Sent new time to {name}")
        except Exception as e:
            print(f"[Berkeley] ❌ Failed to send time to {name}: {e}")

    server_skew += (new_epoch - server_time)
    print(f"[Berkeley] Controller adjusted by {(new_epoch - server_time):+.2f}s (new skew={server_skew:+.2f}s)")
    print("[Berkeley] === Cycle complete ===\n")

# ------------- VIP handling ---------------
def vip_arrival(target_pair):
    """
    RPC: a VIP has arrived wanting priority on target_pair (either [1,2] or [3,4]).
    This may trigger the deadlock-resolution flow.
    """
    key = "12" if target_pair == [1, 2] else "34"
    with state_lock:
        vip_pending[key] = True
    print(f"\n[Controller|VIP] 🚨 VIP arrival reported for {target_pair}. State: {vip_pending}")

    # synchronize clocks before any decisions
    berkeley_cycle_once()

    _resolve_vip_deadlock()
    return True

def _resolve_vip_deadlock():
    """
    If VIPs are pending on both sides, let the currently GREEN side go first,
    then switch to the other. If only one side has VIP, serve it.
    During any switch, we reuse the same pedestrian + traffic routines.
    """
    while True:
        with state_lock:
            both = vip_pending["12"] and vip_pending["34"]
            one12, one34 = vip_pending["12"], vip_pending["34"]

        if not (one12 or one34):
            return  # nothing to do

        if both:
            first = _current_green_pair()         # let current green go first
            second = [3, 4] if first == [1, 2] else [1, 2]
            print(f"[Controller|VIP] Deadlock: VIPs on both pairs. Current green {first} goes first.")

            _let_vip_cross_without_switch(first)
            with state_lock:
                vip_pending["12" if first == [1, 2] else "34"] = False

            print(f"[Controller|VIP] Now switching to {second} so their VIP can pass.")
            _switch_to(second)  # handles pedestrians + traffic
            _let_vip_cross_without_switch(second)
            with state_lock:
                vip_pending["12" if second == [1, 2] else "34"] = False

        else:
            # Single VIP pending
            if one12:
                _serve_single_vip([1, 2], "12")
            elif one34:
                _serve_single_vip([3, 4], "34")

def _serve_single_vip(pair, key):
    if _current_green_pair() == pair:
        print(f"[Controller|VIP] {pair} already GREEN. Letting VIP pass now.")
        _let_vip_cross_without_switch(pair)
    else:
        print(f"[Controller|VIP] Switching to {pair} to let VIP pass.")
        _switch_to(pair)  # full pedestrian + traffic transition
        _let_vip_cross_without_switch(pair)
    with state_lock:
        vip_pending[key] = False
    print(f"[Controller|VIP] ✅ VIP on {pair} served. Pending state: {vip_pending}")

def _let_vip_cross_without_switch(pair):
    # Keep current green as is; just wait to simulate crossing
    print(f"[Controller|VIP] 🟢 VIP crossing on {pair}... (wait {VIP_CROSSING_TIME}s)")
    time.sleep(VIP_CROSSING_TIME)
    print(f"[Controller|VIP] ✅ VIP on {pair} has crossed.")

def _switch_to(target_pair):
    """
    Perform the same steps you already had: pedestrians + traffic updates
    so that 'target_pair' becomes GREEN at the end.
    """
    # Then traffic lights
    handle_traffic_signals(target_pair)
    # Pedestrians first
    handle_pedestrian_signals(target_pair)


# -------- Normal (non-VIP) requests -------
def signal_controller(target_pair):
    # sync clocks before routine switch
    berkeley_cycle_once()

    print(f"\n[Controller] 📥 Received request to switch traffic pair {target_pair}")

    # Acknowledge with p_signal
    try:
        response = ServerProxy(PEDESTRIAN_IP, allow_none=True).p_signal(target_pair)
        if response != "OK":
            print("[Controller] ❌ p_signal returned unexpected response!")
            return False
        print("[Controller] ✅ p_signal acknowledged. Proceeding...")
    except Exception as e:
        print(f"[Controller] ❌ Error contacting p_signal: {e}")
        return False

    _switch_to(target_pair)
    print("[Controller] 🔁 Completed full signal cycle.\n")
    return True

# -------- Pedestrian + Traffic routines ----
def handle_pedestrian_signals(target_pair):
    red_group = target_pair
    green_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    print(f"[Controller] ⚠ Pedestrian {[f'P{x}' for x in red_group]} → BLINK RED (5s)")
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

# --------------- RPC server ----------------
def ping(): return "OK"
def get_signal_status():
    with state_lock:
        return signal_status

if __name__ == "__main__":
    srv = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)
    srv.register_function(signal_controller, "signal_controller")
    srv.register_function(vip_arrival, "vip_arrival")
    srv.register_function(ping, "ping")
    srv.register_function(get_signal_status, "get_signal_status")
    print("[Controller] 🚦 RPC server on :8000 (Berkeley + VIP deadlock handling enabled)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[Controller] Shutting down...")

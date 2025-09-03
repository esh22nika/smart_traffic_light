# improved_controller.py
# Fixed version with proper p_signal logic and multiple VIP support
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from dataclasses import dataclass
from typing import List
import uuid

# ---------------- CONFIG ----------------
CLIENTS = {
    "t_signal": "http://172.16.182.10:7000",
    "p_signal": "http://172.16.182.3:9000",
}
PEDESTRIAN_IP = CLIENTS["p_signal"]
RESPONSE_TIMEOUT = 5
VIP_CROSSING_TIME = 4

server_skew = 0.0
state_lock = threading.Lock()

# ------------- VIP Management -------------
@dataclass
class VIPRequest:
    vehicle_id: str
    priority: int  # 1=highest (ambulance), 2=fire, 3=police, etc.
    arrival_time: float
    target_pair: List[int]
    
    def __lt__(self, other):
        # Higher priority (lower number) goes first
        # If same priority, earlier arrival time goes first
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.arrival_time < other.arrival_time

# Priority queues for VIP requests
vip_queues = {
    "12": [],  # VIPs wanting [1,2] green
    "34": []   # VIPs wanting [3,4] green
}

# ------------- Timeout Transport -------------
class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout
    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn

# ------------- Signal State ---------------
signal_status = {
    1: "RED", 2: "RED", 3: "GREEN", 4: "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}

def _current_green_pair():
    """Return [1,2] or [3,4] depending on current traffic state."""
    with state_lock:
        g12 = (signal_status[1] == "GREEN" and signal_status[2] == "GREEN")
        return [1, 2] if g12 else [3, 4]

# ------------- Clock Helpers --------------
def get_server_time():
    return time.time() + server_skew

def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

# ---------- Berkeley Sync -----------------
def berkeley_cycle_once():
    global server_skew
    print("\n[Berkeley] === Starting Berkeley sync cycle ===")
    server_time = get_server_time()
    print(f"[Berkeley] Step 1 — Broadcasting server time: {format_time(server_time)}")

    clock_values = {"controller": 0.0}
    for name, url in CLIENTS.items():
        try:
            proxy = ServerProxy(url, allow_none=True, transport=TimeoutTransport(RESPONSE_TIMEOUT))
            print(f"[Berkeley] Contacting {name} at {url}...")
            cv = float(proxy.get_clock_value(server_time))
            clock_values[name] = cv
            print(f"[Berkeley] {name} clock_value: {cv:+.2f}s")
        except Exception as e:
            print(f"[Berkeley] ❌ Failed to get clock value from {name}: {e}")

    if not clock_values:
        print("[Berkeley] ❌ No clock values collected.")
        return

    avg_offset = sum(clock_values.values()) / len(clock_values)
    new_epoch = server_time + avg_offset

    # Send new time to clients
    for name, url in CLIENTS.items():
        try:
            proxy = ServerProxy(url, allow_none=True, transport=TimeoutTransport(RESPONSE_TIMEOUT))
            proxy.set_time(new_epoch)
            print(f"[Berkeley] ✅ Sent new time to {name}")
        except Exception as e:
            print(f"[Berkeley] ❌ Failed to send time to {name}: {e}")

    server_skew += (new_epoch - server_time)
    print(f"[Berkeley] Controller adjusted by {(new_epoch - server_time):+.2f}s")
    print("[Berkeley] === Cycle complete ===\n")

# ------------- P_Signal Acknowledgment ----
def _get_pedestrian_ack(target_pair, request_type="normal"):
    """Get acknowledgment from p_signal before any signal change"""
    try:
        proxy = ServerProxy(PEDESTRIAN_IP, allow_none=True)
        response = proxy.p_signal(target_pair)
        if response != "OK":
            print(f"[Controller] ❌ p_signal rejected {request_type} request for {target_pair}")
            return False
        print(f"[Controller] ✅ p_signal acknowledged {request_type} request for {target_pair}")
        return True
    except Exception as e:
        print(f"[Controller] ❌ Error contacting p_signal for {request_type}: {e}")
        return False

# ------------- VIP Management --------------
def vip_arrival(target_pair, priority=1, vehicle_id=None):
    """
    RPC: Register a VIP request with priority handling
    priority: 1=ambulance (highest), 2=fire, 3=police, etc.
    """
    if vehicle_id is None:
        vehicle_id = str(uuid.uuid4())[:8]
    
    # Sync clocks first
    berkeley_cycle_once()
    
    # Create VIP request
    vip_req = VIPRequest(
        vehicle_id=vehicle_id,
        priority=priority,
        arrival_time=get_server_time(),
        target_pair=target_pair
    )
    
    # Add to appropriate queue
    key = "12" if target_pair == [1, 2] else "34"
    with state_lock:
        vip_queues[key].append(vip_req)
        vip_queues[key].sort()  # Keep sorted by priority
    
    print(f"\n[Controller|VIP] 🚨 VIP {vehicle_id} (priority {priority}) registered for {target_pair}")
    print(f"[Controller|VIP] Queue state: 12={len(vip_queues['12'])}, 34={len(vip_queues['34'])}")
    
    # Process VIP requests
    _process_vip_requests()
    return True

def _process_vip_requests():
    """Process all pending VIP requests with proper prioritization"""
    while True:
        with state_lock:
            has_12 = len(vip_queues["12"]) > 0
            has_34 = len(vip_queues["34"]) > 0
        
        if not (has_12 or has_34):
            print("[Controller|VIP] No more VIPs to process")
            return
        
        if has_12 and has_34:
            # Deadlock case: compare priorities
            with state_lock:
                vip_12 = vip_queues["12"][0]
                vip_34 = vip_queues["34"][0]
            
            if vip_12 < vip_34:  # vip_12 has higher priority
                print(f"[Controller|VIP] Deadlock: VIP {vip_12.vehicle_id} (p{vip_12.priority}) > VIP {vip_34.vehicle_id} (p{vip_34.priority})")
                _serve_next_vip([1, 2])
            else:
                print(f"[Controller|VIP] Deadlock: VIP {vip_34.vehicle_id} (p{vip_34.priority}) > VIP {vip_12.vehicle_id} (p{vip_12.priority})")
                _serve_next_vip([3, 4])
                
        elif has_12:
            _serve_next_vip([1, 2])
        elif has_34:
            _serve_next_vip([3, 4])

def _serve_next_vip(pair):
    """Serve the highest priority VIP for the given pair"""
    key = "12" if pair == [1, 2] else "34"
    
    with state_lock:
        if not vip_queues[key]:
            return
        vip = vip_queues[key].pop(0)
    
    print(f"[Controller|VIP] Serving VIP {vip.vehicle_id} (priority {vip.priority}) on {pair}")
    
    # FIXED: Get p_signal acknowledgment for VIP requests too!
    if not _get_pedestrian_ack(pair, "VIP"):
        print(f"[Controller|VIP] ❌ Cannot serve VIP {vip.vehicle_id} - p_signal denied")
        return
    
    # Switch to target pair if needed
    if _current_green_pair() != pair:
        print(f"[Controller|VIP] Switching to {pair} for VIP {vip.vehicle_id}")
        _switch_to(pair)
    else:
        print(f"[Controller|VIP] {pair} already GREEN for VIP {vip.vehicle_id}")
    
    # Let VIP cross
    _let_vip_cross(vip)

def _let_vip_cross(vip):
    """Simulate VIP crossing"""
    print(f"[Controller|VIP] 🟢 VIP {vip.vehicle_id} crossing on {vip.target_pair}... (wait {VIP_CROSSING_TIME}s)")
    time.sleep(VIP_CROSSING_TIME)
    print(f"[Controller|VIP] ✅ VIP {vip.vehicle_id} has crossed")

# ------------- Normal Traffic Handling ----
def signal_controller(target_pair):
    """Handle normal (non-VIP) traffic requests"""
    berkeley_cycle_once()
    
    print(f"\n[Controller] 📥 Received normal traffic request for {target_pair}")
    
    # Check if any VIPs are pending - they get priority
    with state_lock:
        vip_count = len(vip_queues["12"]) + len(vip_queues["34"])
    
    if vip_count > 0:
        print(f"[Controller] ⚠️ {vip_count} VIPs pending - processing them first")
        _process_vip_requests()
    
    # Get p_signal acknowledgment
    if not _get_pedestrian_ack(target_pair, "normal"):
        return False
    
    # Proceed with normal signal change
    _switch_to(target_pair)
    print("[Controller] 🔁 Completed normal signal cycle.\n")
    return True

def _switch_to(target_pair):
    """Perform full pedestrian + traffic signal transition"""
    handle_pedestrian_signals(target_pair)
    handle_traffic_signals(target_pair)

# ------------- Signal Control Routines ----
def handle_pedestrian_signals(target_pair):
    red_group = target_pair
    green_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    print(f"[Controller] ⚠️ Pedestrian {[f'P{x}' for x in red_group]} → BLINKING RED (5s)")
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

# ------------- Utility Functions ----------
def ping():
    return "OK"

def get_signal_status():
    with state_lock:
        return signal_status.copy()

def get_vip_status():
    """Debug function to check VIP queue status"""
    with state_lock:
        return {
            "12": [(vip.vehicle_id, vip.priority) for vip in vip_queues["12"]],
            "34": [(vip.vehicle_id, vip.priority) for vip in vip_queues["34"]]
        }

# ------------- Main Server ----------------
if __name__ == "__main__":
    server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)
    server.register_function(signal_controller, "signal_controller")
    server.register_function(vip_arrival, "vip_arrival")
    server.register_function(ping, "ping")
    server.register_function(get_signal_status, "get_signal_status")
    server.register_function(get_vip_status, "get_vip_status")
    
    print("[Controller] 🚦 Improved RPC server on :8000")
    print("[Controller] Features: Berkeley sync, Multiple VIP priority, Proper p_signal ack")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Controller] Shutting down...")

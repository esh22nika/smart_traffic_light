# controller.py (Updated for ZooKeeper load balancing)
# Original controller - all existing logic preserved, now works with ZooKeeper
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from dataclasses import dataclass
from typing import List
import uuid

# ---------------- CONFIG ----------------
# Note: Now p_signal contacts us through ZooKeeper, but we still need direct access for Ricart-Agrawala
CLIENTS = {
    "t_signal": "http://192.168.0.165:7000",
    "p_signal": "http://192.168.0.176:9000",
}
PEDESTRIAN_IP = CLIENTS["p_signal"]
RESPONSE_TIMEOUT = 5
VIP_CROSSING_TIME = 4
CONTROLLER_PORT = 8000  # Original controller port
CONTROLLER_NAME = "CONTROLLER"

server_skew = 0.0
state_lock = threading.Lock()


# ------------- Enhanced Logging Functions -------------
def log_separator(title="", char="=", width=70):
    """Print formatted separator for better log readability"""
    if title:
        padding = (width - len(title) - 2) // 2
        print(f"\n{char * padding} {title} {char * padding}")
    else:
        print(f"\n{char * width}")


PRIORITY_TO_TYPE = {1: "🚑 AMBULANCE", 2: "🚒 FIRE_TRUCK", 3: "🚓 POLICE", 4: "🚗 VIP_CAR"}


def get_vehicle_type(priority):
    return PRIORITY_TO_TYPE.get(priority, f"🚗 VIP_P{priority}")


def log_vip_queues(context=""):
    """Enhanced VIP queue logging"""
    print(f"[VIP-QUEUES] 📋 {context}")

    for direction in ["12", "34"]:
        queue = vip_queues[direction]
        direction_name = "[1,2]" if direction == "12" else "[3,4]"

        if queue:
            print(f"[VIP-QUEUES] 🚁 Direction {direction_name}: {len(queue)} VIPs waiting")
            for i, vip in enumerate(queue):
                vehicle_type = get_vehicle_type(vip.priority)
                wait_time = get_server_time() - vip.arrival_time
                print(
                    f"[VIP-QUEUES]   {i + 1}. {vehicle_type} {vip.vehicle_id} [P{vip.priority}] (waiting {wait_time:.1f}s)")
        else:
            print(f"[VIP-QUEUES] ✅ Direction {direction_name}: Empty")


def log_mutex_state():
    """Log current intersection mutex state"""
    current = _current_green_pair()
    with state_lock:
        vip_12_count = len(vip_queues["12"])
        vip_34_count = len(vip_queues["34"])
    print(f"[{CONTROLLER_NAME}] 🔐 Current intersection holder: {current}")
    print(f"[{CONTROLLER_NAME}] 📊 VIP queues: [1,2]={vip_12_count} | [3,4]={vip_34_count}")
    print(f"[{CONTROLLER_NAME}] ✅ Current Signal Status: {signal_status}")


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
    return time.strftime("%H:%M:%S", time.localtime(ts))


# ---------- Berkeley Sync -----------------
def berkeley_cycle_once():
    global server_skew
    log_separator("BERKELEY CLOCK SYNCHRONIZATION", "🕐")
    server_time = get_server_time()
    print(f"[Berkeley] Step 1 — Broadcasting server time: {format_time(server_time)}")

    clock_values = {"controller": 0.0}
    successful_clients = []

    for name, url in CLIENTS.items():
        try:
            proxy = ServerProxy(url, allow_none=True, transport=TimeoutTransport(RESPONSE_TIMEOUT))
            print(f"[Berkeley] 📡 Contacting {name} at {url}...")
            cv = float(proxy.get_clock_value(server_time))
            clock_values[name] = cv
            successful_clients.append(name)
            print(f"[Berkeley] ✅ {name} clock_value: {cv:+.2f}s")
        except Exception as e:
            print(f"[Berkeley] ❌ Failed to get clock value from {name}: {e}")

    if len(clock_values) <= 1:  # Only controller
        print("[Berkeley] ⚠️ No clients responded - running in standalone mode")
        log_separator()
        return

    avg_offset = sum(clock_values.values()) / len(clock_values)
    new_epoch = server_time + avg_offset
    print(f"[Berkeley] 🧮 Average offset: {avg_offset:+.2f}s, New time: {format_time(new_epoch)}")

    # Send new time to clients
    for name in successful_clients:
        url = CLIENTS[name]
        try:
            proxy = ServerProxy(url, allow_none=True, transport=TimeoutTransport(RESPONSE_TIMEOUT))
            proxy.set_time(new_epoch)
            print(f"[Berkeley] ✅ Sent new time to {name}")
        except Exception as e:
            print(f"[Berkeley] ❌ Failed to send time to {name}: {e}")

    server_skew += (new_epoch - server_time)
    print(f"[Berkeley] 🔧 Controller adjusted by {(new_epoch - server_time):+.2f}s")
    log_separator()


# ------------- P_Signal Acknowledgment ----
def _get_pedestrian_ack(target_pair, request_type="normal", requester_info=""):
    """Get acknowledgment from p_signal before any signal change (Ricart-Agrawala voting)"""
    print(f"[RICART-AGRAWALA] 🗳️ Requesting permission from p_signal for {request_type}")
    if requester_info:
        print(f"[RICART-AGRAWALA] 📋 Requester: {requester_info}")

    try:
        proxy = ServerProxy(PEDESTRIAN_IP, allow_none=True)
        response = proxy.p_signal(target_pair)
        if response != "OK":
            print(f"[RICART-AGRAWALA] ❌ PERMISSION DENIED by p_signal for {request_type} request for {target_pair}")
            return False
        print(f"[RICART-AGRAWALA] ✅ PERMISSION GRANTED by p_signal for {request_type} request for {target_pair}")
        return True
    except Exception as e:
        print(f"[RICART-AGRAWALA] ❌ p_signal UNREACHABLE for {request_type}: {e}")
        print(f"[RICART-AGRAWALA] ⚠️ Proceeding without vote (degraded mode)")
        return True  # Allow operation to continue


# ------------- VIP Management --------------
def vip_arrival(target_pair, priority=1, vehicle_id=None):
    """
    RPC: Register a VIP request with priority handling
    priority: 1=ambulance (highest), 2=fire, 3=police, etc.
    """
    if vehicle_id is None:
        vehicle_id = str(uuid.uuid4())[:8]

    vehicle_type = get_vehicle_type(priority)

    log_separator(f"VIP ARRIVAL: {vehicle_type} {vehicle_id} [{CONTROLLER_NAME}]", "🚨")
    print(f"[{CONTROLLER_NAME}] 🆘 Emergency vehicle detected: {vehicle_type} {vehicle_id}")
    print(f"[{CONTROLLER_NAME}] 🎯 Target intersection: {target_pair}")
    print(f"[{CONTROLLER_NAME}] ⏱️ Arrival timestamp: {format_time(get_server_time())}")

    # Sync clocks first (Ricart-Agrawala requires synchronized timestamps)
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

    log_vip_queues("After VIP Registration")

    # Check for deadlock condition
    with state_lock:
        both_queues_have_vips = len(vip_queues["12"]) > 0 and len(vip_queues["34"]) > 0

    if both_queues_have_vips:
        print(f"[DEADLOCK-DETECTOR] ⚠️ POTENTIAL DEADLOCK DETECTED!")
        print(f"[DEADLOCK-DETECTOR] 📊 Both directions have pending VIPs")
        _log_deadlock_analysis()

    # Process VIP requests
    _process_vip_requests()
    log_separator()
    return True


def _log_deadlock_analysis():
    """Detailed deadlock analysis logging"""
    with state_lock:
        vip_12 = vip_queues["12"][0] if vip_queues["12"] else None
        vip_34 = vip_queues["34"][0] if vip_queues["34"] else None

    if vip_12 and vip_34:
        type_12 = get_vehicle_type(vip_12.priority)
        type_34 = get_vehicle_type(vip_34.priority)

        print(f"[DEADLOCK-ANALYSIS] 🔍 Competing VIPs:")
        print(
            f"[DEADLOCK-ANALYSIS]   Direction [1,2]: {type_12} {vip_12.vehicle_id} [P{vip_12.priority}] (arrived {format_time(vip_12.arrival_time)})")
        print(
            f"[DEADLOCK-ANALYSIS]   Direction [3,4]: {type_34} {vip_34.vehicle_id} [P{vip_34.priority}] (arrived {format_time(vip_34.arrival_time)})")

        # Determine winner by Ricart-Agrawala rules
        winner = vip_12 if vip_12 < vip_34 else vip_34
        loser = vip_34 if winner == vip_12 else vip_12
        winner_type = get_vehicle_type(winner.priority)
        loser_type = get_vehicle_type(loser.priority)

        print(
            f"[DEADLOCK-RESOLUTION] 🏆 Winner: {winner_type} {winner.vehicle_id} [P{winner.priority}] (higher priority/earlier arrival)")
        print(
            f"[DEADLOCK-RESOLUTION] ⏳ Deferred: {loser_type} {loser.vehicle_id} [P{loser.priority}] (will be served next)")


def _process_vip_requests():
    """Process all pending VIP requests with proper prioritization"""
    log_separator("RICART-AGRAWALA VIP PROCESSING", "🔐")

    while True:
        with state_lock:
            has_12 = len(vip_queues["12"]) > 0
            has_34 = len(vip_queues["34"]) > 0

        if not (has_12 or has_34):
            print("[RICART-AGRAWALA] ✅ All VIPs processed - releasing critical section")
            break

        if has_12 and has_34:
            # Deadlock case: compare priorities
            with state_lock:
                vip_12 = vip_queues["12"][0]
                vip_34 = vip_queues["34"][0]

            if vip_12 < vip_34:  # vip_12 has higher priority
                type_12 = get_vehicle_type(vip_12.priority)
                type_34 = get_vehicle_type(vip_34.priority)
                print(
                    f"[RICART-AGRAWALA] 🏆 Deadlock resolved: {type_12} {vip_12.vehicle_id} [P{vip_12.priority}] > {type_34} {vip_34.vehicle_id} [P{vip_34.priority}]")
                _serve_next_vip([1, 2])
            else:
                type_12 = get_vehicle_type(vip_12.priority)
                type_34 = get_vehicle_type(vip_34.priority)
                print(
                    f"[RICART-AGRAWALA] 🏆 Deadlock resolved: {type_34} {vip_34.vehicle_id} [P{vip_34.priority}] > {type_12} {vip_12.vehicle_id} [P{vip_12.priority}]")
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

    vehicle_type = get_vehicle_type(vip.priority)
    print(f"[RICART-AGRAWALA] 🎯 Next critical section access: {vehicle_type} {vip.vehicle_id} [P{vip.priority}]")

    # Get p_signal acknowledgment for VIP requests too!
    if not _get_pedestrian_ack(pair, "VIP", f"{vehicle_type} {vip.vehicle_id}"):
        print(f"[RICART-AGRAWALA] ❌ Permission denied for {vehicle_type} {vip.vehicle_id}")
        return

    # Enter critical section
    print(f"[CRITICAL-SECTION] 🔐 {vehicle_type} {vip.vehicle_id} ACQUIRED intersection mutex [{CONTROLLER_NAME}]")

    # Switch to target pair if needed
    current_pair = _current_green_pair()
    if current_pair != pair:
        print(f"[CRITICAL-SECTION] 🔄 Switching intersection: {current_pair} → {pair}")
        _switch_to(pair)
    else:
        print(f"[CRITICAL-SECTION] ✅ Intersection already configured for {pair}")

    # Let VIP cross
    _let_vip_cross(vip)

    print(f"[CRITICAL-SECTION] 🔓 {vehicle_type} {vip.vehicle_id} RELEASED intersection mutex [{CONTROLLER_NAME}]")
    log_vip_queues("After VIP Service")


def _let_vip_cross(vip):
    """Simulate VIP crossing"""
    vehicle_type = get_vehicle_type(vip.priority)
    print(f"[CRITICAL-SECTION] 🚑 {vehicle_type} {vip.vehicle_id} crossing intersection... ({VIP_CROSSING_TIME}s)")
    for i in range(VIP_CROSSING_TIME):
        print(f"[CRITICAL-SECTION] ⏳ {vehicle_type} {vip.vehicle_id} crossing... {i + 1}/{VIP_CROSSING_TIME}s")
        time.sleep(1)
    print(f"[CRITICAL-SECTION] ✅ {vehicle_type} {vip.vehicle_id} completed crossing")


# ------------- Normal Traffic Handling ----
def signal_controller(target_pair):
    """Handle normal (non-VIP) traffic requests"""
    log_separator(f"NORMAL TRAFFIC REQUEST: {target_pair} [{CONTROLLER_NAME}]", "🚦")
    print(f"[{CONTROLLER_NAME}] 📥 Request for intersection {target_pair}")

    berkeley_cycle_once()

    # Check if any VIPs are pending - they get priority
    with state_lock:
        vip_count = len(vip_queues["12"]) + len(vip_queues["34"])

    if vip_count > 0:
        print(f"[RICART-AGRAWALA] ⚠️ {vip_count} VIPs have higher priority - deferring normal traffic")
        log_vip_queues("VIPs blocking normal traffic")
        _process_vip_requests()
        print(f"[RICART-AGRAWALA] ✅ VIPs cleared - processing normal traffic")

    # Get p_signal acknowledgment
    if not _get_pedestrian_ack(target_pair, "normal", "Normal Traffic"):
        return False

    # Enter critical section for normal traffic
    print(f"[CRITICAL-SECTION] 🔐 Normal traffic ACQUIRED intersection mutex for {target_pair} [{CONTROLLER_NAME}]")

    # Proceed with normal signal change
    _switch_to(target_pair)

    print(f"[CRITICAL-SECTION] 🔓 Normal traffic RELEASED intersection mutex [{CONTROLLER_NAME}]")
    print(f"[{CONTROLLER_NAME}] 🔁 Completed normal signal cycle.\n")
    log_mutex_state()
    log_separator()
    return True


def _switch_to(target_pair):
    """Perform full pedestrian + traffic signal transition"""
    handle_pedestrian_signals(target_pair)
    handle_traffic_signals(target_pair)


# ------------- Signal Control Routines ----
def handle_pedestrian_signals(target_pair):
    red_group = target_pair
    green_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    print(f"[{CONTROLLER_NAME}] ⚠️ Pedestrian {[f'P{x}' for x in red_group]} → BLINKING RED (5s)")
    for _ in range(5):
        with state_lock:
            for sig in red_group:
                signal_status[f"P{sig}"] = "BLINKING RED"
        time.sleep(1)

    with state_lock:
        for sig in red_group:
            signal_status[f"P{sig}"] = "RED"
    print(f"[{CONTROLLER_NAME}] 🔴 Pedestrian {red_group} → RED")

    with state_lock:
        for sig in green_group:
            signal_status[f"P{sig}"] = "YELLOW"
    print(f"[{CONTROLLER_NAME}] 🟡 Pedestrian {green_group} → YELLOW")
    time.sleep(5)

    with state_lock:
        for sig in green_group:
            signal_status[f"P{sig}"] = "GREEN"
    print(f"[{CONTROLLER_NAME}] 🟢 Pedestrian {green_group} → GREEN")


def handle_traffic_signals(target_pair):
    red_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    with state_lock:
        for sig in red_group:
            signal_status[sig] = "YELLOW"
    print(f"[{CONTROLLER_NAME}] 🟡 Traffic {red_group} → YELLOW")
    time.sleep(2)

    with state_lock:
        for sig in red_group:
            signal_status[sig] = "RED"
    print(f"[{CONTROLLER_NAME}] 🔴 Traffic {red_group} → RED")
    
    with state_lock:
        for sig in target_pair:
            signal_status[sig] = "GREEN"
    print(f"[{CONTROLLER_NAME}] 🟢 Traffic {target_pair} → GREEN")


# ------------- Main Server ----------------
if __name__ == "__main__":
    log_separator(f"SMART TRAFFIC CONTROLLER STARTUP [{CONTROLLER_NAME}]", "🚦")
    print(f"[{CONTROLLER_NAME}] 🎯 Features: Ricart-Agrawala Mutual Exclusion, VIP Priority, Berkeley Clock Sync")
    print(f"[{CONTROLLER_NAME}] 🌐 Client configuration: {CLIENTS}")
    print(f"[{CONTROLLER_NAME}] 🔄 Load balanced via ZooKeeper")

    print("\n--- INITIAL STATE ---")
    log_mutex_state()
    print("---------------------\n")


    server = SimpleXMLRPCServer(("0.0.0.0", CONTROLLER_PORT), allow_none=True)
    server.register_function(signal_controller, "signal_controller")
    server.register_function(vip_arrival, "vip_arrival")
    server.register_function(ping, "ping")
    server.register_function(get_signal_status, "get_signal_status")
    server.register_function(get_vip_status, "get_vip_status")

    log_separator("SERVER READY", "🎉")
    print(f"[{CONTROLLER_NAME}] 🚦 RPC Server listening on port {CONTROLLER_PORT}")
    print(f"[{CONTROLLER_NAME}] 📚 Ricart-Agrawala protocol active for mutual exclusion")
    print(f"[{CONTROLLER_NAME}] 🚨 VIP prioritization with deadlock resolution enabled")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log_separator("SHUTDOWN", "👋")
        print(f"\n[{CONTROLLER_NAME}] Shutting down...")
        current = _current_green_pair() # Define current state before printing
        vip_12_count = len(vip_queues["12"])
        vip_34_count = len(vip_queues["34"])

        print(f"[MUTEX-STATE] 🔐 Current intersection holder: {current}")
        print(f"[MUTEX-STATE] 📊 VIP queues: [1,2]={vip_12_count} | [3,4]={vip_34_count}")

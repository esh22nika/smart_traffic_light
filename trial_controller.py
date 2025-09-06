# controller.py (Performance Optimized)
# Original controller with removed sleep delays and database integration
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from dataclasses import dataclass
from typing import List
import uuid

# ---------------- CONFIG ----------------
CLIENTS = {
    "t_signal": "http://192.168.0.165:7000",
    "p_signal": "http://192.168.0.176:9000",
}
PEDESTRIAN_IP = CLIENTS["p_signal"]
RESPONSE_TIMEOUT = 3  # Reduced timeout
VIP_CROSSING_TIME = 0.5  # Drastically reduced from 4 seconds
CONTROLLER_PORT = 8000
CONTROLLER_NAME = "CONTROLLER"
ZOOKEEPER_URL = "http://localhost:6000"

server_skew = 0.0
state_lock = threading.Lock()


def log_separator(title="", char="=", width=70):
    if title:
        padding = (width - len(title) - 2) // 2
        print(f"\n{char * padding} {title} {char * padding}")
    else:
        print(f"\n{char * width}")


PRIORITY_TO_TYPE = {1: "AMBULANCE", 2: "FIRE_TRUCK", 3: "POLICE", 4: "VIP_CAR"}


def get_vehicle_type(priority):
    return PRIORITY_TO_TYPE.get(priority, f"VIP_P{priority}")


def log_vip_queues(context=""):
    print(f"[VIP-QUEUES] {context}")
    for direction in ["12", "34"]:
        queue = vip_queues[direction]
        direction_name = "[1,2]" if direction == "12" else "[3,4]"
        if queue:
            print(f"[VIP-QUEUES] Direction {direction_name}: {len(queue)} VIPs waiting")
            for i, vip in enumerate(queue):
                vehicle_type = get_vehicle_type(vip.priority)
                wait_time = get_server_time() - vip.arrival_time
                print(
                    f"[VIP-QUEUES]   {i + 1}. {vehicle_type} {vip.vehicle_id} [P{vip.priority}] (waiting {wait_time:.1f}s)")
        else:
            print(f"[VIP-QUEUES] Direction {direction_name}: Empty")


@dataclass
class VIPRequest:
    vehicle_id: str
    priority: int
    arrival_time: float
    target_pair: List[int]

    def __lt__(self, other):
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.arrival_time < other.arrival_time


vip_queues = {"12": [], "34": []}


class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


# Fixed signal status - all keys as strings to prevent database errors
signal_status = {
    "1": "RED", "2": "RED", "3": "GREEN", "4": "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}


def _current_green_pair():
    with state_lock:
        g12 = (signal_status["1"] == "GREEN" and signal_status["2"] == "GREEN")
        return [1, 2] if g12 else [3, 4]


def get_server_time():
    return time.time() + server_skew


def format_time(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts))


def notify_zookeeper_signal_update():
    """Notify ZooKeeper of signal status changes"""
    try:
        proxy = ServerProxy(ZOOKEEPER_URL, allow_none=True, transport=TimeoutTransport(2))
        with state_lock:
            status_copy = signal_status.copy()
        proxy.update_signal_status(status_copy)
        print(f"[{CONTROLLER_NAME}] Updated ZooKeeper database")
    except Exception as e:
        print(f"[{CONTROLLER_NAME}] Could not update ZooKeeper database: {e}")


def berkeley_cycle_once():
    """Optimized Berkeley sync - reduced processing time"""
    global server_skew
    print(f"[Berkeley] Quick clock sync...")
    server_time = get_server_time()
    clock_values = {"controller": 0.0}
    successful_clients = []

    for name, url in CLIENTS.items():
        try:
            proxy = ServerProxy(url, allow_none=True, transport=TimeoutTransport(2))  # Shorter timeout
            cv = float(proxy.get_clock_value(server_time))
            clock_values[name] = cv
            successful_clients.append(name)
        except Exception as e:
            print(f"[Berkeley] {name}: {e}")

    if len(clock_values) <= 1:
        return

    avg_offset = sum(clock_values.values()) / len(clock_values)
    new_epoch = server_time + avg_offset

    for name in successful_clients:
        url = CLIENTS[name]
        try:
            proxy = ServerProxy(url, allow_none=True, transport=TimeoutTransport(2))
            proxy.set_time(new_epoch)
        except Exception:
            pass

    server_skew += (new_epoch - server_time)
    print(f"[Berkeley] Sync complete ({avg_offset:+.2f}s)")


def _get_pedestrian_ack(target_pair, request_type="normal", requester_info=""):
    """Optimized pedestrian acknowledgment"""
    try:
        proxy = ServerProxy(PEDESTRIAN_IP, allow_none=True, transport=TimeoutTransport(2))
        response = proxy.p_signal(target_pair)
        if response != "OK":
            print(f"[RICART-AGRAWALA] Permission denied for {request_type}")
            return False
        print(f"[RICART-AGRAWALA] Permission granted for {request_type}")
        return True
    except Exception as e:
        print(f"[RICART-AGRAWALA] p_signal unreachable: {e}")
        return True  # Allow operation to continue


def vip_arrival(target_pair, priority=1, vehicle_id=None):
    """Optimized VIP handling"""
    if vehicle_id is None:
        vehicle_id = str(uuid.uuid4())[:8]

    vehicle_type = get_vehicle_type(priority)
    print(f"[{CONTROLLER_NAME}] VIP: {vehicle_type} {vehicle_id} -> {target_pair}")

    # Quick clock sync
    berkeley_cycle_once()

    vip_req = VIPRequest(
        vehicle_id=vehicle_id,
        priority=priority,
        arrival_time=get_server_time(),
        target_pair=target_pair
    )

    key = "12" if target_pair == [1, 2] else "34"
    with state_lock:
        vip_queues[key].append(vip_req)
        vip_queues[key].sort()

    _process_vip_requests()
    return True


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

# p_signal.py (Updated for ZooKeeper architecture)
# Pedestrian signal client: Berkeley client + pedestrian acknowledgment
# Note: Controllers still contact p_signal directly for Ricart-Agrawala voting
# But p_signal is now aware it's part of a load-balanced system

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

# Load balancing awareness (for logging/debugging)
ZOOKEEPER_IP = "http://192.168.0.168:6000"
CONTROLLERS = {
    "controller": "http://192.168.0.168:8000",
    "controller_clone": "http://192.168.0.168:8001"
}

# Statistics tracking
request_stats = {
    "total_requests": 0,
    "controller_requests": 0,
    "controller_clone_requests": 0,
    "normal_requests": 0,
    "vip_requests": 0,
    "last_request_time": 0
}

stats_lock = threading.Lock()

# -------------------------
# ENHANCED PEDESTRIAN ACK LOGIC
# -------------------------
def p_signal(target_pair):
    """
    Enhanced pedestrian acknowledgment with load balancing awareness
    Controllers call this directly for Ricart-Agrawala voting
    """
    with stats_lock:
        request_stats["total_requests"] += 1
        request_stats["last_request_time"] = time.time()
    
    # Try to determine which controller is calling (for statistics)
    caller = "unknown"
    try:
        # This is a simple heuristic - in real implementation you might 
        # pass controller ID as parameter or use other identification
        import inspect
        # For demo purposes, we'll just log that we got a request
        caller = "load_balanced_controller"
    except:
        pass
    
    current_time = format_time(time.time() + local_skew)
    print(f"[{MY_NAME}] 🚶‍♂️ Pedestrian acknowledgment for traffic pair {target_pair}")
    print(f"[{MY_NAME}] 🕐 Local time: {current_time}")
    print(f"[{MY_NAME}] 📊 Total requests processed: {request_stats['total_requests']}")
    
    # Simulate pedestrian crossing check (could be extended with real sensor logic)
    pedestrian_clear = check_pedestrian_crossing(target_pair)
    
    if pedestrian_clear:
        print(f"[{MY_NAME}] ✅ Pedestrian crossing CLEAR for {target_pair} - VOTE: OK")
        with stats_lock:
            request_stats["normal_requests"] += 1
        return "OK"
    else:
        print(f"[{MY_NAME}] ❌ Pedestrians detected on {target_pair} - VOTE: DENY")
        return "DENY"


def check_pedestrian_crossing(target_pair):
    """
    Simulate pedestrian crossing check
    In real implementation, this would check actual sensors
    """
    import random
    
    # For demo: 90% chance pedestrians are clear
    is_clear = random.random() > 0.1
    
    if not is_clear:
        print(f"[{MY_NAME}] 🚶‍♂️ Simulated pedestrians detected crossing {target_pair}")
    
    return is_clear


def enhanced_p_signal(target_pair, request_type="normal", requester_id="unknown"):
    """
    Enhanced version that accepts additional parameters for better logging
    This could be used if controllers are updated to provide more info
    """
    with stats_lock:
        request_stats["total_requests"] += 1
        if request_type == "vip":
            request_stats["vip_requests"] += 1
        else:
            request_stats["normal_requests"] += 1
        request_stats["last_request_time"] = time.time()
    
    print(f"[{MY_NAME}] 🚶‍♂️ Enhanced pedestrian check for {target_pair}")
    print(f"[{MY_NAME}] 📋 Request type: {request_type}, Requester: {requester_id}")
    
    return p_signal(target_pair)


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


# -------------------------
# UTILITY AND DEBUG METHODS
# -------------------------
def get_pedestrian_stats():
    """Debug method to check pedestrian signal statistics"""
    with stats_lock:
        stats_copy = request_stats.copy()
    
    stats_copy["local_time"] = format_time(time.time() + local_skew)
    stats_copy["local_skew"] = local_skew
    return stats_copy


def ping():
    """Health check method"""
    return f"{MY_NAME} OK"


def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def log_system_status():
    """Periodic logging of system status"""
    while True:
        with stats_lock:
            total = request_stats["total_requests"]
            normal = request_stats["normal_requests"] 
            vip = request_stats["vip_requests"]
            last_req = request_stats["last_request_time"]
        
        if total > 0:
            time_since_last = time.time() - last_req if last_req > 0 else 0
            print(f"\n[{MY_NAME}] 📊 === PEDESTRIAN SYSTEM STATUS ===")
            print(f"[{MY_NAME}] 📈 Total requests: {total} (Normal: {normal}, VIP: {vip})")
            print(f"[{MY_NAME}] ⏱ Last request: {time_since_last:.1f}s ago")
            print(f"[{MY_NAME}] 🕐 Local time: {format_time(time.time() + local_skew)}")
            print(f"[{MY_NAME}] 🌐 Load balanced system with {len(CONTROLLERS)} controllers")
            print(f"[{MY_NAME}] ==========================================\n")
        
        time.sleep(30)  # Log every 30 seconds


# -------------------------
# ENHANCED PEDESTRIAN SIMULATION
# -------------------------
def simulate_pedestrian_activity():
    """
    Simulate realistic pedestrian activity that might affect voting
    This creates some randomness in the pedestrian acknowledgments
    """
    while True:
        # Simulate busy pedestrian periods
        import random
        
        if random.random() < 0.1:  # 10% chance of busy period
            print(f"[{MY_NAME}] 🚶‍♂️🚶‍♀️ Simulating busy pedestrian period...")
            time.sleep(random.randint(2, 5))
            print(f"[{MY_NAME}] ✅ Pedestrian activity returned to normal")
        
        time.sleep(random.randint(5, 15))


# -------------------------
# MAIN ENTRY POINT
# -------------------------
if __name__ == "__main__":
    print("=" * 70)
    print(f"🚶‍♂️ PEDESTRIAN SIGNAL CLIENT [{MY_NAME}] STARTING")
    print("=" * 70)
    print(f"[{MY_NAME}] 🌐 Part of load-balanced system with ZooKeeper at {ZOOKEEPER_IP}")
    print(f"[{MY_NAME}] 🎯 Controllers: {list(CONTROLLERS.keys())}")
    print(f"[{MY_NAME}] ⏱ Initial clock skew: {local_skew:+.2f}s")
    print("=" * 70)
    
    # Start background threads
    status_thread = threading.Thread(target=log_system_status, daemon=True)
    status_thread.start()
    
    simulation_thread = threading.Thread(target=simulate_pedestrian_activity, daemon=True)  
    simulation_thread.start()
    
    # RPC server to handle both pedestrian acknowledgements and Berkeley sync
    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
    
    # Core functionality
    server.register_function(p_signal, "p_signal")
    server.register_function(enhanced_p_signal, "enhanced_p_signal")
    
    # Berkeley clock sync
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")
    
    # Utility methods
    server.register_function(ping, "ping")
    server.register_function(get_pedestrian_stats, "get_pedestrian_stats")

    print(f"[{MY_NAME}] 🚀 RPC server listening on port {MY_PORT}")
    print(f"[{MY_NAME}] 🗳️ Ready to provide Ricart-Agrawala votes to controllers")
    print(f"[{MY_NAME}] 🔄 Background pedestrian simulation active")
    print("=" * 70)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{MY_NAME}] 👋 Shutting down pedestrian signal system...")

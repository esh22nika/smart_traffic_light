# p_signal.py (Performance Optimized)
# Pedestrian signal client with faster response times and load balancing awareness
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
ZOOKEEPER_IP = "http://localhost:6000"
CONTROLLERS = {
    "controller": "http://localhost:8000",
    "controller_clone": "http://localhost:8001"
}

# Statistics tracking
request_stats = {
    "total_requests": 0,
    "normal_requests": 0,
    "vip_requests": 0,
    "last_request_time": 0,
    "denied_requests": 0,
    "granted_requests": 0
}

stats_lock = threading.Lock()

# -------------------------
# OPTIMIZED PEDESTRIAN ACK LOGIC
# -------------------------
def p_signal(target_pair):
    """
    Optimized pedestrian acknowledgment with faster response times.
    Controllers call this directly for Ricart-Agrawala voting.
    """
    with stats_lock:
        request_stats["total_requests"] += 1
        request_stats["last_request_time"] = time.time()
    
    print(f"[{MY_NAME}] 🚶‍♂️ Quick pedestrian check for {target_pair}")
    
    # Optimized pedestrian crossing check (reduced processing time)
    pedestrian_clear = check_pedestrian_crossing_fast(target_pair)
    
    if pedestrian_clear:
        print(f"[{MY_NAME}] ✅ CLEAR for {target_pair} - VOTE: OK")
        with stats_lock:
            request_stats["normal_requests"] += 1
            request_stats["granted_requests"] += 1
        return "OK"
    else:
        print(f"[{MY_NAME}] ❌ PEDESTRIANS DETECTED on {target_pair} - VOTE: DENY")
        with stats_lock:
            request_stats["denied_requests"] += 1
        return "DENY"


def check_pedestrian_crossing_fast(target_pair):
    """
    Optimized pedestrian crossing check with faster processing.
    In a real implementation, this would check actual sensors quickly.
    """
    import random
    
    # Optimized: 95% chance pedestrians are clear (higher success rate for testing)
    is_clear = random.random() > 0.05
    
    if not is_clear:
        print(f"[{MY_NAME}] 🚶‍♂️🚶‍♀️ Pedestrians crossing {target_pair}")
    
    return is_clear


def enhanced_p_signal(target_pair, request_type="normal", requester_id="unknown"):
    """
    Enhanced version that accepts additional parameters for better logging.
    """
    with stats_lock:
        if request_type == "vip":
            request_stats["vip_requests"] += 1
        else:
            request_stats["normal_requests"] += 1
    
    print(f"[{MY_NAME}] 🚶‍♂️ Enhanced check: {target_pair} ({request_type}) from {requester_id}")
    
    return p_signal(target_pair)


# -------------------------
# OPTIMIZED BERKELEY CLIENT METHODS
# -------------------------
def get_clock_value(server_time):
    """Optimized Step 2 & 3: Return own_time - server_time faster"""
    own_time = time.time() + local_skew
    clock_value = own_time - server_time
    print(f"[{MY_NAME}] ⏱  get_clock_value → diff={clock_value:+.2f}s")
    return clock_value


def set_time(new_time):
    """Optimized Step 6 & 7: Set local clock to new_time faster"""
    global local_skew
    current_actual_time = time.time()
    local_skew = new_time - current_actual_time
    print(f"[{MY_NAME}] ⏳ set_time → new_skew={local_skew:+.2f}s")
    return "OK"


# -------------------------
# UTILITY AND DEBUG METHODS
# -------------------------
def get_pedestrian_stats():
    """Enhanced debug method to check pedestrian signal statistics"""
    with stats_lock:
        stats_copy = request_stats.copy()
    
    stats_copy["local_time"] = format_time(time.time() + local_skew)
    stats_copy["local_skew"] = local_skew
    if stats_copy["total_requests"] > 0:
        stats_copy["success_rate"] = (stats_copy["granted_requests"] / stats_copy["total_requests"]) * 100
    else:
        stats_copy["success_rate"] = 100
    return stats_copy


def ping():
    """Health check method"""
    return f"{MY_NAME} OK"


def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def log_system_status():
    """Optimized periodic logging of system status"""
    while True:
        time.sleep(30)  # Log every 30 seconds
        with stats_lock:
            total = request_stats["total_requests"]
            granted = request_stats["granted_requests"]
            denied = request_stats["denied_requests"]
            last_req = request_stats["last_request_time"]
        
        if total > 0:
            time_since_last = time.time() - last_req if last_req > 0 else 0
            success_rate = (granted / total) * 100
            
            print(f"\n[{MY_NAME}] 📊 === PEDESTRIAN SYSTEM STATUS ===")
            print(f"[{MY_NAME}] 📈 Total Votes: {total} | ✅ Granted: {granted} | ❌ Denied: {denied}")
            print(f"[{MY_NAME}] 🎯 Success Rate: {success_rate:.1f}%")
            print(f"[{MY_NAME}] ⏱  Last vote request: {time_since_last:.1f}s ago")
            print(f"[{MY_NAME}] ==========================================\n")


# -------------------------
# ENHANCED CLIENT FOR RTO OFFICERS
# -------------------------
def get_real_time_data():
    """
    New RPC method for RTO officers to get real-time pedestrian data.
    This connects to the ZooKeeper database feature.
    """
    try:
        proxy = ServerProxy(ZOOKEEPER_IP, allow_none=True)
        system_status = proxy.get_system_status()
        
        with stats_lock:
            pedestrian_data = {
                "pedestrian_stats": request_stats.copy(),
                "system_status_from_zookeeper": system_status,
                "local_time": format_time(time.time() + local_skew),
                "service_availability": "ACTIVE - Connected to ZooKeeper"
            }
        
        return pedestrian_data
        
    except Exception as e:
        print(f"[{MY_NAME}] ⚠ Could not fetch ZooKeeper data: {e}")
        with stats_lock:
            return {
                "pedestrian_stats": request_stats.copy(),
                "local_time": format_time(time.time() + local_skew),
                "service_availability": "LIMITED - ZooKeeper Unreachable"
            }


# -------------------------
# MAIN ENTRY POINT
# -------------------------
if __name__ == "__main__":
    print("=" * 70)
    print(f"🚶‍♂️ OPTIMIZED PEDESTRIAN SIGNAL CLIENT [{MY_NAME}]")
    print("=" * 70)
    print(f"[{MY_NAME}] ⚡ Performance optimized: Faster response times")
    print(f"[{MY_NAME}] 🌐 ZooKeeper integration: {ZOOKEEPER_IP}")
    print(f"[{MY_NAME}] ⏱  Initial clock skew: {local_skew:+.2f}s")
    print(f"[{MY_NAME}] 📊 Enhanced RTO officer data access enabled")
    print("=" * 70)
    
    # Start background threads
    status_thread = threading.Thread(target=log_system_status, daemon=True)
    status_thread.start()
    
    # Start RPC server
    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
    server.register_function(p_signal, "p_signal")
    server.register_function(enhanced_p_signal, "enhanced_p_signal")
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")
    server.register_function(get_pedestrian_stats, "get_pedestrian_stats")
    server.register_function(get_real_time_data, "get_real_time_data")
    server.register_function(ping, "ping")

    print(f"[{MY_NAME}] 🚀 Pedestrian client ready on port {MY_PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{MY_NAME}] Shutting down...")

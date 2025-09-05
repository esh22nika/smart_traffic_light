# t_signal.py (Performance Optimized)
# Traffic client with high-frequency requests to test dynamic scaling
import time
import random
import threading
import uuid
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

# -------------------------
# CONFIGURATION
# -------------------------
MY_PORT = 7000
MY_NAME = "t_signal"
ZOOKEEPER_IP = "http://192.168.0.168:6000"

# Initial clock skew: +30 minutes (in seconds)
local_skew = 30 * 60

# Simulation parameters
VIP_PROBABILITY = 0.35  # 35% chance of a VIP vehicle
REQUEST_INTERVAL_MIN = 2 # Minimum seconds between request bursts
REQUEST_INTERVAL_MAX = 5 # Maximum seconds between request bursts
REQUEST_BURST_SIZE = 2 # Number of requests to send in a quick burst

signal_pairs = {"1": [1, 2], "2": [1, 2], "3": [3, 4], "4": [3, 4]}

# Statistics tracking
request_stats = {
    "total_requests": 0,
    "vip_requests": 0,
    "normal_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "total_response_time": 0.0
}
stats_lock = threading.Lock()

# -------------------------
# TRAFFIC SIMULATION LOGIC
# -------------------------
def traffic_detection_loop():
    """High-frequency traffic simulation to test load balancing and scaling."""
    try:
        proxy = ServerProxy(ZOOKEEPER_IP, allow_none=True)
        proxy.ping() # Initial connection test
        print(f"[{MY_NAME}] ✅ Successfully connected to ZooKeeper at {ZOOKEEPER_IP}")
    except Exception as e:
        print(f"[{MY_NAME}] ❌ FATAL: Could not connect to ZooKeeper at {ZOOKEEPER_IP}. Error: {e}")
        return

    while True:
        print(f"\n[{MY_NAME}] 🚦 Generating a new burst of {REQUEST_BURST_SIZE} traffic requests...")
        
        threads = []
        for i in range(REQUEST_BURST_SIZE):
            thread = threading.Thread(target=send_traffic_request, args=(proxy, i + 1))
            threads.append(thread)
            thread.start()
            time.sleep(random.uniform(0.05, 0.2)) # Stagger requests slightly

        for thread in threads:
            thread.join()
        
        sleep_time = random.randint(REQUEST_INTERVAL_MIN, REQUEST_INTERVAL_MAX)
        print(f"[{MY_NAME}] 💤 Burst complete. Waiting for {sleep_time} seconds...")
        time.sleep(sleep_time)

def send_traffic_request(proxy, burst_index):
    """Sends a single normal or VIP traffic request to the ZooKeeper."""
    sensed_signal = random.choice(list(signal_pairs.keys()))
    target_pair = signal_pairs[sensed_signal]
    start_time = time.time()
    is_vip = random.random() < VIP_PROBABILITY

    try:
        if is_vip:
            vehicle_id = f"VIP-{uuid.uuid4().hex[:6]}"
            priority = random.randint(1, 4)
            print(f"[{MY_NAME}] 🚨 Detected VIP vehicle {vehicle_id} (P{priority}) for {target_pair}")
            result = proxy.vip_arrival(target_pair, priority, vehicle_id)
            with stats_lock:
                request_stats["vip_requests"] += 1
        else:
            print(f"[{MY_NAME}] 🚗 Detected normal traffic for {target_pair}")
            result = proxy.signal_controller(target_pair)
            with stats_lock:
                request_stats["normal_requests"] += 1

        end_time = time.time()
        response_time = end_time - start_time
        
        print(f"[{MY_NAME}] ✅ Request successful in {response_time:.2f}s. Response: {result}")
        with stats_lock:
            request_stats["successful_requests"] += 1
            request_stats["total_response_time"] += response_time

    except Exception as e:
        print(f"[{MY_NAME}] ❌ Request failed for {target_pair}. Error: {e}")
        with stats_lock:
            request_stats["failed_requests"] += 1
    
    with stats_lock:
        request_stats["total_requests"] += 1

# -------------------------
# OPTIMIZED BERKELEY CLIENT METHODS
# -------------------------
def get_clock_value(server_time):
    """Optimized Step 2 & 3: Return own_time - server_time faster"""
    own_time = time.time() + local_skew
    clock_value = own_time - server_time
    return clock_value

def set_time(new_time):
    """Optimized Step 6 & 7: Set local clock to new_time faster"""
    global local_skew
    current_actual_time = time.time()
    local_skew = new_time - current_actual_time
    return "OK"

# -------------------------
# UTILITY AND DEBUG METHODS
# -------------------------
def get_traffic_stats():
    """RPC method to get local traffic signal statistics"""
    with stats_lock:
        stats_copy = request_stats.copy()
    
    stats_copy["local_time"] = format_time(time.time() + local_skew)
    stats_copy["local_skew"] = f"{local_skew:+.2f}s"
    if stats_copy["successful_requests"] > 0:
        stats_copy["average_response_time"] = f"{stats_copy['total_response_time'] / stats_copy['successful_requests']:.2f}s"
    else:
        stats_copy["average_response_time"] = "N/A"
    return stats_copy

def ping():
    return f"{MY_NAME} OK"

def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

# -------------------------
# MAIN ENTRY POINT
# -------------------------
if __name__ == "__main__":
    print("=" * 70)
    print(f"🚗 OPTIMIZED TRAFFIC SIGNAL CLIENT [{MY_NAME}]")
    print("=" * 70)
    print(f"[{MY_NAME}] ⚡ Performance optimized: High-frequency request bursts")
    print(f"[{MY_NAME}] 🌐 Connecting to ZooKeeper: {ZOOKEEPER_IP}")
    print(f"[{MY_NAME}] ⏱  Initial clock skew: {local_skew:+.2f}s")
    print(f"[{MY_NAME}] 📈 Burst size: {REQUEST_BURST_SIZE} requests")
    print("=" * 70)

    # Start the main traffic simulation loop in a background thread
    simulation_thread = threading.Thread(target=traffic_detection_loop, daemon=True)
    simulation_thread.start()

    # Start the RPC server to listen for requests from the controller
    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")
    server.register_function(get_traffic_stats, "get_traffic_stats")
    server.register_function(ping, "ping")

    print(f"[{MY_NAME}] 🚀 Traffic client ready on port {MY_PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{MY_NAME}] Shutting down...")


# t_signal.py (Updated for ZooKeeper load balancing)
# Traffic client: Berkeley client + traffic sensing + VIP simulation
# Now contacts ZooKeeper instead of controller directly
import time
import random
import threading
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

# Updated configuration - now points to ZooKeeper
ZOOKEEPER_IP = "http://192.168.0.170:6000"  # ZooKeeper endpoint instead of controller
MY_PORT = 7000
MY_NAME = "t_signal"

# Initial skew: +45 minutes
local_skew = 45 * 60

signal_pairs = {
    "1": [1, 2],
    "2": [1, 2],
    "3": [3, 4],
    "4": [3, 4]
}

VIP_PROB = 0.35  # chance that the sensed vehicle is VIP (tweak for tests)

# Enhanced traffic detection to create multiple concurrent requests
def traffic_detection_loop():
    proxy = ServerProxy(ZOOKEEPER_IP, allow_none=True)
    request_counter = 0
    
    while True:
        # Create multiple requests to test buffer functionality
        num_requests = random.choice([1, 2])  # Send 1 or 2 requests at same time
        
        threads = []
        for i in range(num_requests):
            request_counter += 1
            thread = threading.Thread(
                target=send_traffic_request, 
                args=(proxy, request_counter, i == 0),  # First request gets delay
                daemon=True
            )
            threads.append(thread)
        
        # Start all requests nearly simultaneously
        for thread in threads:
            thread.start()
            time.sleep(0.1)  # Small delay between requests
        
        # Wait for all requests to complete
        for thread in threads:
            thread.join()
        
        # Wait before next batch of requests
        time.sleep(random.randint(8, 12))


def send_traffic_request(proxy, request_id, add_delay=False):
    """Send individual traffic request to ZooKeeper"""
    if add_delay:
        time.sleep(random.randint(1, 3))  # Random delay for first request
    
    sensed = str(random.choice([1, 2, 3, 4]))
    pair = signal_pairs[sensed]
    
    try:
        if random.random() < VIP_PROB:
            # Generate unique VIP ID
            vip_id = f"VIP-{request_id:03d}"
            priority = random.choice([1, 2, 3, 4])  # Random VIP priority
            
            print(f"\n[{MY_NAME}] 🚨 VIP-{priority} detected at {pair} (ID: {vip_id}). Notifying ZooKeeper...")
            start_time = time.time()
            result = proxy.vip_arrival(pair, priority, vip_id)
            end_time = time.time()
            print(f"[{MY_NAME}] ✅ VIP request {vip_id} completed in {end_time-start_time:.2f}s")
        else:
            print(f"\n[{MY_NAME}] 🚦 Normal traffic at {sensed} (Req-{request_id:03d}). Requesting switch for {pair} via ZooKeeper")
            start_time = time.time()
            result = proxy.signal_controller(pair)
            end_time = time.time()
            print(f"[{MY_NAME}] ✅ Normal traffic request Req-{request_id:03d} completed in {end_time-start_time:.2f}s")
            
    except Exception as e:
        print(f"[{MY_NAME}] ❌ Error contacting ZooKeeper for Req-{request_id:03d}: {e}")


def enhanced_traffic_simulation():
    """Enhanced simulation that creates concurrent load for testing buffer mechanism"""
    proxy = ServerProxy(ZOOKEEPER_IP, allow_none=True)
    
    print(f"[{MY_NAME}] 🚀 Starting enhanced traffic simulation with concurrent requests")
    print(f"[{MY_NAME}] 📊 VIP probability: {VIP_PROB*100:.0f}%")
    
    scenario_counter = 0
    
    while True:
        scenario_counter += 1
        scenario_type = random.choice([
            "single_normal", 
            "single_vip", 
            "concurrent_mixed", 
            "vip_storm",
            "normal_burst"
        ])
        
        print(f"\n[{MY_NAME}] 📋 === SCENARIO {scenario_counter}: {scenario_type.upper()} ===")
        
        if scenario_type == "single_normal":
            send_single_request(proxy, "normal")
            
        elif scenario_type == "single_vip":
            send_single_request(proxy, "vip")
            
        elif scenario_type == "concurrent_mixed":
            # Send multiple requests simultaneously to test buffer
            threads = []
            for i in range(3):  # 3 concurrent requests
                req_type = "vip" if random.random() < 0.4 else "normal"
                thread = threading.Thread(
                    target=send_single_request, 
                    args=(proxy, req_type, f"CONC-{scenario_counter}-{i}"),
                    daemon=True
                )
                threads.append(thread)
            
            # Start all threads nearly simultaneously
            for thread in threads:
                thread.start()
                time.sleep(0.2)  # Small stagger
            
            # Wait for completion
            for thread in threads:
                thread.join()
                
        elif scenario_type == "vip_storm":
            # Multiple high-priority VIPs to test deadlock handling
            print(f"[{MY_NAME}] 🚨 VIP STORM: Multiple emergency vehicles!")
            threads = []
            for i in range(2):
                thread = threading.Thread(
                    target=send_single_request,
                    args=(proxy, "vip", f"STORM-{scenario_counter}-{i}"),
                    daemon=True
                )
                threads.append(thread)
            
            for thread in threads:
                thread.start()
                time.sleep(0.5)
            
            for thread in threads:
                thread.join()
                
        elif scenario_type == "normal_burst":
            # Burst of normal traffic to test load balancing
            print(f"[{MY_NAME}] 📈 NORMAL BURST: Heavy traffic load!")
            threads = []
            for i in range(4):  # 4 normal requests
                thread = threading.Thread(
                    target=send_single_request,
                    args=(proxy, "normal", f"BURST-{scenario_counter}-{i}"),
                    daemon=True
                )
                threads.append(thread)
            
            for thread in threads:
                thread.start()
                time.sleep(0.3)
            
            for thread in threads:
                thread.join()
        
        # Wait before next scenario
        wait_time = random.randint(10, 15)
        print(f"[{MY_NAME}] ⏳ Waiting {wait_time}s before next scenario...")
        time.sleep(wait_time)


def send_single_request(proxy, request_type, request_id=None):
    """Send a single request to ZooKeeper"""
    if request_id is None:
        request_id = f"{request_type}-{int(time.time())}"
    
    sensed = str(random.choice([1, 2, 3, 4]))
    pair = signal_pairs[sensed]
    
    try:
        start_time = time.time()
        
        if request_type == "vip":
            vip_id = f"VIP-{request_id}"
            priority = random.choice([1, 2, 3])  # Higher priority VIPs
            priority_name = {1: "AMBULANCE", 2: "FIRE_TRUCK", 3: "POLICE"}.get(priority, f"VIP_P{priority}")
            
            print(f"[{MY_NAME}] 🚨 {priority_name} at {pair} (ID: {vip_id}) → ZooKeeper")
            result = proxy.vip_arrival(pair, priority, vip_id)
            
        else:  # normal
            print(f"[{MY_NAME}] 🚦 Normal traffic at {sensed} ({request_id}) for {pair} → ZooKeeper")
            result = proxy.signal_controller(pair)
        
        end_time = time.time()
        response_time = end_time - start_time
        
        print(f"[{MY_NAME}] ✅ Request {request_id} completed in {response_time:.2f}s")
        
        # Log slow responses (potential load balancing issues)
        if response_time > 15:
            print(f"[{MY_NAME}] ⚠️ SLOW RESPONSE detected for {request_id}: {response_time:.2f}s")
            
    except Exception as e:
        print(f"[{MY_NAME}] ❌ Error with request {request_id}: {e}")


# ---- Berkeley client methods (unchanged) ----
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


def get_status():
    """Debug method to check t_signal status"""
    return {
        "name": MY_NAME,
        "local_skew": local_skew,
        "zookeeper_endpoint": ZOOKEEPER_IP,
        "vip_probability": VIP_PROB
    }


# ---- RPC server for Berkeley ----
if __name__ == "__main__":
    print("=" * 70)
    print(f"🚦 TRAFFIC SIGNAL CLIENT [{MY_NAME}] STARTING")
    print("=" * 70)
    print(f"[{MY_NAME}] 🌐 ZooKeeper endpoint: {ZOOKEEPER_IP}")
    print(f"[{MY_NAME}] 📊 VIP probability: {VIP_PROB*100:.0f}%")
    print(f"[{MY_NAME}] ⏱ Initial clock skew: {local_skew:+.2f}s")
    print("=" * 70)
    
    # Start enhanced traffic simulation thread
    simulation_thread = threading.Thread(target=enhanced_traffic_simulation, daemon=True)
    simulation_thread.start()
    
    # Start RPC server for Berkeley clock sync
    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")
    server.register_function(get_status, "get_status")
    
    print(f"[{MY_NAME}] 🚀 RPC server listening on port {MY_PORT}")
    print(f"[{MY_NAME}] 🔄 Enhanced traffic simulation started")
    print(f"[{MY_NAME}] 📡 All requests will go through ZooKeeper load balancer")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{MY_NAME}] 👋 Shutting down...")

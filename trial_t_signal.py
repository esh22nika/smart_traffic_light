# t_signal.py (Updated for ZooKeeper load balancing)
# Traffic client: Berkeley client + traffic sensing + VIP simulation
# Now contacts ZooKeeper instead of controller directly

import time
import random
import threading
from xmlrpc.client import ServerProxy, Error
from xmlrpc.server import SimpleXMLRPCServer

# --- CONFIGURATION ---
# Updated configuration - now points to ZooKeeper
ZOOKEEPER_IP = "http://192.168.0.170:6000" # ZooKeeper endpoint instead of controller
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

VIP_PROB = 0.35 # Chance that the sensed vehicle is a VIP (tweak for tests)

# --- UTILITY FUNCTIONS ---
def get_server_time():
    """Returns the local time adjusted by the current skew."""
    return time.time() + local_skew

def format_time(ts):
    """Formats a timestamp for readable logging."""
    return time.strftime("%H:%M:%S", time.localtime(ts))

# --- TRAFFIC SIMULATION ---
def traffic_detection_loop():
    """Continuously simulates traffic and sends requests to ZooKeeper."""
    try:
        proxy = ServerProxy(ZOOKEEPER_IP, allow_none=True)
        # Check if ZooKeeper is online
        proxy.ping()
        print(f"[{MY_NAME}] Successfully connected to ZooKeeper at {ZOOKEEPER_IP}")
    except Exception as e:
        print(f"[{MY_NAME}] X Could not connect to ZooKeeper: {e}")
        print(f"[{MY_NAME}] Please ensure ZooKeeper is running and accessible.")
        return

    request_counter = 0
    while True:
        # Create multiple requests to test the controller's buffer functionality
        num_requests = random.choice([1, 2]) # Send 1 or 2 requests at the same time
        threads = []

        for i in range(num_requests):
            request_counter += 1
            # The first request in a batch has a small random delay
            thread = threading.Thread(
                target=send_traffic_request,
                args=(proxy, request_counter, i == 0),
                daemon=True
            )
            threads.append(thread)

        # Start all request threads nearly simultaneously
        for thread in threads:
            thread.start()
        time.sleep(0.1) # Small stagger between starting threads

        # Wait for all request threads to complete before the next cycle
        for thread in threads:
            thread.join()

        # Wait a random interval before generating the next batch of traffic
        time.sleep(random.randint(8, 12))

def send_traffic_request(proxy, request_id, add_delay=False):
    """Sends an individual normal or VIP traffic request to ZooKeeper."""
    if add_delay:
        time.sleep(random.randint(1, 3)) # Random delay for the first request

    sensed_signal = str(random.choice([1, 2, 3, 4]))
    pair = signal_pairs[sensed_signal]

    try:
        # Decide if the detected vehicle is a VIP
        if random.random() < VIP_PROB:
            vip_id = f"VIP-{request_id:03d}"
            priority = random.choice([1, 2, 3, 4]) # 1=Ambulance, 2=Fire Truck, etc.
            print(f"\n[{MY_NAME}] VIP-{priority} detected at signal {sensed_signal} (pair {pair}, ID: {vip_id}). Notifying ZooKeeper...")
            
            start_time = time.time()
            result = proxy.vip_arrival(pair, priority, vip_id)
            end_time = time.time()

            print(f"[{MY_NAME}] ZooKeeper processed VIP request {vip_id} in {end_time - start_time:.2f}s. Result: {result}")
        else:
            # It's a normal traffic request
            print(f"\n[{MY_NAME}] Normal traffic at signal {sensed_signal} (Req-{request_id:03d}). Requesting switch for pair {pair} via ZooKeeper...")
            
            start_time = time.time()
            result = proxy.signal_controller(pair)
            end_time = time.time()
            
            print(f"[{MY_NAME}] ZooKeeper processed normal request for {pair} in {end_time - start_time:.2f}s. Result: {result}")

    except Error as e:
        print(f"[{MY_NAME}] X RPC Error for request {request_id} for pair {pair}: {e}")
    except Exception as e:
        print(f"[{MY_NAME}] X An unexpected error occurred for request {request_id} for pair {pair}: {e}")


# --- BERKELEY CLOCK SYNCHRONIZATION (RPC METHODS) ---
def get_clock_value(server_time_str):
    """RPC function called by the controller to get this client's clock offset."""
    server_time = float(server_time_str)
    my_time = get_server_time()
    offset = my_time - server_time
    print(f"[Berkeley] Received server time: {format_time(server_time)}, My time: {format_time(my_time)}, Sending offset: {offset:+.2f}s")
    return offset

def set_time(new_time_str):
    """RPC function called by the controller to set a new synchronized time."""
    global local_skew
    new_time = float(new_time_str)
    current_time = time.time()
    local_skew = new_time - current_time
    print(f"[Berkeley] Time adjusted. New time is {format_time(get_server_time())} (skew: {local_skew:+.2f}s)")
    return "OK"

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("=" * 70)
    print(f"  TRAFFIC SIGNAL CLIENT ({MY_NAME}) STARTING")
    print("=" * 70)
    print(f"[{MY_NAME}] Will send requests to ZooKeeper at: {ZOOKEEPER_IP}")
    print(f"[{MY_NAME}] Initial local time: {format_time(get_server_time())} (skew: {local_skew:+.2f}s)")
    
    # Start the traffic detection loop in a background thread
    detection_thread = threading.Thread(target=traffic_detection_loop, daemon=True)
    detection_thread.start()

    # Start the RPC server for this client to handle Berkeley sync requests
    try:
        server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
        server.register_function(get_clock_value, "get_clock_value")
        server.register_function(set_time, "set_time")
        print(f"[{MY_NAME}] Berkeley RPC server listening on port {MY_PORT}...")
        print("=" * 70)
        server.serve_forever()
    except OSError as e:
        print(f"[{MY_NAME}] X Could not start RPC server on port {MY_PORT}: {e}")
        print(f"[{MY_NAME}] Is another process using this port?")
    except KeyboardInterrupt:
        print(f"\n[{MY_NAME}] Shutting down traffic signal client.")

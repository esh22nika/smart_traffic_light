# zookeeper.py
# Load balancer that manages controller and controller_clone
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from typing import Dict, List
import uuid
from collections import deque

# Configuration
ZOOKEEPER_PORT = 6000
CONTROLLERS = {
    "controller": "http://localhost:8000",
    "controller_clone": "http://localhost:8001"  # Clone on different port/IP
}
BUFFER_SIZE = 2  # Maximum concurrent requests per controller
RESPONSE_TIMEOUT = 5

class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn

class ControllerState:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.active_requests = deque(maxlen=BUFFER_SIZE)
        self.is_available = True
        self.last_heartbeat = time.time()
        self.lock = threading.Lock()
    
    def is_free(self) -> bool:
        with self.lock:
            return len(self.active_requests) < BUFFER_SIZE and self.is_available
    
    def add_request(self, request_id: str):
        with self.lock:
            self.active_requests.append(request_id)
            print(f"[ZOOKEEPER] 📊 {self.name} buffer: {len(self.active_requests)}/{BUFFER_SIZE}")
    
    def complete_request(self, request_id: str):
        with self.lock:
            try:
                self.active_requests.remove(request_id)
                print(f"[ZOOKEEPER] ✅ {self.name} completed {request_id}, buffer: {len(self.active_requests)}/{BUFFER_SIZE}")
            except ValueError:
                pass  # Request not found, already removed

class ZooKeeperLoadBalancer:
    def __init__(self):
        self.controllers = {
            name: ControllerState(name, url) 
            for name, url in CONTROLLERS.items()
        }
        self.round_robin_index = 0
        self.lock = threading.Lock()
        
    def log_separator(self, title="", char="=", width=70):
        if title:
            padding = (width - len(title) - 2) // 2
            print(f"\n{char * padding} {title} {char * padding}")
        else:
            print(f"\n{char * width}")

    def get_available_controller(self) -> ControllerState:
        """Select an available controller using round-robin with buffer checking"""
        with self.lock:
            # First try: find a completely free controller
            for controller in self.controllers.values():
                if controller.is_free() and len(controller.active_requests) == 0:
                    print(f"[ZOOKEEPER] 🎯 Selected {controller.name} (completely free)")
                    return controller
            
            # Second try: find any controller with available buffer space
            for controller in self.controllers.values():
                if controller.is_free():
                    print(f"[ZOOKEEPER] 🎯 Selected {controller.name} (has buffer space: {len(controller.active_requests)}/{BUFFER_SIZE})")
                    return controller
            
            # Fallback: round-robin among available controllers
            controller_list = list(self.controllers.values())
            for i in range(len(controller_list)):
                idx = (self.round_robin_index + i) % len(controller_list)
                controller = controller_list[idx]
                if controller.is_available:
                    self.round_robin_index = (idx + 1) % len(controller_list)
                    print(f"[ZOOKEEPER] ⚠️ Selected {controller.name} (fallback round-robin)")
                    return controller
            
            # Emergency fallback: use first available controller
            print("[ZOOKEEPER] ⚠️ All controllers busy! Using emergency fallback")
            return controller_list[0]

    def forward_request(self, method_name: str, *args, **kwargs):
        """Generic request forwarding with load balancing"""
        request_id = str(uuid.uuid4())[:8]
        
        self.log_separator(f"LOAD BALANCER: {method_name.upper()}", "🔄")
        print(f"[ZOOKEEPER] 📥 Incoming request: {method_name} (ID: {request_id})")
        print(f"[ZOOKEEPER] 📊 Request args: {args}")
        
        # Select controller
        controller = self.get_available_controller()
        controller.add_request(request_id)
        
        print(f"[ZOOKEEPER] 🚀 Forwarding {method_name} to {controller.name}")
        
        try:
            # Forward request to selected controller
            proxy = ServerProxy(controller.url, allow_none=True, transport=TimeoutTransport(RESPONSE_TIMEOUT))
            method = getattr(proxy, method_name)
            
            start_time = time.time()
            result = method(*args, **kwargs)
            end_time = time.time()
            
            print(f"[ZOOKEEPER] ✅ {controller.name} completed {method_name} in {end_time-start_time:.2f}s")
            controller.complete_request(request_id)
            
            self.log_separator()
            return result
            
        except Exception as e:
            print(f"[ZOOKEEPER] ❌ Error forwarding to {controller.name}: {e}")
            controller.complete_request(request_id)
            controller.is_available = False
            
            # Try with another controller
            print(f"[ZOOKEEPER] 🔄 Retrying with another controller...")
            return self.forward_request(method_name, *args, **kwargs)

    # RPC Methods that clients will call
    def signal_controller(self, target_pair):
        """Forward normal traffic signal requests"""
        return self.forward_request("signal_controller", target_pair)
    
    def vip_arrival(self, target_pair, priority=1, vehicle_id=None):
        """Forward VIP arrival requests"""
        return self.forward_request("vip_arrival", target_pair, priority, vehicle_id)
    
    def ping(self):
        """Health check"""
        return "ZooKeeper OK"
    
    def get_load_balancer_status(self):
        """Debug method to check load balancer status"""
        status = {}
        for name, controller in self.controllers.items():
            with controller.lock:
                status[name] = {
                    "available": controller.is_available,
                    "active_requests": len(controller.active_requests),
                    "buffer_size": BUFFER_SIZE,
                    "requests": list(controller.active_requests)
                }
        return status

    def health_check_loop(self):
        """Periodic health check for controllers"""
        while True:
            for name, controller in self.controllers.items():
                try:
                    proxy = ServerProxy(controller.url, allow_none=True, transport=TimeoutTransport(2))
                    response = proxy.ping()
                    if response == "OK":
                        controller.is_available = True
                        controller.last_heartbeat = time.time()
                except Exception:
                    print(f"[ZOOKEEPER] ⚠️ {name} health check failed")
                    controller.is_available = False
            
            time.sleep(10)  # Check every 10 seconds

if __name__ == "__main__":
    print("=" * 70)
    print("🦓 ZOOKEEPER LOAD BALANCER STARTING")
    print("=" * 70)
    print(f"[ZOOKEEPER] 🌐 Managing controllers: {list(CONTROLLERS.keys())}")
    print(f"[ZOOKEEPER] 📊 Buffer size per controller: {BUFFER_SIZE}")
    
    # Initialize load balancer
    lb = ZooKeeperLoadBalancer()
    
    # Start health check thread
    health_thread = threading.Thread(target=lb.health_check_loop, daemon=True)
    health_thread.start()
    
    # Start RPC server
    server = SimpleXMLRPCServer(("0.0.0.0", ZOOKEEPER_PORT), allow_none=True)
    server.register_function(lb.signal_controller, "signal_controller")
    server.register_function(lb.vip_arrival, "vip_arrival")
    server.register_function(lb.ping, "ping")
    server.register_function(lb.get_load_balancer_status, "get_load_balancer_status")
    
    print(f"[ZOOKEEPER] 🚀 Load balancer listening on port {ZOOKEEPER_PORT}")
    print("=" * 70)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[ZOOKEEPER] 👋 Shutting down load balancer...")

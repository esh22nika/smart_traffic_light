# zookeeper.py
# Enhanced Load Balancer with Dynamic Scaling, Database, and Performance Optimization
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from typing import Dict, List
import uuid
import json
import sqlite3
from collections import deque
import subprocess
import sys
import os

# Configuration
ZOOKEEPER_PORT = 6000
BASE_CONTROLLERS = {
    "controller": "http://localhost:8000",
    "controller_clone": "http://localhost:8001"
}
BUFFER_SIZE = 5  # Increased buffer size
MAX_DYNAMIC_CLONES = 3  # Maximum additional dynamic clones
RESPONSE_TIMEOUT = 3  # Reduced timeout for faster failure detection
DB_PATH = "traffic_system.db"


class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_database()

    def init_database(self):
        """Initialize the traffic system database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS signal_status (
                    id INTEGER PRIMARY KEY,
                    signal_id TEXT UNIQUE,
                    status TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS controller_status (
                    id INTEGER PRIMARY KEY,
                    controller_name TEXT UNIQUE,
                    url TEXT,
                    is_available BOOLEAN,
                    active_requests INTEGER,
                    buffer_size INTEGER,
                    last_heartbeat TIMESTAMP,
                    total_processed INTEGER DEFAULT 0,
                    is_dynamic BOOLEAN DEFAULT 0
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS request_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT,
                    request_type TEXT,
                    target_pair TEXT,
                    controller_assigned TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    response_time REAL,
                    status TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS vip_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    vehicle_id TEXT,
                    priority INTEGER,
                    target_pair TEXT,
                    arrival_time TIMESTAMP,
                    served_by TEXT,
                    service_time REAL
                )
            ''')
            # Initialize default signal status
            default_signals = {
                '1': 'RED', '2': 'RED', '3': 'GREEN', '4': 'GREEN',
                'P1': 'GREEN', 'P2': 'GREEN', 'P3': 'RED', 'P4': 'RED'
            }
            for signal_id, status in default_signals.items():
                conn.execute(
                    'INSERT OR REPLACE INTO signal_status (signal_id, status) VALUES (?, ?)',
                    (signal_id, status)
                )
            conn.commit()
            print(f"[DATABASE] Database initialized at {db_path}")

    def update_signal_status(self, signal_status_dict):
        """Update signal status in database - Convert all keys to strings"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for signal_id, status in signal_status_dict.items():
                    # Ensure signal_id is always a string
                    signal_id_str = str(signal_id)
                    conn.execute(
                        'INSERT OR REPLACE INTO signal_status (signal_id, status, last_updated) VALUES (?, ?, CURRENT_TIMESTAMP)',
                        (signal_id_str, status)
                    )
                conn.commit()
                print(f"[DATABASE] Updated signal status for {len(signal_status_dict)} signals")

    def get_signal_status(self):
        """Get current signal status"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('SELECT signal_id, status, last_updated FROM signal_status')
                return {row[0]: {'status': row[1], 'last_updated': row[2]} for row in cursor.fetchall()}

    def update_controller_status(self, controller_name, **kwargs):
        """Update controller status in database"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                # Check if controller exists
                cursor = conn.execute('SELECT id FROM controller_status WHERE controller_name = ?', (controller_name,))
                if cursor.fetchone():
                    # Update existing
                    set_clauses = []
                    values = []
                    for key, value in kwargs.items():
                        if key in ['url', 'is_available', 'active_requests', 'buffer_size', 'total_processed',
                                   'is_dynamic']:
                            set_clauses.append(f'{key} = ?')
                            values.append(value)
                    if set_clauses:
                        set_clauses.append('last_heartbeat = CURRENT_TIMESTAMP')
                        values.append(controller_name)
                        query = f'UPDATE controller_status SET {", ".join(set_clauses)} WHERE controller_name = ?'
                        conn.execute(query, values)
                else:
                    # Insert new
                    conn.execute('''
                        INSERT INTO controller_status 
                        (controller_name, url, is_available, active_requests, buffer_size, 
                         last_heartbeat, total_processed, is_dynamic)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
                    ''', (
                        controller_name,
                        kwargs.get('url', ''),
                        kwargs.get('is_available', True),
                        kwargs.get('active_requests', 0),
                        kwargs.get('buffer_size', BUFFER_SIZE),
                        kwargs.get('total_processed', 0),
                        kwargs.get('is_dynamic', False)
                    ))
                conn.commit()

    def log_request(self, request_id, request_type, target_pair, controller_assigned,
                    start_time, end_time=None, status="processing"):
        """Log request to database"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                response_time = (end_time - start_time) if end_time else None
                conn.execute('''
                    INSERT OR REPLACE INTO request_log 
                    (request_id, request_type, target_pair, controller_assigned, start_time, 
                     end_time, response_time, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (request_id, request_type, str(target_pair), controller_assigned,
                      start_time, end_time, response_time, status))
                conn.commit()

    def get_system_stats(self):
        """Get comprehensive system statistics from database"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                # Controller stats
                controllers = conn.execute('''
                    SELECT controller_name, url, is_available, active_requests, buffer_size,
                           total_processed, is_dynamic, last_heartbeat
                    FROM controller_status
                ''').fetchall()

                # Recent requests
                recent_requests = conn.execute('''
                    SELECT request_type, controller_assigned, response_time, status
                    FROM request_log
                    ORDER BY start_time DESC LIMIT 10
                ''').fetchall()

                # Signal status
                signals = self.get_signal_status()

                return {
                    'controllers': [dict(
                        zip(['name', 'url', 'available', 'active', 'buffer_size', 'processed', 'dynamic', 'heartbeat'],
                            c)) for c in controllers],
                    'recent_requests': [dict(zip(['type', 'controller', 'response_time', 'status'], r)) for r in
                                        recent_requests],
                    'signal_status': signals,
                    'timestamp': time.time()
                }


class ControllerState:
    def __init__(self, name: str, url: str, is_dynamic: bool = False):
        self.name = name
        self.url = url
        self.active_requests = deque(maxlen=BUFFER_SIZE)
        self.is_available = True
        self.last_heartbeat = time.time()
        self.lock = threading.Lock()
        self.total_processed = 0
        self.is_dynamic = is_dynamic

    def is_free(self) -> bool:
        with self.lock:
            return len(self.active_requests) < BUFFER_SIZE and self.is_available

    def add_request(self, request_id: str):
        with self.lock:
            self.active_requests.append(request_id)
            print(f"[ZOOKEEPER] Buffer {self.name}: {len(self.active_requests)}/{BUFFER_SIZE}")

    def complete_request(self, request_id: str):
        with self.lock:
            try:
                self.active_requests.remove(request_id)
                self.total_processed += 1
                print(f"[ZOOKEEPER] Completed {self.name} {request_id}, buffer: {len(self.active_requests)}/{BUFFER_SIZE}")
            except ValueError:
                pass


class DynamicCloneManager:
    def __init__(self, base_port=8002):
        self.base_port = base_port
        self.dynamic_clones = {}
        self.clone_counter = 0

    def create_dynamic_clone(self) -> tuple:
        """Create a new dynamic controller clone"""
        if len(self.dynamic_clones) >= MAX_DYNAMIC_CLONES:
            print(f"[CLONE-MANAGER] Maximum dynamic clones ({MAX_DYNAMIC_CLONES}) reached")
            return None, None

        self.clone_counter += 1
        clone_name = f"dynamic_clone_{self.clone_counter}"
        clone_port = self.base_port + self.clone_counter
        clone_url = f"http://localhost:{clone_port}"

        try:
            # Create a modified version of controller_clone.py with different port
            self._create_clone_script(clone_name, clone_port)

            # Start the clone process
            clone_process = subprocess.Popen([
                sys.executable, f"{clone_name}.py"
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Wait a moment for it to start
            time.sleep(2)

            # Test if it's responding
            test_proxy = ServerProxy(clone_url, allow_none=True, transport=TimeoutTransport(2))
            response = test_proxy.ping()
            if response == "OK":
                self.dynamic_clones[clone_name] = {
                    'process': clone_process,
                    'port': clone_port,
                    'url': clone_url
                }
                print(f"[CLONE-MANAGER] Dynamic clone {clone_name} created successfully on port {clone_port}")
                return clone_name, clone_url
            else:
                clone_process.terminate()
                return None, None

        except Exception as e:
            print(f"[CLONE-MANAGER] Failed to create dynamic clone: {e}")
            return None, None

    def _create_clone_script(self, clone_name, port):
        """Create a dynamic clone script file"""
        template_content = f'''
# {clone_name}.py - Dynamically created controller clone
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from dataclasses import dataclass
from typing import List
import uuid

# Configuration for dynamic clone
CLIENTS = {{
    "t_signal": "http://localhost:7000",
    "p_signal": "http://localhost:9000",
}}
PEDESTRIAN_IP = CLIENTS["p_signal"]
RESPONSE_TIMEOUT = 3
VIP_CROSSING_TIME = 0.1  # Minimal processing time
CONTROLLER_PORT = {port}
CONTROLLER_NAME = "{clone_name.upper()}"

server_skew = 0.0
state_lock = threading.Lock()
vip_queues = {{"12": [], "34": []}}

# Simplified signal status - all keys as strings
signal_status = {{
    "1": "RED", "2": "RED", "3": "GREEN", "4": "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}}

def ping():
    return "OK"

def signal_controller(target_pair):
    print(f"[{{CONTROLLER_NAME}}] Processing signal request for {{target_pair}}")
    time.sleep(0.1)
    return True

def vip_arrival(target_pair, priority=1, vehicle_id=None):
    print(f"[{{CONTROLLER_NAME}}] Processing VIP request for {{target_pair}}")
    time.sleep(0.1)
    return True

if __name__ == "__main__":
    print(f"[{{CONTROLLER_NAME}}] Dynamic clone starting on port {{CONTROLLER_PORT}}")
    server = SimpleXMLRPCServer(("0.0.0.0", CONTROLLER_PORT), allow_none=True)
    server.register_function(signal_controller, "signal_controller")
    server.register_function(vip_arrival, "vip_arrival")
    server.register_function(ping, "ping")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\\n[{{CONTROLLER_NAME}}] Shutting down...")
'''
        with open(f"{clone_name}.py", 'w') as f:
            f.write(template_content)


class ZooKeeperLoadBalancer:
    def __init__(self):
        self.db = DatabaseManager(DB_PATH)
        self.controllers = {}
        self.clone_manager = DynamicCloneManager()
        self.round_robin_index = 0
        self.lock = threading.Lock()

        # Initialize base controllers
        for name, url in BASE_CONTROLLERS.items():
            self.controllers[name] = ControllerState(name, url)
            self.db.update_controller_status(
                name, url=url, is_available=True, active_requests=0,
                buffer_size=BUFFER_SIZE, total_processed=0, is_dynamic=False
            )

    def log_separator(self, title="", char="=", width=70):
        if title:
            padding = (width - len(title) - 2) // 2
            print(f"\n{char * padding} {title} {char * padding}")
        else:
            print(f"\n{char * width}")

    def get_available_controller(self) -> ControllerState:
        """Enhanced controller selection with dynamic scaling"""
        with self.lock:
            # First: Find completely free controllers
            free_controllers = [c for c in self.controllers.values()
                                if c.is_free() and len(c.active_requests) == 0]
            if free_controllers:
                controller = min(free_controllers, key=lambda c: c.total_processed)
                print(f"[ZOOKEEPER] Selected {controller.name} (completely free)")
                return controller

            # Second: Find controllers with buffer space
            available_controllers = [c for c in self.controllers.values() if c.is_free()]
            if available_controllers:
                controller = min(available_controllers, key=lambda c: len(c.active_requests))
                print(f"[ZOOKEEPER] Selected {controller.name} (buffer: {len(controller.active_requests)}/{BUFFER_SIZE})")
                return controller

            # Third: Try to create dynamic clone
            print(f"[ZOOKEEPER] All controllers busy! Attempting dynamic scaling...")
            clone_name, clone_url = self.clone_manager.create_dynamic_clone()
            if clone_name and clone_url:
                new_controller = ControllerState(clone_name, clone_url, is_dynamic=True)
                self.controllers[clone_name] = new_controller
                self.db.update_controller_status(
                    clone_name, url=clone_url, is_available=True, active_requests=0,
                    buffer_size=BUFFER_SIZE, total_processed=0, is_dynamic=True
                )
                print(f"[ZOOKEEPER] Dynamic scaling successful! Created {clone_name}")
                return new_controller

            # Fallback: Use least busy controller
            controller = min(self.controllers.values(), key=lambda c: len(c.active_requests))
            print(f"[ZOOKEEPER] Using overloaded controller {controller.name}")
            return controller

    def forward_request(self, method_name: str, *args, **kwargs):
        """Enhanced request forwarding with database logging"""
        request_id = str(uuid.uuid4())[:8]
        start_time = time.time()

        self.log_separator(f"LOAD BALANCER: {method_name.upper()}")
        print(f"[ZOOKEEPER] Request {request_id}: {method_name}{args}")

        controller = self.get_available_controller()
        controller.add_request(request_id)

        # Log request start
        self.db.log_request(request_id, method_name, args[0] if args else "",
                            controller.name, start_time)

        try:
            proxy = ServerProxy(controller.url, allow_none=True,
                                transport=TimeoutTransport(RESPONSE_TIMEOUT))
            method = getattr(proxy, method_name)
            result = method(*args, **kwargs)

            end_time = time.time()
            response_time = end_time - start_time

            print(f"[ZOOKEEPER] {controller.name} completed {request_id} in {response_time:.2f}s")

            # Update database
            controller.complete_request(request_id)
            self.db.log_request(request_id, method_name, args[0] if args else "",
                                controller.name, start_time, end_time, "completed")
            self.db.update_controller_status(controller.name,
                                             active_requests=len(controller.active_requests),
                                             total_processed=controller.total_processed)

            return result

        except Exception as e:
            end_time = time.time()
            print(f"[ZOOKEEPER] Error with {controller.name}: {e}")
            controller.complete_request(request_id)
            controller.is_available = False
            self.db.log_request(request_id, method_name, args[0] if args else "",
                                controller.name, start_time, end_time, "failed")

            # Retry with another controller
            return self.forward_request(method_name, *args, **kwargs)

    # RPC Methods
    def signal_controller(self, target_pair):
        return self.forward_request("signal_controller", target_pair)

    def vip_arrival(self, target_pair, priority=1, vehicle_id=None):
        return self.forward_request("vip_arrival", target_pair, priority, vehicle_id)

    def ping(self):
        return "ZooKeeper OK"

    def get_system_status(self):
        """RPC method for external clients to read system status"""
        return self.db.get_system_stats()

    def update_signal_status(self, signal_status):
        """RPC method for controllers to update signal status"""
        self.db.update_signal_status(signal_status)
        return "OK"

    def get_signal_status(self):
        """RPC method to get current signal status"""
        return self.db.get_signal_status()

    def health_check_loop(self):
        """Enhanced health check with database updates"""
        while True:
            for name, controller in self.controllers.items():
                try:
                    proxy = ServerProxy(controller.url, allow_none=True,
                                        transport=TimeoutTransport(2))
                    response = proxy.ping()
                    if response == "OK":
                        controller.is_available = True
                        controller.last_heartbeat = time.time()
                        self.db.update_controller_status(name, is_available=True,
                                                         active_requests=len(controller.active_requests))
                except Exception:
                    controller.is_available = False
                    self.db.update_controller_status(name, is_available=False)
                    print(f"[ZOOKEEPER] {name} health check failed")

            time.sleep(5)


if __name__ == "__main__":
    print("=" * 35)
    print("ENHANCED ZOOKEEPER LOAD BALANCER")
    print("=" * 35)
    print(f"Buffer size: {BUFFER_SIZE} | Max dynamic clones: {MAX_DYNAMIC_CLONES}")
    print(f"Database: {DB_PATH}")

    lb = ZooKeeperLoadBalancer()

    # Start health check
    health_thread = threading.Thread(target=lb.health_check_loop, daemon=True)
    health_thread.start()

    # Start RPC server
    server = SimpleXMLRPCServer(("0.0.0.0", ZOOKEEPER_PORT), allow_none=True)
    server.register_function(lb.signal_controller, "signal_controller")
    server.register_function(lb.vip_arrival, "vip_arrival")
    server.register_function(lb.ping, "ping")
    server.register_function(lb.get_system_status, "get_system_status")
    server.register_function(lb.update_signal_status, "update_signal_status")
    server.register_function(lb.get_signal_status, "get_signal_status")

    print(f"Enhanced ZooKeeper ready on port {ZOOKEEPER_PORT}")
    print("Features: Dynamic Scaling | Database | Performance Optimized")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nZooKeeper shutting down...")

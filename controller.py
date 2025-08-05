# manipulator.py
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import time

# Connect to pedestrian signal server (only for acknowledgment)
PEDESTRIAN_IP = "http://172.16.182.19:9000"
p_signal_proxy = ServerProxy(PEDESTRIAN_IP, allow_none=True)

# Initial combined state
signal_status = {
    1: "RED", 2: "RED", 3: "GREEN", 4: "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}

def signal_manipulator(target_pair):
    print(f"\n[Manipulator] 📥 Received request to switch traffic pair {target_pair}")
   
    # Step 1: Call p_signal just for acknowledgment
    try:
        response = p_signal_proxy.p_signal(target_pair)
        if response != "OK":
            print("[Manipulator] ❌ p_signal returned unexpected response!")
            return False
        print("[Manipulator] ✅ p_signal acknowledged. Proceeding...")
    except Exception as e:
        print(f"[Manipulator] ❌ Error contacting p_signal: {e}")
        return False

    # Step 2: Handle pedestrian logic
    handle_pedestrian_signals(target_pair)

    # Step 3: Handle traffic logic
    handle_traffic_signals(target_pair)

    print("[Manipulator] 🔁 Completed full signal cycle.\n")
    return True


def handle_pedestrian_signals(target_pair):
    red_group = target_pair
    green_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    # Blinking RED for red_group
    print(f"[Manipulator] ⚠️ Pedestrian signals {[f'P{x}' for x in red_group]} → Blinking RED (5s)")
    for i in range(5):
        for sig in red_group:
            signal_status[f"P{sig}"] = "BLINKING RED"
        time.sleep(1)

    # Static RED
    for sig in red_group:
        signal_status[f"P{sig}"] = "RED"
    print(f"[Manipulator] 🔴 Pedestrian {red_group} → RED")

    # YELLOW for green_group
    for sig in green_group:
        signal_status[f"P{sig}"] = "YELLOW"
    print(f"[Manipulator] 🟡 Pedestrian {green_group} → YELLOW")
    time.sleep(5)

    # GREEN
    for sig in green_group:
        signal_status[f"P{sig}"] = "GREEN"
    print(f"[Manipulator] 🟢 Pedestrian {green_group} → GREEN")


def handle_traffic_signals(target_pair):
    red_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    # YELLOW
    for sig in red_group:
        signal_status[sig] = "YELLOW"
    print(f"[Manipulator] 🟡 Traffic {red_group} → YELLOW")
    time.sleep(2)

    # RED
    for sig in red_group:
        signal_status[sig] = "RED"
    print(f"[Manipulator] 🔴 Traffic {red_group} → RED")

    # GREEN
    for sig in target_pair:
        signal_status[sig] = "GREEN"
    print(f"[Manipulator] 🟢 Traffic {target_pair} → GREEN")

    print(f"[Manipulator] ✅ Final Signal Status: {signal_status}")


# RPC server setup
server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)
print("[Manipulator] 🚦 RPC server running on port 8000")
server.register_function(signal_manipulator, "signal_manipulator")
server.serve_forever()

import time
import random
from xmlrpc.client import ServerProxy

# 👉 Replace this with actual IP of Machine B (Signal Manipulator Server)
MANIPULATOR_IP = "http://192.168.0.231:8000"
proxy = ServerProxy(MANIPULATOR_IP, allow_none=True)

# Initial local signal states (client-side mirror)
local_signal_status = {
    "1": "RED",
    "2": "RED",
    "3": "GREEN",
    "4": "GREEN"
}

# Define signal pairs
signal_pairs = {
    "1": [1, 2],
    "2": [1, 2],
    "3": [3, 4],
    "4": [3, 4]
}


def signal_controller():
    while True:
        sensed = str(random.choice([1, 2, 3, 4]))
        print(f"\n[Sensor] 🚦 Sensed traffic at signal {sensed}")

        if local_signal_status[sensed] == "RED":
            target_pair = signal_pairs[sensed]
            print(f"[Controller] RED at signal {sensed} → requesting change to pair {target_pair}")

            try:
                updated_status = proxy.signal_manipulator(target_pair)
                print("[Controller] ✅ Received update from server:")
                for k in updated_status:
                    print(f"  Signal {k} → {updated_status[k]}")
                    local_signal_status[k] = updated_status[k]
            except Exception as e:
                print(f"[Controller] ❌ Error contacting server: {e}")
        else:
            print(f"[Controller] ✅ Signal {sensed} is already GREEN. No action taken.")

        time.sleep(5)


if __name__ == "__main__":
    signal_controller()

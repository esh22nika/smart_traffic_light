
# controller.py
import time
import random
from xmlrpc.client import ServerProxy

MANIPULATOR_IP = "http://172.16.182.18:8000"
proxy = ServerProxy(MANIPULATOR_IP, allow_none=True)

signal_pairs = {
    "1": [1, 2],
    "2": [1, 2],
    "3": [3, 4],
    "4": [3, 4]
}

def signal_controller():
    while True:
        sensed = str(random.choice([1, 2, 3, 4]))
        target_pair = signal_pairs[sensed]
        try:
            print(f"\n[Controller] 🚦 Sensed traffic at signal {sensed}. Requesting switch for {target_pair}")
            proxy.signal_manipulator(target_pair)
        except Exception as e:
            print(f"[Controller] ❌ Error: {e}")
        time.sleep(6)

if __name__ == "__main__":
    signal_controller()

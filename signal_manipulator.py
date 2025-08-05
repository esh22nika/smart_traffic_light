# signal_manipulator.py (Machine B)

from xmlrpc.server import SimpleXMLRPCServer
import time

# Initial signal state
signal_status = {
    "1": "RED",
    "2": "RED",
    "3": "GREEN",
    "4": "GREEN"
}

def signal_manipulator(target_pair):
    print(f"\n[Manipulator] Switching to pair {target_pair}")
    target_pair =[str(sig) for sig in target_pair]
    # Determine current green pair
    current_pair = ["3", "4"] if target_pair == ["1","2"] else ["1", "2"]
    print(f"\n[Manipulator] Current pair: {current_pair}")
    print(f"\n[Manipulator] transitioning signals")
    # Turn current green to yellow
    for sig in current_pair:
        signal_status[sig] = "YELLOW"
    print(f"[Manipulator] {current_pair} → YELLOW")
    time.sleep(2)

    # Turn current pair to red
    for sig in current_pair:
        signal_status[sig] = "RED"
    print(f"[Manipulator] {current_pair} → RED")

    # Turn target pair to green
    for sig in target_pair:
        signal_status[sig] = "GREEN"
    print(f"[Manipulator] {target_pair} → GREEN")

    print(f"[Manipulator] Final siganl state: ")
    for sig,state in signal_status.items():
        print(f" Signal {sig} : {state}")
    return signal_status

# Bind the RPC server to 0.0.0.0 so it can be accessed remotely
server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)
print("[Manipulator] RPC server listening on port 8000...")
server.register_function(signal_manipulator, "signal_manipulator")
server.serve_forever()

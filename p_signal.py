# p_signal.py
from xmlrpc.server import SimpleXMLRPCServer

def p_signal(target_pair):
    print(f"[P_Signal] ✅ Acknowledged request for traffic pair {target_pair}")
    return "OK"

server = SimpleXMLRPCServer(("0.0.0.0", 9000), allow_none=True)
print("[P_Signal] 👣 RPC server listening at port 9000")
server.register_function(p_signal, "p_signal")
server.serve_forever()

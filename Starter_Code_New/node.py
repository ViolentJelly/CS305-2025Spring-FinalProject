import peer_discovery

print("=== NODE.PY LOADED ===", flush=True)

import json
import threading
import argparse
import time
import traceback
from peer_discovery import start_peer_discovery, known_peers, peer_flags, peer_config
from block_handler import block_generation, request_block_sync
# from message_handler import cleanup_seen_messages
from socket_server import start_socket_server
from dashboard import start_dashboard
from peer_manager import start_peer_monitor, start_ping_loop
from outbox import send_from_queue, start_dynamic_capacity_adjustment
# from link_simulator import start_dynamic_capacity_adjustment
from inv_message import broadcast_inventory
from transaction import transaction_generation

def main():

    # Import the peer's configuration from command line
    from block_handler import received_blocks

    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--fanout", type=int, help="Override fanout for this peer")
    parser.add_argument("--mode", default="normal", help="Node mode: normal or malicious")
    args = parser.parse_args()
    MALICIOUS_MODE = args.mode == 'malicious'

    self_id = args.id

    print(f"[{self_id}] Starting node...", flush=True)



    with open(args.config) as f:
        config = json.load(f)

    # 更新 peer_discovery 模块中的全局变量
    peer_discovery.peer_config = config["peers"]
    self_info = config["peers"][self_id]

    received_blocks.clear()
    # 创建创世区块
    if not self_info.get('light', False):
        from block_handler import create_dummy_block
        genesis_block = create_dummy_block(str(self_id), MALICIOUS_MODE, genesis=True)
        received_blocks.append(genesis_block)
        print(f"[{self_id}] Created genesis block: {genesis_block.hash}")

    peer_flags[self_id] = {
        "nat": self_info.get("nat", False),
        "light": self_info.get("light", False),
        "localnetworkid": self_info.get("localnetworkid",3)
    }

    for peer_id, peer_info in config["peers"].items():
        known_peers[peer_id] = (peer_info["ip"], peer_info["port"])
        # peer_config = config["peers"]
        peer_discovery.peer_flags[peer_id] = {
            "nat": peer_info.get("nat", False),
            "light": peer_info.get("light", False),
            "localnetworkid": self_info.get("localnetworkid", 4)
        }

    if args.fanout:
        peer_config[self_id]["fanout"] = args.fanout
        print(f"[{self_id}] Overriding fanout to {args.fanout}", flush=True)

    ip = self_info["ip"]
    port = self_info["port"]

    # Start socket and listen for incoming messages
    print(f"[{self_id}] Starting socket server on {ip}:{port}", flush=True)
    start_socket_server(self_id, ip, port)

    # Peer Discovery
    print(f"[{self_id}] Starting peer discovery", flush=True)
    start_peer_discovery(self_id, self_info)

    print(f"[{self_id}] Starting ping loop", flush=True)
    start_ping_loop(self_id, known_peers)

    print(f"[{self_id}] Starting peer monitor", flush=True)
    start_peer_monitor()

    # Block and Transaction Generation and Verification
    print(f"[{self_id}] Starting block sync thread", flush=True)
    threading.Thread(target=request_block_sync, args=(self_id,), daemon=True).start()

    if not self_info.get('light', False):
        print(f"[{self_id}] Starting transaction and block generation", flush=True)
        transaction_generation(self_id)
        block_generation(self_id, MALICIOUS_MODE)
    else:  # 轻节点添加创世区块
        from block_handler import create_dummy_block
        genesis_block = create_dummy_block(str(self_id), False, genesis=True)
        received_blocks.append(genesis_block)
        print(f"[{self_id}] Created genesis header")

    print(f"[{self_id}] Starting broadcast inventory thread", flush=True)
    threading.Thread(target=broadcast_inventory, args=(self_id,), daemon=True).start()

    # Sending Message Processing
    print(f"[{self_id}] Starting outbound queue", flush=True)
    send_from_queue(self_id)

    print(f"[{self_id}] Starting dynamic capacity adjustment", flush=True)
    start_dynamic_capacity_adjustment()

    # Start dashboard
    time.sleep(2)
    print(f"[{self_id}] Known peers before dashboard start: {known_peers}", flush=True)
    print(f"[{self_id}] Starting dashboard on port {port + 2000}", flush=True)
    start_dashboard(self_id, port + 2000)

    print(f"[{self_id}] Node is now running at {ip}:{port}", flush=True)
    while True:
        print(f"[{self_id}] Still alive at {time.strftime('%X')}", flush=True)
        time.sleep(60)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] Exception in main(): {e}", flush=True)
        traceback.print_exc()
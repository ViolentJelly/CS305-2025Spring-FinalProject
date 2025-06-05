from flask import Flask, jsonify
from threading import Thread

import peer_discovery
from outbox import get_outbox_status, get_drop_stats, get_capacity
from peer_manager import peer_status, rtt_tracker, get_peer_status, get_peer_latency, is_peer_blacklisted, blacklist
from transaction import get_recent_transactions, tx_pool
# from link_simulator import rate_limiter
from message_handler import get_redundancy_stats
from peer_discovery import known_peers, peer_flags, peer_config
import json
from block_handler import received_blocks, get_block_headers

app = Flask(__name__)
blockchain_data_ref = None
known_peers_ref = None
peer_config_ref = None
self_id = None

def start_dashboard(peer_id, port):
    global blockchain_data_ref, known_peers_ref, peer_config_ref, self_id
    self_id = peer_id
    blockchain_data_ref = received_blocks
    known_peers_ref = peer_discovery.known_peers  # Use module reference
    peer_config_ref = peer_discovery.peer_config  # Use module reference
    def run():
        app.run(host="0.0.0.0", port=port)
    Thread(target=run, daemon=True).start()

@app.route('/')
def home():
    return "Block P2P Network Simulation"

@app.route('/blocks')
def blocks():
    # TODO: display the blocks in the local blockchain.
    try:
        # 确保 peer_config_ref 和 self_id 有效
        print(f"[DASHBOARD] Self ID: {self_id}", flush=True)
        print(f"[DASHBOARD] Peer config ref: {peer_config_ref}", flush=True)

        if not peer_config_ref or not self_id:
            return jsonify({
                "error": "Configuration not initialized",
                "self_id": self_id,
                "peer_config_ref": bool(peer_config_ref),
                "peer_count": len(peer_config_ref) if peer_config_ref else 0
            }), 500

        # 确保节点配置存在
        peer_config = peer_config_ref.get(self_id, {})
        is_light = peer_config.get("light", False)

        if is_light:
            # 轻节点模式 - 使用区块头
            headers = get_block_headers(self_id)
            if not isinstance(headers, list):
                return jsonify({"error": "Block headers not available"}), 500

            return jsonify({
                "node_type": "light",
                "block_count": len(headers),
                "headers": headers
            })
        else:
            # 全节点模式 - 使用完整区块
            if not received_blocks or not isinstance(received_blocks, list):
                return jsonify({"error": "Blockchain not initialized2"}), 500

            blocks = []
            for block in received_blocks:
                # 确保区块对象有必要的属性
                if hasattr(block, 'hash') and hasattr(block, 'prev_hash'):
                    blocks.append({
                        "hash": block.hash,
                        "prev_hash": block.prev_hash,
                        "creator": block.creator,
                        "timestamp": block.timestamp,
                        "tx_count": len(block.transactions) if hasattr(block, 'transactions') else 0
                    })

            return jsonify({
                "node_type": "full",
                "block_count": len(blocks),
                "blocks": blocks
            })

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve blocks: {str(e)}"}), 500

@app.route('/peers')
def peers():
    # TODO: display the information of known peers, including `{peer's ID, IP address, port, status, NATed or non-NATed, lightweight or full}`.
    peers_info = []
    for peer_id,(ip,port) in known_peers.items():
        # status = peer_status[peer_id]
        flags = peer_flags.get(peer_id,{})
        # nat = flags.get("nat",False)
        # light = flags.get("light",False)
        peers_info.append({
            "id": peer_id,
            "ip": ip,
            "port": port,
            "status": get_peer_status(peer_id),
            "nat": flags.get("nat", False),
            "light": flags.get("light", False),
            "network": flags.get("localnetworkid", 0),
            "latency": get_peer_latency(peer_id),
            "blacklisted": is_peer_blacklisted(peer_id)
        })
    # print(peers_info)
    return jsonify(peers_info)

@app.route('/transactions')
def transactions():
    # TODO: display the transactions in the local pool `tx_pool`.
    transactions = []
    for tx in tx_pool:
        transactions.append({
            "id": tx.id,
            "from": tx.from_peer,
            "to": tx.to_peer,
            "amount": tx.amount,
            "timestamp": tx.timestamp
        })
    return jsonify({
        "count": len(tx_pool),
        "transactions": transactions
    })


@app.route('/latency')
def latency():
    # TODO: display the transmission latency between peers.
    return jsonify(rtt_tracker)


@app.route('/capacity')
def capacity():
    # TODO: display the sending capacity of the peer.
    return get_capacity()
    pass

@app.route('/orphans')
def orphan_blocks():
    from block_handler import received_blocks, get_block_headers, orphan_blocks as orphan_blocks_dict
    # TODO: display the orphaned blocks.
    try:
        orphans = []

        # 确保 orphan_blocks_dict 是字典类型
        if not isinstance(orphan_blocks_dict, dict):
            return jsonify({"error": "Orphan blocks data not available"}), 500

        for prev_hash, blocks_list in orphan_blocks_dict.items():
            if not isinstance(blocks_list, list):
                continue

            for block in blocks_list:
                # 确保区块对象有必要的属性
                if hasattr(block, 'hash') and hasattr(block, 'prev_hash'):
                    orphans.append({
                        "hash": block.hash,
                        "prev_hash": block.prev_hash,
                        "creator": block.creator,
                        "timestamp": block.timestamp
                    })

        return jsonify({
            "orphan_count": len(orphans),
            "orphans": orphans
        })

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve orphan blocks: {str(e)}"}), 500

@app.route('/queue')
def message_queue():
    # TODO: display the messages in the outbox queue.
    return jsonify(get_outbox_status())
    pass

@app.route('/redundancy')
def redundancy_stats():
    # TODO: display the number of redundant messages received.
    return jsonify(get_redundancy_stats())

    pass

@app.route('/blacklist')
def blacklist_display():
    # TODO: display the blacklist.
    return jsonify(list(blacklist))


@app.route('/drop_stats')
def drop_stats_route():
        # """显示消息丢弃统计"""
    return jsonify(get_drop_stats())

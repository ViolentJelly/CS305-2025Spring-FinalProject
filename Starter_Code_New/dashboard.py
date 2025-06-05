from flask import Flask, jsonify
from threading import Thread

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
    known_peers_ref = known_peers
    peer_config_ref = peer_config
    def run():
        app.run(host="0.0.0.0", port=port)
    Thread(target=run, daemon=True).start()

@app.route('/')
def home():
    return "Block P2P Network Simulation"

@app.route('/blocks')
def blocks():
    # TODO: display the blocks in the local blockchain.
    if peer_config_ref[self_id].get("light", False):
        headers = [{
            "hash": header["hash"],
            "prev_hash": header["prev_hash"],
            "timestamp": header["timestamp"]
        } for header in get_block_headers(self_id)]
        # print(jsonify({
        #     "node_type": "light",
        #     "block_count": len(headers),
        #     "headers": headers
        # }))
        return jsonify({
            "node_type": "light",
            "block_count": len(headers),
            "headers": headers
        })
    else:
        blocks = [{
            "hash": block.hash,
            "prev_hash": block.prev_hash,
            "creator": block.creator,
            "timestamp": block.timestamp,
            "tx_count": len(block.transactions)
        } for block in received_blocks]
        # print(jsonify({
        #     "node_type": "full",
        #     "block_count": len(blocks),
        #     "blocks": blocks
        # }))
        return jsonify({
            "node_type": "full",
            "block_count": len(blocks),
            "blocks": blocks
        })

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
    # TODO: display the orphaned blocks.
    orphans = []
    for prev_hash, blocks_list in orphan_blocks.items():
        for block in blocks_list:
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
    pass

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

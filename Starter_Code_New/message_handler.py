import json
import threading
import time
import hashlib
import random
from collections import defaultdict

from utils import generate_message_id
from peer_discovery import handle_hello_message, known_peers, peer_config, peer_flags
from block_handler import handle_block, get_block_by_id, create_getblock, received_blocks, header_store, Block, \
    get_block_headers
from inv_message import create_inv, get_inventory
from block_handler import create_getblock
from peer_manager import update_peer_heartbeat, record_offense, create_pong, handle_pong, blacklist
from transaction import add_transaction, TransactionMessage
from outbox import enqueue_message, gossip_message

# === Global State ===
SEEN_EXPIRY_SECONDS = 600  # 10 minutes
seen_message_ids = {}
seen_txs = set()
redundant_blocks = 0
redundant_txs = 0
message_redundancy = 0
peer_inbound_timestamps = defaultdict(list)

# === Inbound Rate Limiting ===
INBOUND_RATE_LIMIT = 10
INBOUND_TIME_WINDOW = 10  # seconds


def is_inbound_limited(peer_id):
    # TODO: Record the timestamp when receiving message from a sender.
    now = time.time()
    window_start = now - INBOUND_TIME_WINDOW
    # TODO: Check if the number of messages sent by the sender exceeds `INBOUND_RATE_LIMIT` during the `INBOUND_TIME_WINDOW`. If yes, return `TRUE`. If not, return `FALSE`.
    peer_inbound_timestamps[peer_id] = [
        ts for ts in peer_inbound_timestamps[peer_id]
        if ts > window_start
    ]
    if len(peer_inbound_timestamps[peer_id]) >= INBOUND_RATE_LIMIT:
        return True
    peer_inbound_timestamps[peer_id].append(now)
    return False


# ===  Redundancy Tracking ===

def get_redundancy_stats():
    # TODO: Return the times of receiving duplicated messages (`message_redundancy`).
    return message_redundancy


# === Main Message Dispatcher ===
def dispatch_message(msg, self_id, self_ip):
    global message_redundancy
    # TODO: Read the message.
    msg = json.loads(msg)
    msg_type = msg.get("type")
    sender_id = msg.get('sender')
    msg_id = msg.get('message_id')
    # TODO: Check if the message has been seen in `seen_message_ids` to prevent replay attacks. If yes, drop the message and add one to `message_redundancy`. If not, add the message ID to `seen_message_ids`.
    if msg_id in seen_message_ids:
        message_redundancy += 1
        return
    seen_message_ids[msg_id] = time.time()
    # TODO: Check if the sender sends message too frequently using the function `in_bound_limited`. If yes, drop the message.
    if is_inbound_limited(sender_id):
        return

    # TODO: Check if the sender exists in the `blacklist` of `peer_manager.py`. If yes, drop the message.
    if sender_id in blacklist:
        return

    if msg_type == "RELAY":
        print("receive RELAY")
        # TODO: Check if the peer is the target peer.
        # If yes, extract the payload and recall the function `dispatch_message` to process the payload.
        # If not, forward the message to target peer using the function `enqueue_message` in `outbox.py`.
        target_id = msg.get('target')
        payload = msg.get('payload')

        if target_id == self_id:
            # Process payload
            dispatch_message(payload, self_id, self_ip)
        else:
            # Forward to target
            ip, port = known_peers[target_id]
            enqueue_message(target_id, ip, port, json.dumps(msg))


    elif msg_type == "HELLO":
        print("receive HELLO")
        # TODO: Call the function `handle_hello_message` in `peer_discovery.py` to process the message.
        handle_hello_message(msg, self_id)

    elif msg_type == "BLOCK":
        print("receive BLOCK")
        # TODO: Check the correctness of block ID. If incorrect, record the sender's offence using the function `record_offence` in `peer_manager.py`.
        block = Block.from_dict(msg)
        computed_id = block.compute_hash()
        if msg.get("hash") != computed_id:
            # print(f"[{self_id}] Invalid block ID from {sender_id}")
            # print(f"  Claimed: {msg.get('hash')}")
            # print(f"  Actual: {computed_id}")
            record_offense(sender_id)
            return
        # TODO: Call the function `handle_block` in `block_handler.py` to process the block.
        handle_block(msg, self_id)
        # TODO: Call the function `create_inv` to create an `INV` message for the block.
        inv_msg = create_inv(self_id, [computed_id])
        gossip_message(self_id, inv_msg)
        # TODO: Broadcast the `INV` message to known peers using the function `gossip_message` in `outbox.py`.



    elif msg_type == "TX":
        print("receive TX")
        # TODO: Check the correctness of transaction ID. If incorrect, record the sender's offence using the function `record_offence` in `peer_manager.py`.
        tx_data = msg

        computed_id = TransactionMessage.compute_hash(TransactionMessage.from_dict(tx_data))
        if tx_data.get("id") != computed_id:
            # print(f"[{self_id}] Invalid TX ID from {sender_id}")
            record_offense(sender_id)
            return
        # TODO: Add the transaction to `tx_pool` using the function `add_transaction` in `transaction.py`.
        tx = TransactionMessage.from_dict(tx_data)
        print("add transaction1 from ", {tx.from_peer}, " to ", {tx.to_peer})
        add_transaction(tx)
        # TODO: Broadcast the transaction to known peers using the function `gossip_message` in `outbox.py`.

        gossip_message(self_id, tx.to_dict())


    elif msg_type == "PING":
        print("receive PING")
        # TODO: Update the last ping time using the function `update_peer_heartbeat` in `peer_manager.py`.
        update_peer_heartbeat(sender_id)
        # TODO: Create a `pong` message using the function `create_pong` in `peer_manager.py`.
        pong_msg = create_pong(self_id, msg)
        # TODO: Send the `pong` message to the sender using the function `enqueue_message` in `outbox.py`.
        if sender_id in known_peers:
            ip, port = known_peers[sender_id]
            # 将PONG加入发送队列
            enqueue_message(sender_id, ip, port, pong_msg)
        pass

    elif msg_type == "PONG":
        print("receive PONG")
        # TODO: Update the last ping time using the function `update_peer_heartbeat` in `peer_manager.py`.
        update_peer_heartbeat(sender_id)
        # TODO: Call the function `handle_pong` in `peer_manager.py` to handle the message.
        handle_pong(msg)

    elif msg_type == "INV":
        print("receive INV")
        # TODO: Read all blocks IDs in the local blockchain using the function `get_inventory` in `block_handler.py`.
        block_ids = msg.get("block_ids", [])
        local_inv = get_inventory(self_id)
        # TODO: Compare the local block IDs with those in the message.
        missing_block_ids = [bid for bid in block_ids if bid not in local_inv]
        # TODO: If there are missing blocks, create a `GETBLOCK` message to request the missing blocks from the sender.
        if missing_block_ids:
            getblock_msg = create_getblock(self_id, missing_block_ids)
            # TODO: Send the `GETBLOCK` message to the sender using the function `enqueue_message` in `outbox.py`.
            if sender_id in known_peers:
                ip, port = known_peers[sender_id]
                enqueue_message(sender_id, ip, port, getblock_msg)
        pass

    elif msg_type == "GETBLOCK":
        print("receive GB")
        # TODO: Extract the block IDs from the message.
        requested_ids = msg.get("requested_ids", [])
        # TODO: Get the blocks from the local blockchain according to the block IDs using the function `get_block_by_id` in `block_handler.py`.
        found_blocks = []
        missing_ids = []
        # TODO: If the blocks are not in the local blockchain, create a `GETBLOCK` message to request the missing blocks from known peers.
        for block_id in requested_ids:
            block = get_block_by_id(block_id)
            if block:
                found_blocks.append(block)
            else:
                missing_ids.append(block_id)
        # TODO: Send the `GETBLOCK` message to known peers using the function `enqueue_message` in `outbox.py`.
        for block in found_blocks:
            block_msg = block.to_dict()
            if sender_id in known_peers:
                ip, port = known_peers[sender_id]
                enqueue_message(sender_id, ip, port, block_msg)
        if missing_ids:
            retry_count = 0
            max_retries = 3
            while retry_count < max_retries and missing_ids:
                # 3.1 向其他节点请求缺失的区块
                getblock_msg = create_getblock(self_id, missing_ids)
                gossip_message(self_id,getblock_msg)
                # for peer_id, (ip, port) in known_peers.items():
                #     if peer_id != self_id and peer_id != sender_id:
                #         enqueue_message(peer_id, ip, port, getblock_msg)

                # 3.3 再次检查本地是否有缺失的区块
                still_missing = []
                for block_id in missing_ids:
                    if not get_block_by_id(block_id):
                        still_missing.append(block_id)

                missing_ids = still_missing
                retry_count += 1
        # TODO: Retry getting the blocks from the local blockchain. If the retry times exceed 3, drop the message.

        # TODO: If the blocks exist in the local blockchain, send the blocks one by one to the requester using the function `enqueue_message` in `outbox.py`.



    elif msg_type == "GET_BLOCK_HEADERS":

        # TODO: Read all block header in the local blockchain and store them in `headers`.
        headers = get_block_headers(self_id)
        # TODO: Create a `BLOCK_HEADERS` message, which should include `{message type, sender's ID, headers}`.
        block_headers_msg = {
            "type": "BLOCK_HEADERS",
            "sender": self_id,
            "headers": headers,
            "message_id": generate_message_id()
        }
        # TODO: Send the `BLOCK_HEADERS` message to the requester using the function `enqueue_message` in `outbox.py`.
        if sender_id in known_peers:
            ip, port = known_peers[sender_id]
            enqueue_message(sender_id, ip, port, block_headers_msg)


    elif msg_type == "BLOCK_HEADERS":

        # TODO: Check if the previous block of each block exists in the local blockchain or the received block headers.
        headers = msg.get("headers", [])
        valid_headers = []
        missing_blocks = []
        for header in headers:
            prev_hash = header.get("prev_hash")
            block_hash = header.get("hash")

            # 检查前一个区块是否存在
            prev_exists = any(
                b.prev_hash == prev_hash
                for b in received_blocks
            ) or any(
                h["prev_hash"] == prev_hash
                for h in header_store
            )
            if prev_exists:
                valid_headers.append(header)
            else:
                print(f"[{self_id}] Orphan header: {block_hash} (prev: {prev_hash})", flush=True)
        # TODO: If yes and the peer is lightweight, add the block headers to the local blockchain.
        if peer_flags.get(self_id, {}).get('light', False):
            for header in valid_headers:
                # 避免重复添加
                if not any(h["hash"] == header["hash"] for h in header_store):
                    header_store.append(header)
                    print(f"[{self_id}] Added header: {header['hash']}", flush=True)
        # TODO: If yes and the peer is full, check if there are missing blocks in the local blockchain. If there are missing blocks, create a `GET_BLOCK` message and send it to the sender.
        else:
            for header in valid_headers:
                block_id = header["hash"]
                if not any(b.hash == block_id for b in received_blocks):
                    missing_blocks.append(block_id)
            if missing_blocks:
                getblock_msg = create_getblock(self_id, missing_blocks)
                if sender_id in known_peers:
                    ip, port = known_peers[sender_id]
                    enqueue_message(sender_id, ip, port, getblock_msg)
        # TODO: If not, drop the message since there are orphaned blocks in the received message and, thus, the message is invalid.

        pass


    else:
        print(f"[{self_id}] Unknown message type: {msg_type}", flush=True)
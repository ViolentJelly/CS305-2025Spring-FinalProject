import time
import json
import hashlib
import random
import threading
from peer_discovery import known_peers
from outbox import gossip_message, enqueue_message
from peer_manager import is_peer_blacklisted
from utils import generate_message_id


class TransactionMessage:
    def __init__(self, sender, receiver, amount, timestamp=None):
        self.type = "TX"
        self.from_peer = sender
        self.to_peer = receiver
        self.amount = amount
        self.timestamp = timestamp if timestamp else time.time()
        self.id = self.compute_hash()

    def compute_hash(self):
        tx_data = {
            "type": self.type,
            "from": self.from_peer,
            "to": self.to_peer,
            "amount": self.amount,
            "timestamp": self.timestamp
        }
        return hashlib.sha256(json.dumps(tx_data, sort_keys=True).encode()).hexdigest()

    def to_dict(self):
        return {
            "type": self.type,
            "id": self.id,
            "from": self.from_peer,
            "to": self.to_peer,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "message_id": generate_message_id()
        }

    @staticmethod
    def from_dict(data):
        return TransactionMessage(
            sender=data["from"],
            receiver=data["to"],
            amount=data["amount"],
            timestamp=data["timestamp"]
        )
    
tx_pool = [] # local transaction pool
tx_ids = set() # the set of IDs of transactions in the local pool
    
def transaction_generation(self_id, interval=15):
    def loop():
        # TODO: Randomly choose a peer from `known_peers` and generate a transaction to transfer arbitrary amount of money to the peer.
        while True:
            try:
                # 随机选择接收者（排除自己和黑名单节点）
                if not known_peers:
                    time.sleep(5)
                    continue

                valid_peers = [pid for pid in known_peers
                               if pid != self_id and not is_peer_blacklisted(pid)]

                if not valid_peers:
                    time.sleep(5)
                    continue

                receiver = random.choice(list(valid_peers))
                amount = random.randint(1, 100)  # 随机金额

                # TODO:  Add the transaction to local `tx_pool` using the function `add_transaction`.
                # 创建交易
                tx = TransactionMessage(self_id, receiver, amount)

                # 添加到本地交易池
                add_transaction(tx)

                # TODO:  Broadcast the transaction to `known_peers` using the function `gossip_message` in `outbox.py`.
                # 广播交易
                print(f"[{self_id}] Broadcasting TX: {tx.id}", flush=True)
                gossip_message(self_id, tx.to_dict())
                # for peer_id, (ip, port) in known_peers:
                #     enqueue_message(peer_id, ip, port, tx.to_dict())
                # 等待间隔
                time.sleep(interval)

            except Exception as e:
                print(f"[{self_id}] TX generation error: {e}", flush=True)
                time.sleep(5)

    threading.Thread(target=loop, daemon=True).start()

def add_transaction(tx):
    # TODO: Add a transaction to the local `tx_pool` if it is in the pool.
    # 避免重复交易
    if tx.id in tx_ids:
        return False

    # TODO: Add the transaction ID to `tx_ids`.
    # 添加到交易池
    print("add transaction3 from ", {tx.from_peer}, " to ", {tx.to_peer})
    tx_pool.append(tx)
    tx_ids.add(tx.id)
    return True

def get_recent_transactions():
    # TODO: Return all transactions in the local `tx_pool`.
    return tx_pool.copy()

def clear_pool():
    # Remove all transactions in `tx_pool` and transaction IDs in `tx_ids`.
    tx_pool.clear()
    tx_ids.clear()
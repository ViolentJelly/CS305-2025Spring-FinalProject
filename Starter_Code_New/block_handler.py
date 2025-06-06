
import time
import hashlib
import json
import threading

# from inv_message import broadcast_inventory
# from inv_message import create_inv
# from inv_message import broadcast_inventory
from transaction import get_recent_transactions, clear_pool, TransactionMessage
from peer_discovery import known_peers, peer_config, peer_flags

from outbox import  enqueue_message, gossip_message
from utils import generate_message_id
from peer_manager import record_offense, is_peer_blacklisted

received_blocks = [] # The local blockchain. The blocks are added linearly at the end of the set.
header_store = [] # The header of blocks in the local blockchain. Used by lightweight peers.
orphan_blocks = {} # The block whose previous block is not in the local blockchain. Waiting for the previous block.
class Block:
    def __init__(self, creator, transactions, prev_hash, timestamp=None):
        self.type = "BLOCK"
        self.creator = creator
        self.transactions = transactions
        self.prev_hash = prev_hash
        self.timestamp = timestamp if timestamp else time.time()
        self.hash = self.compute_hash()

    def compute_hash(self):
        block_data = {
            "creator": self.creator,
            "transactions": [tx.id for tx in self.transactions],
            "prev_hash": self.prev_hash,
            "timestamp": self.timestamp
        }
        return hashlib.sha256(json.dumps(block_data, sort_keys=True).encode()).hexdigest()

    def to_dict(self):
        return {
            "type": self.type,
            "hash": self.hash,
            "creator": self.creator,
            "prev_hash": self.prev_hash,
            "timestamp": self.timestamp,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "message_id": generate_message_id()
        }

    # {message type, peer's ID, timestamp, block ID, previous block's ID, and transactions}
    @staticmethod
    def from_dict(data):
        transactions = [TransactionMessage.from_dict(tx) for tx in data["transactions"]]
        return Block(
            creator=data["creator"],
            transactions=transactions,
            prev_hash=data["prev_hash"],
            timestamp=data["timestamp"]
        )

def request_block_sync(self_id):
    # TODO: Define the JSON format of a `GET_BLOCK_HEADERS`, which should include `{message type, sender's ID}`.

    if peer_config.get(self_id, {}).get('light', False):
        get_headers_msg = {
            "type": "GET_BLOCK_HEADERS",
            "sender": self_id,
            "message_id": generate_message_id()
        }
        # 发送给所有已知全节点
        for peer_id, (ip, port) in known_peers.items():
            if not peer_flags.get(peer_id, {}).get('light', False):
                enqueue_message(peer_id, ip, port, get_headers_msg)
    """请求区块头同步"""
    get_headers_msg = {
        "type": "GET_BLOCK_HEADERS",
        "sender": self_id,
        "message_id": generate_message_id()
    }
    # TODO: Send a `GET_BLOCK_HEADERS` message to each known peer and put the messages in the outbox queue.
    # 发送给所有已知节点
    for peer_id, (ip, port) in known_peers.items():
        if peer_id == self_id or is_peer_blacklisted(peer_id):
            continue
        enqueue_message(peer_id, ip, port, get_headers_msg)
    pass

def block_generation(self_id, MALICIOUS_MODE, interval=30):
    from inv_message import create_inv
    from outbox import gossip_message
    from inv_message import broadcast_inventory
    def mine():
    # TODO: Create a new block periodically using the function `create_dummy_block`.
        while True:
            try:
                # 等待区块链初始化
                if not received_blocks:
                    # print(f"[{self_id}] Creating genesis block")
                    # genesis_block = create_dummy_block(self_id, MALICIOUS_MODE, genesis=True)
                    # receive_block(genesis_block, self_id)  # 添加到区块链
                    time.sleep(5)
                    continue

                # 创建新区块
                new_block = create_dummy_block(str(self_id), MALICIOUS_MODE)

                if new_block is None:
                    print(f"[{self_id}] No transactions to create block, waiting...")
                    time.sleep(10)
                    continue
                # TODO: Create an `INV` message for the new block using the function `create_inv` in `inv_message.py`.
                # 创建INV消息并广播
                inv_msg = create_inv(str(self_id), [new_block.hash])
                # broadcast_inventory(self_id)
                # TODO: Broadcast the `INV` message to known peers using the function `gossip` in `outbox.py`.
                gossip_message(str(self_id), inv_msg)

                print(f"[{self_id}] Mined block: {new_block.hash}", flush=True)

                # 等待下一个区块周期
                time.sleep(interval)

            except Exception as e:
                print(f"[{self_id}] Block generation error: {e}", flush=True)
                time.sleep(10)

    threading.Thread(target=mine, daemon=True).start()
    print(f"[{self_id}] Block generation started", flush=True)


def create_dummy_block(peer_id, MALICIOUS_MODE, genesis=None):
    from inv_message import create_inv
    # TODO: Define the JSON format of a `block`, which should include `{message type, peer's ID, timestamp, block ID, previous block's ID, and transactions}`. 
    # The `block ID` is the hash value of block structure except for the item `block ID`. 
    # `previous block` is the last block in the blockchain, to which the new block will be linked. 
    # If the block generator is malicious, it can generate random block ID.
    # 获取交易池中的交易
    if genesis:
        # 创世区块
        return Block(
            creator=peer_id,
            transactions=[],
            prev_hash="0" * 64,  # 创世区块没有前一个区块
            timestamp=time.time()
        )
    transactions = get_recent_transactions()
    if not transactions:
        return None
    # 确定前一个区块哈希
    prev_hash = received_blocks[-1].hash if received_blocks else "0" * 64
    # 创建区块
    block = Block(
        creator=peer_id,
        transactions=transactions,
        prev_hash=prev_hash
    )
    # 如果是恶意节点，生成无效哈希
    if MALICIOUS_MODE:
        block.hash = hashlib.sha256(f"malicious-{time.time()}".encode()).hexdigest()

    # TODO: Read the transactions in the local `tx_pool` using the function `get_recent_transactions` in `transaction.py`.

    # TODO: Create a new block with the transactions and generate the block ID using the function `compute_block_hash`.

    # TODO: Clear the local transaction pool and add the new block into the local blockchain (`receive_block`).
    from inv_message import broadcast_inventory
    inv_msg = create_inv(str(peer_id), [block.hash])
    # gossip_message(str(peer_id), inv_msg)
    broadcast_inventory(peer_id)
    # for peer ,(ip,port)in known_peers.items():
    #     enqueue_message(peer,ip,port,inv_msg)
    # 清空交易池
    clear_pool()
    time.sleep(30)
    # 添加到本地区块链
    receive_block(block, peer_id)

    return block


def receive_block(block, self_id):
    """处理新区块（内部函数）"""
    from inv_message import broadcast_inventory
    block_exists = (
            any(b.hash == block.hash for b in received_blocks) or  # 主链中已存在
            any(any(o.hash == block.hash for o in orphans) for orphans in orphan_blocks.values())  # 孤儿池中已存在
    )

    if block_exists:
        print(f"[{self_id}] Block already exists: {block.hash}", flush=True)
        return
    # 检查前一个区块是否存在
    if block.prev_hash == "0" * 64 or any(b.hash == block.prev_hash for b in received_blocks):
        # 添加到主链
        received_blocks.append(block)

        print(f"[{self_id}] Added block to chain: {block.hash}", flush=True)
        # 如果是轻节点，只存储区块头
        try:
            if peer_flags[self_id].get("light", False):
                header = {
                    "hash": block.hash,
                    "prev_hash": block.prev_hash,
                    "timestamp": block.timestamp
                }
                header_store.append(header)
        except Exception as e:
            print(f"[{self_id}] light block handling error: {e}", flush=True)
        print("check orphans")
        # 检查是否有依赖此区块的孤儿块
        if block.hash in orphan_blocks:
            for orphan in orphan_blocks[block.hash]:
                receive_block(orphan, self_id)
            del orphan_blocks[block.hash]
    else:
        # 添加到孤儿块
        if block.prev_hash in orphan_blocks:
            if any(o.hash == block.hash for o in orphan_blocks[block.prev_hash]):
                print(f"[{self_id}] Orphan block already exists: {block.hash} (prev: {block.prev_hash})", flush=True)
                return
        orphan_blocks.setdefault(block.prev_hash, []).append(block)
        print(f"[{self_id}] Added orphan block: {block.hash} (prev: {block.prev_hash})", flush=True)


def compute_block_hash(block):
    # TODO: Compute the hash of a block except for the term `block ID`.
    return block.compute_hash()
    pass

def handle_block(msg, self_id):
    # TODO: Check the correctness of `block ID` in the received block. If incorrect, drop the block and record the sender's offence.
    from inv_message import broadcast_inventory
    try:
        # 转换消息为区块对象
        block = Block.from_dict(msg)

        # 验证区块哈希
        if block.hash != block.compute_hash():
            print(f"[{self_id}] Invalid block hash: {block.hash}", flush=True)
            record_offense(msg.get("creator", "unknown"))
            return
    # TODO: Check if the block exists in the local blockchain. If yes, drop the block.
        # 检查是否已存在
        if any(b.hash == block.hash for b in received_blocks):
            print(f"[{self_id}] Block already exists: {block.hash}", flush=True)
            return
    # TODO: Check if the previous block of the block exists in the local blockchain. If not, add the block to the list of orphaned blocks (`orphan_blocks`). If yes, add the block to the local blockchain.
    # TODO: Check if the block is the previous block of blocks in `orphan_blocks`. If yes, add the orphaned blocks to the local blockchain.


        # 处理新区块 后两个todo在receive_block里完成
        receive_block(block, self_id)
        # broadcast_inventory(self_id)
    except KeyError as e:
        print(f"[{self_id}] Block message missing key: {e}", flush=True)
        record_offense(msg.get("creator", "unknown"))
    except Exception as e:
        print(f"[{self_id}] Block handling error: {e}", flush=True)


def create_getblock(sender_id, requested_ids):
    # TODO: Define the JSON format of a `GETBLOCK` message, which should include `{message type, sender's ID, requesting block IDs}`.
    return {
        "type": "GETBLOCK",
        "sender": sender_id,
        "requested_ids": requested_ids,
        "message_id": generate_message_id()
    }


def get_block_by_id(block_id):
    # TODO: Return the block in the local blockchain based on the block ID.
    # 1. 在主链中查找
    for block in received_blocks:
        if block.hash == block_id:
            return block

    # 2. 在孤儿区块中查找
    for parent_hash, blocks in orphan_blocks.items():
        for block in blocks:
            if block.hash == block_id:
                return block
    return None

def get_block_headers(self_id):
    """获取区块头列表
    :param self_id: 当前节点的ID
    """
    if peer_flags.get(self_id, {}).get("light", False):
        return [{
            "hash": h["hash"],
            "prev_hash": h["prev_hash"],
            "timestamp": h["timestamp"]
        } for h in header_store]
    else:
        return [{
            "hash": b.hash,
            "prev_hash": b.prev_hash,
            "timestamp": b.timestamp
        } for b in received_blocks]


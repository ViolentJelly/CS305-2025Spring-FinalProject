import socket
import threading
import time
import json
import random
from collections import defaultdict, deque
from threading import Lock

# === Per-peer Rate Limiting ===
RATE_LIMIT = 10  # max messages
TIME_WINDOW = 10  # per seconds
peer_send_timestamps = defaultdict(list)  # the timestamps of sending messages to each peer

MAX_RETRIES = 3
RETRY_INTERVAL = 5  # seconds
QUEUE_LIMIT = 50

# Priority levels
PRIORITY_HIGH = {"PING", "PONG", "BLOCK", "INV", "GETDATA"}
PRIORITY_MEDIUM = {"TX", "HELLO"}
PRIORITY_LOW = {"RELAY"}

DROP_PROB = 0.05
LATENCY_MS = (20, 100)
SEND_RATE_LIMIT = 5  # messages per second

drop_stats = {
    "BLOCK": 0,
    "TX": 0,
    "HELLO": 0,
    "PING": 0,
    "PONG": 0,
    "OTHER": 0
}

# Queues per peer and priority
queues = defaultdict(lambda: defaultdict(deque))
retries = defaultdict(int)
lock = threading.Lock()


# === Sending Rate Limiter ===
class RateLimiter:
    def __init__(self, rate=SEND_RATE_LIMIT):
        self.capacity = rate  # Max burst size
        self.tokens = rate  # Start full
        self.refill_rate = rate  # Tokens added per second
        self.last_check = time.time()
        self.lock = Lock()

    def allow(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_check
            self.tokens += elapsed * self.refill_rate
            self.tokens = min(self.tokens, self.capacity)
            self.last_check = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False


rate_limiter = RateLimiter()


def enqueue_message(target_id, ip, port, message):
    from peer_manager import blacklist, rtt_tracker

    # TODO: Check if the peer sends message to the receiver too frequently using the function `is_rate_limited`. If yes, drop the message.
    if is_rate_limited(target_id):
        return False
    # TODO: Check if the receiver exists in the `blacklist`. If yes, drop the message.
    if target_id in blacklist:
        return False
    # TODO: Classify the priority of the sending messages based on the message type using the function `classify_priority`.
    priority = classify_priority(message)
    # TODO: Add the message to the queue (`queues`) if the length of the queue is within the limit `QUEUE_LIMIT`, or otherwise, drop the message.
    with lock:  # queues为什么以target_id为键？？？
        # Check queue limit
        total_queued = sum(len(q) for q in queues[target_id].values())
        if total_queued >= QUEUE_LIMIT:
            return False

        # Add to queue
        queues[target_id][priority].append({
            'ip': ip,
            'port': port,
            'message': json.dumps(message),
            'retries': 0,
            'timestamp': time.time()
        })
        return True


def is_rate_limited(peer_id):
    # TODO:Check how many messages were sent from the peer to a target peer during the `TIME_WINDOW` that ends now.
    now = time.time()
    window_start = now - TIME_WINDOW

    # Remove old timestamps
    peer_send_timestamps[peer_id] = [ts for ts in peer_send_timestamps[peer_id] if ts > window_start]

    # Check if limit exceeded
    if len(peer_send_timestamps[peer_id]) >= RATE_LIMIT:
        return True

    # Record new timestamp
    peer_send_timestamps[peer_id].append(now)
    return False
    # TODO: If the sending frequency exceeds the sending rate limit `RATE_LIMIT`, return `TRUE`; otherwise, record the current sending time into `peer_send_timestamps`.


def classify_priority(message):
    # TODO: Classify the priority of a message based on the message type.
    msg_type = message.get('type', '')
    if msg_type in PRIORITY_HIGH:
        return 0  # Highest priority
    elif msg_type in PRIORITY_MEDIUM:
        return 1
    else:
        return 2  # Lowest priority


def send_from_queue(self_id):
    def worker():

        # TODO: Read the message in the queue. Each time, read one message with the highest priority of a target peer. After sending the message, read the message of the next target peer. This ensures the fairness of sending messages to different target peers.
        while True:
            with lock:
                # Find next message to send
                target_id = None
                highest_priority = float('inf')
                message_data = None

                for peer_id, priority_queues in queues.items():
                    for priority, queue in priority_queues.items():
                        if queue and priority < highest_priority:
                            target_id = peer_id
                            highest_priority = priority
                            message_data = queue[0]

                if not target_id:
                    time.sleep(0.1)
                    continue

                # Remove from queue
                queues[target_id][highest_priority].popleft()
            # TODO: Send the message using the function `relay_or_direct_send`, which will decide whether to send the message to target peer directly or through a relaying peer.
            success = relay_or_direct_send(self_id, target_id, message_data['message'])
            # TODO: Retry a message if it is sent unsuccessfully and drop the message if the retry times exceed the limit `MAX_RETRIES`.
            if not success and message_data['retries'] < MAX_RETRIES:
                message_data['retries'] += 1
                with lock:
                    queues[target_id][highest_priority].append(message_data)
                time.sleep(RETRY_INTERVAL)

            time.sleep(0.01)
        pass

    threading.Thread(target=worker, daemon=True).start()


def relay_or_direct_send(self_id, dst_id, message):
    from peer_discovery import known_peers, peer_flags

    # TODO: Check if the target peer is NATed.
    if peer_flags.get(dst_id, {}).get('nat', False):
        relay_peer = get_relay_peer(self_id, dst_id)
        if relay_peer:
            relay_id, relay_ip, relay_port = relay_peer
            relay_msg = json.dumps({
                'type': 'RELAY',
                'sender': self_id,
                'target': dst_id,
                'payload': json.loads(message)  ##为什么不直接使用message？？
            })
            return send_message(relay_ip, relay_port, relay_msg)
    # TODO: If the target peer is NATed, use the function `get_relay_peer` to find the best relaying peer.
    # Define the JSON format of a `RELAY` message, which should include `{message type, sender's ID, target peer's ID, `payload`}`.
    # `payload` is the sending message.
    # Send the `RELAY` message to the best relaying peer using the function `send_message`.

    # TODO: If the target peer is non-NATed, send the message to the target peer using the function `send_message`.
    if dst_id in known_peers:
        ip, port = known_peers[dst_id]
        return send_message(ip, port, message)


def get_relay_peer(self_id, dst_id):
    from peer_manager import rtt_tracker
    from peer_discovery import known_peers, reachable_by

    # TODO: Find the set of relay candidates reachable from the target peer in `reachable_by` of `peer_discovery.py`.
    # 1. 获取可以访问目标节点的候选节点
    candidate_ids = reachable_by.get(dst_id, set())
    candidate_ids = {pid for pid in candidate_ids if pid != self_id}
    if not candidate_ids:
        return None

    best_relay_id = None
    best_rtt = float('inf')
    # TODO: Read the transmission latency between the sender and other peers in `rtt_tracker` in `peer_manager.py`.
    for candidate_id in candidate_ids:
        if candidate_id not in known_peers:
            continue
        candidate_rtt = rtt_tracker.get(candidate_id, float('inf'))
        if candidate_rtt < best_rtt:
            best_rtt = candidate_rtt
            best_relay_id = candidate_id
    if best_relay_id is None:
        return None
    # TODO: Select and return the best relaying peer with the smallest transmission latency.
    relay_ip, relay_port = known_peers[best_relay_id]
    return (best_relay_id, relay_ip, relay_port)
    # (peer_id, ip, port) or None


def apply_network_conditions(send_func):
    def wrapper(ip, port, message):

        # TODO: Use the function `rate_limiter.allow` to check if the peer's sending rate is out of limit.
        # If yes, drop the message and update the drop states (`drop_stats`).
        if not rate_limiter.allow():
            msg_type = json.loads(message).get('type', 'OTHER')
            drop_stats[msg_type] += 1
            return False
        # TODO: Generate a random number. If it is smaller than `DROP_PROB`, drop the message to simulate the random message drop in the channel.
        # Update the drop states (`drop_stats`).
        if random.random() < DROP_PROB:
            msg_type = json.loads(message).get('type', 'OTHER')
            drop_stats[msg_type] += 1
            return False
        # TODO: Add a random latency before sending the message to simulate message transmission delay.
        latency = random.uniform(*LATENCY_MS) / 1000.0
        time.sleep(latency)
        # TODO: Send the message using the function `send_func`.
        return send_func(ip, port, message)

    return wrapper


def send_message(ip, port, message):
    # TODO: Send the message to the target peer.
    # Wrap the function `send_message` with the dynamic network condition in the function `apply_network_condition` of `link_simulator.py`.
    try:
        # 创建TCP socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # 设置超时时间
            s.settimeout(2)
            # 连接目标节点
            s.connect((ip, port))
            # 发送消息
            s.sendall(message.encode())
            return True
    except Exception as e:
        # 记录失败统计
        msg_type = json.loads(message).get('type', 'OTHER')
        drop_stats[msg_type] += 1
        return False


send_message = apply_network_conditions(send_message)


def start_dynamic_capacity_adjustment():
    def adjust_loop():
        # TODO: Peridically change the peer's sending capacity in `rate_limiter` within the range [2, 10].
        while True:
            time.sleep(30)
            new_capacity = random.randint(2, 10)
            with rate_limiter.lock:
                rate_limiter.capacity = new_capacity
                # rate_limiter.refill_rate = new_capacity
                # rate_limiter.tokens = min(rate_limiter.tokens, new_capacity)

    threading.Thread(target=adjust_loop, daemon=True).start()


def gossip_message(self_id, message, fanout=3):
    from peer_discovery import known_peers, peer_config, peer_flags

    # TODO: Read the configuration `fanout` of the peer in `peer_config` of `peer_discovery.py`.

    # TODO: Randomly select the number of target peer from `known_peers`, which is equal to `fanout`. If the gossip message is a transaction, skip the lightweight peers in the `know_peers`.

    # TODO: Send the message to the selected target peer and put them in the outbox queue.

    # Get configured fanout
    config_fanout = peer_config.get(self_id, {}).get('fanout', fanout)

    # Filter peers
    msg_type = message.get('type', '')
    skip_light = (msg_type == 'TX')  # Skip light peers for transactions

    candidates = []
    for peer_id, (ip, port) in known_peers.items():
        if peer_id == self_id:
            continue
        if skip_light and peer_flags.get(peer_id, {}).get('light', False):
            continue
        candidates.append((peer_id, ip, port))

    # Select random peers
    selected = random.sample(candidates, min(config_fanout, len(candidates)))

    # Enqueue messages
    for peer_id, ip, port in selected:
        enqueue_message(peer_id, ip, port, message)


def get_outbox_status():
    # TODO: Return the message in the outbox queue.
    status = []
    with lock:
        for peer_id, priority_queues in queues.items():
            for priority, queue in priority_queues.items():
                for item in queue:
                    status.append({
                        'target': peer_id,
                        'priority': priority,
                        'message': item['message'],
                        'retries': item['retries'],
                        'timestamp': item['timestamp']
                    })
    return status


def get_drop_stats():
    # TODO: Return the drop states (`drop_stats`).
    return dict(drop_stats)


def get_capacity():
    """获取当前发送容量"""
    return {
        "current_capacity": rate_limiter.capacity,
        "current_tokens": rate_limiter.tokens,
        "drop_stats": drop_stats
    }


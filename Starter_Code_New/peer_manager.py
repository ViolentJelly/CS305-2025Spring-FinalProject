import threading
import time
import json
from collections import defaultdict
import peer_discovery

from utils import generate_message_id

peer_status = {} # {peer_id: 'ALIVE', 'UNREACHABLE' or 'UNKNOWN'}
last_ping_time = {} # {peer_id: timestamp}
rtt_tracker = {} # {peer_id: transmission latency}
ping_timeout = 30        # 30秒无响应视为不可达

# === Blacklist Logic ===

blacklist = set() # The set of banned peers
MAX_OFFENSES = 3
peer_offense_counts = {} # The offence times of peers

# === Check if peers are alive ===

def start_ping_loop(self_id, peer_table):
    from outbox import enqueue_message
    def loop():
       # TODO: Define the JSON format of a `ping` message, which should include `{message typy, sender's ID, timestamp}`.
       # for peer_id in peer_table:
       #     ping_message = {
       #         "type": "PING",
       #         "sender": self_id,
       #         "timestamp": last_ping_time[peer_id]
       #     }
       #
       #     if peer_id == self_id:
       #         continue
       #
       #     if peer_status[peer_id] == 'ALIVE':
       #         enqueue_message(peer_id,)
       """周期性发送PING消息给所有已知节点"""
       while True:
           try:
               current_time = time.time()

               # 为每个节点构建PING消息
               for peer_id, (ip, port) in peer_table.items():
                   if peer_id == self_id:
                       continue  # 跳过自己

                   # 检查节点是否在黑名单
                   if peer_id in blacklist:
                       continue

                   # 检查是否在冷却期（避免消息洪泛）
                   last_ping = last_ping_time.get(peer_id, 0)
                   # if current_time - last_ping < 15:  # 每15秒发送一次
                   #     continue

       # TODO: Send a `ping` message to each known peer periodically.
                       # 更新最后PING时间
                   last_ping_time[peer_id] = current_time

                   # 构建PING消息
                   ping_msg = {
                       "type": "PING",
                       "sender": self_id,
                       "timestamp": current_time,
                       "message_id": generate_message_id()
                   }

                   # 添加到发送队列
                   enqueue_message(peer_id, ip, port, ping_msg)

                   # 每60秒检查一次
               time.sleep(60)

           except Exception as e:
               print(f"[{self_id}] Ping loop error: {e}", flush=True)
               time.sleep(5)

    threading.Thread(target=loop, daemon=True).start()
    print(f"[{self_id}] Ping loop started", flush=True)

def create_pong(sender, ping_msg):
    # TODO: Create the JSON format of a `pong` message, which should include `{message type, sender's ID, timestamp in the received ping message}`.
    return {
        "type": "PONG",
        "sender": sender,
        "ping_timestamp": ping_msg["timestamp"],
        "pong_timestamp": time.time(),
        "message_id": generate_message_id()
    }

def handle_pong(msg):
    # TODO: Read the information in the received `pong` message.
    try:
        sender_id = msg["sender"]
        ping_time = msg["ping_timestamp"]
        pong_time = msg["pong_timestamp"]
        # ping_time = msg["timestamp"]  # PING发送时间
        # pong_time = time.time()  # PONG接收时间

        # 计算RTT（往返时间）
        rtt = pong_time - ping_time
        rtt_tracker[sender_id] = rtt

        # 更新节点状态
        peer_status[sender_id] = "ALIVE"
        last_ping_time[sender_id] = pong_time

        print(f" PONG from {sender_id} - RTT: {rtt:.3f}s", flush=True)
        print(f"[Monitor] Received PONG from {sender_id} - RTT: {rtt:.3f}s")

    except KeyError as e:
        print(f" Invalid PONG message: missing {e}", flush=True)
        record_offense(sender_id)
    except Exception as e:
        print(f" Error handling PONG: {e}", flush=True)
    # TODO: Update the transmission latenty between the peer and the sender (`rtt_tracker`).
    pass


def start_peer_monitor():
    import threading
    def loop():
        # TODO: Check the latest time to receive `ping` or `pong` message from each peer in `last_ping_time`.
        """监控节点状态，标记不可达节点"""
        while True:
            while True:
                try:
                    current_time = time.time()
                    for peer_id in list(last_ping_time.keys()):
                        # 跳过自己和黑名单节点
                        if peer_id in blacklist:
                            continue

                        last_active = last_ping_time[peer_id]

                        # 检查是否超时
                        if current_time - last_active > ping_timeout:
                            peer_status[peer_id] = "UNREACHABLE"
                            print(f"[Monitor] Peer {peer_id} marked as UNREACHABLE")
                        else:
                            peer_status[peer_id] = "ALIVE"

                    # 每5秒检查一次
                    time.sleep(5)

                except Exception as e:
                    print(f"[Monitor] Peer monitor error: {e}", flush=True)
                    time.sleep(3)
        # TODO: If the latest time is earlier than the limit, mark the peer's status in `peer_status` as `UNREACHABLE` or otherwise `ALIVE`.

        pass
    threading.Thread(target=loop, daemon=True).start()

def update_peer_heartbeat(peer_id):
    # TODO: Update the `last_ping_time` of a peer when receiving its `ping` or `pong` message.
    """更新节点最后活跃时间"""
    last_ping_time[peer_id] = time.time()
    if peer_id not in peer_status or peer_status[peer_id] != "ALIVE":
        peer_status[peer_id] = "ALIVE"
        print(f"[Monitor] Peer {peer_id} is now ALIVE", flush=True)
    pass




def record_offense(peer_id):
    # TODO: Record the offence times of a peer when malicious behaviors are detected.

    # TODO: Add a peer to `blacklist` if its offence times exceed 3. 
    """记录节点违规行为"""
    try:
        # 更新违规计数
        peer_offense_counts[peer_id] += 1
        print(f"[Security] Offense recorded for {peer_id} (total: {peer_offense_counts[peer_id]})", flush=True)

        # 检查是否达到黑名单阈值
        if peer_offense_counts[peer_id] >= MAX_OFFENSES:
            blacklist.add(peer_id)
            print(f"[Security] ⚠️ Peer {peer_id} added to BLACKLIST", flush=True)

            # 从状态管理中移除
            if peer_id in peer_status:
                del peer_status[peer_id]
            if peer_id in last_ping_time:
                del last_ping_time[peer_id]
            if peer_id in rtt_tracker:
                del rtt_tracker[peer_id]

            return True  # 已加入黑名单

    except Exception as e:
        print(f"[Security] Error recording offense: {e}", flush=True)

    return False  # 尚未加入黑名单

def get_peer_status(peer_id):
    """获取节点状态"""
    return peer_status.get(peer_id, "UNKNOWN")

def get_peer_latency(peer_id):
    """获取节点延迟"""
    return rtt_tracker.get(peer_id, None)

def is_peer_blacklisted(peer_id):
    """检查节点是否在黑名单"""
    return peer_id in blacklist

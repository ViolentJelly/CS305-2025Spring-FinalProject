import json, time, threading

from peer_manager import update_peer_heartbeat, record_offense
from utils import generate_message_id


known_peers = {}        # { peer_id: (ip, port) }
peer_flags = {}         # { peer_id: { 'nat': True/False, 'light': True/False } }
reachable_by = {}       # { peer_id: { set of peer_ids who can reach this peer }}
peer_config={}
local_network_id = None # 当前节点的本地网络ID

def start_peer_discovery(self_id, self_info):
    from outbox import enqueue_message
    global local_network_id
    def loop():
        # TODO: Define the JSON format of a `hello` message, which should include: `{message type, sender’s ID, IP address, port, flags, and message ID}`. 
        # A `sender’s ID` can be `peer_port`. 
        # The `flags` should indicate whether the peer is `NATed or non-NATed`, and `full or lightweight`. 
        # The `message ID` can be a random number.
        """周期性发送HELLO消息给可达节点"""
        while True:
            try:
                # 构建HELLO消息
                hello_msg = {
                    "type": "HELLO",
                    "sender": self_id,
                    "ip": self_info["ip"],
                    "port": self_info["port"],
                    "flags": {
                        "nat": self_info.get("nat", False),
                        "light": self_info.get("light", False)
                    },
                    "localnetworkid": self_info.get("localnetworkid",5),
                    "message_id": generate_message_id()
                }

        # TODO: Send a `hello` message to all reachable peers and put the messages into the outbox queue.
        # Tips: A NATed peer can only say hello to peers in the same local network. 
        #       If a peer and a NATed peer are not in the same local network, they cannot say hello to each other.
                # 发送给所有可达节点
                for peer_id, peer_info in peer_config.items():
                    # 跳过自己
                    if peer_id == self_id:
                        continue

                    # 检查是否可达
                    peer_network_id = peer_info.get('localnetworkid')

                    # 如果自己是NAT节点，只能与同网络节点通信
                    if self_info.get("nat", False):
                        if peer_network_id != local_network_id:
                            continue

                    # 如果对方是NAT节点，只能与同网络节点通信
                    if peer_info.get("nat", False):
                        if peer_network_id != local_network_id:
                            continue

                    # 添加到发送队列
                    ip = peer_info["ip"]
                    port = peer_info["port"]
                    enqueue_message(peer_id, ip, port, hello_msg)

                # 每60秒发送一次
                time.sleep(60)

            except Exception as e:
                print(f"[{self_id}] Peer discovery error: {e}", flush=True)
                time.sleep(10)

    threading.Thread(target=loop, daemon=True).start()

def handle_hello_message(msg, self_id):
    new_peers = []
    
    # TODO: Read information in the received `hello` message.
    try:
        # 解析消息
        sender_id = msg["sender"]
        ip = msg["ip"]
        port = msg["port"]
        flags = msg["flags"]
        sender_network_id = msg.get("localnetworkid")
        # 更新心跳时间
        update_peer_heartbeat(sender_id)

    # TODO: If the sender is unknown, add it to the list of known peers (`known_peer`) and record their flags (`peer_flags`).
        # 如果发送者是新的节点
        if sender_id not in known_peers:
            # 添加到已知节点
            known_peers[sender_id] = (ip, port)
            peer_flags[sender_id] = {
                "nat": flags.get("nat", False),
                "light": flags.get("light", False),
                "localnetworkid": sender_network_id  # 新增
            }
            new_peers.append(sender_id)
            print(f"[{self_id}] Discovered new peer: {sender_id} at {ip}:{port}", flush=True)

    # TODO: Update the set of reachable peers (`reachable_by`).
        # 如果对方是NAT节点，只能与同网络节点通信
        if flags.get("nat", False):
            if sender_network_id == local_network_id:
                # 添加到当前节点的可达集合
                reachable_by.setdefault(self_id, set()).add(sender_id)
                # 将当前节点添加到对方节点的可达集合
                reachable_by.setdefault(sender_id, set()).add(self_id)
        else:
            # 非NAT节点总是可达
            reachable_by.setdefault(self_id, set()).add(sender_id)
            reachable_by.setdefault(sender_id, set()).add(self_id)

    except KeyError as e:
        print(f"[{self_id}] Invalid HELLO message: missing {e}", flush=True)
        record_offense(msg.get("sender", "unknown"))
    except Exception as e:
        print(f"[{self_id}] Error handling HELLO message: {e}", flush=True)

    return new_peers

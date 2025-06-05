import socket
import threading
import time
import json
from message_handler import dispatch_message

RECV_BUFFER = 4096
buffer = b''
def start_socket_server(self_id, self_ip, port):

    def listen_loop():
        # TODO: Create a TCP socket and bind it to the peer’s IP address and port.
        global buffer
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_socket.bind((self_ip,port))

        # TODO: Start listening on the socket for receiving incoming messages.
        tcp_socket.listen(10)

        # TODO: When receiving messages, pass the messages to the function `dispatch_message` in `message_handler.py`.
        def handle_connection(conn, addr):
            global buffer
            print(f"connection is from {addr}")
            # buffer = b''
            try:
                while True:
                    data = conn.recv(RECV_BUFFER)
                    if not data:
                        break
                    buffer += (b'\n'+data)
                    # print("buffer1: ",{buffer})
                    # 处理缓冲区中的所有完整消息（以换行符分隔）
                    while b'\n' in buffer:
                        message_line, buffer = buffer.split(b'\n', 1)
                        # print("buffer3: ",{buffer})
                        if message_line:
                            try:
                                # print("buffer2: ",{buffer})
                                message = message_line.decode('utf-8')
                                dispatch_message(message, self_id, self_ip)
                            except json.JSONDecodeError:
                                print(f"wrong JSON: {message_line}")
            except ConnectionResetError:
                print(f"remake connection: {addr}")
            finally:
                conn.close()
                print(f"close connection: {addr}")

        while True:
            conn, addr = tcp_socket.accept()
            threading.Thread(
                target=handle_connection,
                args=(conn,addr),
                daemon=True
            ).start()


    # ✅ Run listener in background
    threading.Thread(target=listen_loop, daemon=True).start()
    print(f"[{self_id}] Socket server started successfully", flush=True)

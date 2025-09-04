import json
import socket
from typing import Any, Optional

def pad_message(msg):
    DEFAULT_MSG_LEN = 1024
    return msg + b'\0' * (DEFAULT_MSG_LEN- len(msg))

def create_tcp_socket(ip_addr, port, blocking=True):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip_addr, port))
    sock.setblocking(blocking)

def send_tcp_msg(sock: socket.socket, 
                 msg: Any, 
                 json_fmt: bool = False,
                 pad_msg: bool = True, 
                 close_on_sent: bool = False,
                 reconnect_on_failure: bool = False) -> Optional[socket.socket]:
    
    if json_fmt:
        msg = json.dumps(msg)

    if pad_msg:
        msg = pad_message(msg)
    msg = msg.encode()

    try:
        sock.send(msg)
    except:
        #TODO: If it fails log that it failed
        # Get necessary information about connection before closing the failing socket
        ip_addr, port = sock.getsockname()
        sock.close()

        # Re-establish connection
        if reconnect_on_failure:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip_addr, port))
            sock.setblocking(False)
        else:
            return None
    finally:
        if close_on_sent:
            sock.close()
        return sock
    
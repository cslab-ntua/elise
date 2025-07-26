from websockets.sync.server import serve
import socket
from time import time, sleep
import threading
from websocket import create_connection

import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))
from common.utils import pad_message

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(('10.254.254.254', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

ws_ipaddr = get_ip()

def start_tcp_server(host="127.0.0.1", port=55501):
    
    # Create websocket client
    START_TIME = time()
    UPDATE_TIME = 3
    ws_client = create_connection(f"ws://{ws_ipaddr}:55500")

    # Create a TCP/IP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the address and port
    server_socket.bind((host, port))

    # Enable the server to accept connections (max 5 queued connections)
    server_socket.listen(5)
    print(f'Server listening on {host}:{port}')

    while True:
        # Wait for a connection
        client_socket, client_address = server_socket.accept()
        print(f'Connection from {client_address}')

        try:
            while True:
                # Receive data from the client
                data = client_socket.recv(1024)
                if not data:
                    break  # No more data, exit the loop
                # print(f'Received: {data.decode()}')
                if time() - START_TIME > UPDATE_TIME:
                    ws_client.send(data)
                    START_TIME = time()
                
        finally:
            # Clean up the connection
            client_socket.close()
            print(f'Connection with {client_address} closed')
            sleep(UPDATE_TIME)
            ws_client.send(pad_message("100.0".encode("utf-8")))

# The first connection will be the server connection
server_conn = None
clients = set()

def echo(wsocket):
    global server_conn
    global clients

    if server_conn is None:
        server_conn = wsocket
        print("Server", server_conn)
    else:
        if wsocket not in clients:
            clients.add(wsocket)
            print("New Client", wsocket)

    for msg in wsocket:
        for ws_client in clients.copy():
            try:
                ws_client.send(msg, text=True)
            except:
                pass

def websocket_server(host=ws_ipaddr, port=55500):
    with serve(echo, host, port, ping_timeout=None) as ws_server:
        ws_server.serve_forever()

def main():
    t1 = threading.Thread(target=start_tcp_server)
    t2 = threading.Thread(target=websocket_server)
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()

if __name__ == "__main__":
    main()
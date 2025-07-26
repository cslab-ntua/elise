import argparse
import csv
from datetime import timedelta
import json
import os
import select
import socket
import sys
import tabulate

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from common.utils import define_logger, pad_message

logger = define_logger()

def progress_server(server_ipaddr="127.0.0.1", server_port=54321, connections=5, export_reports="", webui=False):

    # Create a socket and set options for resusable ip address
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind socket to the given ip address and port
    server_sock.bind((server_ipaddr, server_port))

    # Listen to incoming connections
    server_sock.listen(connections)

    # List of current available open sockets
    current_sockets = [server_sock]

    # Remaining connections
    rem_connections = connections

    # Progress for all connections
    progress_list = [0] * connections
    
    # Time reports list of tuples(id, scheduler name, real time, simulated time, time ratio)
    time_reports_list: list[tuple[int, str, str, str, str]] = list()
    
    if webui:
        print("Establishing connection to WebUI")
        webui_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        webui_socket.connect(("127.0.0.1", 55501))
    
    while True:

        overall_progress = sum(progress_list) / connections
        
        if webui:
            webui_socket.send(pad_message(str(overall_progress).encode("utf-8")))
        else:
            # Stdout print of overall progress
            print(f"\rOverall Progress: {overall_progress:.2f}%", end="")

        # If the remaining connections is zero and the only socket left is the
        # server then shutdown the progress server
        if rem_connections <= 0 and len(current_sockets) == 1 and current_sockets[0] == server_sock:
            break

        # Select/poll from current_sockets
        read_sockets, _, _ = select.select(current_sockets, [], [])

        for notified_socket in read_sockets:

            # If a new connection arrives
            if notified_socket == server_sock:

                client_sock, client_ipaddr = server_sock.accept()
                # print(f"New connection coming from {client_ipaddr}")
                current_sockets.append(client_sock)

            # A message arrived from a client socket
            else:

                msg = notified_socket.recv(1024)

                # The client has finished execution and exited
                if not msg:
                    # print(f"Closed connection from {notified_socket.getpeername()}")
                    current_sockets.remove(notified_socket)
                    notified_socket.close()
                    # Decrease the amount of remaining socket connections to be fullfilled
                    rem_connections -= 1

                # If the client has given new information about their progress
                else:
                    logger.debug(msg.decode())
                    try:
                        msg_dec = str(msg.decode())
                        start_pos = msg_dec.find("{")
                        end_pos = msg_dec.find("}")
                        msg_dec = msg_dec[start_pos:end_pos+1]
                        msg_dict = json.loads(msg_dec)
                        
                        sim_idx = int(msg_dict["sim_id"])

                        # Check whether it is a progress report or a time report
                        if "progress_perc" in msg_dict:

                            progress_perc = int(msg_dict["progress_perc"])
                            # Update the progress report for the specific simulation run
                            if progress_perc > progress_list[sim_idx]:
                                progress_list[sim_idx] = progress_perc

                        elif "real_time" in msg_dict:
                            inp_idx = int(msg_dict["inp_id"])
                            sched_idx = int(msg_dict["sched_id"])
                            scheduler_name = msg_dict["scheduler"]
                            real_time = float(msg_dict["real_time"])
                            sim_time = float(msg_dict["sim_time"])
                            time_ratio = sim_time / (24 * real_time)

                            time_reports_list.append((
                                sim_idx,
                                inp_idx,
                                sched_idx,
                                scheduler_name,
                                str(timedelta(seconds=real_time)).replace(", ", "_"),
                                str(timedelta(seconds=sim_time)).replace(", ", "_"),
                                str(time_ratio)
                            ))

                    except:
                        print(msg.decode())
                        pass
    
    # Sort time reports based on the simulation run ID
    time_reports_list.sort(key=lambda elem: elem[0])

    # Before closing the server print the time reports of all the simulation runs
    headers = ["Simulation ID", 
               "Input ID",
               "Scheduler ID",
               "Scheduler Name", 
               "Real Time", 
               "Simulated Time", 
               "Time Ratio (Simulated Days / 1 real hour)"]
    if export_reports:
        os.makedirs(export_reports, exist_ok=True)
        with open(f"{export_reports}/time_reports.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in time_reports_list:
                writer.writerow(row)
    else:
        print()
        print(tabulate.tabulate(time_reports_list, headers=headers, tablefmt="fancy_grid"))
    
    # Close websocket client
    if webui:
        webui_socket.close()

    # Close the server socket
    server_sock.close()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog="progress_server", description="A server process to monitor the progress of each simulation")
    parser.add_argument("--server_ipaddr", type=str, required=True)
    parser.add_argument("--server_port", type=int, default=54321)
    parser.add_argument("--connections", type=int, required=True)
    parser.add_argument("--export_reports", default="", type=str, help="Provde a directory to export reports for each scheduler")
    parser.add_argument("--webui", default=False, action="store_true")

    args = parser.parse_args()

    host_ipaddr = args.server_ipaddr
    port = args.server_port
    connections = args.connections
    export_reports = args.export_reports
    webui = args.webui
    
    progress_server(host_ipaddr, port, connections, export_reports, webui)

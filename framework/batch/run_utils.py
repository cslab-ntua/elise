import json
import os
import plotly.graph_objects as go
from plotly.io import from_json
import sys
from time import time
from types import MethodType
from typing import TYPE_CHECKING

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

if TYPE_CHECKING:
    from realsim.database import Database
    from realsim.compengine import ComputeEngine
    from realsim.cluster.cluster import Cluster
    from realsim.scheduler.scheduler import Scheduler
    from realsim.logger.logger import Logger

from common.utils import define_logger, handler_and_formatter, envvar_bool_val, profiling_ctx
from common.communication import create_tcp_socket, send_tcp_msg
logger = define_logger()

def __get_gantt_representation(self):
    res = self.__class__.get_gantt_representation(self) # Have to call this way to avoid infinite recursion
    fig = from_json(res)
    fig.update_layout(width=2048, height=1024)

    output_path = os.path.abspath(f"{self.img_dir}")
    os.makedirs(output_path, exist_ok=True)
    fig.write_image(f"{output_path}/input_{self.inp_idx}_{self.scheduler.name.lower().replace(' ', '_')}.png")

def __get_webui_gantt_representation(self):
    res = self.__class__.get_gantt_representation(self) # Have to call this way to avoid infinite recursion
    output_path = os.path.abspath(f"{self.dir}/gantt")
    os.makedirs(output_path, exist_ok=True)
    filename = f"{output_path}/input_{self.inp_idx}_scheduler_{self.sched_idx}.json"
    
    with open(filename, "w") as fd:
        json.dump(res, fd)

def __get_webui_workload(self):
    res = self.__class__.get_workload(self)

    output_path = os.path.abspath(f"{self.dir}/workloads")
    os.makedirs(output_path, exist_ok=True)

    with open(f"{output_path}/input_{self.inp_idx}_scheduler_{self.sched_idx}.csv", "w") as fd:
        fd.write(res)

def __get_workload(self):
    res = self.__class__.get_workload(self)

    output_path = os.path.abspath(f"{self.workload_dir}")
    os.makedirs(output_path, exist_ok=True)

    with open(f"{output_path}/workload_{self.sim_id}_{self.scheduler.name.lower().replace(' ', '_')}.csv", "w") as fd:
        fd.write(res)

def __get_animated_cluster(self):
    res = self.__class__.get_animated_cluster(self)
    fig = from_json(res)
    fig.show()

def __get_webui_waiting_queue_graph(self):
    output_path = os.path.abspath(f"{self.dir}/waiting_queue")
    os.makedirs(output_path, exist_ok=True)
    filename = f"{output_path}/input_{self.inp_idx}_scheduler_{self.sched_idx}.json"

    res = self.__class__.get_waiting_queue_graph(self) # Have to call this way to avoid infinite recursion
    checks, num_of_jobs = res
    
    fig = go.Figure(data=[
        go.Scatter(x=checks, y=num_of_jobs, mode="lines+markers")
    ])
    fig.update_layout({
        "title": f"<b>Number of jobs inside waiting queue per checkpoint</b><br>{self.scheduler.name}",
        "title_x": 0.5,
        'xaxis': {'title': '<b>Time (s)</b>'},
		'yaxis': {'title': '<b>Number of waiting jobs</b>'}
    })

    with open(filename, "w") as fd:
        json.dump(fig.to_json(), fd)

def __get_webui_jobs_throughput_graph(self):
    output_path = os.path.abspath(f"{self.dir}/jobs_throughput")
    os.makedirs(output_path, exist_ok=True)
    filename = f"{output_path}/input_{self.inp_idx}_scheduler_{self.sched_idx}.json"

    res = self.__class__.get_jobs_throughput(self) # Have to call this way to avoid infinite recursion
    checks, num_of_jobs = res
    
    fig = go.Figure(data=[
        go.Scatter(x=checks, y=num_of_jobs, mode="lines+markers")
    ])
    fig.update_layout({
        "title": f"<b>Number of finished jobs per checkpoint</b><br>{self.scheduler.name}",
        "title_x": 0.5,
        'xaxis': {'title': '<b>Time (s)</b>'},
		'yaxis': {'title': '<b>Number of finished jobs</b>'}
    })

    with open(filename, "w") as fd:
        json.dump(fig.to_json(), fd)

def __get_webui_unused_cores_graph(self):
    output_path = os.path.abspath(f"{self.dir}/unused_cores")
    os.makedirs(output_path, exist_ok=True)
    filename = f"{output_path}/input_{self.inp_idx}_scheduler_{self.sched_idx}.json"

    res = self.__class__.get_unused_cores_graph(self) # Have to call this way to avoid infinite recursion
    checks, num_of_cores = res
    
    fig = go.Figure(data=[
        go.Scatter(x=checks, y=num_of_cores, mode="lines+markers")
    ])
    fig.update_layout({
        "title": f"<b>Number of unused cores per checkpoint</b><br>{self.scheduler.name}",
        "title_x": 0.5,
        'xaxis': {'title': '<b>Time (s)</b>'},
		'yaxis': {'title': '<b>Number of unused cores</b>'}
    })

    with open(filename, "w") as fd:
        json.dump(fig.to_json(), fd)

def __get_webui_animated_cluster(self):
    res = self.__class__.get_animated_cluster(self) # Have to call this way to avoid infinite recursion
    output_path = os.path.abspath(f"{self.dir}/animated_cluster")
    os.makedirs(output_path, exist_ok=True)
    filename = f"{output_path}/input_{self.inp_idx}_scheduler_{self.sched_idx}.json"
    print(filename)
    
    with open(filename, "w") as fd:
        json.dump(res, fd)

def patch(evt_logger, extra_features):
    for arg, val in extra_features:
        evt_logger.__dict__[arg] = val
    
    if evt_logger.webui:
        evt_logger.get_gantt_representation = MethodType(__get_webui_gantt_representation, evt_logger)
        evt_logger.get_workload = MethodType(__get_webui_workload, evt_logger)
        evt_logger.get_waiting_queue_graph = MethodType(__get_webui_waiting_queue_graph, evt_logger)
        evt_logger.get_jobs_throughput = MethodType(__get_webui_jobs_throughput_graph, evt_logger)
        evt_logger.get_unused_cores_graph = MethodType(__get_webui_unused_cores_graph, evt_logger)
        evt_logger.get_animated_cluster = MethodType(__get_webui_animated_cluster, evt_logger)
    else:
        evt_logger.get_gantt_representation = MethodType(__get_gantt_representation, evt_logger)
        evt_logger.get_workload = MethodType(__get_workload, evt_logger)
        evt_logger.get_animated_cluster = MethodType(__get_animated_cluster, evt_logger)


def single_simulation(sim_batch, server_ipaddr, server_port, webui=False):
    """The function that defines the simulation loop and actions
    """

    # Create a TCP socket to communicate with the progress server
    sock = create_tcp_socket(server_ipaddr, server_port, blocking=False)

    sim_idx: int
    inp_idx: int
    sched_idx: int
    database: Database
    cluster: Cluster
    scheduler: Scheduler
    evt_logger: Logger
    compengine: ComputeEngine
    actions: list
    extra_features: list
    sim_idx, inp_idx, sched_idx, database, cluster, scheduler, evt_logger, compengine, actions, extra_features = sim_batch

    comp_logger = logger.getChild("compengine")
    if envvar_bool_val("ELiSE_DEBUG"):
        handler_and_formatter(comp_logger)
    compengine.debug_logger = comp_logger

    logger.debug(f"Setting up the cluster, scheduler and event logger, (input[{inp_idx}], scheduler[{sched_idx}], simulation[{sim_idx}])")

    cluster.setup()
    scheduler.setup()
    evt_logger.setup()

    # Progress counter
    total_jobs = len(database.preloaded_queue)

    # Start timer
    start_time = time()
    
    with profiling_ctx(sim_idx, scheduler.name, logger):

        # THE SIMULATION LOOP
        while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
            try:
                compengine.sim_step()
            except:
                logger.exception("An error occurred during the execution of the simulation")

            progress_perc = 100 * (1 - (len(database.preloaded_queue) + len(cluster.waiting_queue) + len(cluster.execution_list)) / total_jobs)
            sock = send_tcp_msg(sock, msg={"sim_id": sim_idx, "progress_perc": progress_perc}, json_fmt=True, reconnect_on_failure=True)
            if sock is None:
                logger.critical("Can't connect to progress server.")

    
    # Calculate the real time and simulated time
    real_time = time() - start_time
    sim_time = cluster.makespan

    # Send the times back to the progress server
    send_tcp_msg(
        sock, 
        msg={
            "sim_id": sim_idx, 
            "inp_id": inp_idx, 
            "sched_id": sched_idx, 
            "scheduler": scheduler.name, 
            "real_time": real_time, 
            "sim_time": sim_time
        },
        json_fmt=True,
        close_on_sent=True
    )

    # If there are actions provided for this rank
    if actions != []:
        # Overwrite event logger's interface
        extra_features.append(("sim_idx", sim_idx))
        extra_features.append(("inp_idx", inp_idx))
        extra_features.append(("sched_idx", sched_idx))
        extra_features.append(("webui", webui))
        patch(evt_logger, extra_features)

        # Perform actions upon completion
        for action in actions:
            getattr(evt_logger, action)()
    

def multiple_simulations(sim_batches, server_ipaddr, server_port, webui=False):
    for sim_batch in sim_batches:
        logger.debug(f"Starting single simulation with id {sim_batch[0]}")
        single_simulation(sim_batch, server_ipaddr, server_port, webui)
        logger.debug(f"Finished single simulation with id {sim_batch[0]}")

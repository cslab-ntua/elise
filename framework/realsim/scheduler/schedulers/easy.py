import os
import sys
from functools import reduce

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../"
)))

from realsim.cluster.host import Host
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.schedulers.fifo import FIFOScheduler
from math import inf


class EASYScheduler(FIFOScheduler):

    name = "EASY Scheduler"
    description = "FIFO Scheduler with EASY backfilling policy"

    def __init__(self):
        FIFOScheduler.__init__(self)
        self.backfill_enabled = True

    def backfill(self) -> bool:

        deployed = False

        if len(self.cluster.waiting_queue) <= 1:
            return False

        execution_list = deepcopy_list(self.cluster.execution_list)
        execution_list.sort(key=lambda job: job.wall_time + job.start_time - self.cluster.makespan)

        blocked_job = self.cluster.waiting_queue[0]

        # Get all the idle hosts
        idle_hosts = [host for host in list(self.cluster.hosts.values()) if host.state == Host.IDLE]

        idle_hostnames = [
            hostname
            for hostname, host in self.cluster.hosts.items()
            if host.state == Host.IDLE
        ]
        # Find the minimum estimated start time of the job

        # Calculate the number of idle hosts needed
        aggr_hosts = len(idle_hosts)
        min_estimated_time = inf

        eligible_hosts = idle_hostnames

        for xjob in execution_list:

            aggr_hosts += len(xjob.assigned_hosts)

            eligible_hosts.extend(xjob.assigned_hosts)

            if aggr_hosts >= blocked_job.full_socket_nodes:
                blocked_hosts = sorted(
                    eligible_hosts,
                    key=lambda x: int(x.replace('host', ''))
                )[:blocked_job.full_socket_nodes]

                min_estimated_time = xjob.wall_time - (self.cluster.makespan - xjob.start_time)
                break

        # If a job couldn't reserve cores then cancel backfill at this point
        if not min_estimated_time < inf:
            return False

        # Find job(s) that can backfill the execution list

        # Get the backfilling candidates
        backfilling_jobs = deepcopy_list(self.cluster.waiting_queue[1:self.backfill_depth+1])

        # Scan through the rest of the jobs to see if any is fit for backfilling
        for b_job in backfilling_jobs:
            time_clause = False
            if b_job.wall_time <= min_estimated_time:
                time_clause = True

            non_reserved_idle_hosts = [
                host for host in idle_hostnames
                if host not in blocked_hosts
            ]

            can_fit_without_reserved = (
                len(non_reserved_idle_hosts)
                >= b_job.full_socket_nodes
            )

            if not can_fit_without_reserved:

                if time_clause:
                    if self.compact_allocation(b_job, immediate=True):
                        deployed = True
                else:
                    deployed = False
            else:
                if time_clause:
                    if self.compact_allocation(b_job, immediate=True):
                        deployed = True
                else:
                    if self.compact_allocation(b_job, immediate=True, omit_hosts=list(set(blocked_hosts) & set(idle_hostnames))):
                        deployed = True
        
        return deployed


import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coscheduler import Coscheduler
from realsim.cluster.host import Host

from abc import ABC
from math import inf


class RanksCoscheduler(Coscheduler, ABC):

    name = "Ranks Co-Scheduler"
    description = """Rank is the measure of how many good pairs a job can 
    construct. If a job reaches rank 0, then it is not capable for co-scheduling
    and is allocated for exclusive compact execution."""

    def __init__(self):

        Coscheduler.__init__(self)

        self.ranks : dict[int, int] = dict() # jobId --> number of good pairings
        self.ranks_threshold = 1.0

    def update_ranks(self):

        self.ranks = {job.job_id : 0 for job in self.cluster.waiting_queue}

        # Update ranks for each job
        for i, job in enumerate(self.cluster.waiting_queue):

            for co_job in self.cluster.waiting_queue[i+1:]:

                job_speedup = self.database.heatmap[job.job_name][co_job.job_name]
                co_job_speedup = self.database.heatmap[co_job.job_name][job.job_name]

                if job_speedup is None or co_job_speedup is None:
                    continue

                avg_speedup = (job_speedup + co_job_speedup) / 2

                if avg_speedup > self.ranks_threshold:
                    self.ranks[job.job_id] += 1
                    self.ranks[co_job.job_id] += 1

    def setup(self):

        # Whatever setup that may be
        Coscheduler.setup(self)

        # Create ranks
        self.update_ranks()

    def after_deployment(self, *args):
        #self.update_ranks()
        pass

    def compact_allocation(self, job: Job, immediate=True) -> bool:

        # The job is not eligible for compact execution
        # if self.ranks[job.job_id] != 0 and job.age < self.age_threshold:
        # if self.ranks[job.job_id] != 0:
        #     return False

        return super().compact_allocation(job, immediate=immediate)

    def deploy(self) -> bool:

        deployed = False

        # Update the rank of each job before scheduling them
        # self.update_ranks()

        waiting_queue = deepcopy_list(self.cluster.waiting_queue[:self.queue_depth])
        waiting_queue.sort(key=lambda job: self.waiting_queue_reorder(job),
                           reverse=True)

        while waiting_queue != []:

            # Remove from the waiting queue
            job = self.pop(waiting_queue)

            # Colocate
            if self.allocation(job, self.cluster.half_socket_allocation):
                deployed = True
                self.after_deployment()
            else:
                break

        return deployed

    def backfill(self) -> bool:

        deployed = False

        if len(self.cluster.waiting_queue) <= 1:
            return False

        blocked_job = self.cluster.waiting_queue[0]

        execution_list = deepcopy_list(self.cluster.execution_list)
        execution_list.sort(key=lambda job: job.wall_time + job.start_time - self.cluster.makespan)


        # Get all the idle hosts
        suitable_nodeprocs, _ = self.find_suitable_nodes(blocked_job.num_of_processes, self.cluster.half_socket_allocation)

        # Get suitable hosts from names
        suitable_hosts = list(suitable_nodeprocs.keys())

        # Find the minimum estimated start time of the job

        # Calculate the number of idle hosts needed
        aggr_hosts = set(suitable_hosts)

        min_estimated_time = inf

        for xjob in execution_list:

            aggr_hosts = aggr_hosts.union(xjob.assigned_hosts)

            if len(aggr_hosts) >= blocked_job.half_socket_nodes:
                min_estimated_time = xjob.wall_time - (self.cluster.makespan - xjob.start_time)
                break


        # If a job couldn't reserve cores then cancel backfill at this point
        if not min_estimated_time < inf:
            return False

        # Find job(s) that can backfill the execution list

        # Get the backfilling candidates
        backfilling_jobs = deepcopy_list(self.cluster.waiting_queue[1:self.backfill_depth+1])

        # Ascending sorting by their wall time
        #backfilling_jobs.sort(key=lambda b_job: b_job.wall_time)

        for b_job in backfilling_jobs:

            if b_job.wall_time <= min_estimated_time:

                # Colocate
                if self.allocation(b_job, self.cluster.half_socket_allocation):
                    deployed = True
                    self.after_deployment()
                # Compact
                # elif super().compact_allocation(b_job):
                #     deployed = True
                #     self.after_deployment()
                # else:
                #     break

            #else:
                # No other job is capable to backfill based on time
                #break
        
        return deployed

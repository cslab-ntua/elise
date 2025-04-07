from typing import Optional
from .load import Load
from glob import glob
import os
import re
import math
from concurrent.futures import ProcessPoolExecutor
import pymongo
from pymongo.server_api import ServerApi
import pickle
from functools import reduce
from pandas import DataFrame
from numpy import median as med
from json import dumps, loads

Heatmap = dict[str, dict[str, Optional[float]]]


class LoadManager:
    """Class to manage the loads of a specific machine and suite of benchmarks
    """

    def __init__(self, machine: str, suite: str, rootdir = None):
        """Initialize a LoadManager instance

        ⟡ machine ⟡ the name of the machine on which the benchmarks were executed.
        The name can be the overall name of the HPC cluster or the name of the cluster
        and the name of the partition which was used. For example, 
        machine = "machineA.particionC"

        ⟡ suite ⟡ the suite of benchmarks that we experimented on

        ⟡ rootdir ⟡ the root directory to search for the logs, the main search
        path is {root_dir}/Co-Scheduling/logs/

        """
        self.machine: str = machine
        self.suite: str = suite
        self.rootdir: Optional[str] = rootdir

        if self.rootdir is None:
            self.rootdir = os.path.abspath(os.path.join(
                os.path.dirname(__file__), "../../"
            ))

        self.loads: dict[str, Load] = dict()

    def __call__(self, load_name: str) -> Load:
        """Get a load by its name
        """
        return self.loads[load_name]

    def __iter__(self):
        """Iterate through the loads managed when 'in' is called
        """
        return self.loads.items().__iter__()

    def __contains__(self, load_name: str) -> bool:
        """Return true if a Load instance with load_name name is being managed
        """
        return load_name in self.loads

    def __repr__(self) -> str:
        return "\033[1mLoads currently being managed\033[0m\n" + str(list(self.loads.keys()))

    def __str__(self) -> str:
        return "\033[1mLoads currently being managed\033[0m\n" + str(list(self.loads.keys()))

    def __getitem__(self, keys: tuple) -> 'LoadManager':
        """Get a load manager that manages a subset of loads; the subset is keys
        """

        # Create a new LoadManager instance
        new_lm = LoadManager(machine=self.machine, 
                             suite=self.suite,
                             rootdir=self.rootdir)

        # Store only the loads that they appear in list
        for name, load in self.loads.items():
            if name in keys:
                # Get a fresh copy of the accepted load
                copy_load = load.deepcopy()

                # Remove unecessary co-loads
                unecessary_coloads: list[str] = list()
                for co_name in list(copy_load.coscheduled_timelogs.keys()):
                    if co_name not in keys:
                        #copy_load.coscheduled_timelogs.pop(co_name)
                        unecessary_coloads.append(co_name)

                for co_name in unecessary_coloads:
                    copy_load.coscheduled_timelogs.pop(co_name)

                # Add new load in the new load manager
                new_lm.loads[name] = copy_load

        return new_lm

    def __add__(self, other_lm: 'LoadManager') -> 'LoadManager':
        """Return a new LoadManager instance when they are added together
        """

        # Create a new instance of LoadManager
        new_lm = LoadManager(machine=self.machine, suite=self.suite)

        # If the logs were recorded on different machines return an empty load
        # manager
        if self.machine != other_lm.machine:
            return new_lm

        # 1. Create a deep copy of the current load manager
        # by copying all the Loads
        for name, load in self.loads.items():
            new_lm.loads[name] = load.deepcopy()

        # 2. If the new suite name is not in the list of managed suites of the
        # current LoadManager extend it. If it is the return a copy of the
        # original LoadManager
        if other_lm.suite not in self.suite.split(","):
            new_lm.suite += f",{other_lm.suite}"

        # 2. Add new Loads to our copied LoadManager instance if there aren't
        # or update our loads' coloads
        for other_name, other_load in other_lm:
            # If it doesn't exist then add new load
            if other_name not in list(new_lm.loads.keys()):
                new_lm.loads[other_name] = other_load.deepcopy()
            else:
                # The load exists in both load managers
                name = other_name

                # If it introduces any new co-scheduled timelogs
                for other_coname, other_cologs in other_load.coscheduled_timelogs.items():
                    if other_coname not in new_lm(name).coscheduled_timelogs:
                        # Setup an empty list for the new co-scheduled logs
                        new_lm(name).coscheduled_timelogs[other_coname] = list()
                        # Get a copy of the logs
                        new_lm(name).coscheduled_timelogs[other_coname].extend(
                                other_cologs.copy()
                        )

        return new_lm

    def __iadd__(self, lm: 'LoadManager') -> 'LoadManager':
        return self.__add__(lm)

    def deepcopy(self) -> 'LoadManager':
        """Return a deepcopy of the load manager
        """
        # Create a new instance of LoadManager
        new_lm = LoadManager(machine=self.machine, 
                             suite=self.suite,
                             rootdir=self.rootdir)

        # Deepcopy all the loads that are currently being managed
        for name, load in self.loads.items():
            new_lm.loads[name] = load.deepcopy()

        return new_lm

    @staticmethod
    def to_seconds(runtime) -> float:
        sec = 0
        timestamps = runtime.split(':')[::-1]
        for i, t in enumerate(timestamps):
            sec += float(t) * (60 ** i)
        return sec

    @staticmethod
    def init_compact(cmp_dir) -> tuple[str, int, list[float]]:
        """Gather all the necessary data from the compact experiments,
        create each load and initialize their execution timelogs

        ⟡ cmp_dir ⟡ the directory to which the output logs of the
        experiments are saved
        """

        # Get the name of a load from the directory's name
        load = os.path.basename(cmp_dir).replace("_cmp", "")

        # Check if the log file for the specific
        # load exists
        try:
            files = os.listdir(cmp_dir)
            file = list(filter(lambda f: ".out" in f, files))[0]
        except:
            # If not print that nothing was found
            # and continue to the next directory
            print(f"No log file found inside {cmp_dir}")
            return load, -1, []

        # Open the log file
        fd = open(cmp_dir + "/" + file, "r")

        num_of_processes = -1
        time_logs = list()
        for line in fd.readlines():
            if 'Total number of processes' in line or 'Total processes' in line:
                num_of_processes = int(line.split()[-1])
            if "Time in seconds" in line:
                time_logs.append(float(line.split()[-1]))
            if "Overall Time:" in line:
                time_logs.append(LoadManager.to_seconds(line.split()[-1]))

        fd.close()

        return load, num_of_processes, time_logs

    @staticmethod
    def init_coschedule(cos_dir) -> list[tuple[str, str, list[float]]]:
        """Get the execution time logs of a load and coload in a coscheduled
        experiment. The function is static because it is called in parallel
        and also because logically it can be called beside's a LoadManager's
        instantiation.

        ⟡ cos_dir ⟡ the directory where the time logs for the experiment for
        each load are stored

        ⟡⟡ returnVal ⟡⟡ A list with the following scheme:
        [
            [loadA, loadB, loadA_runtimes_besides_loadB],
            [loadB, loadA, loadB_runtimes_besides_loadA],
        ]

        A dictionary was avoided because of how many coscheduled 
        experiments have the same benchmarks as load and coload.
        The definition of keys would overlap with each other.
        """

        # The list that will be returned
        out = list()

        # Discern the individual names of the loads
        loads = re.split(r'(.+\d+)_', os.path.basename(cos_dir))
        loads = list(filter(None, loads))
        first_load, second_load = loads

        first_name, first_num_of_processes = list(filter(None, re.split(r'(.+)\.(\d+)', first_load)))
        first_files = [cos_dir + '/' + file
                       for file in os.listdir(cos_dir)
                       if re.match('^' + first_name, file)]

        second_name, second_num_of_processes = list(filter(None, re.split(r'(.+)\.(\d+)', second_load)))
        second_files = [cos_dir + '/' + file
                        for file in os.listdir(cos_dir)
                        if re.match('^' + second_name, file)]

        # If the loads are the same then exclude
        # the same logs
        if first_load == second_load:
            first_files = [first_files[0]]
            second_files = [second_files[1]]
        else:
            # If the first and second load are the same but different
            # with respect to their number of processes requested
            # then for the first load allow logs of the same number of processes
            # and the same thing for the second load, accountably
            if first_name == second_name:
                files_to_remove = set()
                for file in first_files:
                    fd = open(file)
                    for line in fd.readlines():
                        if 'Total number of processes' in line or 'Total processes' in line:
                            # If the number of processes is not the same
                            # as the one stated inside the file then this
                            # log is not a log of the first load
                            if line.split()[-1] != first_num_of_processes:
                                fd.close()
                                files_to_remove.add(file)
                                continue

                for file in files_to_remove:
                    first_files.remove(file)

                # The files needed to be removed from
                # the first load are the necessary logs
                # of the second load
                second_files = list(files_to_remove)

        # Gather all the time-logs for the first load
        f_load_cos_times = list()
        for file in first_files:
            logfile_times = list()
            with open(file) as fd:
                for line in fd.readlines():
                    if "Time in seconds" in line:
                        logfile_times.append(float(line.split()[-1]))
                    if "Overall Time:" in line:
                        logfile_times.append(LoadManager.to_seconds(line.split()[-1]))
            f_load_cos_times.append(logfile_times)

        # Gather all the time-logs for the second load
        s_load_cos_times = list()
        for file in second_files:
            logfile_times = list()
            with open(file) as fd:
                for line in fd.readlines():
                    if "Time in seconds" in line:
                        logfile_times.append(float(line.split()[-1]))
                    if "Overall Time:" in line:
                        logfile_times.append(LoadManager.to_seconds(line.split()[-1]))
            s_load_cos_times.append(logfile_times)

        # If the same workloads then get the same lists
        # of coscheduled times
        if first_load == second_load:
            s_load_cos_times += f_load_cos_times
            f_load_cos_times = s_load_cos_times

        out.append((first_load, second_load, f_load_cos_times))
        out.append((second_load, first_load, s_load_cos_times))

        return out

    def init_loads(self, runs_dir=None) -> None:
        """Create and initialize the time bundles of loads of a specified
        benchmark suite on a specific machine. Firstly, it creates the
        loads. Secondly, it populates their compact execution time logs.
        Lastly, it bonds together different loads based on the coscheduled
        experiments that were ran and saves their execution time logs for
        each pair.

        ⟡ runs_dir ⟡ if a user needs to point manually where the loads are
        saved; if not then the process of finding them becomes automatic
        and is based on the directory tree structure of the project
        """
        if runs_dir is None:
            runs_dir = f"{self.rootdir}/Co-Scheduling/logs"

        if self.suite is None:
            raise RuntimeError("A suite name was not given")

        # If suites were mixed on the experiments then 
        # get their compact counterparts from their
        # respective direcories
        if "_" in self.suite:
            compact_dirs = list()
            masks = os.listdir(f"{runs_dir}/{self.machine}/{self.suite}")

            for suite in self.suite.split("_"):
                compact_dirs.extend([
                    f"{runs_dir}/{self.machine}/{suite}/{dire}"
                    for dire in os.listdir(f"{runs_dir}/{self.machine}/{suite}")
                    if '_cmp' in dire and
                    reduce(lambda a, b: a or b, map(lambda d: dire.replace("_cmp", "") in d, masks))
                ])
        else:
            # Get the compact experiments' directories
            compact_dirs = [
                f"{runs_dir}/{self.machine}/{self.suite}/{dire}"
                for dire in os.listdir(f"{runs_dir}/{self.machine}/{self.suite}")
                if '_cmp' in dire
            ]

        # Gather all the data from the compact runs of each load
        with ProcessPoolExecutor() as pool:
            res = pool.map(LoadManager.init_compact, compact_dirs)
            for name, num_of_processes, time_logs in res:
                if time_logs != []:
                    self.loads[name] = Load(load_name=name,
                                            num_of_processes=num_of_processes,
                                            machine=self.machine, 
                                            suite=self.suite)

                    self.loads[name].compact_timelogs = time_logs

        # Get the coschedule experiments' directories
        coschedule_dirs = [
            f"{runs_dir}/{self.machine}/{self.suite}/{dire}"
            for dire in os.listdir(f"{runs_dir}/{self.machine}/{self.suite}")
            if '_cmp' not in dire and 'spare' not in dire
        ]

        # Gather all the data from the coscheduled runs of each load
        with ProcessPoolExecutor() as pool:
            res = pool.map(LoadManager.init_coschedule, coschedule_dirs)
            for elem in res:
                first_load_list, second_load_list = elem
                first_load, first_coload, first_time_logs = first_load_list
                second_load, second_coload, second_time_logs = second_load_list

                try:
                    self.loads[first_load].set_coload(first_coload, first_time_logs)
                except Exception:
                    print(f"\033[31m{self.machine} : {self.suite} -> {first_load}: Couldn't build load\033[0m")
                    pass

                try:
                    self.loads[second_load].set_coload(second_coload, second_time_logs)
                except Exception:
                    print(f"\033[31m{self.machine} : {self.suite} -> {second_load}: Couldn't build load\033[0m")
                    pass

    def profiling_data(self, ppn, profiling_dir=None) -> None:
        """Gather all the perf and mpi attributes and save them to their
        respective loads

        ⟡ ppn ⟡ processors per node; used to calculate the nodes binded
        by a load

        ⟡ profiling_dir ⟡ if someone wants to set manually where the
        perf and mpiP logs are kept for each load
        """

        # If the directory where all the perf and mpiP are kept is not
        # manually provided by the user then use the project's root directory.
        if profiling_dir is None:
            profiling_dir = f"{self.rootdir}/Performance_Counters/logs"

        # Setup the logs directory for the specific machine
        # and benchmark suite
        logs_dir = f"{profiling_dir}/{self.machine}/{self.suite}"

        # Check if the directory exists
        if not os.path.exists(logs_dir):
            return
        
        for load_dir in os.listdir(logs_dir):

            # Every directory's name is a load
            load = load_dir
            # We need to know how many nodes where binded for the experiment
            nodes_binded = math.ceil(self.loads[load].num_of_processes / ppn)

            # Open and get the perf logs for the specific load
            # The perf logs are located inside a file called PERF_COUNTERS
            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/PERF_COUNTERS", "r")
                # cycles: how many CPU cycles were consumed
                # to execute the load
                cycles = int(fd.readline().split(':')[1])
                # instructions: how many CPU specific instructions 
                # were executed for the load
                instructions = int(fd.readline().split(':')[1])
                # dpops: how many double precision floating point
                # operations were executed when we ran the load
                dpops = int(fd.readline().split(':')[1])
                # bytes_transferred: how many bytes were transferred when
                # we executed the load
                bytes_transferred = 64 * int(fd.readline().split(':')[1])
                fd.close()

                # From the previous values we get ipc, dpops per node
                # and bytes_transferred per node for each load
                # The last two are divided by the number of nodes
                # to include impartial architectural characteristics
                # to the experiments
                self.loads[load].ipc = instructions / cycles
                self.loads[load].dpops = dpops / nodes_binded
                self.loads[load].bytes_transferred = bytes_transferred / nodes_binded

            except:
                print(f"\033[33m{load} -> EXTRACTED/PERF_COUNTERS: File doesn't exist\033[0m")

            # Open and get information about the compute and MPI times
            # spent on each load
            # The values are saved on a file called LOAD_AGGR_TIME
            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/LOAD_AGGR_TIME", "r")
                app_time = float(fd.readline().split(':')[1])
                mpi_time = float(fd.readline().split(':')[1])
                fd.close()

                # Compute time = app_time - mpi_time
                mpi_time = mpi_time

                # Also add the percentage
                self.loads[load].compute_time_norm = (app_time - mpi_time) / app_time
                self.loads[load].mpi_time_norm = mpi_time / app_time

            except Exception:
                print(f"\033[33m{load} -> EXTRACTED/LOAD_AGGR_TIME : File doesn't exist\033[0m")

            # Open and get which and how many times specific MPI
            # functions were called
            # The values can be found on a file called MPI_CMDS_CALLS
            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/MPI_CMDS_CALLS")
                for line in fd.readlines():
                    # The line is formatted as MPI cmd:value
                    mpi_cmd, val = line.split(':')
                    # Process the fields
                    mpi_cmd = f"mpi_{mpi_cmd.lower()}"
                    val = int(float(val))
                    # Save the values
                    self.loads[load].mpi_noc[mpi_cmd] = val
                fd.close()

            except Exception:
                print(f"\033[33m{load} -> EXTRACTED/MPI_CMDS_CALLS : File doesn't exist\033[0m")

            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/MPI_CMDS_TIME")
                for line in fd.readlines():
                    # The line is formatted as MPI cmd:value
                    mpi_cmd, val = line.split(':')
                    # Process the fields
                    mpi_cmd = f"mpi_{mpi_cmd.lower()}"
                    val = float(val)
                    # Save the values
                    self.loads[load].mpi_atime[mpi_cmd] = val
                fd.close()

            except Exception:
                print(f"\033[33m{load} -> EXTRACTED/MPI_CMDS_TIME : File doesn't exist\033[0m")

            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/MPI_CMDS_BYTES")
                for line in fd.readlines():
                    # The line is formatted as MPI cmd:value
                    mpi_cmd, val = line.split(':')
                    # Process the fields
                    mpi_cmd = f"mpi_{mpi_cmd.lower()}"
                    val = int(float(val))
                    # Save the values
                    self.loads[load].mpi_abytes[mpi_cmd] = val
                fd.close()

            except Exception:
                print(f"\033[33m{load} -> EXTRACTED/MPI_CMDS_BYTES : File doesn't exist\033[0m")

    def export_to_json(self):
        repres =\
        {
                "machine": self.machine,
                "suite": self.suite,
                "loads": [
                    load.to_json() for load in list(self.loads.values())
                ]
        }

        with open(f"lm-{self.machine}-{self.suite}.json", "w") as fd:
            fd.write(dumps(repres))

    def import_from_json(self, file: Optional[str] = None):
        if file is None:
            print("Can't build LoadManager if no file is given")
            return
 
        with open(file, "r") as fd:
                repres = loads(fd.read())
            
        # Update the object's attributes from the JSON data
        self.machine = repres["machine"]
        self.suite = repres["suite"]
        
        # Reconstruct the loads dictionary from the list of load JSON representations
        self.loads = {}
        for load_json in repres["loads"]:
            load = Load.from_json(load_json)  # Convert dict back to JSON string
            self.loads[load.load_name] = load  # Assuming load_name is a unique key

    def export_to_db(self, 
                     host="localhost", 
                     port=8080, 
                     username=None, 
                     password=None, 
                     dbname=None, 
                     collection="loads") -> None:

        # Get the credentials from the user in order to hide
        # them from the source code
        try:
            # Create a Mongo client to communicate with the database
            if "mongodb+srv://" in host or "mongodb://" in host:
                client = pymongo.MongoClient(host, server_api=ServerApi("1"))
            else:

                if username is None:
                    raise RuntimeError("Didn't provide a username")
                if password is None:
                    raise RuntimeError("Didn't provide a password")

                client = pymongo.MongoClient(host, port, username=username, password=password)
        except Exception:
            print("Couldn't connect to MongoServer. Is the server up?")
            return

        # Connect to or create the database
        if dbname is None:
            raise Exception("Please provide a database name")

        db = client[dbname]

        # Get reference or create the 'loads' collection
        coll = db[collection]

        # Add or update a load
        for name, load in self:
            # Create the id of the load
            _id = {
                "machine": load.machine,
                "suite": load.suite,
                "load": name
            }

            # First check if the load already exists in
            # the collection
            query = {"_id": _id}

            # Get a list of loads with the same id
            findings = list(coll.find(query))

            if findings != []:
                # If the load exists then update all occurencies
                #coll.update_many(query, {"$set": {"bin": pickle.dumps(self.loads[load])}})
                coll.update_many(query, {"$set": {"repres": load.to_json()}})
            else:
                coll.insert_one({"_id": _id,  "repres": load.to_json()})

    def import_from_db(self, 
                       host="localhost", 
                       port=8080, 
                       username=None, 
                       password=None, 
                       dbname=None, 
                       collection="loads") -> None:

        try:
            # Create a Mongo client to communicate with the database
            if "mongodb+srv://" in host or "mongodb://" in host:
                client = pymongo.MongoClient(host, server_api=ServerApi("1"))
            else:

                if username is None:
                    raise RuntimeError("Didn't provide a username")
                if password is None:
                    raise RuntimeError("Didn't provide a password")

                client = pymongo.MongoClient(host, port, username=username, password=password)

        except Exception:
            print("Couldn't connect to MongoServer. Is the server up?")
            return

        # Connect to or create the database
        if dbname is None:
            raise Exception("Please provide a database name")
        db = client[dbname]

        # Get reference or create the 'loads' collection
        coll = db[collection]

        # Query based on machine and/or suite
        if self.suite is not None:
            query = { "_id.machine": self.machine, "_id.suite": self.suite }
        else:
            query = { "_id.machine": self.machine }

        for doc in coll.find(query):
            # load = pickle.loads(doc["bin"])
            load = Load.from_json(doc["repres"])
            # load.coloads_median_speedup = dict()
            # for coload_name in load.coloads:
            #     load.set_median_speedup(coload_name)
            self.loads[doc["_id"]["load"]] = load

        # Filter out coloads
        for _, load in self.loads.items():

            correct_coloads = dict()

            for coload_name in load.coscheduled_timelogs:
                if coload_name in self.loads:
                    correct_coloads[coload_name] = load.coscheduled_timelogs[coload_name]

            load.coscheduled_timelogs = correct_coloads

    def export_coschedules(self) -> DataFrame:
        """Export the co-scheduled load names and their execution time with
        respect to one another.
        """
        columns = [
                "name_A",
                "procs_A",
                "compact_A",
                "name_B",
                "procs_B",
                "compact_B",
                "co_A_B",
                "co_B_A"
        ]

        data: list[list] = list()
        load_names = sorted(list(self.loads.keys()))

        for i, name in enumerate(load_names):

            load = self.loads[name]

            for co_name in load_names[i:]:

                co_load = self.loads[co_name]
            
                if co_name not in list(load.coscheduled_timelogs.keys()):
                    co_A_B = None
                    co_B_A = None
                else:
                    if name != co_name:
                        co_A_B = load.get_med_time(co_load)
                        co_B_A = co_load.get_med_time(load)
                    else:
                        co_A_B = float(med(load.coscheduled_timelogs[co_name][1]))
                        co_B_A = float(med(load.coscheduled_timelogs[co_name][0]))


                data.append([
                    name, 
                    load.num_of_processes,
                    load.get_med_time(), 
                    co_name, 
                    co_load.num_of_processes,
                    co_load.get_med_time(),
                    co_A_B,
                    co_B_A
                ])

        return DataFrame(data=data, columns=columns)

    def export_ml_table(self) -> DataFrame:
        """Export a DataFrame for the machine learning model to train
        """

        columns = [
                "names",
                "time A",
                "mpi time normalized A",
                "ipc A",
                "FLOPS A",
                "BW A",
                "time B",
                "mpi time normalized B",
                "ipc B",
                "FLOPS B",
                "BW B",
                "speedup"
        ]

        data: list[list] = list()

        for name, load in self.loads.items():

            for co_name, co_load in self.loads.items():

                # For each possible co-schedule record the outcome
                load_data: list = list()

                # Their names in order to discern them from other pairs
                load_data.append(f"{name}_{co_name}")

                # Add both tags
                load_data.extend(load.get_tag())
                load_data.extend(co_load.get_tag())

                # If we have their co-scheduled speedup then put the value
                if co_name in load.coscheduled_timelogs:
                    load_data.append(load.get_med_speedup(co_name))
                # If we do not then add a None value to show that it's empty
                else:
                    load_data.append(None)

                data.append(load_data)

        return DataFrame(data=data, columns=columns)

    def export_heatmap(self) -> Heatmap:

        heatmap: Heatmap = dict()

        for name, load in self.loads.items():
            heatmap[name] = dict()
            for co_name, co_load in self.loads.items():
                try:
                    heatmap[name][co_name] = load.get_med_speedup(co_load)
                except:
                    heatmap[name][co_name] = None

        return heatmap

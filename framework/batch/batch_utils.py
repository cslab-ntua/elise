from json import loads as json_loads
from yaml import safe_load
from pickle import load as pickle_load
import importlib.util
import inspect
import os
import sys

# Introduce path to realsim
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../")
))

# Introduce path to api.loader
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
))

# Get logger
from common.utils import define_logger
logger = define_logger()

# LoadManager
from api.loader import LoadManager

# Database
from realsim.database import Database

# Generators
from realsim.generators.AGenerator import AbstractGenerator
from realsim.generators.random import RandomGenerator
from realsim.generators.randomfromlist import RandomFromListGenerator
from realsim.generators.keysdict import KeysDictGenerator
from realsim.generators.keyslist import KeysListGenerator
from realsim.generators.shufflekeyslist import ShuffleKeysListGenerator
from realsim.generators.swf import SWFGenerator

# Distributions
from realsim.generators.distribution.idistribution import IDistribution
from realsim.generators.distribution.constantdistr import ConstantDistribution
from realsim.generators.distribution.randomdistr import RandomDistribution
from realsim.generators.distribution.poissondistr import PoissonDistribution

# Cluster
from realsim.cluster.cluster import Cluster

# Schedulers
from realsim.scheduler.scheduler import Scheduler
from realsim.scheduler.schedulers.fifo import FIFOScheduler
from realsim.scheduler.schedulers.easy import EASYScheduler
from realsim.scheduler.schedulers.conservative import ConservativeScheduler
from realsim.scheduler.coschedulers.ranks.random import RandomRanksCoscheduler

# Logger
from realsim.logger.logger import Logger

# ComputeEngine
from realsim.compengine import ComputeEngine

def import_module(path):
    mod_name = os.path.basename(path).replace(".py", "")
    spec = importlib.util.spec_from_file_location(mod_name, path)
    gen_mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = gen_mod
    spec.loader.exec_module(gen_mod)
    return spec.name


class BatchCreator:

    def __init__(self, project_file_path: str):

        # Ready to use generators implementing the AbstractGenerator interface
        self.__impl_generators = {
            RandomGenerator.name: RandomGenerator,
            RandomFromListGenerator.name: RandomFromListGenerator,
            KeysDictGenerator.name: KeysDictGenerator,
            KeysListGenerator.name: KeysListGenerator,
            ShuffleKeysListGenerator.name: ShuffleKeysListGenerator,
            SWFGenerator.name: SWFGenerator
        }

        # Ready to use schedulers implementing the Distribution interface
        self.__impl_distributions = {
            "Constant": ConstantDistribution,
            "Random": RandomDistribution,
            "Poisson": PoissonDistribution
        }

        # Ready to use schedulers implementing the Scheduler interface
        self.__impl_schedulers = {
            FIFOScheduler.name: FIFOScheduler,
            EASYScheduler.name: EASYScheduler,
            ConservativeScheduler.name: ConservativeScheduler,
            RandomRanksCoscheduler.name: RandomRanksCoscheduler
        }
        
        logger.debug(f"Opening project file: {project_file_path}")
        # Load the configuration file
        with open(project_file_path, "r") as fd:

            self.config = safe_load(fd)
            
            sanity_entries = ["name", "workloads", "schedulers", "actions"]

            if list(filter(lambda x: x not in self.config, sanity_entries)):
                raise RuntimeError("The configuration file is not properly designed")

            self.__project_name = self.config["name"]
            self.__project_workloads = self.config["workloads"]
            self.__project_schedulers = self.config["schedulers"]
            self.__project_actions = self.config["actions"] if "actions" in self.config else dict()

        # If using MPI store modules that should be exported to other MPI procs
        self.mods_export = list()

    def get_sim_configs_num(self) -> int:
        logger.debug("Calculating the total number of simulation configurations")
        workloads_num = 0
        for workload in self.__project_workloads:
            workloads_num += 1 if "repeat" not in workload else int(workload["repeat"])

        return workloads_num * (1 + len(self.__project_schedulers["others"]))

    def process_workloads(self) -> None:

        logger.debug("Begin processing the workloads")

        # Process the workloads
        self.__workloads = list()

        for workload in self.__project_workloads:
        
            # Create a LoadManager based on the options given
            lm = LoadManager(machine=workload["loads-machine"],
                             suite=workload["loads-suite"])
            # A LoadManager instance can be created using
            if "path" in workload:
                # A path to a directory with the real logs
                path = workload["path"]
                lm.init_loads(runs_dir=path)
            elif "load-manager" in workload:
                # A pickled LoadManager instance (or json WIP)
                with open(workload["load-manager"], "rb") as fd:
                    lm = pickle_load(fd)
            elif "db" in workload:
                # A mongo database url
                lm.import_from_db(host=workload["db"], dbname="storehouse")
            elif "json" in workload:
                lm.import_from_json(workload["json"])
            else:
                raise RuntimeError("Couldn't provide a way to create a LoadManager")

            # Create a heatmap from the LoadManager instance or use a user-defined
            # if a path is provided
            if "heatmap" in workload:
                with open(workload["heatmap"], "r") as fd:
                    heatmap = json_loads(fd.read())
            else:
                heatmap = lm.export_heatmap()

            logger.debug(f"Finished calculating the heatmap: {heatmap}")

            # Create the workload using the generator provided
            if "generator" in workload:
                generator = workload["generator"]
                gen_type = generator["type"]
                gen_arg = generator["arg"]

                # If a python file is provided for the generator
                if os.path.exists(gen_type) and ".py" in gen_type:
                    # Import generator module
                    spec_name = import_module(gen_type)
                    gen_mod = sys.modules[spec_name]
                    # Get the generator class from the module
                    classes = inspect.getmembers(gen_mod, inspect.isclass)
                    # It must be a concrete class implementing the AbstractGenerator interface
                    classes = list(filter(lambda it: not inspect.isabstract(it[1]) and issubclass(it[1], AbstractGenerator), classes))

                    # If there are multiple then inform the user that the first will be used
                    if len(classes) > 1:
                        print(f"Multiple generator definitions were found. Using the first definition: {classes[0][0]}")

                    _, gen_cls = classes[0]

                    # Export module for MPI procs
                    self.mods_export.append(gen_type)
                else:
                    try:
                        gen_cls = self.__impl_generators[gen_type]
                    except:
                        raise RuntimeError(f"The name {gen_type} of the generator provided does not exist")

                # Create instance of generator
                gen_inst = gen_cls(load_manager=lm)
                # gen_inst = gen_cls()
            
                logger.debug(f"Got the generator: {gen_inst.name}")

                if "repeat" in workload:
                    repeat = int(workload["repeat"])
                else:
                    repeat = 1

                for _ in range(repeat):

                    # Generate the workload
                    if gen_type in ["List Generator","Shuffle List Generator"]:
                        with open(gen_arg, 'r') as _f:
                            gen_data = _f.read()
                        gen_workload = gen_inst.generate_jobs_set(gen_data)

                    elif gen_type in ["Random From List Generator"]:
                        with open(gen_arg[1], 'r') as _f:
                            gen_data = _f.read()
                        gen_workload = gen_inst.generate_jobs_set([gen_arg[0], gen_data])

                    else:
                        gen_workload = gen_inst.generate_jobs_set(gen_arg)


                    logger.debug(f"Finished generating the workload")

                    # Check if a transformer distribution is provided by the user
                    if "distribution" in generator:
                    
                        distribution = generator["distribution"]
                        distr_type = distribution["type"]
                        distr_arg = distribution["arg"]

                        # If a path is provided for the distribution transformer
                        if os.path.exists(distr_type) and ".py" in distr_type:
                            spec_name = import_module(distr_type)
                            distr_mod = sys.modules[spec_name]
                            classes = inspect.getmembers(distr_mod, inspect.isclass)
                            classes = list(filter(lambda it: not inspect.isabstract(it[1]) and issubclass(it[1], IDistribution), classes))
                            # If there are multiple then inform the user that the first will be used
                            if len(classes) > 1:
                                print(f"Multiple distribution definitions were found. Using the first definition: {classes[0][0]}")

                            _, distr_cls = classes[0]
                            # Export module for MPI procs
                            self.mods_export.append(distr_type)
                        else:
                            try:
                                distr_cls = self.__impl_distributions[distr_type]
                            except:
                                raise RuntimeError(f"Distribution of type {distr_type} does not exist")

                        distr_inst = distr_cls()
                        distr_inst.apply_distribution(gen_workload, time_step=distr_arg)

                        logger.debug(f"A distribution was applied to the workload: {distr_inst.name}")


                    nodes = int(workload["cluster"]["nodes"])
                    socket_conf = tuple(workload["cluster"]["socket-conf"])
                    self.__workloads.append((gen_workload, heatmap, nodes, socket_conf))

            else:
                raise RuntimeError("A generator was not provided")

        logger.debug("Finished processing the workloads")

    def process_schedulers(self) -> None:

        logger.debug("Begin processing the schedulers")

        # Process the schedulers
        # The first one in the list will always be the default
        self.__schedulers = list()

        for scheduler in [self.__project_schedulers["default"]] + self.__project_schedulers["others"]:

            if os.path.exists(scheduler) and ".py" in scheduler:
                spec_name = import_module(scheduler)
                sched_mod = sys.modules[spec_name]
                classes = inspect.getmembers(sched_mod, inspect.isclass)
                classes = list(filter(lambda it: not inspect.isabstract(it[1]) and issubclass(it[1], Scheduler), classes))
                # If there are multiple then inform the user that the first will be used
                if len(classes) > 1:
                    print(f"Multiple scheduler definitions were found. Using the first definition: {classes[0][0]}")

                _, sched_cls = classes[0]

                # To export modules for MPI procs
                print(scheduler)
                self.mods_export.append(scheduler)
            else:
                try:
                    sched_cls = self.__impl_schedulers[scheduler]
                except:
                    raise RuntimeError(f"Scheduler of type {scheduler} does not exist")

            self.__schedulers.append(sched_cls)

        logger.debug(f"Finished processing the schedulers: {self.__schedulers}")

    def process_actions(self) -> None:
        """
        The structure of self.__actions
        actions = {
            workload0 = {
                scheduler0 = [],
                scheduler1 = [],
                ..
                schedulerM = []
            },
            ..
            workloadN = {
                scheduler0 = []
                scheduler1 = []
                ..
                schedulerM = []
            }
        }
        The structure of self.__extra_features is a list of (arg: str, val: T) tuples
        self.__extra_features = [(arg0, val0), (arg1, val1), ...]
        """

        logger.debug("Begin processing the postprocessing actions")

        # Define __actions
        self.__actions = dict()
        for i in range(len(self.__workloads)):
            workload_dict = dict()
            for sched_cls in self.__schedulers:
                workload_dict.update({sched_cls.name: []})
            self.__actions.update({i: workload_dict})

        # Define __extra_features
        self.__extra_features: list[tuple] = list()

        for action in self.__project_actions:
            action_workloads = self.__project_actions[action]["workloads"]
            action_schedulers = self.__project_actions[action]["schedulers"]

            action_extra_features = [(arg, val) 
                                     for arg, val in self.__project_actions[action].items() 
                                     if arg not in ["workloads", "schedulers"]]

            # Simple implementation is to overwrite an argument with the latest
            # value provided in the project file
            self.__extra_features.extend(action_extra_features)

            if action_workloads == "all":
                for workload_dict in self.__actions.values():
                    if action_schedulers == "all":
                        for sched_dict in workload_dict.values():
                            sched_dict.append(action)
                    else:
                        for sched_name in action_schedulers:
                            workload_dict[sched_name].append(action)
            else:
                for i in action_workloads:
                    if action_schedulers == "all":
                        for sched_dict in self.__actions[i].values():
                            sched_dict.append(action)
                    else:
                        for sched_name in action_schedulers:
                            self.__actions[i][sched_name].append(action)

        logger.debug(f"Finished processing the postprocessing actions: {self.__extra_features}")

    def create_ranks(self) -> None:
        self.process_workloads()
        self.process_schedulers()
        self.process_actions()

        # Id for the simulation run
        sim_id = 0

        # Create the ranks
        self.ranks = list()
        for jdx, [workload, heatmap, nodes, socket_conf] in enumerate(self.__workloads):
            for sched_cls in self.__schedulers:

                # Create a database instance
                database = Database(workload, heatmap)
                database.setup()

                # Create a cluster instance
                cluster = Cluster(nodes, socket_conf)

                # Create a scheduler instance
                scheduler = sched_cls()
                # Apply all the general options if any exists
                if "general-options" in self.__project_schedulers:
                    for opt, val in self.__project_schedulers["general-options"].items():
                        scheduler.__dict__[opt] = val

                # Create a logger instance
                logger = Logger(debug=False)

                # Create a compute engine instance
                compengine = ComputeEngine(database, cluster, scheduler, logger)
                compengine.setup_preloaded_jobs()

                # Set actions for this simulation
                actions = self.__actions[jdx][sched_cls.name]

                self.ranks.append((sim_id, database, cluster, scheduler, logger, compengine, actions, self.__extra_features))

                sim_id += 1

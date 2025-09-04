from contextlib import contextmanager
from cProfile import Profile
import importlib.util
import inspect
import io
import logging
import os
import platform
import pstats
import psutil
import socket
import sys

def import_module(path):
    mod_name = os.path.basename(path).replace(".py", "")
    spec = importlib.util.spec_from_file_location(mod_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return spec.name, module

def get_ancestry_tree() -> list[str]:
    proc = psutil.Process()
    ancestors = [f"{proc.pid}: {proc.name()}"]
    while True:
        parent = proc.parent()
        # Windows does not give the full information depending on the user
        if parent is not None:
            ancestors.append(f"{parent.pid}: {parent.name()}")
        else:
            return ancestors
        if parent.pid == 1:
            return ancestors
        else:
            proc = parent

def handler_and_formatter(logger: logging.Logger):

    file_handler = logging.FileHandler(filename=f"log_{logger.name}_{socket.gethostname()}_{os.getpid()}", mode="a", encoding="utf-8")
    file_formatter = logging.Formatter(fmt="[%(levelname)s] {%(thread)s} (%(asctime)s) - %(filename)s:%(funcName)s - %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

def define_logger(log_ancestry=False, log_env=False) -> logging.Logger:
    """
    Creates a logger instance with the name based on the current filename and
    logs it to a file. If debug mode is enabled (through an environment variable),
    sets the logger level to DEBUG, adds handlers and formatters for logging,
    and optionally logs the process ancestry and environment variables.

    Args:
        log_ancestry (bool): Whether to log the process ancestry (default: False)
        log_env (bool): Whether to log the environment variables (default: False)

    Returns:
        logger (logging.Logger): The created logger instance
    """
    # Get the current stack trace and extract the filename of the caller.
    stack_trace = inspect.stack()
    name = os.path.basename(stack_trace[1].filename).replace(".py", "")

    # Create a new logger with the specified name
    logger = logging.getLogger(name)

    # Check if debug mode is enabled through the ELiSE_DEBUG environment variable
    debug_enabled = os.environ.get("ELiSE_DEBUG", "false").lower()
    if debug_enabled in ["1", "yes", "true"]:
        logger.setLevel("DEBUG")

        # Define and apply a handler and formatter for logging
        handler_and_formatter(logger)

        # If log_ancestry is True, log the process ancestry
        if log_ancestry:
            ancestry = "\n".join(["\n\tANCESTRY", "\t--------"] + [f"\t{proc_info}" for proc_info in get_ancestry_tree()])
            logger.info(ancestry)

        # If log_env is True, log the environment variables
        if log_env:
            env = "\n".join(["\n\tENVIRONMENT", "\t-----------"] + [f"\t{it[0]}={it[1]}" for it in os.environ.items()])
            logger.info(env)


    return logger

def envvar_path_val(envvar_name):
    envvar_val_str = os.environ.get(envvar_name, ".")
    return envvar_val_str

def envvar_bool_val(envvar_name):
    envvar_val_str = os.environ.get(envvar_name, "0")
    if envvar_val_str.lower() in ["1", "y", "yes", "true"]:
        return True
    return False

def envvar_int_val(envvar_name, default_val: int):
    envvar_val_str = os.environ.get(envvar_name, str(default_val))
    if str.isnumeric(envvar_val_str):
        return int(envvar_val_str)
    else:
        return default_val

@contextmanager
def profiling_ctx(idx: int, scheduler: str, logger):
    profiler = Profile()
    if envvar_bool_val("ELiSE_PROFILING"):
        # Get ELiSE working directory
        working_dir = envvar_path_val("ELiSE_WORKING_DIR")
        
        # Profiling directory
        profiling_dir = f"{working_dir}/profiling"
        os.makedirs(profiling_dir, exist_ok=True)

        # Filename for the log file
        filename = f"input_{idx}_{scheduler.lower().replace(' ', '_')}_profile.log"

        logger.debug("Profiling is enabled")
        profiler.enable()

        try:
            yield
        finally:
            profiler.disable()
            
            strstream = io.StringIO()
            stats = pstats.Stats(profiler, stream=strstream).sort_stats("cumtime")

            # Get profiling depth if given by the user
            profiling_depth = envvar_int_val("ELiSE_PROFILING_DEPTH", 30)
            stats.print_stats(profiling_depth)
            
            with open(f"{profiling_dir}/{filename}", "w") as fd:
                fd.write(strstream.getvalue())
    else:
        yield

def is_bundled():
    """Check if we are inside the bundled folder - meaning we are using the executable"""
    return getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")

def process_name(name: str):
    if is_bundled():
        if platform.system() == "Windows":
            return name + ".exe"
        return name
    else:
        return name + ".py"

def get_executable(path):
    exe = [str(path)]
    return exe if is_bundled() else ["python"] + exe
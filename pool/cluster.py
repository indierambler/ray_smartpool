"""
Configuration and management functions for a ray cluster

CONTENTS
--------
init_ray : function
    make sure ray is initaialized with proper connection and runtime environment
"""

# Import dependencies
from cProfile import run
import copy
import psutil
import ray

# Classes
@ray.remote
class RayConfig:
    def __init__(self, config):
        self.config = config

    def get_config(self, verbose=False):
        if verbose:
            for key,val in self.config.items():
                print(f'{key}: {val}')
        return self.config

    def set_config(self, config):
        pass

# Functions
def init_ray(
    head_ip=None,
    existing_cluster = False,
    is_remote = False,
    namespace=None,
    cpus=None,
    gpus=None,
    cwd=None, 
    local_pkgs=None, 
    pip_pkgs=None, 
    verbose=False
):
    """Make sure ray is initialized with proper connection and runtime environment
    """
    # set runtime environment
    runtime_env = {}
    if cwd is not None:
        runtime_env['working_dir'] = cwd
    if pip_pkgs is not None:
        runtime_env['pip'] = pip_pkgs
    if local_pkgs is not None:
        runtime_env['py_modules'] = local_pkgs

    # allocate cpus, gpus, and memory to ray
    if existing_cluster:  # choose number of cpus to allocate to ray (apparently this actually just sets the max amount of concurrent tasks allowed)
        cpus = None
        gpus = None
        memory = None
    else:
        max_cpus = psutil.cpu_count(logical=True)  # get max available cpus (logical=True may include hyperthreaded cores)
        if cpus is None or cpus > max_cpus:
            cpus = max_cpus
        if gpus is None:  # choose number of gpus to allocate to ray
            gpus = 0
        memory = None  # allocate memory to ray in bytes (default is the available system memory)

    # build ray head address
    if existing_cluster and is_remote:
        address = f'ray://{head_ip}:10001'  # ray address for remote head
    elif existing_cluster and not is_remote:
        address = 'auto'  # existing ray address resides on local machine
    else:
        address = None  # create new ray instance on local machine

    # create ray config dict
    config1 = {
        'address': address,
        'num_cpus': cpus,
        'num_gpus': gpus,
        'object_store_memory': memory,  # defaults to available system memory
        'local_mode': False,  # for debugging
        'ignore_reinit_error': False,  # to ignore an additional init call
        'include_dashboard': False,  # select whether to run the ray dashboard
        'dashboard_host': '127.0.0.1',  # 127.0.0.1=localhost, 0.0.0.0=universally available
        #'job_config': None,  # ray job configuration object
        #'configure_logging': True,
        #'logging_level': 'info',
        #'logging_format': '%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s',
        #'log_to_driver': True,
        'namespace': namespace,
        'runtime_env': runtime_env,  # may need to: copy.deepcopy(runtime_env), in order to save the environment
        #'_enable_object_reconstruction': False,  # enable auto-rebuilding of lost objects in the store
        '_redis_password': '5241590000000000',
        #'_temp_dir=None': ,  # manually set the temp directory for the ray process
    }
    
    # initialize ray cluster/spontaneous and remote/local
    if ray.is_initialized() and ray.get_runtime_context().namespace == namespace:
        config = ray.get_actor(namespace).get_config.remote()
        if verbose: print(f'Ray namespace "{namespace}" is already running')
    else:
        config2 = ray.init(**config1)  # initialize ray
        config = config1 | config2  # merge the input dict and the runtime dict
        config['runtime_env'] = runtime_env
        config3 = ray.available_resources()  # get the few available readable params from the cluster
        config['num_cpus'] = config3['CPU']
        config['memory'] = config3['memory']
        config['object_store_memory'] = config3['object_store_memory']
        config_act = RayConfig.options(name=config['namespace']).remote(config)  # spawn an actor that holds all the initialization settings
        if verbose: print(f'Ray namespace "{namespace}" has been initialized')
    return config_act


def terminate_ray():
    """Disconnect the worker, and terminate processes started by ray.init()
    """
    # TODO: terminate by namespace?
    if ray.is_initialized():
        ray.shutdown()


def restart_ray(config_dict=None,config_actor=None):
    #get the ray config
    if config_dict is not None:
        config = config_dict
    elif config_actor is not None:
        config = ray.get(config_actor.get_config.remote())
    else:
        config = {'address': None, 'namespace': None}

    # set runtime environment
    if 'working_dir' in config['runtime_env']:
        cwd = config['runtime_env']['working_dir']
    else:
        cwd = None
    if 'pip' in config['runtime_env']:
        pip_pkgs = config['runtime_env']['pip']
    else:
        pip_pkgs = None
    if 'py_modules' in config['runtime_env']:
        local_pkgs = config['runtime_env']['py_modules']
    else:
        local_pkgs = None

    terminate_ray()
    ray_config = init_ray(
        head_ip=config['address'],
        namespace=config['namespace'],
        cwd=cwd,
        local_pkgs=local_pkgs,
        pip_pkgs=pip_pkgs,
    )
    return ray_config
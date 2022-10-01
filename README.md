# ray_smartpool
A more configurable abstraction of Ray's multiprocessing pool

## Usage
### Clone the project
```git clone https://github.com/indierambler/ray_smartpool.git```

### Format input for parallel computing
```
ids = ['id1', 'id2', ..., 'idN']  # IDs used to identify the task through the transformation and in results
inputs = [{'argname1A': arg1A, 'argname1B: arg1B}, {'argname2A': arg2A, 'argname2B: arg2B}, ..., {'argnameNA': argNA, 'argnameNB: argNB}]
```
- length of ```ids``` and ```inputs``` must be equal

### Run parallel tasks
Import ray_smartpool  
```import ray_smartpool as rsp```

Initialize Ray  
```
cluster = rsp.cluster.init_ray(
    head_ip='127.0.0.1',
    existing_cluster=True,
    is_remote=False,
    namespace=None,
    cpus=0,
    gpus=0,
    cwd=None,
    local_pkgs=[pkg1, pkg2, pkg3]
)
```

Initialize the worker pool  
```pool = rsp.advanced.Master(transform_func, max_workers=8, worker_task_limit=5)```

Run a list of tasks through the ray_smartpool  
```result = pool.run(ids, inputs, func=transform_func, verbose=True)```

Result format  
- the result is in the form of a list of dicts
- each dict has an 'id' key with a value that identifies which task the result goes to
- each dict has a 'result' key with the returned value from the task that was run
```[{'id': 'id1', 'result': result1}, {'id': 'id2', 'result': result2}, ..., {'id': 'idN', 'result': resultN}]```
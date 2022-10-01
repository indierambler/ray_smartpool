"""
***WORK IN PROGRESS***
Work group generator for ray
    Master spawns a pool of actors which are intefaced with using ray.util.ActorPool.
    It delegates jobs to and collects jobs from those actors in order to parallelize work.
    Very little low level actor information is available during runtime but the leader
    keeps a global (but unconfirmed by the actors in the pool) status based on work that 
    should be done.

Contents
--------
Master : class
    master used to control and manage the pool of actors
Worker : class/ray actor
    lower level actor meant to do a single, swappable job

Future
------
- add a monitor thread to periodically retrieve leader, work, and job statuses
"""

# Import dependencies
from dataclasses import replace
import os, sys, gc, copy, psutil
import threading, asyncio
import time
import numpy as np
import pandas as pd
import ray

#from .. import base
#from ..similarity import similarity

# Classes
@ray.remote#(memory=1.0*(1024**3))#, object_store_memory=1*(1024**3))#num_cpus=1)  # a dedicated cpu is probably too much
class Worker:
    def __init__(self, func):
        self.func = func

    def set_func(self, new_func):
        self.func = new_func

    def submit(self, id, dict_in):
        result = self.func(**dict_in)
        return {'id': id, 'result': result}


class Master:
    def __init__(self, func, max_workers=4, worker_task_limit=5):
        # worker statuses and info
        self.func = func
        self._max_workers = max_workers
        self._worker_task_limit = worker_task_limit
        self._total_workers = 0
        self._active_workers = []
        self._idle_workers = []
        self._worker_stats = {}  # initially just to record the number of tasks run by each worker
        self.idle = False  # leader is deployed but no workers are working and results are empty
        self.working = False  # workers are working or there may be uncollected results
        self.done = False  # work is complete and results are available

        # job statuses and info
        self._jobs_pending = []
        self._jobs_running = []
        self._jobs_complete = []
        self._job_order = []
        self.results = []

        # manager statuses and info
        self.stop = False  # set this flag to stop work after the current cycle
        self._fray_handler1 = None  # delegation thread if needed
        self._fray_handler2 = None  # collection thread if needed
        self._handler1_stopped = False  # this flag confirms that the delegation handler stopped successfully
        self._handler2_stopped = False  # this flag confirms that the collection handler stopped successfully
        self.waiting = True  # leader is stopped and not deployed
        self.running = False # leader is deployed and running
        self.status = self.get_status()

        # cluster status and info
        self.ram_usage = 0
        self.plasma_usage = 0
        self.manage_cycle = 10
        self.current_cycle = 0

    def get_props(self):
        return self.__dict__

    def _set_status(self):
        # leader statuses (waiting and running) are taken care of in self.deploy() and self.end()
        # set work status
        if self._jobs_pending or self._jobs_running:
            self.idle = False
            self.working = True
            self.done = False
        elif self._jobs_complete and self.results:
            self.idle = False
            self.working = False
            self.done = True
        else:
            self.idle = True
            self.working = False
            self.done = False

        # management cycle operations (only run on the management cycle)
        self.current_cycle += 1
        if self.current_cycle >= self.manage_cycle:
            #self.ram_usage = psutil.virtual_memory().percent
            #self.ram_usage = round((ray.available_resources()['memory'] / ray.cluster_resources()['memory']), 2)  # causes scheduler errors
            #self.plasma_usage = round((ray.available_resources()['object_store_memory'] / ray.cluster_resources()['object_store_memory']), 2)
            self.current_cycle = 0

    def get_status(self):
        status = {}
        # get leader status
        if self.waiting:
            status['leader'] = 'waiting'
        elif self.running:
            status['leader'] = 'running'
        else:
            status['leader'] = 'unknown'

        # get work status
        if self.working:
            status['work'] = 'working'
        elif self.done:
            status['work'] = 'done'
        elif self.idle:
            status['work'] = 'idle'
        else:
            status['work'] = 'idle'

        # get job status
        if self._jobs_running:
            status['job'] = 'running'
        elif self._jobs_pending:
            status['job'] = 'pending'
        elif self._jobs_complete:
            status['job'] = 'complete'
        else:
            status['job'] = 'waiting'

        self.status = status
        return status

    def _sort_results(self):
        sortfunc = lambda x: self._job_order.index(x['id'])
        self.results.sort(key=sortfunc) 

    def get_results(self, sort=True, clear=False):
        if sort:
            self._sort_results()

        results = self.results[:]  # make a copy of the list (does not copy deep)
        #results = [x for x in self.results]  # may need this to iterate results

        if clear:
            for x in results:
                self.results.remove(x)  # remove from class attribute
                self._job_order.remove(x['id'])  # remove from job_order
                self._jobs_complete.remove(x['id'])  # remove from jobs_complete
        
            self._set_status()
        return results

    def clear(self):
        self._jobs_complete = []
        self.results = []
        self._job_order = []
        self.done = False
        self.idle = True

    def auto_garbage_collect(self, mem_limit_gb=None, pct=80.0):
        """Call the garbage collection if memory used is greater than a Gb limit or percent of total available memory.

        PARAMS
        ------
        mem_limit_gb : float
            max amount of memory to allow before collecting garbage (not the max mem usage allowed)
        pct : float
            max percentage of memory to allow before collecting garbage (not the max mem usage allowed)
        """
        if mem_limit_gb is not None:
            if psutil.virtual_memory().active >= mem_limit_gb*(1024**3):
                gc.collect()
        else:
            if psutil.virtual_memory().percent >= pct:
                gc.collect()
        return

    def end(self):
        if self.running:
            self.stop = True
            timeout = 0
            if self.fray_handler1 is not None and self.fray_handler1.is_alive():  # check if delegation thread exists and is alive
                while not self._handler1_stopped:
                    time.sleep(0.1)
                    timeout += 0.1
                    if timeout >= 10:
                        print('Failed to end fray_handler1 thread: request time out')
                        return
                self.fray_handler1.join()
            if self.fray_handler2 is not None and self.fray_handler2.is_alive():  # check if collection thread exists and is alive
                while not self._handler1_stopped:
                    time.sleep(0.1)
                    timeout += 0.1
                    if timeout >= 10:
                        print('Failed to end fray_handler2 thread: request time out')
                        return
                self.fray_handler2.join()
        self.running = False
        self.waiting = True

    def reset(self):
        self.end()
        while self._idle_workers:
            [self._kill_worker(worker=worker) for worker in self._idle_workers]
        while self._active_workers:
            [self._kill_worker(worker=worker) for worker in self._active_workers]
        self._total_workers = len(self._idle_workers) + len(self._active_workers)
        self._jobs_pending = []
        self._jobs_running = []
        self._jobs_complete = []
        self.results = []
        self._job_order = []
        self._handler_stopped = False
        self.stop = False
        self.waiting = True
        self.running = False
        self.working = False
        self.idle = False
        self.done = False

    def _spawn_worker(self):
        if self._total_workers < self._max_workers:
            self._idle_workers.append(Worker.remote(self.func))
            self._total_workers = len(self._idle_workers) + len(self._active_workers)
            self._worker_stats[str(self._idle_workers[-1])] = 0

    def _kill_worker(self, worker):
        if worker in self._idle_workers:
            self._idle_workers.remove(worker)
            #idx = self._idle_workers.index(worker)
            #ray.kill(worker)
        if worker in self._active_workers:
            self._active_workers.remove(worker)
            #worker._exit.remote()  # doesn't appear to work
            #worker.__ray_terminate__.remote()  # doesn't appear to work
        if str(worker) in self._worker_stats:
            self._worker_stats.pop(str(worker))
            
        self._total_workers = len(self._idle_workers) + len(self._active_workers)

    def deploy(self, verbose=False):
        self._fray_handler1 = threading.Thread(target=self._handler1, args=(verbose,), name='fray_handler1', daemon=True)
        #self._fray_handler2 = threading.Thread(target=self._handler2, args=(verbose,), name='fray_handler2', daemon=True)
        self._fray_handler1.start()
        #self._fray_handler2.start()
        self.waiting = False
        self.running = True

    def set_func(self, new_func):
        if not self._active_workers:
            self.func = new_func
            [worker.set_func.remote(new_func) for worker in self._idle_workers]
        else:
            # raise exception for workers not being able to change functions because they are active?
            pass

    def submit_blind(self, inputs):
        """Submit a series of worker jobs without IDs"""
        # create new IDs for each input
        id = []  # unique method for assigning IDs
        for i in range(len(inputs)):
            unq = 1
            while unq in self._job_order or unq in id:
                unq += 1
            id.append(unq)

        # submit the jobs with their new IDs
        self.submit(ids, inputs)   

    def submit(self, ids, inputs):
        """Request a series of worker jobs to be run
        
        PARAMS
        ------
        id : string or int
            unique identifier for reconciling data after running
        input : list of dicts
            keys of each dict describe the args for the current function to be run
        """
        self._job_order.extend(list(ids)) # save the order of submission for sorting later

        assert len(ids) == len(inputs)  # TODO: turn this into a raised exception

        # method 1:  append all jobs to pending queue immediately
        #[self._jobs_pending.append({'id': ids[x], 'dict_in': inputs[x]}) for x in range(len(ids))]
        [self._jobs_pending.append({'id': id, 'dict_in': input}) for id, input in zip(ids, inputs)]

    def run(self, ids, inputs, func=None, sort=True, verbose=False):
        """Run a full job using ray parallelism with actors (deploy, submit, get results, reset)"""
        start = time.time()  # record starting time
        
        # determine which function to run for each input
        if func:
            self.set_func(func)  # change the default function to the inputted function
        else:
            func = self.func  # use the current default function
                
        # deploy the workers, submit the jobs, and record the job submission order
        #self.deploy()
        self.submit(ids, inputs)
        #job_order = self._job_order[:]  # copy job order because it will be cleared as results are gathered

        # method 1: fetch results as they come in
        #results = []
        #if verbose: print('Fetching Results...')
        #while self._jobs_pending or self._jobs_running or self._jobs_complete:
        #    if self.results:  # get results eagerly
        #        new_results = self.get_results(sort=False, clear=True)
        #        if verbose: print(f'Adding result {len(results)+1} out of {len(id)}...')
        #        results.extend(new_results)
        ## put the results back in the order they were submitted in
        #if sort:
        #    res_ids = [x['id'] for x in results]
        #    results = [results[res_ids.index(id)] for id in _job_order if id in res_ids]  # sort results into order
        

        # method 2: fetch all results when they are completed
        #if verbose: print('Fetching Results...')
        #while self._jobs_pending or self._jobs_running or self._jobs_complete:
        #    if not self._jobs_pending and not self._jobs_running and self.results:  # get results when finished
        #        results = self.get_results(sort=sort, clear=True)
        #        if verbose: print(f'Adding {len(results)} out of {len(id)} results...')
        
        # method 3: fetch all results when they are completed using the manage method
        while self._jobs_pending or self._jobs_running:
            self._manage(verbose=verbose)
        results = self.get_results(sort=sort, clear=True)
        if verbose: print(f'Added {len(results)} out of {len(ids)} results...')

        # turn the results into a dataframe and reset the leader/workers
        res_df = pd.DataFrame(results)
        self.reset()

        # print the total processing time
        if verbose: print(f'Processing time: {time.time() - start}s')
        return res_df

    def _manage(self, verbose=False):
        # delegation condition (prioritized over collection)
        if self._jobs_pending and (self._idle_workers or self._total_workers < self._max_workers):
            # assign a job to an actor
            if not self._idle_workers:
                self._spawn_worker()
            actor = self._idle_workers.pop()
            self._active_workers.append(actor)
            self._worker_stats[str(actor)] += 1
            job = self._jobs_pending.pop(0)
            self._jobs_running.append((job['id'],actor,actor.submit.remote(**job)))  # append to running (id,actor,future)

        # collection condition
        elif self._jobs_running:
            # check for the first result that is ready
            [future], _ = ray.wait([x[2] for x in self._jobs_running], num_returns=1)  # TODO: add timeout=timeout to func call for control

            # move job from running (id,actor,future) to complete (id)
            idx = next((x for x,y in enumerate(self._jobs_running) if y[2] == future), None)  # retrieve index of finished job from _jobs_running
            id, actor, _ = self._jobs_running.pop(idx)  # remove the job from _jobs_running
            self._jobs_complete.append(id)

            # return worker to idle or kill it
            # method 1: kill actors if they have reached their task limit
            if self._worker_stats[str(actor)] < self._worker_task_limit and self._jobs_pending:
                self._idle_workers.append(self._active_workers.pop(self._active_workers.index(actor)))  # move actor from active to idle
            else:
                self._kill_worker(actor)  # kill the actor if needed instead of adding it back to idle

            # method 2: kill actors if too much ram is being used
            #if self.ram_usage < 80 and self._jobs_pending:  # start killing actors only if ram usage goes over this percentage
            #    self._idle_workers.append(self._active_workers.pop(self._active_workers.index(actor)))  # move actor from active to idle
            #else:
            #    self._kill_worker(actor)  # kill the actor if needed instead of adding it back to idle

            # convert future, append result, and remove future
            result = (ray.get(future))
            self.results.append(copy.deepcopy(result))
            if verbose: print(f'Adding result {len(self.results)}')
            #del result
            #ray.internal.internal_api.free([future])

        # set flags on state changes
        self._set_status()

    def _handler1(self, verbose=False):
        while not self.stop:
            self._manage(verbose=verbose)
        self._handler1_stopped = True

    def _handler2(self, verbose=False):
        while not self.stop:
            self._manage(verbose=verbose)
        self._handler2_stopped = True

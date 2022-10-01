"""
Work group generator for ray
    Master spawns a pool of actors which are inteface with using ray.util.ActorPool.
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
import time, gc, copy, psutil
import threading, asyncio
import numpy as np
import pandas as pd
import ray


# Classes
@ray.remote(memory=1.0*(1024**3))#, object_store_memory=1*(1024**3))#num_cpus=1)  # a dedicated cpu is probably too much
class Worker:
    def __init__(self, name, func):
        self.working = False  # worker is processing a job
        self.done = False  # worker has completed a job and is waiting to be collected
        self.idle = True  # worker is waiting for a job
        self.name = name
        self.func = func
        self.job_id = None
        self.result = None

    def get_result(self):
        return self.result

    def get_jobid(self):
        return self.job_id

    def get_status(self):
        if self.working:
            return 'working'
        elif self.done:
            return 'done'
        elif self.idle:
            return 'idle'

    def clear(self):
        self.result = None
        self.job_id = None
        self.done = False
        self.idle = True

    def set_func(self, new_func):
        self.func = new_func

    def submit(self, id, dict_in):
        self.idle = False
        self.working = True
        self.job_id = id

        result = self.func(**dict_in)

        #self.result = result
        self.working = False
        self.done = True
        return {'id': id, 'result': result}


class Master:
    def __init__(self, func, max_workers=4):
        # worker statuses and info
        self.func = func
        self.max_workers = max_workers
        self.active_workers = 0
        self.pool = None
        self.workers = []
        self.worker_names = []
        self.working = False  # workers are working or there may be uncollected results
        self.idle = False  # leader is deployed but no workers are working and results are empty
        self.done = False  # work is complete and results are available

        # job statuses and info
        self.jobs_pending = []
        self.jobs_running = []
        self.jobs_complete = []
        self.results = []
        self.job_order = []

        # manager statuses and info
        self.stop = False  # set this flag to stop work after the current cycle
        self.fray_handler1 = None  # delegation thread if needed
        self.fray_handler2 = None  # collection thread if needed
        self._handler1_stopped = False  # this flag confirms that the delegation handler stopped successfully
        self._handler2_stopped = False  # this flag confirms that the collection handler stopped successfully
        self.waiting = True  # leader is stopped and not deployed
        self.submitting = False  # leader is ingesting new jobs to be delegated
        self.running = False # leader is deployed and running
        self.status = self.get_status()

    def get_props(self):
        return self.__dict__

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
        if self.jobs_running:
            status['job'] = 'running'
        elif self.jobs_pending:
            status['job'] = 'pending'
        elif self.jobs_complete:
            status['job'] = 'complete'
        else:
            status['job'] = 'waiting'

        self.status = status
        return status

    def _sort_results(self):
        # method 1: manually sort
        #res_ids = [x['id'] for x in self.results]
        #results = [self.results[res_ids.index(id)] for id in self.job_order if id in res_ids]
        #self.results = results

        # method 2: use list.sort()
        sortfunc = lambda x: self.job_order.index(x['id'])
        self.results.sort(key=sortfunc) 

    def get_results(self, sort=True, clear=False):
        if sort:
            self._sort_results()

        results = self.results[:]  # make a copy of the list (does not copy deep)
        #results = [x for x in self.results]  # may need this to iterate results

        if clear:
            for x in results:
                self.results.remove(x)  # remove from class attribute
                self.job_order.remove(x['id'])  # remove from job_order
                for y in self.jobs_complete:  # remove from jobs_complete
                    if y['id'] == x['id']:
                        self.jobs_complete.remove(y)
            
            #[self.results.remove(x) for x in results]  # remove from class attribute
            #[self.jobs_complete.remove(y) for y in self.jobs_complete for x in results if y['id'] == x['id']]  # remove from jobs_complete
            #[self.job_order.remove(x['id']) for x in results]  # remove from job_order
        
            if not self.results and not self.jobs_complete:
                self.done = False
                self.idle = True
        return results

        

    def get_results2(self, sort=True, clear=False):
        results = copy.deepcopy(self.results)
        res_ids = [x['id'] for x in results]
        if sort:
            results = [results[res_ids.index(id)] for id in self.job_order if id in res_ids]
        
        if clear:
            for id in res_ids:
                # clear the jobs_complete
                comp_ids = [x['id'] for x in self.jobs_complete]
                if id in comp_ids:
                    idx = comp_ids.index(id)
                    self.jobs_complete.pop(idx)

                # clear the self.results
                slfres_ids = [x['id'] for x in self.results]
                if id in slfres_ids:
                    idx = slfres_ids.index(id)
                    self.results.pop(idx)

                # clear the job order
                if id in self.job_order:
                    self.job_order.remove(id)

            if not self.results and not self.jobs_complete:
                self.done = False
                self.idle = True
        return results

    def clear(self):
        self.jobs_complete = []
        self.results = []
        self.job_order = []
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
        if self.pool is not None:
            while self.pool.has_free() and self.workers:
                worker = self.pool.pop_idle()
                self._kill_worker(worker=worker, force=True)
            self.pool = None
        #while self.workers:
        #    [self._kill_worker(worker=worker, force=True) for worker in self.workers]
        self.jobs_pending = []
        self.jobs_running = []
        self.jobs_complete = []
        self.results = []
        self.job_order = []
        self._handler_stopped = False
        self.stop = False
        self.waiting = True
        self.submitting = False
        self.running = False
        self.working = False
        self.idle = False
        self.done = False

    def _new_name(self):
        if self.active_workers < self.max_workers:
            for i in range(self.max_workers):
                name = 'worker'+str(i)
                if name not in self.worker_names:
                    break
        else:
            name = None
        return name

    def _spawn_worker(self):
        if self.active_workers < self.max_workers:
            name = self._new_name()
            self.workers.append(Worker.options(name=name).remote(name, self.func))
            self.worker_names.append(name)
            self.active_workers = len(self.workers)
            if self.pool is None:
                self.pool = ray.util.ActorPool(self.workers)
            else:
                self.pool.push(self.workers[-1])

            return self.workers[-1]
        else:
            # could include a warning that max was reached here
            return None

    def _kill_worker(self, worker=None, name=None, force=True):
        if worker is not None and name is None:
            idx = self.workers.index(worker)
            ray.kill(worker)
        elif worker is None and name is not None:
            idx = self.worker_names.index(name)
            ray.kill(ray.get_actor(name))
            #ray.get_actor(name)._exit.remote()  # doesn't appear to work
            #ray.get_actor(name).__ray_terminate__.remote()  # doesn't appear to work
            
        self.workers.pop(idx)
        self.worker_names.pop(idx)
        self.active_workers = len(self.workers)

    def deploy(self, verbose=False):
        #[self._spawn_worker() for _ in range(self.max_workers)]
        #self.pool = ray.util.ActorPool(self.workers)
        self.fray_handler1 = threading.Thread(target=self._handler1, args=(verbose,), name='fray_handler1', daemon=True)
        self.fray_handler2 = threading.Thread(target=self._handler2, args=(verbose,), name='fray_handler2', daemon=True)
        self.fray_handler1.start()
        self.fray_handler2.start()
        self.waiting = False
        self.running = True

    def set_func(self, new_func):
        self.func = new_func
        [worker.set_func.remote(new_func) for worker in self.workers]

    def _job_approve(self, input):
        # unused function
        # to be used for checking that the inputs have correct type and shape
        # before a job is submitted
        pass 

    def job_submit_blind(self, input):
        """Submit a series of worker jobs without IDs"""
        id = []  # unique method for assigning IDs
        for i in range(len(input)):
            unq = 1
            while unq in self.job_order or unq in id:
                unq += 1
            id.append(unq)

        #id = [x for x in range(len(input))]  # non-unique method for assigning IDs
        self.job_submit(id, input)   

    def job_submit(self, id, input):
        """Request a series of worker jobs to be run
        
        PARAMS
        ------
        id : string or int
            unique identifier for reconciling data after running
        input : list of dicts
            keys of each dict describe the args for the current function to be run
        """
        self.submitting = True
        self.job_order.extend(list(id)) # save the order of submission for sorting later

        assert len(id) == len(input)

        # method 1:  check, approve, and append jobs to queue as they are recognized
        [self.jobs_pending.append({'id': id[x], 'dict_in': input[x]}) for x in range(len(id))]

        # method 2: check and approve all submitted jobs and then add all to the queue together
        #new_jobs = [{'id': id[x], 'input': input[x]} for x in range(len(id))]
        #self.jobs_pending.extend(new_jobs)

        self.submitting = False

    def run(self, id, input, func=None, sort=True, verbose=False):
        """Run a full job using ray parallelism with actors (deploy, submit, get results, reset)"""
        start = time.time()  # record starting time
        
        # determine which function to run for each input
        if func is None:
            func = self.func
        else:
            self.set_func(func)
        
        # deploy the workers, submit the jobs, and record the job submission order
        self.deploy()
        self.job_submit(id, input)
        job_order = self.job_order[:]  # copy job order because it will be cleared as results are gathered

        # wait for jobs to start running
        #while not self.jobs_running:
        #    time.sleep(0.01)

        # method 1: fetch results as they come in
        results = []
        if verbose: print('Fetching Results...')
        while self.jobs_pending or self.jobs_running or self.jobs_complete:
            if self.results:  # get results eagerly
                new_results = self.get_results(sort=False, clear=True)
                if verbose: print(f'Adding result {len(results)+1} out of {len(id)}...')
                results.extend(new_results)
        # put the results back in the order they were submitted in
        if sort:
            res_ids = [x['id'] for x in results]
            results = [results[res_ids.index(id)] for id in job_order if id in res_ids]  # sort results into order
        

        # method 2: fetch all results when they are completed
        #if verbose: print('Fetching Results...')
        #while self.jobs_pending or self.jobs_running or self.jobs_complete:
        #    if not self.jobs_pending and not self.jobs_running and self.results:  # get results when finished
        #        results = self.get_results(sort=sort, clear=True)
        #        if verbose: print(f'Adding {len(results)} out of {len(id)} results...')
        
        # turn the results into a dataframe and reset the leader/workers
        res_df = pd.DataFrame(results)
        self.reset()

        # print the total processing time
        if verbose: print(f'Processing time: {time.time() - start}s')
        return res_df

    def _job_delegate(self, verbose=False):
        if self.jobs_pending:# and self.active_workers < self.max_workers:
            self.idle = False
            self.working = True

            # method 1: submit, and change status of job one by one
            while self.jobs_pending:# and self.active_workers < self.max_workers:
                self._spawn_worker()

                while not self.pool.has_free():
                    pass

                self.pool.submit(lambda actor, _kwargs: actor.submit.remote(**_kwargs), self.jobs_pending[0])
                self.jobs_running.append(self.jobs_pending[0])
                self.jobs_pending.pop(0)
                if self.stop:
                    return

            # method 2: wait for jobs_pending to populate, map chunks of jobs to pool, then change chunk statuses
            # - waits for full chunks to process before new jobs are assigned
            #if not self.submitting:
            #    results = (self.pool.map_unordered(lambda actor, _kwargs: actor.submit.remote(**_kwargs), self.jobs_pending[:self.max_workers]))
            #    [self.results.append(x) for x in results]
            #    #self.jobs_running.extend(self.jobs_pending)
            #    self.jobs_complete.extend(self.jobs_pending[:self.max_workers])
            #    del self.jobs_pending[:self.max_workers]

            # method 3: submit all jobs, then change all statuses (less safe maybe?)
            #[self.pool.submit(lambda actor, args: actor.submit.remote(**args), job) for job in self.jobs_pending if not self.stop]
            #self.jobs_running.extend(self.jobs_pending)
            #self.jobs_pending.clear()

    def _job_collect(self, verbose=False):
        # return an available result from the actor pool
        if self.jobs_running and self.pool.has_next():
            res = self.pool.get_next_unordered()
            res['result'] = np.array(res['result'], copy=True)
            ray.internal.internal_api.free([res['result']])
            self.results.append(res)
            # self.results.append(self.pool.get_next_unordered())  # add the next available result to self.results
            id = res['id']  # get id of the last result
            idx = [job['id'] for job in self.jobs_running].index(id)  # get index of current id in jobs_running
            self.jobs_complete.append(self.jobs_running.pop(idx))  # move from running to complete

        # kill an idle worker to free/reset its memory
        #if self.pool is not None and self.pool.has_free():
        #    worker = self.pool.pop_idle()
        #    self._kill_worker(worker=worker)
        
        # kill the function if a stop signal is sent (unneeded if not returning results in a while loop)
        #if self.stop:
        #    return
        
        # run garbage collection (does not appear to help)
        # https://stackoverflow.com/questions/55749394/how-to-fix-the-constantly-growing-memory-usage-of-ray
        #self.auto_garbage_collect(mem_limit_gb=8.0)

        # set flags on state changes
        if not self.jobs_pending and not self.jobs_running:
            self.working = False
        if self.jobs_complete and not self.pool.has_next():
            self.done = True

    def _handler1(self, verbose=False):
        while not self.stop:
            self._job_delegate(verbose=verbose)
        self._handler1_stopped = True

    def _handler2(self, verbose=False):
        while not self.stop:
            self._job_collect(verbose=verbose)
        self._handler2_stopped = True

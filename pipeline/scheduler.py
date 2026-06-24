import multiprocessing
from pipeline.task import Task, TaskStatus
from pipeline.partition import Partition
from pipeline.worker import execute_task


class Scheduler:
    """
    Manages a pool of worker processes and distributes
    tasks across them.

    This is the equivalent of Spark's TaskScheduler —
    it takes a list of tasks, assigns each one to a worker,
    collects the results, and updates each task's status.

    The key difference from doing work sequentially:
    all tasks in a stage are submitted to the pool at once
    and run in parallel across multiple CPU cores.

    Attributes:
        num_workers : how many parallel worker processes to spin up
        event_log   : a record of every dispatch and completion
        _pool       : the underlying multiprocessing.Pool
        _pids       : process IDs of the workers (for logging/UI)
    """

    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
        self.event_log:  list = []
        self._pool:      multiprocessing.Pool = None
        self._pids:      list = []

    def start(self):
        """
        Spin up the worker pool.

        This is where Python actually forks N subprocesses.
        Each subprocess is idle, waiting for tasks to arrive.
        Equivalent to Spark launching executor JVMs on worker nodes.
        """
        self._pool = multiprocessing.Pool(processes=self.num_workers)

    def stop(self):
        """
        Cleanly shut down the worker pool.

        close() tells workers to finish current tasks then stop.
        join() blocks until all workers have exited.

        Always call stop() after you are done — otherwise
        worker processes keep running in the background.
        """
        if self._pool:
            self._pool.close()
            self._pool.join()

    def submit_tasks(self, tasks: list[Task]) -> list[Task]:
        """
        Send all tasks to the worker pool and collect results.

        Steps:
            1. Build an args tuple for each task
            2. Assign workers round-robin (task 0 -> worker 0,
               task 1 -> worker 1, task 4 -> worker 0 again...)
            3. Submit everything to the pool at once via pool.map()
            4. pool.map() blocks until ALL tasks are done
            5. Update each Task object with the result

        Args:
            tasks : list of Task objects ready to execute

        Returns:
            the same list of Tasks, now with results filled in
        """
        # step 1 — build args for each task
        args_list = []

        for i, task in enumerate(tasks):
            worker_id      = i % self.num_workers
            task.worker_id = worker_id
            task.status    = TaskStatus.RUNNING

            args_list.append((
                task.task_id,
                task.node.op_type.value,
                task.node.func_blob,
                task.partition.data,
                task.node.metadata,
                worker_id,
            ))

            self.event_log.append({
                "event":     "dispatch",
                "task_id":   task.task_id,
                "worker_id": worker_id,
                "op":        task.node.op_type.value,
                "partition": task.partition.partition_id,
            })

        # step 2 — submit all tasks to the pool at once
        # pool.map() distributes args_list across workers in parallel
        # it blocks here until every task has returned a result
        results = self._pool.map(execute_task, args_list)

        # step 3 — map results back onto Task objects
        results_by_id = {r["task_id"]: r for r in results}
        pids_seen     = set()

        for task in tasks:
            res = results_by_id[task.task_id]

            task.status     = TaskStatus(res["status"])
            task.result     = Partition(task.partition.partition_id, res["data"])
            task.error      = res["error"]
            task.start_time = res["start"]
            task.end_time   = res["end"]

            pids_seen.add(res["pid"])

            self.event_log.append({
                "event":       "complete",
                "task_id":     task.task_id,
                "worker_id":   task.worker_id,
                "status":      task.status.value,
                "duration_ms": task.duration_ms,
                "pid":         res["pid"],
            })

        self._pids = sorted(pids_seen)
        return tasks
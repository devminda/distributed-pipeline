# pipeline/executor.py

import time
from pipeline.task import Task, TaskStatus
from pipeline.partition import Partition
from pipeline.plan import PlanNode
from pipeline.scheduler import Scheduler


class Executor:
    """
    Walks the DAG stage by stage, creating Tasks and
    handing them to the Scheduler for parallel execution.

    This is the equivalent of Spark's DAGScheduler —
    it understands the shape of the execution plan and
    knows how to break it into stages of parallel tasks.

    The key insight:
        - Within a stage, all tasks run in PARALLEL
        - Between stages, we wait for ALL tasks to finish
          before moving to the next stage
        - The output partitions of stage N become the
          input partitions of stage N+1

    Attributes:
        scheduler  : the Scheduler that manages the worker pool
        stage_log  : timing and row counts for each stage
    """

    def __init__(self, scheduler: Scheduler):
        self.scheduler  = scheduler
        self.stage_log: list = []

    def execute(self, dag: list[PlanNode], partitions: list[Partition]) -> list[Partition]:
        """
        Execute every stage in the DAG against the data.

        For each node in the DAG:
            1. Create one Task per partition
            2. Submit all tasks to the Scheduler (runs in parallel)
            3. Collect results — each task's output becomes
               the input partition for the next stage
            4. If any task failed, abort the whole job

        Args:
            dag        : ordered list of PlanNode (from build_dag)
            partitions : the initial data split into Partition objects

        Returns:
            list of output Partitions after all stages complete
        """
        current_partitions = list(partitions)

        for stage_num, node in enumerate(dag):
            stage_start = time.time()

            # create one task per partition for this stage
            tasks = [
                Task(node=node, partition=p)
                for p in current_partitions
            ]

            rows_in = sum(len(p) for p in current_partitions)

            # hand all tasks to the scheduler — runs in parallel
            completed_tasks = self.scheduler.submit_tasks(tasks)

            # check for failures — abort if any task failed
            failed = [t for t in completed_tasks if t.failed]
            if failed:
                errors = "\n".join(t.error for t in failed)
                raise RuntimeError(
                    f"Stage {stage_num} ({node.op_type.value}) failed:\n{errors}"
                )

            # output partitions of this stage become input of next
            current_partitions = [t.result for t in completed_tasks]

            rows_out = sum(len(p) for p in current_partitions)

            self.stage_log.append({
                "stage":       stage_num,
                "op":          node.op_type.value,
                "tasks":       len(tasks),
                "rows_in":     rows_in,
                "rows_out":    rows_out,
                "duration_ms": round((time.time() - stage_start) * 1000, 2),
            })

        return current_partitions
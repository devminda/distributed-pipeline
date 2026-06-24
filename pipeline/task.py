# pipeline/task.py

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import uuid

from pipeline.partition import Partition
from pipeline.plan import PlanNode


class TaskStatus(Enum):
    """
    The lifecycle of a single task.

    Every task starts PENDING, moves to RUNNING when a worker
    picks it up, then lands on DONE or FAILED.

    This mirrors Spark's internal task state machine:
    PENDING -> RUNNING -> SUCCEEDED / FAILED
    """
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE    = "DONE"
    FAILED  = "FAILED"


@dataclass
class Task:
    """
    The atomic unit of work in the pipeline.

    One Task = one operation applied to one partition.
    This is exactly how Spark thinks about work — if you have
    4 partitions and 3 stages, Spark creates 12 tasks total.

    Attributes:
        task_id    : unique identifier for this task
        node       : the DAG node (which operation to run)
        partition  : the data slice this task operates on
        status     : current lifecycle state
        result     : the output Partition after execution
        error      : error message if the task failed
        worker_id  : which worker was assigned this task
        start_time : unix timestamp when execution began
        end_time   : unix timestamp when execution finished
    """
    task_id:    str                  = field(default_factory=lambda: str(uuid.uuid4())[:8])
    node:       Optional[PlanNode]   = None
    partition:  Optional[Partition]  = None
    status:     TaskStatus           = TaskStatus.PENDING
    result:     Optional[Partition]  = None
    error:      str                  = ""
    worker_id:  int                  = -1
    start_time: float                = 0.0
    end_time:   float                = 0.0

    @property
    def duration_ms(self) -> float:
        """
        How long this task took in milliseconds.

        Using @property means you call it like an attribute:
            task.duration_ms
        not like a method:
            task.duration_ms()

        This is cleaner for a value that feels like data,
        not an action.
        """
        if self.end_time and self.start_time:
            return round((self.end_time - self.start_time) * 1000, 2)
        return 0.0

    @property
    def succeeded(self) -> bool:
        return self.status == TaskStatus.DONE

    @property
    def failed(self) -> bool:
        return self.status == TaskStatus.FAILED

    def __repr__(self):
        op   = self.node.op_type.value if self.node else "?"
        part = self.partition.partition_id if self.partition else "?"
        return f"Task(id={self.task_id}, op={op}, partition={part}, status={self.status.value})"
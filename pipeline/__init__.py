"""
Distributed Data Pipeline Simulator
=====================================
A mini-Spark built from scratch in Python.

Demonstrates:
    - Lazy DAG-based execution planning
    - Horizontal data partitioning
    - Parallel execution via multiprocessing.Pool
    - Lambda serialization with dill (like Spark's cloudpickle)
    - Decorator and context manager patterns

Basic usage:
    from pipeline import DataSet

    data   = [{"name": "Alice", "age": 35}, ...]
    result = (
        DataSet(data, num_partitions=4)
        .filter(lambda r: r["age"] > 30)
        .map(lambda r: {**r, "senior": True})
        .collect()
    )

With context manager (recommended for multiple pipelines):
    from pipeline import DataSet, pipeline_context

    with pipeline_context(num_workers=4) as ctx:
        result = DataSet(data, context=ctx).filter(...).collect()
"""

from pipeline.dataset   import DataSet
from pipeline.utils     import traced, pipeline_context
from pipeline.plan      import OpType
from pipeline.partition import Partition, partition_data
from pipeline.task      import Task, TaskStatus

__all__ = [
    "DataSet",
    "pipeline_context",
    "traced",
    "OpType",
    "Partition",
    "partition_data",
    "Task",
    "TaskStatus",
]

__version__ = "1.0.0"
__author__  = "Devminda Abeynayake"
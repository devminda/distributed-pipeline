import time
from typing import Callable

from pipeline.partition import Partition, partition_data
from pipeline.plan import OpType, build_dag, dag_to_dict
from pipeline.scheduler import Scheduler
from pipeline.executor import Executor


class DataSet:
    """
    The public API of the pipeline engine.

    This is what the user writes against — a clean, chainable
    interface that hides all the complexity of partitioning,
    DAG planning, scheduling, and parallel execution.

    This mirrors Spark's DataFrame API:
        spark.read(...).filter(...).groupBy(...).agg(...).show()

    Ours looks like:
        DataSet(data).filter(...).groupby(...).reduce(...).collect()

    Key design: LAZY EVALUATION
        Calling .filter(), .map(), .groupby() does NOT run anything.
        Each call simply records the operation into self._ops.
        Nothing executes until .collect() is called.

    Attributes:
        _data           : the raw input data as a flat list
        _num_partitions : how many parallel slices to split into
        _ops            : the recorded chain of operations
        _context        : optional external Scheduler context
    """

    def __init__(self, data: list, num_partitions: int = 4, context: dict = None):
        self._data           = data
        self._num_partitions = num_partitions
        self._context        = context
        self._ops:  list     = [(OpType.READ, None, {})]

    # ── transformations ────────────────────────────────────────────
    # Each method records the operation and returns a NEW DataSet.
    # Returning self would mutate the original — instead we clone
    # so that you can branch pipelines:
    #
    #   base    = DataSet(data).filter(...)
    #   branch1 = base.map(func_a).collect()
    #   branch2 = base.map(func_b).collect()
    #
    # base is unchanged. Both branches start from the same filter.

    def filter(self, func: Callable) -> "DataSet":
        """
        Keep only rows where func(row) returns True.

        Spark equivalent: df.filter(col("age") > 30)
        """
        return self._clone(OpType.FILTER, func)

    def map(self, func: Callable) -> "DataSet":
        """
        Transform every row through func.

        Spark equivalent: df.select(...) or df.withColumn(...)
        """
        return self._clone(OpType.MAP, func)

    def groupby(self, key_func: Callable) -> "DataSet":
        """
        Group rows by the value returned by key_func(row).
        Output shape becomes [(key, [rows]), ...].

        Spark equivalent: df.groupBy("dept")
        """
        return self._clone(OpType.GROUPBY, key_func)

    def reduce(self, agg_func: Callable) -> "DataSet":
        """
        Aggregate each group produced by groupby().
        agg_func receives the list of rows for one key.

        Must follow groupby() — just like Spark's
        .groupBy() must be followed by .agg()

        Spark equivalent: .agg(avg("salary"))
        """
        return self._clone(OpType.REDUCE, agg_func)

    def sort(self, key_func: Callable, descending: bool = False) -> "DataSet":
        """
        Sort rows by the value returned by key_func(row).

        Spark equivalent: df.orderBy(col("salary").desc())
        """
        return self._clone(OpType.SORT, key_func, {"descending": descending})

    def limit(self, n: int) -> "DataSet":
        """
        Keep only the first n rows per partition.

        Spark equivalent: df.limit(10)
        Note: like Spark, this is per-partition not global.
        """
        return self._clone(OpType.LIMIT, None, {"n": n})

    # ── terminal action ────────────────────────────────────────────

    def collect(self) -> dict:
        """
        Trigger execution of the entire pipeline.

        This is the only method that actually runs anything.
        Everything before this was just recording a plan.

        Steps:
            1. Build the DAG from recorded ops
            2. Split data into partitions
            3. Start a Scheduler (or use injected one)
            4. Hand DAG + partitions to the Executor
            5. Flatten output partitions into a single list
            6. Return data + metrics + dag + event log

        Spark equivalent: .collect(), .show(), .write()
        Any of these trigger execution in Spark.

        Returns:
            dict with keys:
                data      : the output rows as a flat list
                dag       : the execution plan as a list of dicts
                metrics   : timing, row counts, stage breakdown
                event_log : every worker dispatch and completion
        """
        total_start = time.time()

        # step 1 — build the execution DAG
        dag = build_dag(self._ops)

        # step 2 — split data into partitions
        partitions = partition_data(self._data, self._num_partitions)

        # step 3 — set up the scheduler
        # if a context was injected (pipeline_context()) use that
        # otherwise create and manage our own
        if self._context:
            scheduler = self._context["scheduler"]
            own_scheduler = False
        else:
            scheduler = Scheduler(self._num_partitions)
            scheduler.start()
            own_scheduler = True

        # step 4 — execute all stages via the Executor
        executor = Executor(scheduler)

        try:
            result_partitions = executor.execute(dag, partitions)
        finally:
            # always stop the scheduler if we created it
            # finally ensures this runs even if an exception is raised
            if own_scheduler:
                scheduler.stop()

        # step 5 — flatten output partitions into one list
        merged = []
        for p in result_partitions:
            merged.extend(p.data)

        # step 6 — package everything up
        metrics = {
            "total_ms":        round((time.time() - total_start) * 1000, 2),
            "num_partitions":  self._num_partitions,
            "num_stages":      len(dag),
            "rows_in":         len(self._data),
            "rows_out":        len(merged),
            "worker_pids":     scheduler._pids,
            "stages":          executor.stage_log,
        }

        return {
            "data":      merged,
            "dag":       dag_to_dict(dag),
            "metrics":   metrics,
            "event_log": scheduler.event_log,
        }

    def explain(self) -> str:
        """
        Print the logical plan without running anything.

        Spark equivalent: df.explain()

        Useful for debugging — see what the engine will do
        before committing to running it.
        """
        lines = ["=== Logical Plan ==="]
        for op, func, meta in self._ops:
            name = getattr(func, "__name__", "") if func else ""
            meta_str = f" {meta}" if meta else ""
            lines.append(f"  {op.value}({name}){meta_str}")
        return "\n".join(lines)

    # ── internal ───────────────────────────────────────────────────

    def _clone(self, op_type: OpType, func, meta: dict = None) -> "DataSet":
        """
        Create a new DataSet with one additional operation recorded.

        We copy self._ops into the new instance so the original
        DataSet is never mutated. This is immutability — the same
        principle behind Spark's immutable RDDs.
        """
        new_ds               = DataSet.__new__(DataSet)
        new_ds._data         = self._data
        new_ds._num_partitions = self._num_partitions
        new_ds._context      = self._context
        new_ds._ops          = list(self._ops) + [(op_type, func, meta or {})]
        return new_ds
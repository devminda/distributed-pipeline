import time
import dill
import multiprocessing

from pipeline.plan import OpType


def _apply_op(op: OpType, func, data: list, meta: dict) -> list:
    """
    Apply a single operation to a list of records.

    This is a pure function — it takes data in, returns data out,
    touches nothing outside itself. No shared state, no side effects.

    This is exactly how Spark's executor applies transformations
    to each partition — one operation at a time, pure input/output.

    Args:
        op   : which operation to perform
        func : the deserialized callable (lambda or named function)
        data : the partition's rows as a plain list
        meta : extra config e.g. {"descending": True, "n": 10}

    Returns:
        transformed list of records
    """
    if op == OpType.READ:
        return data

    if op == OpType.FILTER:
        return [row for row in data if func(row)]

    if op == OpType.MAP:
        return [func(row) for row in data]

    if op == OpType.GROUPBY:
        groups = {}
        for row in data:
            key = func(row)
            groups.setdefault(key, []).append(row)
        return list(groups.items())   # [(key, [rows]), ...]

    if op == OpType.REDUCE:
        return [(key, func(rows)) for key, rows in data]

    if op == OpType.SORT:
        return sorted(data, key=func, reverse=meta.get("descending", False))

    if op == OpType.LIMIT:
        return data[:meta.get("n", len(data))]

    return data


def execute_task(args: tuple) -> dict:
    """
    Top-level function that runs inside a worker subprocess.

    This MUST be a top-level function (not a method, not a lambda,
    not nested inside another function). Python's multiprocessing
    can only pickle top-level functions to send them to subprocesses.

    It receives everything it needs as a single tuple argument
    because multiprocessing.Pool.map() only passes one argument.

    Args:
        args: a tuple of
            task_id   (str)   : which task this is
            op_val    (str)   : the OpType value e.g. "FILTER"
            func_blob (bytes) : dill-serialized callable
            data      (list)  : the partition's rows
            meta      (dict)  : extra config for the operation
            worker_id (int)   : which worker slot was assigned

    Returns:
        dict with task_id, status, data, error, timing, pid
    """
    task_id, op_val, func_blob, data, meta, worker_id = args

    start = time.time()
    pid   = multiprocessing.current_process().pid

    try:
        # deserialize the function from bytes back into a callable
        func   = dill.loads(func_blob) if func_blob else None
        op     = OpType(op_val)

        result = _apply_op(op, func, data, meta)

        return {
            "task_id":   task_id,
            "status":    "DONE",
            "data":      result,
            "error":     "",
            "start":     start,
            "end":       time.time(),
            "pid":       pid,
            "worker_id": worker_id,
        }

    except Exception as exc:
        return {
            "task_id":   task_id,
            "status":    "FAILED",
            "data":      [],
            "error":     str(exc),
            "start":     start,
            "end":       time.time(),
            "pid":       pid,
            "worker_id": worker_id,
        }   
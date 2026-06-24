# Distributed Data Pipeline Simulator

A mini-Spark built from scratch in Python.

Most engineers say "I know Spark." Very few understand
why distributed systems exist or what Spark is actually
doing under the hood. This project builds those internals
from scratch — real parallel workers, lazy DAG execution,
and a task scheduler — to demonstrate that understanding.

---

## What it does

Provides a chainable, lazy dataset API that looks like this:

```python
from pipeline import DataSet

result = (
    DataSet(data, num_partitions=4)
    .filter(lambda r: r["age"] > 30)
    .map(lambda r: {**r, "level": "senior" if r["salary"] > 120_000 else "mid"})
    .groupby(lambda r: r["dept"])
    .reduce(lambda rows: {"count": len(rows), "avg": sum(r["salary"] for r in rows) // len(rows)})
    .collect()
)
```

Calling `.filter()`, `.map()`, `.groupby()` records operations
but runs nothing. Only `.collect()` triggers execution — this
is lazy evaluation, the same model Spark uses.

---

## Architecture

User Query  (DataSet API)
↓
Logical Plan  (chain of ops recorded into self._ops)
↓
DAG  (PlanNode graph built by build_dag())
↓
Executor  (walks DAG stage by stage)
↓
Scheduler  (dispatches tasks to worker pool)
↓
Workers  (multiprocessing.Pool subprocesses)
↓
Collect  (merge partition results into flat list)

---

## Core concepts implemented

### Partition
Data is split horizontally into N equal slices.
Each worker operates on its own slice independently —
no shared memory, no locking.

200 rows / 4 partitions = 50 rows each
Partition 0: rows[0:50]    → Worker 0
Partition 1: rows[50:100]  → Worker 1
Partition 2: rows[100:150] → Worker 2
Partition 3: rows[150:200] → Worker 3

### Task
One Task = one operation × one partition.
A pipeline with 3 stages and 4 partitions creates 12 tasks.
This mirrors how Spark's TaskScheduler thinks about work.

### DAG Planning
Operations are recorded lazily into a logical plan.
On `.collect()`, the plan is compiled into a DAG of
`PlanNode` objects — each node stores the operation type
and the serialized callable.

### Lambda Serialization
Python's built-in `pickle` cannot serialize lambdas across
process boundaries. We use `dill` to serialize callables
to bytes before sending them to worker subprocesses —
exactly how PySpark uses `cloudpickle`.

```python
func_blob = dill.dumps(lambda r: r["age"] > 30)
# safely crosses the process boundary
func = dill.loads(func_blob)
```

### Scheduler
Manages a `multiprocessing.Pool` and dispatches tasks
round-robin across workers. All tasks in a stage run
in parallel. Execution blocks between stages until all
tasks in the current stage complete.

### Advanced Python patterns
- `@traced` decorator for automatic timing and logging
- `pipeline_context()` context manager for safe scheduler lifecycle
- Immutable `DataSet` — every transformation returns a new instance
- `@dataclass` for clean, self-documenting data structures
- `Enum` for type-safe operation definitions

---

## Project structure

distributed-pipeline/
│
├── pipeline/               the engine (importable package)
│   ├── init.py         public API
│   ├── partition.py        Partition + partition_data()
│   ├── plan.py             PlanNode, OpType, build_dag()
│   ├── task.py             Task, TaskStatus
│   ├── worker.py           execute_task() runs in subprocess
│   ├── scheduler.py        Scheduler — manages Pool
│   ├── executor.py         Executor — walks DAG
│   ├── dataset.py          DataSet — user facing API
│   └── utils.py            @traced, pipeline_context()
│
├── examples/
│   ├── data_gen.py         synthetic employee dataset
│   └── employees.py        four example pipelines
│
├── tests/
│   ├── test_partition.py
│   ├── test_plan.py
│   ├── test_dataset.py
│   └── test_scheduler.py
│
└── docs/
└── index.html          interactive portfolio UI

---

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/distributed-pipeline.git
cd distributed-pipeline
pip install -r requirements.txt
```

---

## Run the examples

```bash
# single pipeline
python -m examples.employees filter_map
python -m examples.employees groupby_reduce
python -m examples.employees sort_limit
python -m examples.employees full_pipeline

# all four
python -m examples.employees all
```

---

## Available pipelines

| Pipeline | Operations | Description |
|---|---|---|
| `filter_map` | FILTER → MAP | Employees 35+, salary in thousands |
| `groupby_reduce` | FILTER → GROUPBY → REDUCE | Avg salary per dept for high performers |
| `sort_limit` | SORT → LIMIT | Top 10 earners per partition |
| `full_pipeline` | FILTER → MAP → GROUPBY → REDUCE | Dept stats for NYC and SF employees |

---

## How it compares to Spark

| Concept | Apache Spark | This project |
|---|---|---|
| Lazy evaluation | DataFrame transformations | `.filter()`, `.map()` etc |
| DAG planning | Catalyst optimizer | `build_dag()` |
| Serialization | cloudpickle | dill |
| Parallelism | JVM executor threads | multiprocessing.Pool |
| Task scheduling | TaskScheduler | `Scheduler` class |
| Stage execution | DAGScheduler | `Executor` class |
| Partitioning | RDD partitions | `Partition` dataclass |

---

## Live demo

[View the interactive pipeline visualizer](https://devminda.github.io/distributed-pipeline)

---

## Author

Devminda Abeynayake — [github.com/devminda](https://github.com/devminda)
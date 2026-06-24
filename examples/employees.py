"""
Four example pipelines running against the synthetic employee dataset.

Run a specific pipeline:
    python examples/employees.py filter_map
    python examples/employees.py groupby_reduce
    python examples/employees.py sort_limit
    python examples/employees.py full_pipeline

Run all four:
    python examples/employees.py all
"""

import sys
import json
import multiprocessing

from pipeline import DataSet, pipeline_context
from examples.data_gen import make_dataset, print_sample


# ── Pipeline 1 ────────────────────────────────────────────────────

def run_filter_map(data: list) -> dict:
    """
    Filter employees aged 35 or over,
    then add a salary_k field (salary in thousands).

    Concepts demonstrated:
        - chaining two transformations
        - dict unpacking in a map (**r copies all existing fields)

    Spark equivalent:
        df.filter(col("age") >= 35)
          .withColumn("salary_k", col("salary") / 1000)
    """
    print("pipeline: filter -> map")
    print("intent:   employees aged 35+, salary in thousands")
    print()

    return (
        DataSet(data, num_partitions=4)
        .filter(lambda r: r["age"] >= 35)
        .map(lambda r: {**r, "salary_k": r["salary"] // 1000})
        .collect()
    )


# ── Pipeline 2 ────────────────────────────────────────────────────

def run_groupby_reduce(data: list) -> dict:
    """
    Keep only high performers (score >= 3.0),
    group by department, compute average salary per dept.

    Concepts demonstrated:
        - named function passed to reduce (not a lambda)
        - groupby always paired with reduce
        - output shape changes: rows become (key, value) pairs

    Spark equivalent:
        df.filter(col("score") >= 3.0)
          .groupBy("dept")
          .agg(avg("salary"))
    """
    print("pipeline: filter -> groupby -> reduce")
    print("intent:   avg salary per dept for high performers")
    print()

    def avg_salary(rows: list) -> dict:
        salaries = [r["salary"] for r in rows]
        return {
            "count":      len(rows),
            "avg_salary": round(sum(salaries) / len(salaries), 2),
            "max_salary": max(salaries),
        }

    return (
        DataSet(data, num_partitions=4)
        .filter(lambda r: r["score"] >= 3.0)
        .groupby(lambda r: r["dept"])
        .reduce(avg_salary)
        .collect()
    )


# ── Pipeline 3 ────────────────────────────────────────────────────

def run_sort_limit(data: list) -> dict:
    """
    Sort all employees by salary descending,
    keep the top 10 per partition.

    Concepts demonstrated:
        - sort with descending flag
        - limit is per-partition (same as Spark)
        - this is NOT a global top-10 across all data

    Spark equivalent:
        df.orderBy(col("salary").desc())
          .limit(10)

    Note: a true global top-N would need a second stage
    to merge and re-sort the per-partition results.
    """
    print("pipeline: sort -> limit")
    print("intent:   top 10 earners per partition")
    print()

    return (
        DataSet(data, num_partitions=4)
        .sort(lambda r: r["salary"], descending=True)
        .limit(10)
        .collect()
    )


# ── Pipeline 4 ────────────────────────────────────────────────────

def run_full_pipeline(data: list) -> dict:
    """
    Full analytics pipeline:
        filter by city -> enrich with level -> group by dept -> aggregate stats

    Concepts demonstrated:
        - all five operation types chained together
        - named functions for readability at each stage
        - context manager managing the scheduler lifecycle
        - reusing the same worker pool across the whole pipeline

    Spark equivalent:
        df.filter(col("city").isin("NYC", "SF"))
          .withColumn("level", when(col("salary") > 120000, "senior").otherwise("mid"))
          .groupBy("dept")
          .agg(count("*"), avg("salary"), max("salary"))
    """
    print("pipeline: filter -> map -> groupby -> reduce")
    print("intent:   dept stats for NYC and SF employees")
    print()

    def enrich(r: dict) -> dict:
        return {
            **r,
            "level": "senior" if r["salary"] > 120_000 else "mid"
        }

    def dept_stats(rows: list) -> dict:
        salaries = [r["salary"] for r in rows]
        levels   = list({r["level"] for r in rows})
        return {
            "count":      len(rows),
            "avg_salary": round(sum(salaries) / len(salaries), 0),
            "max_salary": max(salaries),
            "levels":     sorted(levels),
        }

    with pipeline_context(num_workers=4) as ctx:
        return (
            DataSet(data, num_partitions=4, context=ctx)
            .filter(lambda r: r["city"] in ("NYC", "SF"))
            .map(enrich)
            .groupby(lambda r: r["dept"])
            .reduce(dept_stats)
            .collect()
        )


# ── Output printer ─────────────────────────────────────────────────

def print_result(result: dict, pipeline_name: str) -> None:
    """Print results, metrics, and stage breakdown cleanly."""

    print(f"=== results: {pipeline_name} ===")
    print()

    data = result["data"]
    print(f"output ({len(data)} rows):")
    for row in data[:10]:
        print(" ", row)
    if len(data) > 10:
        print(f"  ... {len(data) - 10} more rows")
    print()

    m = result["metrics"]
    print("metrics:")
    print(f"  total time   : {m['total_ms']} ms")
    print(f"  rows in      : {m['rows_in']}")
    print(f"  rows out     : {m['rows_out']}")
    print(f"  partitions   : {m['num_partitions']}")
    print(f"  stages       : {m['num_stages']}")
    print(f"  worker pids  : {m['worker_pids']}")
    print()

    print("stage breakdown:")
    for s in m["stages"]:
        bar_width = int((s["duration_ms"] / m["total_ms"]) * 30)
        bar       = "#" * bar_width
        print(f"  stage {s['stage']} {s['op']:8} "
              f"{s['rows_in']:4} -> {s['rows_out']:4} rows  "
              f"{s['duration_ms']:6}ms  [{bar}]")
    print()

    print("dag:")
    for node in result["dag"]:
        indent = "  " * len(node["children"])
        print(f"  {indent}{node['op']}({node['name']})")
    print()


# ── Entry point ────────────────────────────────────────────────────

PIPELINES = {
    "filter_map":     run_filter_map,
    "groupby_reduce": run_groupby_reduce,
    "sort_limit":     run_sort_limit,
    "full_pipeline":  run_full_pipeline,
}


def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else "full_pipeline"

    data = make_dataset(n=200, seed=42)
    print(f"dataset: {len(data)} employee records")
    print()

    if arg == "all":
        for name, fn in PIPELINES.items():
            print("=" * 55)
            result = fn(data)
            print_result(result, name)
    else:
        fn = PIPELINES.get(arg)
        if not fn:
            print(f"unknown pipeline: {arg}")
            print(f"choices: {list(PIPELINES.keys())} or 'all'")
            sys.exit(1)
        result = fn(data)
        print_result(result, arg)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
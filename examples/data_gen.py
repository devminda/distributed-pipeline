"""
Synthetic employee dataset generator.

Kept separate from the pipeline examples so the data
generation logic is reusable across multiple example
scripts and test files.
"""

import random
import string

DEPARTMENTS = ["Engineering", "Marketing", "Sales", "HR", "Finance"]
CITIES      = ["NYC", "SF", "Chicago", "Austin", "Seattle"]


def make_dataset(n: int = 200, seed: int = 42) -> list[dict]:
    """
    Generate n fake employee records.

    Using a fixed seed means the data is the same every time
    you run it — essential for reproducible examples and tests.

    Args:
        n    : number of records to generate
        seed : random seed for reproducibility

    Returns:
        list of employee dicts

    Example record:
        {
            "id":     1,
            "name":   "FNAFQ",
            "dept":   "Finance",
            "city":   "Austin",
            "salary": 79700,
            "age":    37,
            "score":  3.05,
        }
    """
    random.seed(seed)

    return [
        {
            "id":     i,
            "name":   "".join(random.choices(string.ascii_uppercase, k=5)),
            "dept":   random.choice(DEPARTMENTS),
            "city":   random.choice(CITIES),
            "salary": random.randint(50_000, 200_000),
            "age":    random.randint(22, 65),
            "score":  round(random.uniform(1.0, 5.0), 2),
        }
        for i in range(n)
    ]


def print_sample(data: list[dict], n: int = 5) -> None:
    """Print the first n rows of a dataset cleanly."""
    print(f"showing {n} of {len(data)} rows:")
    for row in data[:n]:
        print(" ", row)
    print()
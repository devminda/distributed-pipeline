from dataclasses import dataclass


@dataclass
class Partition:
    """
    A horizontal slice of the dataset.

    This is the unit of parallelism — instead of one worker
    processing 1000 rows, four workers each process 250 rows
    in a Partition of their own.

    Attributes:
        partition_id : which slice this is (0, 1, 2 ...)
        data         : the actual rows belonging to this slice
    """
    partition_id: int
    data: list

    def __len__(self):
        return len(self.data)

    def __repr__(self):
        return f"Partition(id={self.partition_id}, rows={len(self.data)})"


def partition_data(data: list, num_partitions: int) -> list[Partition]:
    """
    Split a flat list into N balanced Partition objects.

    This is the equivalent of Spark's sc.parallelize(data, N).
    Each partition gets roughly the same number of rows —
    the last partition absorbs any remainder.

    Args:
        data           : the full dataset as a list of rows
        num_partitions : how many slices to create

    Returns:
        list of Partition objects

    Example:
        >>> rows = list(range(10))
        >>> parts = partition_data(rows, 3)
        >>> parts
        [Partition(id=0, rows=4), Partition(id=1, rows=3), Partition(id=2, rows=3)]
    """
    if not data:
        return [Partition(partition_id=i, data=[]) for i in range(num_partitions)]

    size = max(1, len(data) // num_partitions)

    partitions = []
    for i in range(num_partitions):
        start = i * size
        end   = start + size if i < num_partitions - 1 else len(data)
        partitions.append(Partition(partition_id=i, data=data[start:end]))

    return partitions
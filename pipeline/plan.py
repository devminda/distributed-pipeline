import uuid
import dill
from enum import Enum
from dataclasses import dataclass, field
from typing import Callable, Optional


class OpType(Enum):
    """
    Every operation the pipeline understands.

    Enums are used here instead of plain strings so that
    typos are caught at import time, not at runtime.
    """
    READ    = "READ"
    FILTER  = "FILTER"
    MAP     = "MAP"
    GROUPBY = "GROUPBY"
    REDUCE  = "REDUCE"
    SORT    = "SORT"
    LIMIT   = "LIMIT"


@dataclass
class PlanNode:
    """
    A single node in the execution DAG.

    Each node represents one operation — a FILTER, a MAP, etc.
    Nodes are linked via the children list, forming a directed
    acyclic graph (DAG) that the Executor walks stage by stage.

    Attributes:
        node_id   : unique ID for this node
        op_type   : which operation this node performs
        func_blob : the callable serialized with dill so it can
                    safely cross the process boundary to a worker
        func_name : human-readable name (for logging and the UI)
        children  : IDs of upstream nodes this node depends on
        metadata  : extra config e.g. {"descending": True} for SORT
    """
    # autogenerate a short UUID for each node so we can reference it in the DAG
    node_id:   str            = field(default_factory=lambda: str(uuid.uuid4())[:8])
    op_type:   OpType         = OpType.READ
    func_blob: bytes          = b""
    func_name: str            = ""
    children:  list[str]      = field(default_factory=list)
    metadata:  dict           = field(default_factory=dict)

    def get_func(self) -> Optional[Callable]:
        """Deserialize the callable back from bytes."""
        return dill.loads(self.func_blob) if self.func_blob else None

    def __repr__(self):
        return f"PlanNode(op={self.op_type.value}, name={self.func_name})"


def build_dag(operations: list[tuple]) -> list[PlanNode]:
    """
    Convert a list of recorded operations into a linked DAG.

    Each operation is a tuple of (OpType, callable, metadata).
    Nodes are linked in order — each node points back to the
    previous one as its child (upstream dependency).

    Args:
        operations : list of (OpType, func, meta) tuples

    Returns:
        list of PlanNode in execution order

    Example:
        >>> ops = [
        ...     (OpType.READ,   None,              {}),
        ...     (OpType.FILTER, lambda r: r > 10,  {}),
        ...     (OpType.MAP,    lambda r: r * 2,   {}),
        ... ]
        >>> dag = build_dag(ops)
        >>> [n.op_type.value for n in dag]
        ['READ', 'FILTER', 'MAP']
    """
    nodes = []
    prev  = None

    for op_type, func, meta in operations:
        name = getattr(func, "__name__", op_type.value.lower()) if func else op_type.value.lower()

        node = PlanNode(
            op_type   = op_type,
            func_blob = dill.dumps(func) if func else b"",
            func_name = name,
            metadata  = meta,
        )

        if prev:
            node.children = [prev.node_id]

        nodes.append(node)
        prev = node

    return nodes


def dag_to_dict(nodes: list[PlanNode]) -> list[dict]:
    """
    Serialize the DAG to a list of plain dicts.

    Used for JSON output — the UI and metrics reporting
    consume this format.

    Args:
        nodes : the DAG as a list of PlanNode

    Returns:
        list of dicts safe for json.dumps()
    """
    return [
        {
            "id":       node.node_id,
            "op":       node.op_type.value,
            "name":     node.func_name,
            "children": node.children,
            "meta":     node.metadata,
        }
        for node in nodes
    ]
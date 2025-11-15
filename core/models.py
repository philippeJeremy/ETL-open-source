# core/models.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class StepType(Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"


@dataclass
class ConnectionConfig:
    id: Optional[int]
    name: str
    type: str                 # <<< IMPORTANT : ce champ doit s’appeler "type"
    params: Dict[str, Any]    # host, port, user, password, dbname, etc.


@dataclass
class Step:
    id: Optional[int]
    task_id: Optional[int]
    name: str
    step_type: StepType
    order: int
    connection_id: Optional[int]
    config: Dict[str, Any]


@dataclass
class ScheduledTask:
    id: Optional[int]
    name: str
    recurrence: str
    enabled: bool = True
    steps: List[Step] = field(default_factory=list)

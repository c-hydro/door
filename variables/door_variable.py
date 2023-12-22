from abc import ABC, abstractmethod
from typing import Optional

from ..lib.time import TimeRange
from ..lib.space import Grid

class DOORvariable(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def make(self, bbox: list, time_range: TimeRange) -> None:
        pass
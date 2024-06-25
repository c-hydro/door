import pandas as pd
import xarray as xr
import os

from .space import BoundingBox

import logging
logger = logging.getLogger(__name__)


def save_csv(src: pd.DataFrame, destination: str) -> None:
    """
    Save a DataFrame to csv file.
    """
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    src.to_csv(destination)
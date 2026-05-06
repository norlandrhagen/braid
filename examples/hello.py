# /// script
# requires-python = ">=3.11"
# dependencies = ["xarray", "numpy"]
# ///
"""Smoke test for `braid batch run` — builds a tiny xarray Dataset and prints it."""

import numpy as np
import xarray as xr

ds = xr.Dataset(
    {"temperature": (("x", "y"), np.random.rand(4, 4))},
    coords={"x": np.arange(4), "y": np.arange(4)},
)

print(ds)

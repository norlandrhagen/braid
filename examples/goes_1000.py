#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "braid",
#   "icechunk",
#   "obspec-utils",
#   "obstore",
#   "virtualizarr",
# ]
#
# [tool.uv.sources]
# braid = { path = ".." }
# ///
"""
Virtualize GOES-16 ABI-L2-MCMIPF files in parallel using braid, writing to
an IceChunk virtual store via concat + append writes.
"""

import time

import icechunk as ic
import xarray as xr
from braid import fan
from virtualizarr.writers.icechunk import virtual_dataset_to_icechunk

# --- Configuration ---
GOES_BUCKET = "s3://noaa-goes16"
GOES_REGION = "us-east-1"

ICECHUNK_BUCKET = "carbonplan-scratch"
ICECHUNK_PREFIX = "goes16/goes16.icechunk"
ICECHUNK_REGION = "us-west-2"

BRAID_BATCH = 4
COMMIT_EVERY = 200

# Automatically generate variables to keep (CMI_C01-16 and DQF_C01-16)
_VARS = [f"{prefix}_C{i:02}" for prefix in ["CMI", "DQF"] for i in range(1, 17)]
KEEP_VARIABLES = _VARS + ["x", "y", "t"]

# --- IceChunk Helpers ---


def _repo_storage():
    return ic.s3_storage(
        bucket=ICECHUNK_BUCKET, prefix=ICECHUNK_PREFIX, region=ICECHUNK_REGION
    )


def _repo_config() -> ic.RepositoryConfig:
    config = ic.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        ic.VirtualChunkContainer(
            f"{GOES_BUCKET}/", store=ic.s3_store(region=GOES_REGION)
        )
    )
    # Optimized manifest splitting for time-series appends
    split_cfg = ic.ManifestSplittingConfig.from_dict(
        {
            ic.ManifestSplitCondition.AnyArray(): {
                ic.ManifestSplitDimCondition.DimensionName("t"): COMMIT_EVERY
            }
        }
    )
    config.manifest = ic.ManifestConfig(splitting=split_cfg)
    return config


def _open_or_create_repo() -> ic.Repository:
    credentials = ic.containers_credentials({f"{GOES_BUCKET}/": None})
    return ic.Repository.open_or_create(
        _repo_storage(),
        config=_repo_config(),
        authorize_virtual_chunk_access=credentials,
    )


def _find_resume_index(repo) -> int:
    """Finds the last successfully processed file index from commit metadata."""
    for commit in repo.ancestry(branch="main"):
        if commit.metadata and "last_hi" in commit.metadata:
            return int(commit.metadata["last_hi"])
    return 0


# --- File Listing ---


def list_goes_files(
    n: int | None = None, cache_path: str = "/tmp/goes16_2024_paths.txt"
) -> list[str]:
    import asyncio
    from pathlib import Path

    import obstore as obs
    from obstore.store import from_url

    p = Path(cache_path)
    if p.exists():
        paths = p.read_text().splitlines()
        print(f"Loaded {len(paths)} paths from cache ({cache_path})")
        return paths[:n] if n else paths

    async def _list_all() -> list[str]:
        store = from_url(GOES_BUCKET, region=GOES_REGION, skip_signature=True)
        top = await obs.list_with_delimiter_async(store, prefix="ABI-L2-MCMIPF/2024/")
        day_prefixes = top["common_prefixes"]

        async def list_day(prefix: str) -> list[str]:
            return [
                item["path"]
                for batch in obs.list(store, prefix=prefix + "/")
                for item in batch
                if item["path"].endswith(".nc")
            ]

        results = await asyncio.gather(*[list_day(p) for p in day_prefixes])
        return sorted(p for day in results for p in day)

    print("Listing all GOES-16 2024 files...")
    paths = asyncio.run(_list_all())
    p.write_text("\n".join(paths))
    return paths[:n] if n else paths


# --- VirtualiZarr Parsing ---


def open_vds(path: str):
    """Open a GOES-16 file as a virtual dataset."""
    from obspec_utils.registry import ObjectStoreRegistry
    from obstore.store import from_url
    from virtualizarr import open_virtual_dataset
    from virtualizarr.parsers import HDFParser

    store = from_url(GOES_BUCKET, region=GOES_REGION, skip_signature=True)
    registry = ObjectStoreRegistry({GOES_BUCKET: store})

    # We specify loadable_variables for coords to ensure 't' is available for concat
    vds = open_virtual_dataset(
        url=f"{GOES_BUCKET}/{path}",
        parser=HDFParser(),
        registry=registry,
        loadable_variables=["t", "y", "x"],
        drop_variables=["y_image", "x_image"],
    )

    # Only keep the variables we actually want
    vds = vds[KEEP_VARIABLES]

    # Standardize 't' and expand for concatenation
    vds = vds.expand_dims("t")
    return vds


# --- Main Logic ---


def main():
    start_total = time.time()

    print("Initializing listing and repo...")
    t_init = time.time()
    all_paths = list_goes_files(n=5000)
    n = len(all_paths)

    repo = _open_or_create_repo()
    resume_from = _find_resume_index(repo)
    print(f"Init took: {time.time() - t_init:.2f}s")

    remaining = list(range(resume_from, n))
    if not remaining:
        print("Dataset already up to date.")
        return

    batches = [
        remaining[i : i + COMMIT_EVERY] for i in range(0, len(remaining), COMMIT_EVERY)
    ]
    print(
        f"Resuming from {resume_from}. Processing {len(remaining)} files in {len(batches)} batches."
    )

    for batch in batches:
        batch_start = time.time()
        lo, hi = batch[0], batch[-1] + 1
        print(f"\n── Batch {lo}-{hi} ──")

        # 1. FAN / VIRTUALIZE
        t_fan = time.time()
        results = list(
            fan(
                open_vds,
                [all_paths[i] for i in batch],
                batch_size=BRAID_BATCH,
                timeout_s=900,
            )
        )
        dur_fan = time.time() - t_fan

        # 2. CONCAT
        t_concat = time.time()
        concat_ds = xr.concat(results, dim="t")
        dur_concat = time.time() - t_concat

        # 3. WRITE & COMMIT
        t_write = time.time()
        session = repo.writable_session("main")
        is_first_write = resume_from == 0 and lo == 0

        virtual_dataset_to_icechunk(
            concat_ds, session.store, append_dim=None if is_first_write else "t"
        )
        dur_write = time.time() - t_write

        t_commit = time.time()
        session.commit(f"goes16 t={lo}:{hi} of {n}", metadata={"last_hi": str(hi)})
        dur_commit = time.time() - t_commit

        # Summary for the batch
        batch_total = time.time() - batch_start
        print(f"  - Fan:    {dur_fan:6.2f}s")
        print(f"  - Concat: {dur_concat:6.2f}s")
        print(f"  - Write:  {dur_write:6.2f}s")
        print(f"  - Commit: {dur_commit:6.2f}s")
        print(f"  - Total:  {batch_total:6.2f}s (Index {hi})")

    print(f"\nProcessing complete. Total wall time: {time.time() - start_total:.2f}s")


if __name__ == "__main__":
    main()

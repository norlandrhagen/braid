# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
# BRAID vcpu=2 memory=4096
"""~4 minutes of load in phases, one chart at a time, for watching `braid dashboard`.

    uv run braid batch run examples/dashboard_demo.py
    uv run braid dashboard
"""

import multiprocessing as mp
import os
import time
import urllib.request


def burn(seconds: float) -> None:
    end = time.time() + seconds
    while time.time() < end:
        sum(x * x for x in range(10_000))


def phase(name: str) -> None:
    print(f"--- {name}", flush=True)


if __name__ == "__main__":
    phase("idle")
    time.sleep(20)

    phase("cpu: 1 core")
    burn(30)
    phase("cpu: 2 cores")
    procs = [mp.Process(target=burn, args=(30,)) for _ in range(2)]
    [p.start() for p in procs]
    [p.join() for p in procs]

    phase("memory: ramp to ~2.5 GiB, then free")
    blobs = []
    for _ in range(10):
        blobs.append(bytearray(256 * 2**20))
        time.sleep(3)
    del blobs
    time.sleep(15)

    phase("disk: write ~4 GiB, then delete")
    for i in range(8):
        with open(f"/tmp/demo{i}", "wb") as f:
            f.write(os.urandom(512 * 2**20))
        time.sleep(2)
    time.sleep(10)
    for i in range(8):
        os.remove(f"/tmp/demo{i}")

    phase("network: download ~1 GB")
    with urllib.request.urlopen("https://proof.ovh.net/files/1Gb.dat") as r:
        while r.read(2**20):
            pass

    phase("idle")
    time.sleep(20)
    print("done", flush=True)

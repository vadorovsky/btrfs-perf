# SPDX-License-Identifier: GPL-2.0
# Copyright (c) 2020 SUSE LLC.

import json
import os
import pathlib
import shutil
import subprocess
import sys

DEFAULT_FIO_JOB_SINGLETHREAD = b"""
[global]
name=btrfs-raid1-seqread-singlethread
filename=btrfs-raid1-seqread-singlethread
rw=read
bs=64k
direct=0
numjobs=1
time_based=0

[file1]
size=10G
ioengine=libaio
"""
DEFAULT_FIO_JOB_MULTITHREAD = b"""
[global]
name=btrfs-raid1-seqread-singlethread
filename=btrfs-raid1-seqread-singlethread
rw=read
bs=64k
direct=0
numjobs=%d
time_based=0

[file1]
size=10G
ioengine=libaio
""" % os.cpu_count()


def check_prerequisities() -> None:
    if os.geteuid() != 0:
        sys.exit("This script must be run as root")
    if shutil.which("fio") is None:
        sys.exit("fio is not installed")


def get_bandwidth(out: str) -> int:
    """Gets the bandwidth value from the given fio raw json output.

    Args:
        out: fio output.

    Returns:
        Bandwidth.

    """
    j = json.loads(out)
    jobs = j["jobs"]

    assert(len(jobs) == 1)

    return jobs[0]["read"]["bw"]


def bandwidth_to_mibs(bw: int):
    """Converts the bandwidth from KB/s to MiB/s for human readability.

    Args:
        bw: Bandwidth.

    Returns:
        Bandwidth in MiB/s.

    """
    return round(bw / 1024)


def run_fio_pipe(job_cfg: str) -> int:
    """Runs the fio job given as a string.

    Args:
        job_cfg: fio job configuration.

    Returns:
        Bandwidth.

    """
    p = subprocess.run(["fio", "--output-format=json", "-"],
                       stdout=subprocess.PIPE, input=job_cfg)
    return get_bandwidth(p.stdout)


def run_fio(job: pathlib.Path) -> int:
    """Runs the fio job given as a path.

    Args:
        job: Path to the fio job.

    Returns:
        Bandwidth.

    """
    p = subprocess.run(["fio", "--output-format=json", job],
                       stdout=subprocess.PIPE)
    return get_bandwidth(p.stdout)


def run_fio_pipe_raw(job_cfg: str) -> str:
    """Runs the fio job given as a string.

    Args:
        job_cfg: fio job configuration.

    Returns:
        Raw fio output.

    """
    p = subprocess.run(["fio", "-"], stdout=subprocess.PIPE,
                       input=job_cfg)
    return p.stdout


def run_fio_raw(job: pathlib.Path) -> str:
    """Runs the fio job given as a path.

    Args:
        job: Path to the fio job.

    Returns:
        Raw fio output.

    """
    p = subprocess.run(["fio", job], stdout=subprocess.PIPE)
    return p.stdout

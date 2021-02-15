# SPDX-License-Identifier: GPL-2.0
# Copyright (c) 2020 SUSE LLC.

import json
import os
import pathlib
import shutil
import subprocess
import sys
import typing

DEFAULT_FIO_JOB = b"""
[global]
name=btrfs-raid1
filename=btrfs-raid1
rw=%s
bs=64k
direct=0
numjobs=%d
time_based=0

[file1]
size=10G
ioengine=libaio
"""


def job_seqread_singlethread() -> bytes:
    return DEFAULT_FIO_JOB % (b"read", 1)


def job_seqread_multithread() -> bytes:
    return DEFAULT_FIO_JOB % (b"read", os.cpu_count())


def job_randread_singlethread() -> bytes:
    return DEFAULT_FIO_JOB % (b"randread", 1)


def job_randread_multithread() -> bytes:
    return DEFAULT_FIO_JOB % (b"randread", os.cpu_count())


def check_prerequisities() -> None:
    if os.geteuid() != 0:
        sys.exit("This script must be run as root")
    if shutil.which("fio") is None:
        sys.exit("fio is not installed")


def bandwidth_to_mibs(bw: int) -> int:
    """Converts the bandwidth from KB/s to MiB/s for human readability.

    Args:
        bw: Bandwidth.

    Returns:
        Bandwidth in MiB/s.

    """
    return round(bw / 1024)


def get_bandwidth(out: str, to_mibs: typing.Optional[bool] =
                  False) -> typing.Tuple[int, typing.Optional[int]]:
    """Gets the bandwidth value from the given fio raw json output.

    Args:
        out: fio output.

    Returns:
        Bandwidth.

    """
    j = json.loads(out)
    jobs = j["jobs"]

    assert(len(jobs) > 0)

    bw_single_job = jobs[0]["read"]["bw"]
    if to_mibs:
        bw_single_job = bandwidth_to_mibs(bw_single_job)

    if len(jobs) == 1:
        return bw_single_job, None

    bw_sum = sum(map(lambda job: job["read"]["bw"], jobs))
    if to_mibs:
        bw_sum = bandwidth_to_mibs(bw_sum)

    return bw_single_job, bw_sum


def run_fio_pipe(job_cfg: str, to_mibs: typing.Optional[bool] =
                 False) -> typing.Tuple[int, typing.Optional[int]]:
    """Runs the fio job given as a string.

    Args:
        job_cfg: fio job configuration.

    Returns:
        Bandwidth.

    """
    p = subprocess.run(["fio", "--output-format=json", "-"],
                       stdout=subprocess.PIPE, input=job_cfg)
    return get_bandwidth(p.stdout, to_mibs)


def run_fio(job: pathlib.Path) -> typing.Tuple[int, typing.Optional[int]]:
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

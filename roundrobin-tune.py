#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-2.0
# Copyright (c) 2020 SUSE LLC.
"""\
Script for finding the best values of sysfs settings related to the roundrobin
raid1 read policy for the given btrfs filesystem. The settings are:

- /sys/fs/btrfs/[fsid]/read_policies/roundrobin_nonrot_nonlocal_inc
- /sys/fs/btrfs/[fsid]/read_policies/roundrobin_rot_nonlocal_inc
"""

import argparse
import enum
import functools
import json
import logging
import os
import pathlib
import subprocess
import typing

import btrfs
import fio


N_ITER = 10

TYPE_SEQREAD = "seqread"
TYPE_RANDREAD = "randread"

log = logging.getLogger(__name__)


class BenchmarkType(enum.Enum):
    seqread = 0
    randread = 1

    def __str__(self):
        return self.name


FIO_JOBS_SINGLETHREAD = {
    BenchmarkType.seqread: fio.job_seqread_singlethread,
    BenchmarkType.randread: fio.job_randread_singlethread,
}
FIO_JOBS_MULTITHREAD = {
    BenchmarkType.seqread: fio.job_seqread_multithread,
    BenchmarkType.randread: fio.job_randread_multithread,
}


@functools.lru_cache()
def path_sysfs_nonrot_inc(fsid: str) -> pathlib.Path:
    """Gets the path to the setting of the penalty value for non-rotational
    disks.

    Args:
        fsid: The btrfs filesystem id.

    Returns:
        Path to the setting of the penalty value for rotational disks.

    """
    return os.path.join("/sys", "fs", "btrfs", fsid, "read_policies",
                        "roundrobin_nonrot_nonlocal_inc")

@functools.lru_cache()
def path_sysfs_rot_inc(fsid: str) -> pathlib.Path:
    """Gets the path to the setting of the penalty value for rotational disks.

    Args:
        fsid: The btrfs filesystem id.

    Returns:
        Path to the setting of the penalty value for rotational disks.

    """
    return os.path.join("/sys", "fs", "btrfs", fsid, "read_policies",
                        "roundrobin_rot_nonlocal_inc")


def set_nonrot_inc(fsid: str, inc: int) -> None:
    """Set the penalty value for non-rotational disks.

    Args:
        fsid: The btrfs filesystem id.
        inc: The value to set.

    """
    with open(path_sysfs_nonrot_inc(fsid), "w") as f:
        f.write(str(inc))


def set_rot_inc(fsid: str, inc: int) -> None:
    """Set the penalty value for rotational disks.

    Args:
        fsid: The btrfs filesystem id.
        inc: The value to set.

    """
    with open(path_sysfs_rot_inc(fsid), "w") as f:
        f.write(str(inc))


def run_fio(multithread: bool, benchmark_type: BenchmarkType,
            loops: int = fio.DEFAULT_LOOPS, size: str = fio.DEFAULT_SIZE,
            job: typing.Optional[pathlib.Path] =
            None) -> typing.Tuple[int, typing.Optional[int]]:
    """Runs fio to validate the currently set penalty values.

    Args:
        job: Optional; Path to the fio job to run.

    Returns:
        Bandwidth.

    """
    if job is not None:
        return fio.run_fio(job)
    if multithread:
        job_content = FIO_JOBS_MULTITHREAD[benchmark_type](loops=loops,
                                                           size=size)
    else:
        job_content = FIO_JOBS_SINGLETHREAD[benchmark_type](loops=loops,
                                                            size=size)
    return fio.run_fio_pipe(job_content)


def tune_mixed_inc(fsid: str, multithread: bool, benchmark_type: BenchmarkType,
                   loops: int = fio.DEFAULT_LOOPS, size: str = fio.DEFAULT_SIZE,
                   n_nonrot: typing.Optional[int] = N_ITER,
                   n_rot: typing.Optional[int] = N_ITER,
                   job: typing.Optional[pathlib.Path] = None) -> None:
    """Searches for the best penalty value for both non-rotational and
    rotational disks.

    Args:
        fsid: The btrfs filesystem id.
        multithread: Enable multithreaded fio job (if no optioal job provided)
        benchmark_type: Type of benchmark (if no optional job provided)
        job: Optional; Path to the fio job to run.

    """
    max_bw_1 = float("-inf")
    max_bw_2 = float("-inf")
    max_bw_3 = float("-inf")

    best_n_nonrot_1 = 0
    best_n_nonrot_2 = 0
    best_n_nonrot_3 = 0

    best_n_rot_1 = 0
    best_n_rot_2 = 0
    best_n_rot_3 = 0

    for i_nonrot in range(n_nonrot):
        for i_rot in range(n_rot):
            btrfs.drop_caches()

            log.debug(f"checking with roundrobin_nonrot_nonlocal_inc "
                      f"{i_nonrot} and roundrobin_rot_nonlocal_inc {i_rot}")
            set_nonrot_inc(fsid, i_nonrot)
            set_rot_inc(fsid, i_rot)

            bw, bw_sum = run_fio(multithread, benchmark_type, loops, size, job)
            bw_mibs = fio.bandwidth_to_mibs(bw)
            log.debug(f"bw: {bw} ({bw_mibs} MiB/s)")

            if bw > max_bw_1:
                max_bw_3 = max_bw_2
                max_bw_2 = max_bw_1
                max_bw_1 = bw

                best_n_nonrot_3 = best_n_nonrot_2
                best_n_nonrot_2 = best_n_nonrot_2
                best_n_nonrot_1 = i_nonrot

                best_n_rot_3 = best_n_rot_2
                best_n_rot_2 = best_n_rot_1
                best_n_rot_1 = i_rot

            elif bw > max_bw_2:
                max_bw_3 = max_bw_2
                max_bw_2 = bw

                best_n_nonrot_3 = best_n_nonrot_2
                best_n_nonrot_2 = i_nonrot

                best_n_rot_3 = best_n_rot_3
                best_n_rot_2 = i_rot

            elif bw > max_bw_3:
                max_bw_3 = bw
                best_n_nonrot_3 = i_nonrot
                best_n_rot_3 = i_rot

    max_bw_mibs_1 = fio.bandwidth_to_mibs(max_bw_1)
    max_bw_mibs_2 = fio.bandwidth_to_mibs(max_bw_2)
    max_bw_mibs_3 = fio.bandwidth_to_mibs(max_bw_3)

    print("Three best values")
    print(f"roundrobin_nonrot_nonlocal_inc: {best_n_nonrot_1}, "
          f"roundrobin_rot_nonlocal_inc: {best_n_rot_1} "
          f"with bw: {max_bw_1} ({max_bw_mibs_1} MiB/s)")
    print(f"roundrobin_nonrot_nonlocal_inc: {best_n_nonrot_2}, "
          f"roundrobin_rot_nonlocal_inc: {best_n_rot_2} "
          f"with bw: {max_bw_2} ({max_bw_mibs_2} MiB/s)")
    print(f"roundrobin_nonrot_nonlocal_inc: {best_n_nonrot_3}, "
          f"roundrobin_rot_nonlocal_inc: {best_n_rot_3} "
          f"with bw: {max_bw_3} ({max_bw_mibs_3} MiB/s)")

    set_nonrot_inc(fsid, best_n_nonrot_1)
    set_rot_inc(fsid, best_n_rot_1)


def tune_nonrot_inc(fsid: str, multithread: bool, benchmark_type: BenchmarkType,
                    loops: int = fio.DEFAULT_LOOPS, size: str = fio.DEFAULT_SIZE,
                    job: typing.Optional[pathlib.Path] = None) -> None:
    """Searches for the best penalty value for non-rotational disks.

    Args:
        fsid: The btrfs filesystem id.
        multithread: Enable multithreaded fio job (if no optioal job provided)
        benchmark_type: Type of benchmark (if no optional job provided)
        job: Optional; Path to the fio job to run.

    """
    tune_mixed_inc(fsid, multithread, benchmark_type, n_rot=1, job=job)


def tune_rot_inc(fsid: str, multithread: bool, benchmark_type: BenchmarkType,
                 loops: int = fio.DEFAULT_LOOPS, size: str = fio.DEFAULT_SIZE,
                 job: typing.Optional[pathlib.Path] = None) -> None:
    """Searches for the best penalty value for rotational disks.

    Args:
        fsid: The btrfs filesystem id.
        multithread: Enable multithreaded fio job (if no optioal job provided)
        benchmark_type: Type of benchmark (if no optional job provided)
        job: Optional; Path to the fio job to run.

    """
    tune_mixed_inc(fsid, multithread, benchmark_type, n_nonrot=1, job=job)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--debug", help="Verbose debug log",
                        action="store_true")
    parser.add_argument("--fio-job", "-j",
                        help=("Path to the fio job to use. If not set, the "
                              "default pre-defined job will be used."),
                        type=pathlib.Path)
    parser.add_argument("--nonrotational", action="store_true",
                        help=("Find the best value for "
                              "roundrobin_nonrot_nonlocal_inc"))
    parser.add_argument("--rotational", action="store_true",
                        help=("Find the best value for "
                              "roundrobin_rot_nonlocal_inc"))
    parser.add_argument("--benchmark-type",
                        type=lambda t: BenchmarkType[t],
                        default=BenchmarkType.seqread,
                        choices=list(BenchmarkType))
    parser.add_argument("--multithread", action="store_true",
                        help="Run multithreaded benchmarks")
    parser.add_argument("--loops", type=int,
                        help="Number of loops to run fio jobs in")
    parser.add_argument("--size", type=str, default=fio.DEFAULT_SIZE,
                        help="Default size of I/O to test")
    parser.add_argument("mountpoint",
                        help="Mountpoint of the btrfs filesystem to tune",
                        type=pathlib.Path)
    args = parser.parse_args()

    fio.check_prerequisities()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    os.chdir(args.mountpoint)
    fsid = btrfs.get_fsid(args.mountpoint)

    with btrfs.set_policy(fsid, "roundrobin"):
        if args.nonrotational and args.rotational:
            tune_mixed_inc(fsid, args.multithread, args.benchmark_type,
                           args.loops, args.size, job=args.fio_job)
        elif args.nonrotational:
            tune_nonrot_inc(fsid, args.multithread, args.benchmark_type,
                            args.loops, args.size, job=args.fio_job)
        elif args.rotational:
            tune_rot_inc(fsid, args.multithread, args.benchmark_type,
                         args.loops, args.size, job=args.fio_job)


if __name__ == "__main__":
    main()

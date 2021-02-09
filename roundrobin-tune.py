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

log = logging.getLogger(__name__)

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


def run_fio(job: typing.Optional[pathlib.Path] = None) -> float:
    """Runs fio to validate the currently set penalty values.

    Args:
        job: Optional; Path to the fio job to run.

    Returns:
        Bandwidth.

    """
    if job is not None:
        return fio.run_fio(job)
    return fio.run_fio_pipe(fio.DEFAULT_FIO_JOB_SINGLETHREAD)


def tune_mixed_inc(fsid: str,
                   job: typing.Optional[pathlib.Path] = None) -> None:
    """Searches for the best penalty value for both non-rotational and
    rotational disks.

    Args:
        fsid: The btrfs filesystem id.
        job: Optional; Path to the fio job to run.

    """
    max_bw= float("-inf")
    best_n_nonrot = 0
    best_n_rot = 0

    for i_nonrot in range(N_ITER):
        for i_rot in range(N_ITER):
            btrfs.drop_caches()

            log.debug(f"checking with roundrobin_nonrot_nonlocal_inc "
                      f"{i_nonrot} and roundrobin_rot_nonlocal_inc {i_rot}")
            set_nonrot_inc(fsid, i_nonrot)
            set_rot_inc(fsid, i_rot)

            bw = run_fio(job)
            bw_mibs = fio.bandwidth_to_mibs(bw)
            log.debug(f"bw: {bw} ({bw_mibs} MiB/s)")

            if bw > max_bw:
                max_bw = bw
                best_n_nonrot = i_nonrot
                best_n_rot = i_rot

    max_bw_mibs = fio.bandwidth_to_mibs(max_bw)
    print(f"The best {path_sysfs_nonrot_inc(fsid)} value: {best_n_nonrot}, "
          f"the best {path_sysfs_rot_inc(fsid)} value: {best_n_rot}, "
          f"with bw: {max_bw} ({max_bw_mibs} MiB/s)")

    set_nonrot_inc(fsid, best_n_nonrot)
    set_rot_inc(fsid, best_n_rot)


def tune_nonrot_inc(fsid: str,
                    job: typing.Optional[pathlib.Path] = None) -> None:
    """Searches for the best penalty value for non-rotational disks.

    Args:
        fsid: The btrfs filesystem id.
        job: Optional; Path to the fio job to run.

    """
    max_bw = float("-inf")
    best_n = 0

    for i in range(N_ITER):
        btrfs.drop_caches()

        log.debug(f"checking with roundrobin_nonrot_nonlocal_inc {i}")
        set_nonrot_inc(fsid, i)

        bw = run_fio(job)
        bw_mibs = fio.bandwidth_to_mibs(bw)
        log.debug(f"bw: {bw} ({bw_mibs} MiB/s)")

        if bw > max_bw:
            max_bw = bw
            best_n = i

    max_bw_mibs = fio.bandwidth_to_mibs(max_bw)
    print(f"The best {path_sysfs_nonrot_inc(fsid)} value: {best_n} "
          f"with bw: {max_bw} ({max_bw_mibs} MiB/s)")

    set_nonrot_inc(fsid, best_n)


def tune_rot_inc(fsid: str,
                 job: typing.Optional[pathlib.Path] = None) -> None:
    """Searches for the best penalty value for rotational disks.

    Args:
        fsid: The btrfs filesystem id.
        job: Optional; Path to the fio job to run.

    """
    max_bw = float("-inf")
    best_n = 0

    for i in range(N_ITER):
        btrfs.drop_caches()

        log.debug(f"checking with roundrobin_rot_nonlocal_inc {i}")
        set_rot_inc(fsid, i)

        bw = run_fio(job)
        bw_mibs = fio.bandwidth_to_mibs(bw)
        log.debug(f"bw: {bw} ({bw_mibs} MiB/s)")

        if bw > max_bw:
            max_bw = bw
            best_n = i

    max_bw_mibs = fio.bandwidth_to_mibs(max_bw)
    print(f"The best {path_sysfs_rot_inc(fsid)} value: {best_n} "
          f"with bw: {max_bw} ({max_bw_mibs} MiB/s)")

    set_rot_inc(fsid, best_n)


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
            tune_mixed_inc(fsid, args.fio_job)
        elif args.nonrotational:
            tune_nonrot_inc(fsid, args.fio_job)
        elif args.rotational:
            tune_rot_inc(fsid, args.fio_job)


if __name__ == "__main__":
    main()

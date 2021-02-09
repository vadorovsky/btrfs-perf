#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-2.0
# Copyright (c) 2020 SUSE LLC.
"""\
Script for benchmarking raid1 read policies. It uses fio on every available
policy for comparison.
"""

import argparse
import os
import pathlib
import typing

import btrfs
import fio


def bench_policy(fsid: str, policy: str,
                 job: typing.Optional[pathlib.Path] = None) -> None:
    """Runs fio benchmark for the given raid1 read policy.

    Args:
        fsid: The btrfs filesystem id.
        policy: The raid1 read policy to benchmark.
        job: Optional; Path to the fio job to run.

    """
    print(f"Testing policy: {policy}...")
    print("===")
    print()

    btrfs.drop_caches()

    if job is not None:
        out = fio.run_fio_raw(job)
        print(out.decode("utf-8"))
    else:
        print("singlethread")
        print("---")
        print()
        out = fio.run_fio_pipe_raw(fio.DEFAULT_FIO_JOB_SINGLETHREAD)
        print(out.decode("utf-8"))

        btrfs.drop_caches()

        print("multithread")
        print("---")
        print()
        out = fio.run_fio_pipe_raw(fio.DEFAULT_FIO_JOB_MULTITHREAD)
        print(out.decode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fio-job", "-j",
                        help=("Path to the fio job to use. If not set, the "
                              "default pre-defined job will be used."),
                        type=pathlib.Path)
    parser.add_argument("mountpoint",
                        help="Mountpoint of the btrfs filesystem to tune",
                        type=pathlib.Path)
    args = parser.parse_args()

    fio.check_prerequisities()

    os.chdir(args.mountpoint)
    fsid = btrfs.get_fsid(args.mountpoint)

    for policy in btrfs.get_policies(fsid):
        with btrfs.set_policy(fsid, policy):
            bench_policy(fsid, policy, args.fio_job)


if __name__ == "__main__":
    main()

# SPDX-License-Identifier: GPL-2.0
# Copyright (c) 2020 SUSE LLC.

import contextlib
import os
import pathlib
import subprocess
import typing


def get_fsid(mountpoint: pathlib.Path) -> str:
    """Gets the btrfs filesystem id.

    Args:
        mountpoint: Mountpoint of the filesystem.

    Returns:
        btrfs filesystem id.

    """
    p = subprocess.run(["btrfs", "filesystem", "show", mountpoint],
                       stdout=subprocess.PIPE)
    return p.stdout.splitlines()[0].split()[-1].decode("utf-8")


def path_sysfs_policy(fsid: str) -> pathlib.Path:
    """Gets the path to the read policy sysfs setting.

    Args:
        fsid: The btrfs filesystem id.

    Returns:
        Path to the read policy sysfs settings.

    """
    return os.path.join("/sys", "fs", "btrfs", fsid, "read_policies",
                        "policy")


def get_policies(fsid: str) -> typing.Iterator[str]:
    """Gets names of available read policies.

    Args:
        fsid: The btrfs filesystem id.

    Yields:
        Names of read policies.

    """
    path = path_sysfs_policy(fsid)

    with open(path, "r") as f:
        policies = f.read()

    for policy in policies.split():
        if policy.startswith("["):
            yield policy[1:-1]
        else:
            yield policy


@contextlib.contextmanager
def set_policy(fsid: str, policy: str) -> None:
    """Contextmanager which sets the given read policy and restores the old one
    on exit.

    Args:
        fsid: The btrfs filesystem id.
        policy: The raid1 read policy to set.

    """
    old_policy = "pid"
    path = path_sysfs_policy(fsid)

    with open(path, "r") as f:
        policies = f.read()

    for p in policies.split():
        if p.startswith("["):
            old_policy = p[1:-1]

    with open(path, "w") as f:
        f.write(policy)

    try:
        yield
    finally:
        with open(path, "w") as f:
            f.write(old_policy)


def drop_caches() -> None:
    """Drops memory caches."""
    path = os.path.join("/proc", "sys", "vm", "drop_caches")
    with open(path, "w") as f:
        f.write("1")

"""
General utility functions 

"""
import logging
import os
import uuid
import hashlib

from framework import config
from common_ci_utils.command_runner import exec_cmd
from framework.ssh_connection_manager import SSHConnectionManager

log = logging.getLogger(__name__)


def get_noobaa_sa_host_home_path():
    """
    Get the full path of the home directory on the remote machine

    Returns:
        str: The full path of the home directory on the remote machine

    """
    cmd = "echo $HOME"
    _, stdout, _ = SSHConnectionManager().connection.exec_cmd(cmd)
    return stdout


def generate_random_hex(length=20):
    """
    Generates a random hexadecimal string with the given length

    Args:
        length (int): The length of the hexadecimal string.

    Returns:
        str: A random hexamiadecimal string.

    """
    cmd = f"openssl rand -hex {length}"
    completed_process = exec_cmd(cmd)
    stdout = completed_process.stdout
    return stdout.decode("utf-8").strip()


def generate_unique_resource_name(prefix="resource"):
    """
    Generates a unique resource name with the given prefix

    Args:
        prefix (str): The prefix of the resource name.

    Returns:
        str: The unique resource name.

    """
    unique_id = str(uuid.uuid4()).split("-")[0]
    return f"{prefix}-{unique_id}"


def get_current_test_name():
    """
    Get the name of the current test

    Returns:
        str: The name of the current PyTest test

    """
    return os.environ.get("PYTEST_CURRENT_TEST").split(":")[-1].split(" ")[0]


def get_config_root_full_path():
    """
    Get the full path of the configuration root directory on the remote machine

    Returns:
        str: The full path of the configuration root directory on the remote machine

    """
    config_root = config.ENV_DATA["config_root"]

    if config_root.startswith("~/") == False:
        return config_root

    config_root = config_root.split("~/")[1]
    return f"{get_noobaa_sa_host_home_path()}/{config_root}"


def generate_random_files(dir, amount, size):
    """
    Generate random files in a given directory

    Args:
        dir (str): The directory in which to generate the files
        amount (int): The number of files to generate
        size (str): The size of each file

    Returns:
        list: A list of the files generated

    """
    log.info(f"Generating {amount} files of size {size} in {dir}")
    files_generated = []
    for i in range(amount):
        obj_name = f"obj_{i}"
        obj_path = os.path.join(dir, obj_name)
        exec_cmd(f"dd if=/dev/urandom of={obj_path} bs={size} count=1")
        files_generated.append(obj_name)

    return files_generated


def get_md5sum(file):
    """
    Calculate the md5sum of a file using the md5sum bash command

    Args:
        file (str): The file to calculate the md5sum of

    Returns:
        str: The md5sum of the file

    """
    result = exec_cmd(f"md5sum {file}")
    if result.returncode != 0:
        raise Exception(
            f"Failed to calculate md5sum of {file}: {result.stderr.decode()}"
        )
    return result.stdout.decode().split(" ")[0]


def compare_md5sums(file1, file2):
    """
    Compare the md5sums of two files

    Args:
        file1 (str): The first file to compare (full path)
        file2 (str): The second file to compare (full path)

    Returns:
        bool: True if the md5sums are equal, False otherwise

    """
    log.info(f"Comparing md5sums of {file1} and {file2}")
    file1_md5sum = get_md5sum(file1)
    file2_md5sum = get_md5sum(file2)
    return file1_md5sum == file2_md5sum

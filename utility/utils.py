"""
General utility functions 

"""
import uuid

from framework import config
from common_ci_utils.command_runner import exec_cmd
from framework.ssh_connection_manager import SSHConnectionManager


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

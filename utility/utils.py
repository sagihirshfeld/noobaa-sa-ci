"""
General utility functions 

"""

import logging
import os

from framework import config
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

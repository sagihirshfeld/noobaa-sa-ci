"""
Utility functions to run on the remote machine that hosts the NSFS server

"""
import json
import logging
import tempfile

from common_ci_utils.templating import Templating
from common_ci_utils.command_runner import exec_cmd
from framework.ssh_connection_manager import SSHConnectionManager
from noobaa_sa import constants
from framework import config
from noobaa_sa.exceptions import MissingFileOrDirectoryException

log = logging.getLogger(__name__)


def run_systemctl_command_on_nsfs_service(cmd):
    """
    Run a systemctl command on the NSFS service

    Return:
        Tuple[int, str, str]: The return code, stdout and stderr of the command

    """
    return SSHConnectionManager().connection.exec_cmd(
        f"sudo systemctl {cmd} {constants.NSFS_SERVICE_NAME}"
    )


def get_nsfs_service_status():
    """
    Get the status of the NSFS service using systemctl

    Returns:
        str: The raw output of the systemctl status command

    """
    log.info("Getting the status of the NSFS service")
    _, stdout, _ = run_systemctl_command_on_nsfs_service("status")
    log.info(f"NSFS service status: {stdout}")
    return stdout


def is_nsfs_service_running():
    """
    Get the status of the NSFS service using systemctl

    Returns:
        bool: True if the NSFS service is running, False otherwise

    """
    status_str = get_nsfs_service_status()
    return "Active: active (running)" in status_str


def stop_nsfs_service():
    """
    Use systemctl to stop the NSFS service

    """
    log.info("Stopping the NSFS service")
    run_systemctl_command_on_nsfs_service("stop")


def restart_nsfs_service():
    """
    Use systemctl to restart the NSFS service

    """
    log.info("Restarting the NSFS service")
    run_systemctl_command_on_nsfs_service("restart")


def create_tls_key_and_cert(credentials_dir):
    """
    Create a TLS key and certificate for the NSFS server

    Args:
        credentials_dir (str): The full path to the credentials directory on the remote machine

    Returns:
        str: The full path to the TLS certificate file that was created

    """
    conn = SSHConnectionManager().connection

    log.info(
        f"Generating TLS key and certificate using openssl under {credentials_dir}"
    )

    # Create the TLS key
    conn.exec_cmd(f"sudo openssl genpkey -algorithm RSA -out {credentials_dir}/tls.key")

    # Create a SAN (Subject Alternative Name) configuration file to use with the CSR
    with tempfile.NamedTemporaryFile(mode="w+") as tmp_file:
        templating = Templating(base_path=config.ENV_DATA["template_dir"])
        account_template = "openssl_san.cnf"
        account_data_full = templating.render_template(
            account_template, data={"nsfs_server_ip": conn.host}
        )
        tmp_file.write(account_data_full)
        tmp_file.flush()
        conn.upload_file(tmp_file.name, "/tmp/openssl_san.cnf")

    # Create a CSR (Certificate Signing Cequest) file
    conn.exec_cmd(
        "sudo openssl req -new "
        f"-key {credentials_dir}/tls.key "
        f"-out {credentials_dir}/tls.csr "
        "-config /tmp/openssl_san.cnf "
        "-subj '/CN=localhost' "
    )

    # Use the TLS key and CSR to create a self-signed certificate
    conn.exec_cmd(
        "sudo openssl x509 -req -days 365 "
        f"-in {credentials_dir}/tls.csr "
        f"-signkey {credentials_dir}/tls.key "
        f"-out {credentials_dir}/tls.crt "
        "-extfile /tmp/openssl_san.cnf "
        "-extensions req_ext "
    )

    return f"{credentials_dir}/tls.crt"


def set_nsfs_service_certs_dir(creds_dir, config_root=config.ENV_DATA["config_root"]):
    """
    Edit the NSFS system.json file to specify the path to the TLS key and certificate

    Args:
        creds_dir (str): The full path to the credentials directory on the remote machine
        config_root(str): The full path to the configuration root directory on the remote machine

    Raises:
        MissingFileOrDirectoryException: In case the system.json file is not found under config_root

    """

    conn = SSHConnectionManager().connection
    log.info(
        "Editing the NSFS system.json file to specify the path to the TLS key and certificate"
    )
    retcode, stdout, _ = conn.exec_cmd(f"cat {config_root}/system.json")
    if retcode != 0:
        raise MissingFileOrDirectoryException(
            f"system.json file not found in {config_root}: {stdout}"
        )
    system_json = json.loads(stdout)
    system_json["nsfs_ssl_key_dir"] = creds_dir
    conn.exec_cmd(f"echo '{json.dumps(system_json)}' > {config_root}/system.json")

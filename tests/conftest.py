import os
import logging
import tempfile
import pytest
import uuid

from common_ci_utils.templating import Templating
from common_ci_utils.command_runner import exec_cmd
from framework.ssh_connection_manager import SSHConnectionManager
from noobaa_sa import constants
from noobaa_sa.factories import AccountFactory
from noobaa_sa.bucket import BucketManager
from noobaa_sa.bucket import BucketOperation
from framework import config
from noobaa_sa.s3_client import S3Client

log = logging.getLogger(__name__)


@pytest.fixture
def account_manager(account_json=None):
    account_factory = AccountFactory()
    return account_factory.get_account(account_json)


@pytest.fixture
def bucket_manager(request):
    bucket_manager = BucketManager()

    def bucket_cleanup():
        for bucket in bucket_manager.list():
            bucket_manager.delete(bucket)

    request.addfinalizer(bucket_cleanup)
    return bucket_manager


# TODO: Add descriptibe error handling and logging
@pytest.fixture(scope="session")
def setup_nsfs_server_tls_certificate():
    """
    Setup the NSFS server TLS certification and download the certificate in
    a local file.

    Returns:
        str: The path to the downloaded certificate file.
    """

    # TODO: rename function
    def implementation(config_root=config.ENV_DATA["config_root"]):
        """
        Configure the NSFS server TLS certification and download the certificate
        in a local file.

        Args:
            config_root (str): The path to the configuration root directory.

        Returns:
            str: The path to the downloaded certificate file.

        """
        conn = SSHConnectionManager().connection
        # Omit the ~/ prefix if exists
        config_root_path = (
            config_root.split("~/")[1] if config_root.startswith("~/") else config_root
        )
        remote_credentials_dir = f"{config_root_path}/certificates"
        conn.exec_cmd(f"sudo mkdir -p {remote_credentials_dir}")

        # Create the TLS key
        conn.exec_cmd(
            f"sudo openssl genpkey -algorithm RSA -out {remote_credentials_dir}/tls.key"
        )

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
            f"-key {remote_credentials_dir}/tls.key "
            f"-out {remote_credentials_dir}/tls.csr "
            "-config /tmp/openssl_san.cnf "
            "-subj '/CN=localhost' "
        )

        # Use the TLS key and CSR to create a self-signed certificate
        conn.exec_cmd(
            "sudo openssl x509 -req -days 365 "
            f"-in {remote_credentials_dir}/tls.csr "
            f"-signkey {remote_credentials_dir}/tls.key "
            f"-out {remote_credentials_dir}/tls.crt "
            "-extfile /tmp/openssl_san.cnf "
            "-extensions req_ext "
        )

        # Restart the NSFS service to apply the new key and certificate
        conn.exec_cmd(f"sudo systemctl restart {constants.NSFS_SERVICE_NAME}")

        # Download the certificate to a local file
        local_path = "/tmp/tls.crt"
        conn.download_file(
            remotepath=f"{remote_credentials_dir}/tls.crt",
            localpath=local_path,
        )
        return local_path

    return implementation


@pytest.fixture
def s3_client_factory(setup_nsfs_server_tls_certificate, account_manager):
    """
    Factory to create S3Client instances.

    Returns:
        func: A function that creates S3Client instances.

    """
    tls_crt_path = setup_nsfs_server_tls_certificate()

    def create_s3client(
        access_and_secret_keys_tuple=None,
        verify_tls=True,
        endpoint_port=constants.DEFAULT_NSFS_PORT,
    ):
        # Set the AWS access and secret keys
        access_key, secret_key = None, None
        if access_and_secret_keys_tuple is None:
            account_name = unique_resource_name(prefix="account")
            access_key = random_hex()
            secret_key = random_hex()
            config_root = config.ENV_DATA["config_root"]
            account_manager.create(account_name, access_key, secret_key, config_root)
        else:
            access_key, secret_key = access_and_secret_keys_tuple

        nb_sa_host_address = config.ENV_DATA["noobaa_sa_host"]
        return S3Client(
            endpoint=f"https://{nb_sa_host_address}:{endpoint_port}",
            access_key=access_key,
            secret_key=secret_key,
            tls_crt_path=tls_crt_path if verify_tls else None,
        )

    return create_s3client


@pytest.fixture
def unique_resource_name(request):
    """
    Generates a unique resource name with the given prefix
    """

    def _get_unique_name(prefix="resource"):
        unique_id = str(uuid.uuid4()).split("-")[0]
        return f"{prefix}-{unique_id}"

    return _get_unique_name


@pytest.fixture(scope="function")
def random_hex(request):
    """
    Generates a random hexadecimal string.
    """

    def _get_random_hex():
        cmd = "openssl rand -hex 20"
        completed_process = exec_cmd(cmd)
        stdout = completed_process.stdout
        return stdout.decode("utf-8").strip()

    return _get_random_hex


@pytest.fixture()
def tmp_directories_factory(request, random_hex):
    """
    Factory to create temporary local testing directories, and cleanup after the test.

    """
    current_test_name = (
        os.environ.get("PYTEST_CURRENT_TEST").split(":")[-1].split(" ")[0]
    )
    tmp_testing_dirs_root = f"/tmp/{current_test_name}-{random_hex()[:5]}"
    os.mkdir(tmp_testing_dirs_root)

    def create_tmp_testing_dirs(dirs_to_create):
        """
        Create temporary local testing directories.

        Args:
            dirs_to_create (list): List of directories to create.

        """
        created_dirs_paths = []
        for dir in dirs_to_create:
            new_tmp_dir_path = f"{tmp_testing_dirs_root}/{dir}"
            os.mkdir(new_tmp_dir_path)
            created_dirs_paths.append(new_tmp_dir_path)

        return created_dirs_paths

    def cleanup():
        """
        Cleanup local test directories.

        """
        exec_cmd(f"rm -rf {tmp_testing_dirs_root}")

    request.addfinalizer(cleanup)
    return create_tmp_testing_dirs

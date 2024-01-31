import os
import logging
import pytest

from common_ci_utils.templating import Templating
from common_ci_utils.command_runner import exec_cmd
from framework.ssh_connection_manager import SSHConnectionManager
from noobaa_sa import constants
from noobaa_sa.factories import AccountFactory
from noobaa_sa.bucket import BucketManager
from framework import config
from noobaa_sa.s3_client import S3Client
from noobaa_sa.exceptions import MissingFileOrDirectoryException
from utility.utils import (
    get_noobaa_sa_host_home_path,
    get_config_root_full_path,
    generate_random_hex,
    generate_unique_resource_name,
)
from utility.nsfs_server_utils import (
    restart_nsfs_service,
    create_tls_key_and_cert,
    set_nsfs_service_certs_dir,
)


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


@pytest.fixture(scope="session")
def nsfs_server_tls_cert_path():
    """
    Configure the NSFS server TLS certification and download the certificate
    in a local file.

    Args:
        config_root (str): The path to the configuration root directory.

    Returns:
        str: The path to the downloaded certificate file.

    """
    conn = SSHConnectionManager().connection
    config_root_path = get_config_root_full_path()
    remote_credentials_dir = f"{config_root_path}/certificates"

    # Create the TLS credentials and configure the NSFS service to use them
    conn.exec_cmd(f"sudo mkdir -p {remote_credentials_dir}")
    remote_tls_crt_path = create_tls_key_and_cert(remote_credentials_dir)
    set_nsfs_service_certs_dir(remote_credentials_dir)
    restart_nsfs_service()

    # Download the certificate to a local file
    local_path = "/tmp/tls.crt"
    conn.download_file(
        remotepath=remote_tls_crt_path,
        localpath=local_path,
    )
    return local_path


@pytest.fixture
def s3_client_factory(nsfs_server_tls_cert_path, account_manager):
    """
    Factory to create S3Client instances.

    Returns:
        func: A function that creates S3Client instances.

    """

    def create_s3client(
        access_and_secret_keys_tuple=None,
        verify_tls=True,
        endpoint_port=constants.DEFAULT_NSFS_PORT,
    ):
        """
        Create an S3Client instance using the given credentials.

        Args:
            access_and_secret_keys_tuple (tuple): A tuple of access and secret keys.
            verify_tls (bool): Whether to verify the TLS certificate.
            endpoint_port (int): The port to use for the endpoint.

        Returns:
            S3Client: An S3Client instance.

        """
        # Set the AWS access and secret keys
        access_key, secret_key = None, None
        if access_and_secret_keys_tuple is None:
            account_name = generate_unique_resource_name(prefix="account")
            access_key = generate_random_hex()
            secret_key = generate_random_hex()
            config_root = config.ENV_DATA["config_root"]
            account_manager.create(account_name, access_key, secret_key, config_root)
        else:
            access_key, secret_key = access_and_secret_keys_tuple

        nb_sa_host_address = config.ENV_DATA["noobaa_sa_host"]
        return S3Client(
            endpoint=f"https://{nb_sa_host_address}:{endpoint_port}",
            access_key=access_key,
            secret_key=secret_key,
            tls_crt_path=nsfs_server_tls_cert_path if verify_tls else None,
        )

    return create_s3client


@pytest.fixture()
def tmp_directories_factory(request):
    """
    Factory to create temporary local testing directories, and cleanup after the test.

    """
    random_hex = generate_random_hex()
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

import os
import tempfile
import logging
import hashlib
from framework.ssh_connection_manager import SSHConnectionManager

from framework import config

log = logging.getLogger(__name__)


def test_basic_s3(
    account_manager, bucket_manager, s3_client_factory, unique_resource_name, random_hex
):
    """
    Test basic s3 operations using a noobaa bucket:
    1. Create an account and a bucket
    2. Write objects to the bucket
    3. List the bucket's contents
    4. Read the objects from the bucket and verify data integrity

    """
    SSHConnectionManager().connection
    # 1. Create an account and a bucket
    # TODO: create support for default account / bucket creation without params
    account_name = unique_resource_name(prefix="account")
    access_key = random_hex()
    secret_key = random_hex()
    account_manager.create(account_name, access_key, secret_key)

    # TODO: make create_bucket return a bucket name instead of passing it as a param
    bucket_name = unique_resource_name(prefix="bucket")
    bucket_manager.create(account_name, bucket_name)

    s3_client = s3_client_factory(access_and_secret_keys_tuple=(access_key, secret_key))

    # 2. Write objects to the bucket
    objs_names, tmp_dir = s3_client.write_random_objs_to_bucket(
        bucket_name, num_files=10, file_size=1
    )

    # 3. List the bucket's contents
    listed_objs = s3_client.list_objects(bucket_name)
    assert len(listed_objs) == len(
        objs_names
    ), "Number of listed objects does not match number of written objects"

    # 4. Download the objects from the bucket and verify data integrity
    with tempfile.TemporaryDirectory() as tmp_dir:
        s3_client.sync(f"s3://bucket_name", tmp_dir)
        for original, downloaded in zip(objs_names, os.listdir(tmp_dir)):
            # TODO: use an md5 helper function
            with open(tmp_dir + "/" + original, "rb") as f:
                file_contents = f.read()
            original_md5 = hashlib.md5(file_contents).hexdigest()
            with open(tmp_dir + "/" + downloaded, "rb") as f:
                file_contents = f.read()
            downloaded_md5 = hashlib.md5(file_contents).hexdigest()
            assert (
                original_md5 == downloaded_md5
            ), "Downloaded object does not match original object"

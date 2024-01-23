import os
import tempfile
import logging
import hashlib
from framework.ssh_connection_manager import SSHConnectionManager
from common_ci_utils.command_runner import exec_cmd

from framework import config

log = logging.getLogger(__name__)


def test_basic_s3(
    account_manager,
    bucket_manager,
    s3_client_factory,
    unique_resource_name,
    random_hex,
    tmp_directories_factory,
):
    """
    Test basic s3 operations using a noobaa bucket:
    1. Create an account and a bucket
    2. Write objects to the bucket
    3. List the bucket's contents
    4. Read the objects from the bucket and verify data integrity
    5. Delete the objects from the bucket

    """
    origin_dir, results_dir = tmp_directories_factory(
        dirs_to_create=["origin", "result"]
    )

    # 1. Create an account and a bucket
    # TODO: create support for default account / bucket creation without params
    account_name = unique_resource_name(prefix="account")
    access_key = random_hex()
    secret_key = random_hex()
    account_manager.create(account_name, access_key, secret_key)

    # TODO: make create_bucket return a bucket name instead of passing it as a param
    bucket_name = unique_resource_name(prefix="bucket")
    bucket_manager.create(account_name, bucket_name)

    # TODO: add support for passing an account object instead of these credentials
    s3_client = s3_client_factory(access_and_secret_keys_tuple=(access_key, secret_key))

    # 2. Write objects to the bucket
    original_objs_names = s3_client.write_random_objs_to_bucket(
        bucket_name, amount=10, obj_size="1M", prefix="", files_dir=origin_dir
    )

    # 3. List the bucket's contents
    listed_objs = s3_client.list_objects(bucket_name)
    assert len(listed_objs) == len(
        original_objs_names
    ), "Number of listed objects does not match number of written objects"

    # 4. Download the objects from the bucket and verify data integrity
    s3_client.sync(f"s3://{bucket_name}", results_dir)
    downloaded_objs_names = os.listdir(results_dir)
    assert len(downloaded_objs_names) == len(
        original_objs_names
    ), "Number of downloaded objects does not match number of written objects"
    original_objs_names.sort()
    downloaded_objs_names.sort()
    for original, downloaded in zip(original_objs_names, downloaded_objs_names):
        # TODO: use an md5 helper function
        with open(origin_dir + "/" + original, "rb") as f:
            file_contents = f.read()
        original_md5 = hashlib.md5(file_contents).hexdigest()
        with open(results_dir + "/" + downloaded, "rb") as f:
            file_contents = f.read()
        downloaded_md5 = hashlib.md5(file_contents).hexdigest()
        assert (
            original_md5 == downloaded_md5
        ), "Downloaded object md5 hash does not match the original object"

    # 5. Delete the objects from the bucket
    s3_client.delete_all_objects_in_bucket(bucket_name)
    assert (
        len(s3_client.list_objects(bucket_name)) == 0
    ), "Bucket is not empty after attempting to delete all objects"

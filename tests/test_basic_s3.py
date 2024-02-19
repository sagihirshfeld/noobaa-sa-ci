from datetime import datetime, timedelta, timezone
import logging
import os

import pytest
from common_ci_utils.file_system_utils import compare_md5sums
from common_ci_utils.random_utils import (
    generate_random_hex,
    generate_unique_resource_name,
)
from noobaa_sa.exceptions import NoSuchBucketException

from utility.nsfs_server_utils import *

log = logging.getLogger(__name__)


class TestBasicS3:
    """
    Test basic s3 operations using NSFS noobaa buckets
    """

    @pytest.fixture(scope="class")
    def s3_client_and_account_setup(
        self, account_manager_class, s3_client_factory_class
    ):
        """
        Create an account and an s3 client

        Args:
            account_manager: account manager object
            s3_client_factory: s3 client factory object

        Returns:
            s3_client: s3 client object
            account_name: account name

        """
        account_name = generate_unique_resource_name(prefix="account")
        access_key = generate_random_hex()
        secret_key = generate_random_hex()
        account_manager_class.create(account_name, access_key, secret_key)

        s3_client = s3_client_factory_class(
            access_and_secret_keys_tuple=(access_key, secret_key)
        )
        return s3_client, account_name

    def test_bucket_creation_and_deletion(self, s3_client_and_account_setup):
        """
        Test bucket creation and deletion via S3

        Args:
            s3_client_and_account_setup: s3 client and account setup fixture

        """
        s3_client, _ = s3_client_and_account_setup
        bucket_name = s3_client.create_bucket()
        assert bucket_name in s3_client.list_buckets(), "Bucket was not created"
        s3_client.delete_bucket(bucket_name)
        assert bucket_name not in s3_client.list_buckets(), "Bucket was not deleted"

    def test_missing_bucket_deletion(self, s3_client_and_account_setup):
        """
        Test that deletion of a non existing bucket raises a NoSuchBucket exception

        """
        s3_client, _ = s3_client_and_account_setup
        with pytest.raises(NoSuchBucketException):
            s3_client.delete_bucket("non_existing_bucket")

    def test_list_buckets(self, s3_client_and_account_setup):
        """
        Test listing buckets before creation and after deletion via S3

        Args:
            s3_client_and_account_setup: s3 client and account setup fixture

        """
        s3_client, _ = s3_client_and_account_setup
        buckets, listed_buckets = [], []
        AMOUNT = 5
        log.info(f"Creating {AMOUNT} buckets")
        try:
            for _ in range(AMOUNT):
                buckets.append(s3_client.create_bucket())

            listed_buckets = s3_client.list_buckets()
            assert all(
                bucket in listed_buckets for bucket in buckets
            ), "Created bucket was not listed!"

            log.info("Deleting one of the buckets")
            s3_client.delete_bucket(buckets[-1])
            listed_buckets = s3_client.list_buckets()
            assert (
                buckets[-1] not in listed_buckets
            ), "Deleted bucket was still listed post deletion!"
            assert all(
                bucket in listed_buckets for bucket in buckets[:-1]
            ), "Non deleted buckets were not listed post bucket deletion"

            log.info(f"Deleting the remaining {AMOUNT - 1} buckets")
            for i in range(AMOUNT - 1):
                s3_client.delete_bucket(buckets[i])

            assert all(
                bucket not in s3_client.list_buckets() for bucket in buckets
            ), "Some buckets that were deleted were still listed"

        except AssertionError as e:
            log.error(f"Created buckets: {buckets}")
            log.error(f"Listed buckets: {listed_buckets}")
            raise e

    @pytest.mark.parametrize("use_v2", [False, True])
    def test_list_objects(
        self, s3_client_and_account_setup, tmp_directories_factory, use_v2
    ):
        """
        Test S3 ListObjects and S3 ListObjectsV2 operations

        """
        origin_dir = tmp_directories_factory(dirs_to_create=["origin"])[0]
        s3_client, _ = s3_client_and_account_setup
        bucket = s3_client.create_bucket()

        written_objs_names = s3_client.put_random_objects(
            bucket, amount=5, min_size="1M", max_size="2M", files_dir=origin_dir
        )
        listed_objs_md_dicts = s3_client.list_objects(
            bucket, use_v2=use_v2, get_metadata=True
        )

        # Verify that number of listed objects matches the number of written objects
        assert len(listed_objs_md_dicts) == len(
            written_objs_names
        ), "Listed objects count does not match original objects count"

        # Sorting the two lists should align any written object with its listed counterpart
        written_objs_names.sort()
        listed_objs_md_dicts.sort(key=lambda x: x["Key"])

        log.info("Verifying listed objects metadata")
        for written, listed in zip(written_objs_names, listed_objs_md_dicts):
            # Verify the listed object key matches the expected written object name
            assert (
                written == listed["Key"]
            ), "Listed object key does not match expected written object name"

            # Verify that the last modified time is within a reasonable range
            # of the time the object was written
            last_modified = listed["LastModified"]
            now = datetime.now(timezone.utc)
            assert now - timedelta(minutes=5) < last_modified < now, (
                "Listed object last modified time is not within a reasonable range",
                f"Object: {written}, Last Modified: {last_modified}",
            )
            # Verify that the listed object size matches size of the original written object
            expected_size = os.path.getsize(os.path.join(origin_dir, written))
            listed_size = listed["Size"]
            assert expected_size == listed_size, (
                "Listed object size does not match written object size",
                f"Object: {written}, Expected: {expected_size}, Actual: {listed_size}",
            )


# def test_basic_s3(
#     account_manager,
#     s3_client_factory_implementation,
#     tmp_directories_factory,
# ):
#     """
#     Test basic s3 operations using a noobaa bucket:
#     1. Create an account
#     2. Create a bucket using S3
#     3. Write objects to the bucket
#     4. List the bucket's contents
#     5. Read the objects from the bucket and verify data integrity
#     6. Delete the objects from the bucket
#     7. Delete the bucket using S3

#     """
#     origin_dir, results_dir = tmp_directories_factory(
#         dirs_to_create=["origin", "result"]
#     )

#     # 1. Create an account and a bucket
#     # TODO: create support for default account / bucket creation without params
#     account_name = generate_unique_resource_name(prefix="account")
#     access_key = generate_random_hex()
#     secret_key = generate_random_hex()
#     account_manager.create(account_name, access_key, secret_key)

#     # TODO: add support for passing an account object instead of these credentials
#     s3_client = s3_client_factory_implementation(
#         access_and_secret_keys_tuple=(access_key, secret_key)
#     )

#     # 2. Create a bucket using S3
#     bucket_name = s3_client.create_bucket()
#     assert bucket_name in s3_client.list_buckets(), "Bucket was not created"

#     # 3. Write objects to the bucket
#     original_objs_names = s3_client.put_random_objects(
#         bucket_name, amount=10, min_size="1M", max_size="2M", files_dir=origin_dir
#     )

#     # 4. List the bucket's contents
#     listed_objs = s3_client.list_objects(bucket_name)
#     obj_count_match = len(listed_objs) == len(original_objs_names)
#     assert obj_count_match, "Listed objects count does not match original objects count"

#     s3_client.get_object(bucket_name, listed_objs[0])

#     # 5. Download the objects from the bucket and verify data integrity
#     s3_client.download_bucket_contents(bucket_name, results_dir)
#     downloaded_objs_names = os.listdir(results_dir)
#     obj_count_match = len(original_objs_names) == len(downloaded_objs_names)
#     assert obj_count_match, "Downloaded and original objects count does not match"
#     original_objs_names.sort()
#     downloaded_objs_names.sort()
#     for original, downloaded in zip(original_objs_names, downloaded_objs_names):
#         original_full_path = os.path.join(origin_dir, original)
#         downloaded_full_path = os.path.join(results_dir, downloaded)
#         md5sums_match = compare_md5sums(original_full_path, downloaded_full_path)
#         assert md5sums_match == True, f"MD5 sums do not match for {original}"

#     # 6. Delete the objects from the bucket
#     s3_client.delete_all_objects_in_bucket(bucket_name)
#     bucket_is_empty = len(s3_client.list_objects(bucket_name)) == 0
#     assert bucket_is_empty, "Bucket is not empty after attempting to delete all objects"

#     # 7.Delete the bucket using S3
#     s3_client.delete_bucket(bucket_name)
#     assert bucket_name not in s3_client.list_buckets(), "Bucket was not deleted"

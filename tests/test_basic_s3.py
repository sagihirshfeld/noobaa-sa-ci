from datetime import datetime, timedelta, timezone
import logging
import os

import pytest
from common_ci_utils.file_system_utils import compare_md5sums
from noobaa_sa.exceptions import (
    BucketAlreadyExistsException,
    BucketNotEmptyException,
    NoSuchBucketException,
    AccessDeniedException,
)

from utility.nsfs_server_utils import *

log = logging.getLogger(__name__)


class TestBasicS3:
    """
    Test basic s3 operations using NSFS noobaa buckets
    """

    @pytest.fixture(scope="class")
    def class_scope_s3_client(self, s3_client_factory_class):
        """
        Create a class scoped S3Client instance

        Args:
            s3_client_factory: s3 client factory object

        Returns:
            s3_client: s3 client object

        """
        s3_client = s3_client_factory_class()
        return s3_client

    def test_bucket_creation_and_deletion(self, class_scope_s3_client):
        """
        Test bucket creation and deletion via S3

        Args:
            class_scope_s3_client(S3Client): s3 client object

        """
        bucket_name = class_scope_s3_client.create_bucket()
        assert (
            bucket_name in class_scope_s3_client.list_buckets()
        ), "Bucket was not created"
        class_scope_s3_client.delete_bucket(bucket_name)
        assert (
            bucket_name not in class_scope_s3_client.list_buckets()
        ), "Bucket was not deleted"

    def test_expected_bucket_creation_failures(
        self, class_scope_s3_client, account_manager, s3_client_factory
    ):
        """
        Test bucket creation scenarios that are expected to fail

        """
        # Test creating a bucket with the name of a bucket that already exists
        log.info("Creating a bucket with a name that already exists")
        bucket_name = class_scope_s3_client.create_bucket()
        with pytest.raises(BucketAlreadyExistsException):
            class_scope_s3_client.create_bucket(bucket_name)
            log.error("Bucket creation with existing name succeeded")
        log.info("Bucket creation with existing name failed as expected")

        # Test creating a bucket using the credentials of a user that's not allowed to create buckets

        # TODO: Comment out once https://bugzilla.redhat.com/show_bug.cgi?id=2262992 is fixed
        # log.info("Creating a bucket using restricted account credentials")
        # _, restricted_acc_access_key, restricted_acc_secret_key = (
        #     account_manager.create(allow_bucket_creation=False)
        # )
        # restricted_s3_client = s3_client_factory(
        #     access_and_secret_keys_tuple=(
        #         restricted_acc_access_key,
        #         restricted_acc_secret_key,
        #     )
        # )
        # with pytest.raises(AccessDeniedException):
        #     restricted_s3_client.create_bucket()
        #     log.error("Bucket creation using restricted account credentials succeeded")
        # log.info(
        #     "Bucket creation using restricted account credentials failed as expected"
        # )

    def test_expected_bucket_deletion_failures(self, class_scope_s3_client):
        """
        Test bucket deletion scenarios that are expected to fail

        """
        # Test deleting a non existing bucket
        log.info("Deleting a non existing bucket")
        with pytest.raises(NoSuchBucketException):
            class_scope_s3_client.delete_bucket("non_existing_bucket")
            log.error("Bucket deletion of non existing bucket succeeded")
        log.info("Bucket deletion of non existing bucket failed as expected")

        # Test deleting a non empty bucket
        log.info("Deleting a non empty bucket")
        with pytest.raises(BucketNotEmptyException):
            bucket_name = class_scope_s3_client.create_bucket()
            class_scope_s3_client.put_random_objects(bucket_name, amount=1)
            class_scope_s3_client.delete_bucket(bucket_name)
            log.error("Bucket deletion of non empty bucket succeeded")
        log.info("Bucket deletion of non empty bucket failed as expected")

    def test_list_buckets(self, class_scope_s3_client):
        """
        Test listing buckets before creation and after deletion via S3

        Args:
            class_scope_s3_client(S3Client): s3 client object

        """
        buckets, listed_buckets = [], []
        AMOUNT = 5
        log.info(f"Creating {AMOUNT} buckets")
        try:
            for _ in range(AMOUNT):
                buckets.append(class_scope_s3_client.create_bucket())

            listed_buckets = class_scope_s3_client.list_buckets()
            assert all(
                bucket in listed_buckets for bucket in buckets
            ), "Created bucket was not listed!"

            log.info("Deleting one of the buckets")
            class_scope_s3_client.delete_bucket(buckets[-1])
            listed_buckets = class_scope_s3_client.list_buckets()
            assert (
                buckets[-1] not in listed_buckets
            ), "Deleted bucket was still listed post deletion!"
            assert all(
                bucket in listed_buckets for bucket in buckets[:-1]
            ), "Non deleted buckets were not listed post bucket deletion"

            log.info(f"Deleting the remaining {AMOUNT - 1} buckets")
            for i in range(AMOUNT - 1):
                class_scope_s3_client.delete_bucket(buckets[i])

            assert all(
                bucket not in class_scope_s3_client.list_buckets() for bucket in buckets
            ), "Some buckets that were deleted were still listed"

        except AssertionError as e:
            log.error(f"Created buckets: {buckets}")
            log.error(f"Listed buckets: {listed_buckets}")
            raise e

    @pytest.mark.parametrize("use_v2", [False, True])
    def test_list_objects(self, class_scope_s3_client, tmp_directories_factory, use_v2):
        """
        Test S3 ListObjects and S3 ListObjectsV2 operations

        Args:
            class_scope_s3_client(S3Client): s3 client object
            tmp_directories_factory: tmp directories factory object
            use_v2(bool): whether to use ListObjectsV2

        """
        origin_dir = tmp_directories_factory(dirs_to_create=["origin"])[0]
        bucket = class_scope_s3_client.create_bucket()

        written_objs_names = class_scope_s3_client.put_random_objects(
            bucket, amount=5, min_size="1M", max_size="2M", files_dir=origin_dir
        )
        listed_objs_md_dicts = class_scope_s3_client.list_objects(
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

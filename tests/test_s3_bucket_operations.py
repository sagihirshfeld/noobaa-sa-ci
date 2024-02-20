from datetime import datetime, timedelta, timezone
import logging
import os

import pytest
from noobaa_sa.exceptions import (
    BucketAlreadyExistsException,
    BucketNotEmptyException,
    NoSuchBucketException,
)

from utility.nsfs_server_utils import *

log = logging.getLogger(__name__)


class TestS3BucketOperations:
    """
    Test S3 bucket operations using NSFS
    """

    def test_bucket_creation_and_deletion(self, c_scope_s3client):
        """
        Test bucket creation and deletion via S3

        Args:
            class_scope_s3_client(S3Client): s3 client object

        """
        bucket_name = c_scope_s3client.create_bucket()
        assert bucket_name in c_scope_s3client.list_buckets(), "Bucket was not created"
        c_scope_s3client.delete_bucket(bucket_name)
        assert (
            bucket_name not in c_scope_s3client.list_buckets()
        ), "Bucket was not deleted"

    def test_expected_bucket_creation_failures(
        self, c_scope_s3client, account_manager, s3_client_factory
    ):
        """
        Test bucket creation scenarios that are expected to fail

        """
        # Test creating a bucket with the name of a bucket that already exists
        log.info("Creating a bucket with a name that already exists")
        bucket_name = c_scope_s3client.create_bucket()
        with pytest.raises(BucketAlreadyExistsException):
            c_scope_s3client.create_bucket(bucket_name)
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

    def test_expected_bucket_deletion_failures(self, c_scope_s3client):
        """
        Test bucket deletion scenarios that are expected to fail

        """
        # Test deleting a non existing bucket
        log.info("Deleting a non existing bucket")
        with pytest.raises(NoSuchBucketException):
            c_scope_s3client.delete_bucket("non_existing_bucket")
            log.error("Bucket deletion of non existing bucket succeeded")
        log.info("Bucket deletion of non existing bucket failed as expected")

        # Test deleting a non empty bucket
        log.info("Deleting a non empty bucket")
        with pytest.raises(BucketNotEmptyException):
            bucket_name = c_scope_s3client.create_bucket()
            c_scope_s3client.put_random_objects(bucket_name, amount=1)
            c_scope_s3client.delete_bucket(bucket_name)
            log.error("Bucket deletion of non empty bucket succeeded")
        log.info("Bucket deletion of non empty bucket failed as expected")

    def test_list_buckets(self, c_scope_s3client):
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
                buckets.append(c_scope_s3client.create_bucket())

            listed_buckets = c_scope_s3client.list_buckets()
            assert all(
                bucket in listed_buckets for bucket in buckets
            ), "Created bucket was not listed!"

            log.info("Deleting one of the buckets")
            c_scope_s3client.delete_bucket(buckets[-1])
            listed_buckets = c_scope_s3client.list_buckets()
            assert (
                buckets[-1] not in listed_buckets
            ), "Deleted bucket was still listed post deletion!"
            assert all(
                bucket in listed_buckets for bucket in buckets[:-1]
            ), "Non deleted buckets were not listed post bucket deletion"

            log.info(f"Deleting the remaining {AMOUNT - 1} buckets")
            for i in range(AMOUNT - 1):
                c_scope_s3client.delete_bucket(buckets[i])

            assert all(
                bucket not in c_scope_s3client.list_buckets() for bucket in buckets
            ), "Some buckets that were deleted were still listed"

        except AssertionError as e:
            log.error(f"Created buckets: {buckets}")
            log.error(f"Listed buckets: {listed_buckets}")
            raise e

    def test_head_bucket(self, c_scope_s3client):
        """
        Test S3 HeadBucket operation

        Args:
            class_scope_s3_client(S3Client): s3 client object

        """
        # Test whether the head bucket operations succeeds for a newly created bucket
        bucket = c_scope_s3client.create_bucket()
        assert c_scope_s3client.head_bucket(bucket) == True, (
            "Head bucket operation failed for a newly created bucket",
        )

        # Test whether the head bucket operation fails for a non existing bucket
        assert c_scope_s3client.head_bucket("non_existing_bucket") == False, (
            "Head bucket operation succeeded for non existing bucket",
        )

    @pytest.mark.parametrize("use_v2", [False, True])
    def test_list_objects(self, c_scope_s3client, tmp_directories_factory, use_v2):
        """
        Test S3 ListObjects and S3 ListObjectsV2 operations

        Args:
            class_scope_s3_client(S3Client): s3 client object
            tmp_directories_factory: tmp directories factory object
            use_v2(bool): whether to use ListObjectsV2

        """
        origin_dir = tmp_directories_factory(dirs_to_create=["origin"])[0]
        bucket = c_scope_s3client.create_bucket()

        written_objs_names = c_scope_s3client.put_random_objects(
            bucket, amount=5, min_size="1M", max_size="2M", files_dir=origin_dir
        )
        listed_objs_md_dicts = c_scope_s3client.list_objects(
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

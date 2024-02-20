from datetime import datetime, timedelta, timezone
import logging
import os

import pytest
from common_ci_utils.random_utils import (
    generate_random_hex,
    generate_unique_resource_name,
)
from common_ci_utils.file_system_utils import compare_md5sums
from noobaa_sa.exceptions import (
    NoSuchBucketException,
    NoSuchKeyException,
)

from utility.nsfs_server_utils import *

log = logging.getLogger(__name__)


class TestS3ObjectOperations:
    """
    Test S3 object operations using NSFS
    """

    def test_put_and_get_obj(self, c_scope_s3client):
        """
        Test S3 PutObject and GetObject operations

        """
        bucket = c_scope_s3client.create_bucket()
        put_obj_contents = generate_random_hex(500)
        c_scope_s3client.put_object(bucket, "random_str.txt", body=put_obj_contents)
        response = c_scope_s3client.get_object(bucket, "random_str.txt")
        get_obj_contents = response["Body"].read().decode("utf-8")
        assert (
            put_obj_contents == get_obj_contents
        ), "Retrieved object content does not match"

    def test_delete_object(self, c_scope_s3client):
        """
        Test S3 DeleteObject operation

        """
        bucket = c_scope_s3client.create_bucket()
        written_objects = c_scope_s3client.put_random_objects(bucket, amount=10)
        c_scope_s3client.delete_object(bucket, written_objects[0])
        post_deletion_objects = c_scope_s3client.list_objects(bucket)

        assert (
            written_objects[0] not in post_deletion_objects
        ), "Deleted object was still listed post deletion"

        assert (
            written_objects[1:] == post_deletion_objects
        ), "Non deleted objects were not listed post deletion"

    def test_delete_objects(self, c_scope_s3client):
        """
        Test S3 DeleteObjects operation

        """
        bucket = c_scope_s3client.create_bucket()
        written_objects = c_scope_s3client.put_random_objects(bucket, amount=10)
        c_scope_s3client.delete_objects(bucket, written_objects[:5])
        post_deletion_objects = c_scope_s3client.list_objects(bucket)

        assert all(
            obj not in post_deletion_objects for obj in written_objects[:5]
        ), "Deleted objects were still listed post deletion"

        assert all(
            obj in post_deletion_objects for obj in written_objects[5:]
        ), "Non deleted objects were not listed post deletion"

    def test_delete_all_objects_in_bucket(self, c_scope_s3client):
        """
        Test deleting all objects in a bucket via S3

        """
        bucket = c_scope_s3client.create_bucket()
        c_scope_s3client.put_random_objects(bucket, amount=10)
        c_scope_s3client.delete_all_objects_in_bucket(bucket)
        assert (
            len(c_scope_s3client.list_objects(bucket)) == 0
        ), "Bucket is not empty after attempting to delete all objects"

    def test_expected_put_and_get_failures(self, c_scope_s3client):
        """
        Test S3 PutObject and GetObject operations that are expected to fail

        """
        bucket = c_scope_s3client.create_bucket()

        # Test putting an object to a non existing bucket
        log.info("Putting an object to a non existing bucket")
        with pytest.raises(NoSuchBucketException):
            c_scope_s3client.put_object(
                bucket_name="non-existant-bucket", object_key="obj", body="body"
            )
            log.error("Putting an object to a non existing bucket succeeded")
        log.info("Putting an object to a non existing bucket failed as expected")

        # Test getting a non existing object
        log.info("Getting a non existing object")
        with pytest.raises(NoSuchKeyException):
            c_scope_s3client.get_object(bucket, "non_existing_obj")
            log.error("Getting a non existing object succeeded")
        log.info("Getting a non existing object failed as expected")

    def test_copy_object(self, c_scope_s3client):
        """
        Test S3 CopyObject operation

        """
        bucket_a = c_scope_s3client.create_bucket()
        bucket_b = c_scope_s3client.create_bucket()

        obj_name = generate_unique_resource_name(prefix="obj")
        obj_data_body = generate_random_hex(500)
        c_scope_s3client.put_object(bucket_a, obj_name, body=obj_data_body)

        # Test copying an object from the bucket to itself
        c_scope_s3client.copy_object(
            src_bucket=bucket_a,
            src_key=obj_name,
            dest_bucket=bucket_a,
            dest_key=obj_name,
        )
        copied_obj_data = (
            c_scope_s3client.get_object(bucket_a, obj_name)["Body"]
            .read()
            .decode("utf-8")
        )
        assert obj_data_body == copied_obj_data, "Copied object content does not match"

        # Test copying an object from one bucket to another
        c_scope_s3client.copy_object(
            src_bucket=bucket_a,
            src_key=obj_name,
            dest_bucket=bucket_b,
            dest_key=obj_name,
        )
        copied_obj_data = (
            c_scope_s3client.get_object(bucket_b, obj_name)["Body"]
            .read()
            .decode("utf-8")
        )
        assert obj_data_body == copied_obj_data, "Copied object content does not match"

    def test_data_integrity(self, c_scope_s3client, tmp_directories_factory):
        """
        Test data integrity of objects written and read via S3

        Args:
            class_scope_s3_client(S3Client): s3 client object
            tmp_directories_factory: tmp directories factory object

        """
        origin_dir, results_dir = tmp_directories_factory(
            dirs_to_create=["origin", "result"]
        )
        bucket = c_scope_s3client.create_bucket()
        original_objs_names = c_scope_s3client.put_random_objects(
            bucket, amount=10, min_size="1M", max_size="2M", files_dir=origin_dir
        )
        c_scope_s3client.download_bucket_contents(bucket, results_dir)
        downloaded_objs_names = os.listdir(results_dir)
        assert len(original_objs_names) == len(
            downloaded_objs_names
        ), "Downloaded and original objects count does not match"
        original_objs_names.sort()
        downloaded_objs_names.sort()
        for original, downloaded in zip(original_objs_names, downloaded_objs_names):
            original_full_path = os.path.join(origin_dir, original)
            downloaded_full_path = os.path.join(results_dir, downloaded)
            md5sums_match = compare_md5sums(original_full_path, downloaded_full_path)
            assert md5sums_match, f"MD5 sums do not match for {original}"


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

#     # 3. Write objects to the bucket
#     original_objs_names = s3_client.put_random_objects(
#         bucket_name, amount=10, min_size="1M", max_size="2M", files_dir=origin_dir
#     )

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

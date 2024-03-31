import logging

from common_ci_utils.random_utils import generate_unique_resource_name

log = logging.getLogger(__name__)


class TestS3BucketOperations:
    """
    Test S3 bucket operations using NSFS
    """

    def test_bucket_creation_deletion_and_head(self, c_scope_s3client):
        """
        Test bucket creation and deletion via S3:
        1. Create a bucket via S3
        2. Verify the bucket was created via S3 HeadBucket
        3. Delete the bucket via S3
        4. Verify the bucket was deleted via S3 HeadBucket

        """
        # 1. Create a bucket via S3
        bucket_name = generate_unique_resource_name(prefix="bucket")
        response = c_scope_s3client.create_bucket(bucket_name, get_response=True)
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), f"create_bucket failed with an unexpected response: {response}"

        # 2. Verify the bucket was created via S3 HeadBucket
        response = c_scope_s3client.head_bucket(bucket_name)
        assert response["Code"] == 200, "Bucket was not created"

        # 3. Delete the bucket via S3
        response = c_scope_s3client.delete_bucket(bucket_name)
        assert (
            response["Code"] == 204
        ), f"delete_bucket failed with an unexpected response: {response}"

        # 4. Verify the bucket was deleted via S3 HeadBucket
        response = c_scope_s3client.head_bucket(bucket_name)
        assert response["Code"] == 404, "Bucket was not deleted"

    def test_list_buckets(self, c_scope_s3client):
        """
        Test listing buckets before creation and after deletion via S3

        """
        buckets, listed_buckets = [], []
        AMOUNT = 5
        try:
            for _ in range(AMOUNT):
                buckets.append(c_scope_s3client.create_bucket())

            listed_buckets = c_scope_s3client.list_buckets()

            # listed_buckets might contain buckets from before the test
            assert set(buckets).issubset(
                set(listed_buckets)
            ), "Created buckets were not listed!"

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

            listed_buckets = c_scope_s3client.list_buckets()
            assert all(
                bucket not in listed_buckets for bucket in buckets
            ), "Some buckets that were supposed to be deleted were still listed"

        except AssertionError as e:
            log.error(f"Created buckets: {buckets}")
            log.error(f"Listed buckets: {listed_buckets}")
            raise e

    def test_expected_bucket_creation_failures(
        self, c_scope_s3client, account_manager, s3_client_factory
    ):
        """
        Test bucket creation scenarios that are expected to fail:
        1. Test creating a bucket with the name of a bucket that already exists
        2. Test creating a bucket using the credentials of a user that's not allowed to create buckets

        """
        # 1. Test creating a bucket with the name of a bucket that already exists
        bucket_name = c_scope_s3client.create_bucket()
        response = c_scope_s3client.create_bucket(bucket_name, get_response=True)
        assert (
            response["Code"] == "BucketAlreadyExists"
        ), "Bucket creation did not fail with the expected error"

        # 2. Test creating a bucket using the credentials of a user that's not allowed to create buckets

        # TODO: Comment out once https://bugzilla.redhat.com/show_bug.cgi?id=2262992 is fixed
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
        #     log.error("Attempting to create a bucket with restricted credentials did not fail as expected")

    def test_expected_bucket_deletion_failures(self, c_scope_s3client):
        """
        Test bucket deletion scenarios that are expected to fail:
        1. Test deleting a non existing bucket
        2. Test deleting a non empty bucket

        """
        # 1. Test deleting a non existing bucket
        response = c_scope_s3client.delete_bucket("non_existing_bucket")
        assert response["Code"] == "NoSuchBucket", (
            "Attempting to delete a non existing bucket did not fail as expected",
            response,
        )

        # 2. Test deleting a non empty bucket
        bucket_name = c_scope_s3client.create_bucket()
        c_scope_s3client.put_random_objects(bucket_name, amount=1)
        response = c_scope_s3client.delete_bucket(bucket_name)
        assert response["Code"] == "BucketNotEmpty", (
            "Attempting to delete a non empty bucket did not fail as expected",
            response,
        )

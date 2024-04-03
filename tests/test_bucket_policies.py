import copy
import json
import logging

import pytest

log = logging.getLogger(__name__)


class TestBucketPolicies:
    """
    Test S3 bucket policies
    """

    @pytest.fixture(scope="function")
    def bucket_policy_setup(self, c_scope_s3client):
        """
        Create a bucket and a template policy that refers it

        Returns:
            tuple: bucket (str), policy (dict)

        """
        bucket = c_scope_s3client.create_bucket()
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket}/*",
                }
            ],
        }
        return bucket, policy

    def test_bucket_policy_put_get_delete(self, c_scope_s3client, bucket_policy_setup):
        bucket, policy = bucket_policy_setup

        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        log.info(f"Result of put_bucket_policy: {json.dumps(response, indent=4)}")
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), "put_bucket_policy failed"

        response = c_scope_s3client.get_bucket_policy(bucket)
        log.info(f"Result of get_bucket_policy: {json.dumps(response, indent=4)}")
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), "get_bucket_policy failed"

        response = c_scope_s3client.delete_bucket_policy(bucket)
        log.info(f"Result of delete_bucket_policy: {json.dumps(response, indent=4)}")
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 204
        ), "delete_bucket_policy failed"

    @pytest.mark.parametrize(
        "invalidate",
        [
            (lambda d, d_key: d.pop(d_key)),
            (lambda d, d_key: d.update({d_key: "invalid_value"})),
        ],
        ids=["remove_property", "invalid_value"],
    )
    @pytest.mark.parametrize(
        "property_to_invalidate",
        [
            "Effect",
            "Principal",
            "Action",
            "Resource",
        ],
    )
    def test_put_bucket_policy_with_invalid_policies(
        self, c_scope_s3client, bucket_policy_setup, invalidate, property_to_invalidate
    ):
        """
        Test applying invalid bucket policies by either
        removing a property or setting it to an invalid value:

        1. Create an invalid policy
        2. Apply the invalid policy and check that it fails
        3. Check that the bucket is still usable
        4. Apply a valid policy and check that it works

        """
        bucket, valid_bucket_policy = bucket_policy_setup

        # 1. Create an invalid policy
        invalid_policy = copy.deepcopy(valid_bucket_policy)
        invalidate(invalid_policy["Statement"][0], property_to_invalidate)

        # 2. Apply the invalid policy and check that it fails
        response = c_scope_s3client.put_bucket_policy(bucket, invalid_policy)
        log.info(f"Result of put_bucket_policy: {json.dumps(response, indent=4)}")
        assert (
            response["Code"] == 400 or response["Code"] == "MalformedPolicy"
        ), "put_bucket_policy with invalid policy did not fail"

        # 3. Check that the bucket is still usable
        response = c_scope_s3client.put_random_objects(bucket, amount=5)
        assert response["Code"] == 200, "put_random_objects failed"

        response = c_scope_s3client.list_objects(bucket)
        assert response["Code"] == 200, "list_objects failed"

        # 4. Apply a valid policy and check that it works
        response = c_scope_s3client.put_bucket_policy(bucket, valid_bucket_policy)
        assert response["Code"] == 200, "get_bucket_policy failed"

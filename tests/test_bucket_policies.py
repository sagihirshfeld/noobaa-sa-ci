import logging
import json
import copy
import time
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

    @pytest.mark.parametrize(
        "action",
        [
            (
                "s3:GetObject",
                lambda s3_client, bucket: s3_client.get_object(bucket, "test_obj_0"),
            ),
            (
                "s3:PutObject",
                lambda s3_client, bucket: s3_client.put_random_objects(
                    bucket, amount=5
                ),
            ),
            (
                "s3:ListBucket",
                lambda s3_client, bucket: s3_client.list_objects(bucket),
            ),
            (
                "s3:DeleteObject",
                lambda s3_client, bucket: s3_client.delete_object(bucket, "test_obj_0"),
            ),
        ],
        ids=["GetObject", "PutObject", "ListBucket", "DeleteObject"],
    )
    def test_deny_principal(
        self,
        c_scope_s3client,
        account_manager,
        s3_client_factory,
        bucket_policy_setup,
        action,
    ):
        bucket, policy = bucket_policy_setup
        denied_acc_name, denied_access_key, denied_secret_key = account_manager.create()
        other_acc_client = s3_client_factory(
            access_and_secret_keys_tuple=(denied_access_key, denied_secret_key)
        )

        action, action_validation_func = action

        # Make sure the bucket contains an object with an expected key
        prereq_obj_name = "test_obj_0"
        random_obj = c_scope_s3client.put_random_objects(bucket, amount=1)[0]
        c_scope_s3client.copy_object(bucket, random_obj, bucket, prereq_obj_name)

        # 1. Apply a policy that denies access to the denied account
        policy["Statement"][0]["Effect"] = "Deny"
        policy["Statement"][0]["Action"] = action
        policy["Statement"][0]["Principal"] = "*"
        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # Wait for the policy to take effect
        time.sleep(60)

        # 2. Check that the original account can still access the bucket
        response = action_validation_func(c_scope_s3client, bucket)
        assert response["Code"] == 200, f"{action} failed for the original account"

        # 3. Check that the denied account cannot access the bucket
        response = action_validation_func(other_acc_client, bucket)
        assert response["Code"] == 403, f"{action} succeeded for the denied account"

import copy
import json
import logging

import pytest

log = logging.getLogger(__name__)


class TestBucketPolicies:
    """
    Test S3 bucket policies
    """

    default_test_obj_name = "test_obj_0"
    default_content = "test_content"

    op_dicts = {
        "GetObject": {
            "action": "s3:GetObject",
            "resource_template": "arn:aws:s3:::{bucket}/*",
            "validation_func": lambda s3_client, bucket: s3_client.get_object(
                bucket, TestBucketPolicies.default_test_obj_name
            ),
            "success_code": 200,
        },
        "PutObject": {
            "action": "s3:PutObject",
            "resource_template": "arn:aws:s3:::{bucket}/*",
            "validation_func": lambda s3_client, bucket: s3_client.put_object(
                bucket,
                TestBucketPolicies.default_test_obj_name,
                TestBucketPolicies.default_content,
            ),
            "success_code": 200,
        },
        "ListBucket": {
            "action": "s3:ListBucket",
            "resource_template": "arn:aws:s3:::{bucket}",
            "validation_func": lambda s3_client, bucket: s3_client.list_objects(
                bucket, get_response=True
            ),
            "success_code": 200,
        },
        "DeleteObject": {
            "action": "s3:DeleteObject",
            "resource_template": "arn:aws:s3:::{bucket}/*",
            "validation_func": lambda s3_client, bucket: s3_client.delete_object(
                bucket, TestBucketPolicies.default_test_obj_name
            ),
            "success_code": 204,
        },
        "DeleteBucket": {
            "action": "s3:DeleteBucket",
            "resource_template": "arn:aws:s3:::{bucket}",
            "validation_func": lambda s3_client, bucket: s3_client.delete_bucket(
                bucket
            ),
            "success_code": 204,
        },
    }

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
        "subject_op",
        [
            "GetObject",
            "PutObject",
            "ListBucket",
            "DeleteObject",
            "DeleteBucket",
        ],
    )
    def test_default_denial_of_other_accounts(
        self, c_scope_s3client, s3_client_factory, subject_op
    ):
        """
        Test that different accounts can't operate on each other's buckets by default

        """
        # Create a new S3Client of a new account
        other_acc_s3_client = s3_client_factory()

        # Create a new bucket using the client of one account
        bucket = c_scope_s3client.create_bucket()

        # Check that the new account can't operate on the bucket by default
        response = self.op_dicts[subject_op]["validation_func"](
            other_acc_s3_client, bucket
        )
        assert response["Code"] == "AccessDenied"

    @pytest.mark.parametrize(
        "subject_op",
        [
            "GetObject",
            "PutObject",
            "ListBucket",
            "DeleteObject",
        ],
    )
    def test_allow_principal(
        self,
        c_scope_s3client,
        account_manager,
        s3_client_factory,
        bucket_policy_setup,
        subject_op,
    ):
        # Unpack fixture data
        bucket, policy = bucket_policy_setup
        subject_op_dict = self.op_dicts[subject_op]
        action = subject_op_dict["action"]
        resource_template = subject_op_dict["resource_template"]
        validation_func = subject_op_dict["validation_func"]
        expected_code = subject_op_dict["success_code"]

        # Create a new S3Client instance a new account's credentials
        new_account, new_acc_access_key, new_acc_secret_key = account_manager.create()
        other_acc_client = s3_client_factory(
            access_and_secret_keys_tuple=(new_acc_access_key, new_acc_secret_key)
        )

        # Make sure the bucket contains an object with an expected key
        random_obj = c_scope_s3client.put_random_objects(bucket, amount=1)[0]
        c_scope_s3client.copy_object(
            bucket, random_obj, bucket, self.default_test_obj_name
        )

        # 1. Apply a policy that allows the operation for the new account
        policy["Statement"][0].update(
            {
                "Effect": "Allow",
                "Action": action,
                "Principal": {"AWS": new_account},
                "Resource": resource_template.format(bucket=bucket),
            }
        )
        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that the original account can still perform the operation
        response = validation_func(c_scope_s3client, bucket)
        assert (
            response["Code"] == expected_code
        ), f"{action} failed for the original account"

        # 3. Check that the new account can perform the operation
        response = validation_func(other_acc_client, bucket)
        assert response["Code"] == expected_code, f"{action} failed for the new account"

        # 4. Check that the new account still can't perform the other operations
        other_ops = [op for op in self.op_dicts.keys() if op != op]
        for other_op in other_ops:
            other_op_dict = self.op_dicts[other_op]
            other_op_validation_func = other_op_dict["validation_func"]
            assert (
                other_op_validation_func(other_acc_client, bucket)["Code"]
                == "AccessDenied"
            )

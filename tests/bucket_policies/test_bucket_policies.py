import copy
import json
import logging

import pytest

from framework.bucket_policies.bucket_policy import BucketPolicy, BucketPolicyBuilder
from framework.bucket_policies.s3_operation_access_tester import S3OperationAccessTester
from noobaa_sa import constants

log = logging.getLogger(__name__)


class TestBucketPolicies:
    """
    Test the Bucket Policies feature

    """

    def test_bucket_policy_put_get_delete(self, c_scope_s3client):
        """
        Test the basic s3 bucket policy operations using a valid policy template:
        1. Put a bucket policy and verify the response
        2. Get the bucket policy and verify the response
        3. Delete the bucket policy and verify the response

        """
        bucket = c_scope_s3client.create_bucket()
        policy = BucketPolicy.default_template()

        # 1. Put a bucket policy and verify the response
        response = c_scope_s3client.put_bucket_policy(bucket, str(policy))
        log.info(f"Result of put_bucket_policy: {json.dumps(response, indent=4)}")
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), "put_bucket_policy failed"

        # 2. Get the bucket policy and verify the response
        response = c_scope_s3client.get_bucket_policy(bucket)
        log.info(f"Result of get_bucket_policy: {json.dumps(response, indent=4)}")
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), "get_bucket_policy failed"

        # 3. Delete the bucket policy and verify the response
        response = c_scope_s3client.delete_bucket_policy(bucket)
        log.info(f"Result of delete_bucket_policy: {json.dumps(response, indent=4)}")
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 204
        ), "delete_bucket_policy failed"

    @pytest.mark.parametrize(
        "invalidate",
        [
            (lambda p, d_key: p.statements[0].pop(d_key)),
            (lambda p, d_key: p.statements[0].update({d_key: "invalid_value"})),
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
        self, c_scope_s3client, invalidate, property_to_invalidate
    ):
        """
        Test applying invalid bucket policies by either
        removing a property or setting it to an invalid value:
        1. Create an invalid policy
        2. Apply the invalid policy and check that it fails
        3. Check that the bucket is still usable
        4. Apply a valid policy and check that it works

        """
        bucket = c_scope_s3client.create_bucket()
        valid_bucket_policy = BucketPolicy.default_template()

        # 1. Create an invalid policy
        invalid_policy = copy.deepcopy(valid_bucket_policy)
        invalidate(invalid_policy, property_to_invalidate)

        # 2. Apply the invalid policy and check that it fails
        response = c_scope_s3client.put_bucket_policy(bucket, str(invalid_policy))
        log.info(f"Result of put_bucket_policy: {json.dumps(response, indent=4)}")
        assert (
            response["Code"] == "MalformedPolicy"
        ), "put_bucket_policy with invalid policy did not fail"

        # 3. Check that the bucket is still usable
        response = c_scope_s3client.put_object(bucket, "test_obj", "test_data")
        assert response["Code"] == 200, "put_object failed"

        response = c_scope_s3client.list_objects(bucket, get_response=True)
        assert response["Code"] == 200, "list_objects failed"

        # 4. Apply a valid policy and check that it works
        response = c_scope_s3client.put_bucket_policy(bucket, str(valid_bucket_policy))
        assert response["Code"] == 200, "get_bucket_policy failed"

    @pytest.mark.parametrize(
        "operation",
        [
            "GetObject",
            "HeadObject",
            "PutObject",
            "CopyObject",
            "ListBucket",
            "DeleteObject",
            "DeleteBucket",
        ],
    )
    def test_default_denial_of_other_accounts(
        self, c_scope_s3client, s3_client_factory, operation
    ):
        """
        Test that different accounts can't operate on each other's buckets by default:
        1. Create a new S3Client of a new account
        2. Create a new bucket using the client of one account
        3. Check that the new account can't operate on the bucket by default

        """
        # 1. Create a new S3Client of a new account
        other_acc_s3_client = s3_client_factory()

        # 2. Create a new bucket using the client of one account
        bucket = c_scope_s3client.create_bucket()

        # 3. Check that the new account can't operate on the bucket by default
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )
        assert not access_tester.check_client_access_to_bucket_op(
            other_acc_s3_client, bucket, operation
        ), f"{operation} was allowed for a different account by default"

    @pytest.mark.parametrize(
        "access_effect",
        [
            "Allow",
            "Deny",
        ],
    )
    @pytest.mark.parametrize(
        "tested_op",
        [
            "GetObject",
            "PutObject",
            "DeleteObject",
            "ListBucket",
            "PutBucketPolicy",
            "GetBucketPolicy",
        ],
    )
    def test_operation_access_policies(
        self,
        c_scope_s3client,
        s3_client_factory,
        tested_op,
        access_effect,
    ):
        """
        Test the use of Allow or Deny statements in bucket policies on specific operations:
        1. Create a bucket using the first account
        2. Create a new account
        3. Apply a policy that allows or denies only the tested operation to the new account
        4. Check that the original account can still perform the operation
        5. Check that the new account is allowed/denied the operation
        6. Check that the new account has the original access for other operations

        """
        # 1. Create a bucket using the first account
        bucket = c_scope_s3client.create_bucket()

        # 2. Create a new account
        new_acc_client = s3_client_factory()

        # 3. Apply a policy that allows or denies only the tested operation to the new account
        bpb = BucketPolicyBuilder()
        if access_effect == "Deny":
            # Allow all operations by default
            bpb = (
                bpb.add_allow_statement()
                .add_principal("*")
                .add_action("*")
                .add_resource("*")
                .add_resource(f"{bucket}/*")
            )
            # Start building a deny policy
            bpb.add_deny_statement()
        else:
            # Start building an allow policy
            bpb.add_allow_statement()

        # Finish building the policy for the tested operation
        bpb = (
            bpb.add_action(tested_op)
            .add_principal("*")
            .add_resource(
                bucket if tested_op in constants.BUCKET_OPERATIONS else f"{bucket}/*"
            )
        )
        policy = bpb.build()

        # Apply the policy
        response = c_scope_s3client.put_bucket_policy(bucket, str(policy))
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )

        # 4. Check that the original account can still perform the operation
        assert access_tester.check_client_access_to_bucket_op(
            c_scope_s3client, bucket, tested_op
        ), f"{tested_op} was denied for the original account"

        # 5. Check that the new account is allowed/denied the operation
        expected_access_to_op = access_effect == "Allow"
        can_access = access_tester.check_client_access_to_bucket_op(
            new_acc_client, bucket, tested_op
        )
        assert (
            can_access == expected_access_to_op
        ), f"{tested_op} was {'denied' if not can_access else 'allowed'} for the new account"

        # 6. Check that the new account has the original access for other operations
        other_ops = {
            "GetObject",
            "PutObject",
            "DeleteObject",
            "CopyObject",
            "ListBucket",
        }
        # Other operations should have the opposite access
        expected_access_to_op = not expected_access_to_op

        # Omit operations that have overlapping permissions with the tested operation
        other_ops -= {tested_op}
        other_ops -= set(BucketPolicy.get_ops_with_perm_overlap(tested_op))

        for op in other_ops:
            can_access = access_tester.check_client_access_to_bucket_op(
                new_acc_client, bucket, op
            )
            assert can_access == expected_access_to_op, (
                f"{op} was {'denied' if not can_access else 'allowed'}"
                f" for the new account after only allowing {tested_op}"
            )

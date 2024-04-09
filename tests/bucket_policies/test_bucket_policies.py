import copy
import json
import logging

import pytest

from noobaa_sa import constants
from noobaa_sa.bucket_policy import BucketPolicy, BucketPolicyBuilder
from tests.bucket_policies.s3_operation_access_tester import S3OperationAccessTester

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
        response = c_scope_s3client.put_bucket_policy(bucket, policy)
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
        valid_bucket_policy = BucketPolicy.build_default_template()

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
        response = c_scope_s3client.put_object(bucket, "test_obj", "test_data")
        assert response["Code"] == 200, "put_object failed"

        response = c_scope_s3client.list_objects(bucket, get_response=True)
        assert response["Code"] == 200, "list_objects failed"

        # 4. Apply a valid policy and check that it works
        response = c_scope_s3client.put_bucket_policy(bucket, valid_bucket_policy)
        assert response["Code"] == 200, "get_bucket_policy failed"

    @pytest.mark.parametrize(
        "operation",
        [
            "GetObject",
            "PutObject",
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
        "operation",
        [
            "GetObject",
            "PutObject",
            "ListBucket",
            "PutBucketPolicy",
            "GetBucketPolicy",
            "DeleteBucketPolicy",
        ],
    )
    def test_allow_action(
        self,
        c_scope_s3client,
        account_manager,
        s3_client_factory,
        operation,
    ):
        bucket = c_scope_s3client.create_bucket()

        # Create a new S3Client instance using a new account's credentials
        tested_acc_name, new_acc_access_key, new_acc_secret_key = (
            account_manager.create()
        )
        tested_client = s3_client_factory(
            access_and_secret_keys_tuple=(new_acc_access_key, new_acc_secret_key)
        )

        # 1. Apply a policy that allows the operation for the new account
        policy = (
            BucketPolicyBuilder()
            .add_allow_statement()
            .on_action(operation)
            .for_principal(tested_acc_name)
            .with_resource(
                bucket if operation in constants.BUCKET_OPERATIONS else f"{bucket}/*"
            )
            .build()
        )

        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that the original account can still perform the operation
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )
        assert access_tester.check_client_access_to_bucket_op(
            c_scope_s3client, bucket, operation
        ), f"{operation} was denied for the original account"

        # 3. Check that tested account can perform the operation
        assert access_tester.check_client_access_to_bucket_op(
            tested_client, bucket, operation
        ), f"{operation} was denied for the new account"

        # 4. Check that the tested account still can't perform the other operations
        other_ops = [op for op in self.op_dicts.keys() if op != op]
        for other_op in other_ops:
            assert not access_tester.check_client_access_to_bucket_op(
                tested_client, bucket, other_op
            ), f"{other_op} was allowed for the new account after only allowing {operation}"

    @pytest.mark.parametrize(
        "operation",
        [
            "GetObject",
            "PutObject",
            "ListBucket",
            "DeleteObject",
            "PutBucketPolicy",
            "GetBucketPolicy",
            "DeleteBucketPolicy",
        ],
    )
    def test_deny_action(
        self,
        account_manager,
        s3_client_factory,
        operation,
    ):
        # Create a new S3Client instance using a new account's credentials
        acc_name, access_key, secret_key = account_manager.create()
        s3_client = s3_client_factory(
            access_and_secret_keys_tuple=(access_key, secret_key)
        )

        bucket = s3_client.create_bucket()
        access_tester = S3OperationAccessTester(
            admin_client=s3_client,
        )

        # 1. Apply a policy that denies an account from performing the operation
        # on a bucket it owns
        policy = (
            BucketPolicyBuilder()
            .add_deny_statement()
            .on_action(operation)
            .for_principal(acc_name)
            .with_resource(
                bucket if operation in constants.BUCKET_OPERATIONS else f"{bucket}/*"
            )
            .build()
        )
        response = s3_client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that the account can't perform the operation
        assert not access_tester.check_client_access_to_bucket_op(
            s3_client, bucket, operation
        ), f"{operation} was allowed for the account"

        # 3. Check that the account can still perform the other operations
        other_ops = [op for op in self.op_dicts.keys() if op != op]
        for other_op in other_ops:
            assert access_tester.check_client_access_to_bucket_op(
                s3_client, bucket, other_op
            ), f"{other_op} was not allowed for the owning account after only denying {operation}"

    @pytest.mark.parametrize(
        "access_effect",
        [
            "Allow",
            "Deny",
        ],
    )
    def test_resource_access(self, c_scope_s3client, s3_client_factory, access_effect):
        bucket = c_scope_s3client.create_bucket()
        test_objs = c_scope_s3client.put_random_objects(bucket, 2)

        bpb = BucketPolicyBuilder()
        if access_effect == "Allow":
            # Use a new account which is denied all access to the bucket by default
            tested_client = s3_client_factory()
            # Start buildng an allow policy
            bpb.add_allow_statement()
        else:
            # Use the account that owns the bucket which is allowed all access by default
            tested_client = c_scope_s3client
            # Start building a deny policy
            bpb.add_deny_statement()

        # 1. Modify access to the first object
        # Finish building the policy
        policy = (
            bpb.on_action("*").for_principal("*").with_resource(test_objs[0]).build()
        )

        # Apply the policy
        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that the account has the expected access for each object
        # The first object should have the applied access, and the second should have the opposite
        denial_expectations = {
            test_objs[0]: False if access_effect == "Allow" else True,
        }
        denial_expectation[test_objs[1]] = not denial_expectations[0]

        # Check the expected acces for each object per operation
        for obj in test_objs:
            denial_expectation = denial_expectations[obj]
            err_message = (
                f"Access was {'allowed' if denial_expectation else 'denied'} "
                "to the object when it shouldn't have been"
            )
            for op in [
                tested_client.get_object,
                tested_client.copy_object,
                tested_client.delete_object,
            ]:
                response = op(bucket, obj)
                access_denied = response["Code"] == "AccessDenied"
                assert access_denied == denial_expectation, err_message

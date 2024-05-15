from framework.bucket_policies.bucket_policy import BucketPolicyBuilder
from framework.bucket_policies.s3_operation_access_tester import S3OperationAccessTester


class TestNotBucketPolicies:
    """
    Test bucket policies that use the NotPrincipal, NotAction, and NotResource fields

    """

    def test_not_principal_bucket_policy(
        self, c_scope_s3client, account_manager, s3_client_factory
    ):
        """
        Test the NotPrincipal field in a bucket policy:
        1. Setup:
            1.1 Create a bucket using the admin client
            1.2 Create two accounts
        2. Allow all access to all principals except for the account specified by NotPrincipal
        3. Check that the allowed account can access the bucket
        4. Check that the denied account cannot access the bucket

        """
        # 1. Setup
        bucket = c_scope_s3client.create_bucket()
        allowed_client = s3_client_factory()
        denied_acc_name, access_key, secret_key = account_manager.create()
        denied_client = s3_client_factory(
            access_and_secret_keys_tuple=(access_key, secret_key)
        )

        # 2. Alllow all access to all principals except the account specified by NotPrincipal
        policy = (
            BucketPolicyBuilder()
            .add_allow_statement()
            .add_resource(f"{bucket}/*")
            .add_action("*")
            .add_not_principal(denied_acc_name)
            .build()
        )

        response = c_scope_s3client.put_bucket_policy(bucket, str(policy))
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 3. Check that the allowed account can access the bucket
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )
        assert access_tester.check_client_access_to_bucket_op(
            allowed_client, bucket, "GetObject"
        ), "Access was denied for the allowed account"

        # 4. Check that the denied account cannot access the bucket
        assert not access_tester.check_client_access_to_bucket_op(
            denied_client, bucket, "GetObject"
        ), "The denied account was allowed acces when it shouldn't have been"

    def test_not_action_bucket_policy(self, c_scope_s3client, s3_client_factory):
        """
        Test the NotAction field in a bucket policy:
        1. Setup:
            1.1 Create a bucket using the admin client
            1.2 Create a new account
        2. Allow all actions on the bucket's objects except for DeleteObject
        3. Check that the DeleteObject action is denied
        4. Check that other operations are allowed

        """
        # 1. Setup
        bucket = c_scope_s3client.create_bucket()
        new_acc_client = s3_client_factory()

        # 2. Allow all actions on the bucket's objects except for DeleteObject
        policy = (
            BucketPolicyBuilder()
            .add_allow_statement()
            .add_resource(f"{bucket}/*")
            .add_principal("*")
            .add_not_action("DeleteObject")
            .build()
        )

        response = c_scope_s3client.put_bucket_policy(bucket, str(policy))
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 3. Check that the DeleteObject action is denied
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )
        assert not access_tester.check_client_access_to_bucket_op(
            new_acc_client, bucket, "DeleteObject"
        ), "DeleteObject was allowed when it should have been denied"

        # 4. Check that other actions are allowed
        for op in ["GetObject", "PutObject"]:
            assert access_tester.check_client_access_to_bucket_op(
                new_acc_client, bucket, op
            ), f"{op} was denied when it shouldn't have been"

    def test_not_resource_bucket_policy(self, c_scope_s3client, s3_client_factory):
        """
        Test the NotResource field in a bucket policy:
        1. Setup:
            1.1 Create a bucket using the admin client
            1.2 Create a new account
        2. Allow access to all objects on the bucket except for the object specified by NotResource
        3. Check that the object specified by NotResource is still inaccessible
        4. Check that access to the other object is allowed

        """
        # 1. Setup
        bucket = c_scope_s3client.create_bucket()
        new_acc_client = s3_client_factory()
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )

        denied_obj, allowed_obj = c_scope_s3client.put_random_objects(bucket, 2)

        # 2. Allow access to all objects on the bucket except for the
        # object specified by NotResource
        policy = (
            BucketPolicyBuilder()
            .add_allow_statement()
            .add_action("*")
            .add_principal("*")
            .add_not_resource(f"{bucket}/{denied_obj}")
            .build()
        )

        response = c_scope_s3client.put_bucket_policy(bucket, str(policy))
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 3. Check that the object specified by NotResource is still inaccessible
        assert not access_tester.check_client_access_to_bucket_op(
            new_acc_client, bucket, "GetObject", obj_key=denied_obj
        ), f"Access to {denied_obj} was allowed when it should have been denied"

        # 4. Check that access to the other object is allowed
        assert access_tester.check_client_access_to_bucket_op(
            new_acc_client, bucket, "GetObject", obj_key=allowed_obj
        ), f"Acess to {allowed_obj} was denied when it should have been allowed"

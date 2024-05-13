from noobaa_sa.bucket_policy import BucketPolicyBuilder
from tests.bucket_policies.s3_operation_access_tester import S3OperationAccessTester


class TestNotBucketPolicies:
    """
    Test bucket policies that use the NotPrincipal, NotAction, and NotResource fields

    """

    def test_not_principal_bucket_policy(
        self, c_scope_s3client, account_manager, s3_client_factory
    ):
        bucket = c_scope_s3client.create_bucket()
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )

        denied_client = s3_client_factory()
        allowed_acc_name, access_key, secret_key = account_manager.create()
        allowed_client = s3_client_factory(
            access_and_secret_keys_tuple=(access_key, secret_key)
        )

        # 1. Deny all access to all principals except the account specified by NotPrincipal
        bpb = BucketPolicyBuilder()
        bpb.add_deny_statement().add_resource(f"{bucket}/*").add_action("*")
        bpb.add_not_principal(allowed_acc_name)
        policy = bpb.build()

        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that the allowed account can access the bucket
        assert access_tester.check_client_access_to_bucket_op(
            allowed_client, bucket, "GetObject"
        ), "GetObject was denied for the allowed account"

        # 3. Check that the denied account cannot access the bucket
        assert not access_tester.check_client_access_to_bucket_op(
            denied_client, bucket, "GetObject"
        ), "GetObject was allowed for the denied account when it shouldn't have been"

    def test_not_action_bucket_policy(self, c_scope_s3client):
        bucket = c_scope_s3client.create_bucket()
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )

        # 1. Deny all actions on the bucket's objects except for the
        # action specified by NotAction
        bpb = BucketPolicyBuilder()
        bpb.add_allow_statement().add_resource(f"{bucket}/*").add_principal("*")
        bpb.add_not_action("GetObject")
        policy = bpb.build()

        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that the GetObject action is still allowed
        assert access_tester.check_client_access_to_bucket_op(
            c_scope_s3client, bucket, "GetObject"
        ), "GetObject was denied when it should have been allowed"

        # 3. Check that other actions are denied
        for op in ["PutObject", "DeleteObject"]:
            assert not access_tester.check_client_access_to_bucket_op(
                c_scope_s3client, bucket, op
            ), f"{op} was allowed when it shouldn't have been"

    def test_not_resource_bucket_policy(self, c_scope_s3client):
        bucket = c_scope_s3client.create_bucket()
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )

        allowed_obj, denied_obj = c_scope_s3client.put_random_objects(bucket, 2)

        # 1. Deny access to all objects in the bucket except for the
        # object specified by NotResource
        bpb = BucketPolicyBuilder()
        bpb.add_allow_statement().add_action("GetObject").add_principal("*")
        bpb.add_not_resource(f"{bucket}/{allowed_obj}")
        policy = bpb.build()

        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that the object specified by NotResource is still accessible
        assert access_tester.check_client_access_to_bucket_op(
            c_scope_s3client, bucket, "GetObject", obj_key=allowed_obj
        ), f"Access to {allowed_obj} was denied when it should have been allowed"

        # 3. Check that access to the other object is denied
        assert not access_tester.check_client_access_to_bucket_op(
            c_scope_s3client, bucket, "GetObject", obj_key=denied_obj
        ), f"Acess to {denied_obj} was allowed when it shouldn't have been"

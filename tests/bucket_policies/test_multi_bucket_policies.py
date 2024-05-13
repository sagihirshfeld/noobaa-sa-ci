from noobaa_sa.bucket_policy import BucketPolicyBuilder
from tests.bucket_policies.s3_operation_access_tester import S3OperationAccessTester


class TestMultiBucketPolicies:
    def test_multi_statement_policy(self, c_scope_s3client, s3_client_factory):
        # Setup
        bucket = c_scope_s3client.create_bucket()
        denied_obj, allowed_obj = c_scope_s3client.put_random_objects(bucket, 2)

        bpb = BucketPolicyBuilder()
        # 1. Apply a multi-statement policy
        # Allow all accounts all access to bucket's objects
        bpb.add_allow_statement().add_action("*").add_principal("*").add_resource(
            f"{bucket}/*"
        )
        # Deny all access specifically to the test object
        bpb.add_deny_statement().add_action("*").add_principal("*").add_resource(
            denied_obj
        )

        # Apply the policy
        policy = bpb.build()
        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that the two statements are applied correctly
        # 2.1 Check that a new account can access the allowed object
        new_acc_client = s3_client_factory()
        response = new_acc_client.get_object(bucket, allowed_obj)
        assert (
            response["Code"] == 200
        ), "New account was denied access to the allowed object"

        # 2.2 Check that the original account can't access the denied object
        response = c_scope_s3client.get_object(bucket, denied_obj)
        assert (
            response["Code"] == "AccessDenied"
        ), "Original account was allowed access to the denied object"

    def test_multi_action_statement_policy(self, c_scope_s3client):
        bucket = c_scope_s3client.create_bucket()
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )

        tested_ops = ["GetObject", "PutObject", "DenyObject"]

        # 1. Apply a multi-action statement policy
        bpb = BucketPolicyBuilder()
        bpb.add_deny_statement().add_principal("*").add_resource(f"{bucket}/*")

        for op in tested_ops:
            bpb.add_action(op)

        policy = bpb.build()
        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        for op in tested_ops:
            assert not access_tester.check_client_access_to_bucket_op(
                c_scope_s3client, bucket, op
            ), f"{op} was allowed for the account when it shouldn't have been"

    def test_multi_resource_statement_policy(self, c_scope_s3client):
        bucket = c_scope_s3client.create_bucket()
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )

        tested_objs = c_scope_s3client.put_random_objects(bucket, 2)

        # 1. Apply a multi-resource statement policy
        bpb = BucketPolicyBuilder()
        bpb.add_deny_statement().add_principal("*").add_action("*")

        for obj in tested_objs:
            bpb.add_resource(obj)

        policy = bpb.build()
        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that access is denied to all objects
        for obj in tested_objs:
            assert not access_tester.check_client_access_to_bucket_op(
                c_scope_s3client, bucket, "GetObject", obj_key=obj
            ), f"Access was allowed to {obj} when it shouldn't have been"

    def test_multi_principal_statement_policy(
        self, c_scope_s3client, account_manager, s3_client_factory
    ):
        bucket = c_scope_s3client.create_bucket()
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )

        acc_a_name, access_key, secret_key = account_manager.create()
        acc_a_client = s3_client_factory(
            access_and_secret_keys_tuple=(access_key, secret_key)
        )
        acc_b_name, access_key, secret_key = account_manager.create()
        acc_b_client = s3_client_factory(
            access_and_secret_keys_tuple=(access_key, secret_key)
        )

        # 1. Apply a multi-principal statement policy
        bpb = BucketPolicyBuilder()
        bpb.add_deny_statement().add_action("*").add_resource(f"{bucket}/*")
        bpb.add_principal(acc_a_name).add_principal(acc_b_name)

        policy = bpb.build()
        response = c_scope_s3client.put_bucket_policy(bucket, policy)
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 2. Check that the two principals are denied access
        acc_names = [acc_a_name, acc_b_name]
        acc_clients = [acc_a_client, acc_b_client]
        for acc, s3_client in zip(acc_names, acc_clients):
            assert not access_tester.check_client_access_to_bucket_op(
                s3_client, bucket, acc, "GetObject"
            ), f"Access was allowed for account {acc} when it shouldn't have been"

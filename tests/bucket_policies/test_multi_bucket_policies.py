from framework.bucket_policies.bucket_policy import BucketPolicyBuilder
from framework.bucket_policies.s3_operation_access_tester import S3OperationAccessTester


class TestMultiBucketPolicies:
    def test_multi_statement_policy(self, c_scope_s3client, s3_client_factory):
        """
        Test multi-statement bucket policies:

        1. Setup:
            1.1 Create a bucket
            1.2 Put two objects in the bucket
            1.3 Create a new account
        2. Apply a multi-statement policy to a bucket:
            2.1 Allow all accounts all access to a bucket's objects
            2.2 Deny all access to a specific object
            2.3 Apply the policy
        3. Check that the two statements were applied correctly:
            3.1 Check that the new account can access the allowed object
            3.2 Check that the new account can't access the denied object

        """
        # 1. Setup
        bucket = c_scope_s3client.create_bucket()
        denied_obj, allowed_obj = c_scope_s3client.put_random_objects(bucket, 2)
        new_acc_client = s3_client_factory()

        # 2. Apply a multi-statement policy
        # 2.1 Allow all accounts all access to a bucket's objects
        bpb = BucketPolicyBuilder()
        bpb = (
            bpb.add_allow_statement()
            .add_action("*")
            .add_principal("*")
            .add_resource(f"{bucket}/*")
        )
        # 2.2 Deny all access to a specific object
        bpb = (
            bpb.add_deny_statement()
            .add_action("*")
            .add_principal("*")
            .add_resource(f"{bucket}/{denied_obj}")
        )
        policy = bpb.build()

        # 2.3 Apply the policy
        response = c_scope_s3client.put_bucket_policy(bucket, str(policy))
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 3. Check that the two statements are applied correctly
        # 3.1 Check that a new account can access the allowed object
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )
        assert access_tester.check_client_access_to_bucket_op(
            new_acc_client, bucket, "GetObject", obj_key=allowed_obj
        ), "Access was denied to the allowed object"
        # 3.2 Check that the original account can't access the denied object
        assert not access_tester.check_client_access_to_bucket_op(
            new_acc_client, bucket, "GetObject", obj_key=denied_obj
        ), "Access was allowed to the denied object"

    def test_multi_operation_statement_policy(
        self, c_scope_s3client, s3_client_factory
    ):
        """
        Test multi-operation statement policies:

        1. Setup:
            1.1 Create a bucket
            1.2 Create a new account
        2. Apply a multi-operation statement policy to a bucket
            2.1 Build a policy that allows multiple operations to all accounts
            2.2 Apply the policy
        3. Check that the new account can perform the allowed operations

        """
        # 1. Setup
        bucket = c_scope_s3client.create_bucket()
        new_acc_client = s3_client_factory()

        # 2. Apply a multi-action statement policy
        # 2.1 Build a policy that allows multiple operations to all accounts
        tested_ops = ["GetObject", "PutObject", "DeleteObject"]
        bpb = (
            BucketPolicyBuilder()
            .add_allow_statement()
            .add_principal("*")
            .add_resource(f"{bucket}/*")
        )
        for op in tested_ops:
            bpb.add_action(op)
        policy = bpb.build()

        # 2.2 Apply the policy
        response = c_scope_s3client.put_bucket_policy(bucket, str(policy))
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 3. Check that the new account can perform the allowed operations
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )
        for op in tested_ops:
            assert access_tester.check_client_access_to_bucket_op(
                new_acc_client, bucket, op
            ), f"{op} was denied for the account when it shouldn't have been"

    def test_multi_resource_statement_policy(self, c_scope_s3client, s3_client_factory):
        """
        Test multi-resource statement policies:

        1. Setup:
            1.1 Create a bucket
            1.2 Put multiple objects in the bucket
            1.3 Create a new account
        2. Apply a multi-resource statement policy to a bucket
            2.1 Build a policy that allows access to multiple objects to all accounts
            2.2 Apply the policy
        3. Check that access is allowed to all objects

        """

        # 1. Setup
        new_acc_client = s3_client_factory()
        bucket = c_scope_s3client.create_bucket()
        tested_objs = c_scope_s3client.put_random_objects(bucket, 5)

        # 1. Apply a multi-resource statement policy
        # 1.1 Build a policy that allows access to multiple objects to all accounts
        bpb = (
            BucketPolicyBuilder()
            .add_allow_statement()
            .add_principal("*")
            .add_action("*")
        )
        for obj in tested_objs:
            bpb.add_resource(f"{bucket}/{obj}")
        policy = bpb.build()

        # 2.2 Apply the policy
        response = c_scope_s3client.put_bucket_policy(bucket, str(policy))
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 3. Check that access is denied to all objects
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )
        for obj in tested_objs:
            assert access_tester.check_client_access_to_bucket_op(
                new_acc_client, bucket, "GetObject", obj_key=obj
            ), f"Access was denied to {obj} when it shouldn't have been"

    def test_multi_principal_statement_policy(
        self, c_scope_s3client, account_manager, s3_client_factory
    ):
        """
        Test multi-principal statement policies:

        1. Setup:
            1.1 Create a bucket
            1.2 Create multiple accounts
        2. Apply a multi-principal statement policy to a bucket:
            2.1 Build a policy that allows multiple principals access to a bucket
            2.2 Apply the policy
        3. Check that all principals are allowed access

        """
        # 1. Setup
        bucket = c_scope_s3client.create_bucket()
        acc_names, acc_clients = [], []
        for _ in range(3):
            acc_name, access_key, secret_key = account_manager.create()
            acc_names.append(acc_name)
            acc_clients.append(
                s3_client_factory(access_and_secret_keys_tuple=(access_key, secret_key))
            )

        # 2. Apply a multi-principal statement policy
        # 2.1 Build a policy that allows multiple principals access to a bucket
        bpb = (
            BucketPolicyBuilder()
            .add_allow_statement()
            .add_action("*")
            .add_resource(f"{bucket}/*")
        )
        for acc_name in acc_names:
            bpb.add_principal(acc_name)
        policy = bpb.build()

        # 2.2 Apply the policy
        response = c_scope_s3client.put_bucket_policy(bucket, str(policy))
        assert (
            response["Code"] == 200
        ), f"put_bucket_policy failed with code {response['Code']}"

        # 3. Check that all principals are allowed access
        access_tester = S3OperationAccessTester(
            admin_client=c_scope_s3client,
        )
        for acc, s3_client in zip(acc_names, acc_clients):
            assert access_tester.check_client_access_to_bucket_op(
                s3_client, bucket, "GetObject"
            ), f"Access was denied for account {acc} when it shouldn't have been"

from abc import ABC
from botocore.exceptions import ClientError
from common_ci_utils.random_utils import generate_unique_resource_name
from s3_operation_access_tester import AccessValidationStrategy
from noobaa_sa.bucket_policy import BucketPolicy


class AccessValidationStrategyFactory:
    """
    A factory for creating AccessValidationStrategy instances
    """

    @staticmethod
    def create_strategy_for_operation(operation):
        """
        Create an AccessValidationStrategy instance for the given operation.
        Dynamically imports the appropriate strategy class based on the operation,
        and returns an instance of that class.

        Supported operatoins need to have a corresponding strategy class defined in this module,
        with the class name being the operation name followed by "ValidationStrategy".


        Args:
            operation (str): The operation to create a strategy for

        Returns:
            AccessValidationStrategy: An instance of the appropriate strategy

        Raises:
            NotImplementedError: If the operation is not supported

        """
        # Dynamically import the appropriate strategy class based on the operation
        try:
            concrete_strategy_subclass = eval(operation + "ValidationStrategy")
        except NameError:
            raise NotImplementedError(f"Operation not supported: {operation}")

        strategy_instance = concrete_strategy_subclass()
        return strategy_instance


class GetObjectValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the GetObject operation
    """

    @property
    def expected_success_code(self):
        return 200

    def setup(self, **setup_kwargs):
        if "obj_key" in setup_kwargs:
            self.test_obj_key = setup_kwargs["obj_key"]
        else:
            self.test_obj_key = generate_unique_resource_name(prefix="test-obj-")
            self.admin_client.put_object(self.bucket, self.test_obj_key, "test_data")

    def do_operation(self, s3_client, bucket, **setup_kwargs):
        return s3_client.get_object(bucket, self.test_obj_key)


class PutObjectValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the PutObject operation
    """

    @property
    def expected_success_code(self):
        return 200

    def do_operation(self, s3_client, bucket):
        obj_key = generate_unique_resource_name(prefix="test-obj-")
        return s3_client.put_object(bucket, obj_key, "test_data")


class DeleteObjectValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the DeleteObject operation
    """

    @property
    def expected_success_code(self):
        return 204

    def setup(self, **setup_kwargs):
        if "obj_key" in setup_kwargs:
            self.test_obj_key = setup_kwargs["obj_key"]
        else:
            self.test_obj_key = generate_unique_resource_name(prefix="test-obj-")
            self.admin_client.put_object(self.bucket, self.test_obj_key, "test_data")

    def do_operation(self, s3_client, bucket):
        return s3_client.delete_object(bucket, self.test_obj_key)


class ListBucketValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the ListBucket operation
    """

    @property
    def expected_success_code(self):
        return 200

    def setup(self, **setup_kwargs):
        obj_key = generate_unique_resource_name(prefix="test-obj-")
        self.admin_client.put_object(self.bucket, obj_key, "test_data")

    def do_operation(self, s3_client, bucket):
        return s3_client.list_objects(bucket, get_response=True)


class DeleteBucketValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the DeleteBucket operation
    """

    @property
    def expected_success_code(self):
        return 204

    def do_operation(self, s3_client, bucket):
        return s3_client.delete_bucket(bucket)


class PolicyOperationValidationStrategy(AccessValidationStrategy, ABC):
    """
    An abstract subclass for strategies used in validating access to bucket policy operations
    """

    def setup(self, **setup_kwargs):
        if "policy" in setup_kwargs:
            self.test_policy = setup_kwargs["policy"]
        else:
            self.test_policy = BucketPolicy.default_template()

        try:
            self.original_policy = self.admin_client.get_bucket_policy(self.bucket)
        except ClientError as e:
            self.original_policy = None

    def cleanup(self):
        if self.original_policy:
            self.admin_client.put_bucket_policy(self.bucket, self.original_policy)


class PutBucketPolicyValidationStrategy(PolicyOperationValidationStrategy):
    """
    A strategy for validating access to the PutBucketPolicy operation
    """

    @property
    def expected_success_code(self):
        return 204

    def do_operation(self, s3_client, bucket):
        return s3_client.put_bucket_policy(bucket, self.test_policy)


class GetBucketPolicyValidationStrategy(PolicyOperationValidationStrategy):
    """
    A strategy for validating access to the GetBucketPolicy operation
    """

    @property
    def expected_success_code(self):
        return 200

    def do_operation(self, s3_client, bucket):
        return s3_client.get_bucket_policy(bucket)


class DeleteBucketPolicyValidationStrategy(PolicyOperationValidationStrategy):
    """
    A strategy for validating access to the DeleteBucketPolicy operation
    """

    @property
    def expected_success_code(self):
        return 204

    def do_operation(self, s3_client, bucket):
        return s3_client.delete_bucket_policy(bucket)

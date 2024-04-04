from abc import ABC


class S3OperationAccessTester:
    """
    This class assess whether a specified s3 client is allowed access
    for performing various operations on an S3 bucket, utilizing the strategy pattern.

    A prvilieged S3 client (admin_client) is required for setting up preconditions.

    """

    def __init__(self, admin_client):
        """
        Args:
            admin_client (S3Client): A privileged client to set up preconditions

        """
        self.admin_client = admin_client

    def _get_strategy_for_operation(self, operation):
        """
        Args:
            operation (str): The operation to test

        Returns:
            OperationTestStrategy: A strategy for testing the operation

        Raises:
            NotImplementedError: If the operation is not supported

        """
        if operation == "GetObject":
            return GetObjectValidationStrategy(self.admin_client)
        elif operation == "PutObject":
            return PutObjectValidationStrategy(self.admin_client)
        elif operation == "ListBucket":
            return ListBucketValidationStrategy(self.admin_client)
        elif operation == "DeleteObject":
            return DeleteObjectValidationStrategy(self.admin_client)
        else:
            raise NotImplementedError(f"Unsupported operation: {operation}")

    def check_client_access_to_bucket_op(self, s3_client, bucket, operation):
        """
        Args:
            s3_client (S3Client): The client to test access for
            bucket (str): The bucket to test access for
            operation (str): The operation to test

        Returns:
            bool: True if the operation was permitted, False otherwise

        Raises:
            NotImplementedError: If the operation is not supported
            Exception: If the operation returned an unexpected response code

        """
        test_strategy = self._get_strategy_for_operation(operation)
        setup_data = test_strategy.setup()
        response = test_strategy.do_operation(s3_client, bucket, setup_data)
        if response["Code"] == test_strategy.expected_success_code:
            return True
        elif response["Code"] == "AccessDenied":
            return False
        else:
            raise Exception(f"Unexpected response code: {response['Code']}")


class AccessValidationStrategy(ABC):
    """
    An abstract base class defining the interface for strategies used
    in validating access permissions for S3 operations.

    """

    @property
    def expected_success_code(self):
        """
        Returns:
            int: The expected success code

        """
        raise NotImplementedError

    def setup(self):
        """
        Returns:
            Any: Data to be used in the operation

        """
        raise NotImplementedError

    def do_operation(self, s3_client, bucket, setup_data):
        """
        Args:
            s3_client (S3Client): The client to test access for
            bucket (str): The bucket to test access for
            setup_data (Any): Data returned from setup

        Returns:
            dict: Response from the operation

        """
        raise NotImplementedError


class GetObjectValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the GetObject operation
    """

    @property
    def expected_success_code(self):
        return 200

    def setup(self):
        obj = self.admin_client.put_object(self.bucket, "test_obj", "test_data")
        return obj

    def do_operation(self, s3_client, bucket, setup_data):
        obj = setup_data
        return s3_client.get_object(bucket, obj)


class PutObjectValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the PutObject operation
    """

    @property
    def expected_success_code(self):
        return 200

    def setup(self):
        return

    def do_operation(self, s3_client, bucket, setup_data):
        return s3_client.put_object(bucket, "test_obj", "test_data")


class ListBucketValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the ListBucket operation
    """

    @property
    def expected_success_code(self):
        return 200

    def setup(self):
        obj = self.admin_client.put_object(self.bucket, "test_obj", "test_data")
        return obj

    def do_operation(self, s3_client, bucket, setup_data):
        return s3_client.list_objects(bucket, get_response=True)


class DeleteObjectValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the DeleteObject operation
    """

    @property
    def expected_success_code(self):
        return 204

    def setup(self):
        obj = self.admin_client.put_object(self.bucket, "test_obj", "test_data")
        return obj

    def do_operation(self, s3_client, bucket, setup_data):
        obj = setup_data
        return s3_client.delete_object(bucket, obj)


class DeleteBucketValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the DeleteBucket operation
    """

    @property
    def expected_success_code(self):
        return 204

    def setup(self):
        return

    def do_operation(self, s3_client, bucket, setup_data):
        return s3_client.delete_bucket(bucket)

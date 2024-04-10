from abc import ABC

from access_validation_strategies import AccessValidationStrategyFactory


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

    def check_client_access_to_bucket_op(
        self, s3_client, bucket, operation, **setup_kwargs
    ):
        """
        Args:
            s3_client (S3Client): The client to test access for
            bucket (str): The bucket to test access for
            operation (str): The operation to test
            setup_kwargs (dict): Additional optional arguments for setting up the operation

        Returns:
            bool: True if the operation was permitted, False otherwise

        Raises:
            NotImplementedError: If the operation is not supported
            Exception: If the operation returned an unexpected response code

        """
        test_strategy = AccessValidationStrategyFactory.create_strategy(operation)
        test_strategy.setup(setup_kwargs)
        response = test_strategy.do_operation(s3_client, bucket)
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

    def setup(self, **setup_kwargs):
        """
        Perform any necessary setup before the operation

        """
        pass

    def do_operation(self, s3_client, bucket):
        """
        Args:
            s3_client (S3Client): The client to test access for
            bucket (str): The bucket to test access for

        Returns:
            dict: Response from the operation

        """
        raise NotImplementedError

    def cleanup(self):
        """
        Perform any necessary cleanup after the operation

        """
        pass

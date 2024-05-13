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
        test_strategy = AccessValidationStrategyFactory.create_strategy_for_operation(
            self.admin_client, bucket, operation
        )
        test_strategy.setup(**setup_kwargs)
        response = test_strategy.do_operation(s3_client, bucket)
        test_strategy.cleanup()
        if response["Code"] == test_strategy.expected_success_code:
            return True
        elif response["Code"] == "AccessDenied" or response["Code"] == 403:
            return False
        else:
            raise Exception(f"Unexpected response code: {response['Code']}")

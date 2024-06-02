from abc import ABC


class AccessValidationStrategy(ABC):
    """
    An abstract base class defining the interface for strategies used
    in validating access permissions for S3 operations.

    """

    TEST_OBJ_PREFIX = "test-obj-"

    def __init__(self, admin_client, bucket):
        super().__init__()
        self.admin_client = admin_client
        self.bucket = bucket

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

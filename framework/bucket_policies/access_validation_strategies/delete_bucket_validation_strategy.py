from framework.bucket_policies.access_validation_strategies.access_validation_strategy_interface import (
    AccessValidationStrategy,
)


class DeleteBucketValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the DeleteBucket operation
    """

    @property
    def expected_success_code(self):
        return 204

    def do_operation(self, s3_client, bucket):
        return s3_client.delete_bucket(bucket)

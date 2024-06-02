from framework.bucket_policies.access_validation_strategies.policy_operation_validation_strategy import (
    PolicyOperationValidationStrategy,
)


class GetBucketPolicyValidationStrategy(PolicyOperationValidationStrategy):
    """
    A strategy for validating access to the GetBucketPolicy operation
    """

    @property
    def expected_success_code(self):
        return 200

    def do_operation(self, s3_client, bucket):
        return s3_client.get_bucket_policy(bucket)

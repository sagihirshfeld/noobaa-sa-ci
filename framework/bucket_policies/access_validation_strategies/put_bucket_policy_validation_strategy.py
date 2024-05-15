from framework.bucket_policies.access_validation_strategies.policy_operation_validation_strategy import (
    PolicyOperationValidationStrategy,
)
from framework.bucket_policies.bucket_policy import BucketPolicyBuilder


class PutBucketPolicyValidationStrategy(PolicyOperationValidationStrategy):
    """
    A strategy for validating access to the PutBucketPolicy operation
    """

    @property
    def expected_success_code(self):
        return 200

    def do_operation(self, s3_client, bucket):

        test_policy = (
            BucketPolicyBuilder()
            .add_allow_statement()
            .add_principal("*")
            .add_action("PutBucketPolicy")
            .add_resource("*")
            .build()
        )
        return s3_client.put_bucket_policy(bucket, str(test_policy))

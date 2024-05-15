from framework.bucket_policies.access_validation_strategies.access_validation_strategy_interface import (
    AccessValidationStrategy,
)
from framework.bucket_policies.bucket_policy import BucketPolicy


from botocore.exceptions import ClientError


from abc import ABC


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
            self.original_policy = BucketPolicy.from_json(
                self.admin_client.get_bucket_policy(self.bucket).get("Policy")
            )
        except ClientError as e:
            self.original_policy = None

    def cleanup(self):
        if self.original_policy:
            self.admin_client.put_bucket_policy(self.bucket, str(self.original_policy))

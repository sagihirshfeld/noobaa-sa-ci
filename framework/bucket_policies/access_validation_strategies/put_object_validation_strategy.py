from common_ci_utils.random_utils import generate_unique_resource_name
from framework.bucket_policies.access_validation_strategies.access_validation_strategy_interface import (
    AccessValidationStrategy,
)


class PutObjectValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the PutObject operation
    """

    @property
    def expected_success_code(self):
        return 200

    def do_operation(self, s3_client, bucket):
        obj_key = generate_unique_resource_name(prefix=self.TEST_OBJ_PREFIX)
        return s3_client.put_object(bucket, obj_key, "test_data")

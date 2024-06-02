from common_ci_utils.random_utils import generate_unique_resource_name
from framework.bucket_policies.access_validation_strategies.access_validation_strategy_interface import (
    AccessValidationStrategy,
)


class CopyObjectValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the CopyObject operation
    """

    @property
    def expected_success_code(self):
        return 200

    def setup(self, **setup_kwargs):
        self.test_obj_key = generate_unique_resource_name(prefix=self.TEST_OBJ_PREFIX)
        self.admin_client.put_object(self.bucket, self.test_obj_key, "test_data")

    def do_operation(self, s3_client, bucket):
        new_obj_key = generate_unique_resource_name(prefix=self.TEST_OBJ_PREFIX)
        return s3_client.copy_object(bucket, self.test_obj_key, bucket, new_obj_key)

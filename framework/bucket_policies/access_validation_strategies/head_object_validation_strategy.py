from common_ci_utils.random_utils import generate_unique_resource_name
from framework.bucket_policies.access_validation_strategies.access_validation_strategy_interface import (
    AccessValidationStrategy,
)


class HeadObjectValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the HeadObject operation
    """

    @property
    def expected_success_code(self):
        return 200

    def setup(self, **setup_kwargs):
        if "obj_key" in setup_kwargs:
            self.test_obj_key = setup_kwargs["obj_key"]
        else:
            self.test_obj_key = generate_unique_resource_name(prefix="test-obj-")
            self.admin_client.put_object(self.bucket, self.test_obj_key, "test_data")

    def do_operation(self, s3_client, bucket, **setup_kwargs):
        return s3_client.head_object(bucket, self.test_obj_key)

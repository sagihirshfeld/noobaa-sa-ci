from common_ci_utils.random_utils import generate_unique_resource_name
from framework.bucket_policies.access_validation_strategies.access_validation_strategy_interface import (
    AccessValidationStrategy,
)


class ListBucketValidationStrategy(AccessValidationStrategy):
    """
    A strategy for validating access to the ListBucket operation
    """

    @property
    def expected_success_code(self):
        return 200

    def setup(self, **setup_kwargs):
        obj_key = generate_unique_resource_name(prefix="test-obj-")
        self.admin_client.put_object(self.bucket, obj_key, "test_data")

    def do_operation(self, s3_client, bucket):
        return s3_client.list_objects(bucket, get_response=True)

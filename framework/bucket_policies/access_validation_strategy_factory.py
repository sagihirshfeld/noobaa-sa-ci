import importlib

from utility.utils import camel_to_snake


class AccessValidationStrategyFactory:
    """
    A factory for creating AccessValidationStrategy instances
    """

    @staticmethod
    def create_strategy_for_operation(admin_client, bucket, operation):
        """
        Create an AccessValidationStrategy instance for the given operation.
        Dynamically imports the appropriate strategy class and module based on the operation,
        and returns an instance of that class.

        Supported operations need to have a corresponding strategy class defined in this module,
        with the class name being the operation followed by "ValidationStrategy".
            - For example: HeadObject -> HeadObjectValidationStrategy

        Args:
            admin_client (S3Client): The privileged client for setting up preconditions
            bucket (str): The bucket to test access on
            operation (str): The operation to test

        Returns:
            AccessValidationStrategy: An instance of the appropriate strategy

        Raises:
            NotImplementedError: If the operation is not supported
        """
        prefix = "framework.bucket_policies.access_validation_strategies."
        module = prefix + camel_to_snake(operation) + "_validation_strategy"

        # Dynamically import the strategy module
        try:
            strategy_module = importlib.import_module(module)
        except ImportError:
            raise NotImplementedError(
                f"No strategy module found for operation: {operation}"
            )

        # Get the strategy class from the module
        class_name = f"{operation}ValidationStrategy"
        try:
            concrete_strategy_subclass = getattr(strategy_module, class_name)
        except AttributeError:
            raise NotImplementedError(
                f"Strategy class {class_name} not found in module {module}"
            )

        # Create an instance of the strategy class
        strategy_instance = concrete_strategy_subclass(admin_client, bucket)
        return strategy_instance

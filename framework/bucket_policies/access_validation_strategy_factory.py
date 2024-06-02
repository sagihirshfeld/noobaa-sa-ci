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

        Supported operations need to have a corresponding strategy class
        defined the access_validation_strategies module so that the factory can create an instance of it.
        The file name should start with the operation name in snake case, followed by "_validation_strategy.py".
        The class name should be the operation name in camel case, followed by "ValidationStrategy".

        i.e. for the operation "CopyObject", the file should be "copy_object_validation_strategy.py",
        and the class should be "CopyObjectValidationStrategy".

        Args:
            admin_client (S3Client): The privileged client for setting up preconditions
            bucket (str): The bucket to test access on
            operation (str): The operation to test

        Returns:
            AccessValidationStrategy: An instance of the appropriate strategy

        Raises:
            NotImplementedError: If the operation is not supported
        """

        # Dynamically import the strategy module
        try:
            # __package__ is the parent package of the current module
            package = __package__ + ".access_validation_strategies"
            module = camel_to_snake(operation) + "_validation_strategy"
            full_module_path = package + "." + module
            strategy_module = importlib.import_module(full_module_path)
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
                f"Strategy class {class_name} not found in {full_module_path}"
            )

        # Create an instance of the strategy class
        strategy_instance = concrete_strategy_subclass(admin_client, bucket)
        return strategy_instance

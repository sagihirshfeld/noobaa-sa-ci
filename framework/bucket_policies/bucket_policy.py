import json


class BucketPolicy:
    """
    A class representing an S3 bucket policy.
    """

    DEFAULT_VERSION = "2012-10-17"
    ACTION_PREFIX = "s3:"
    RESOURCE_PREFIX = "arn:aws:s3:::"

    def __init__(self):
        self.version = self.DEFAULT_VERSION
        self.statements = []

    def as_dict(self):
        """
        Returns:
            dict: The policy as a dictionary
        """
        return {"Version": self.version, "Statement": self.statements}

    def __str__(self):
        """
        Returns:
            str: The policy as a JSON string
        """
        return json.dumps(self.as_dict(), indent=4)

    @staticmethod
    def from_json(json_str):
        """
        Args:
            json_str (str): The JSON string to create the policy from

        Returns:
            BucketPolicy: The created policy
        """
        data = json.loads(json_str)
        policy = BucketPolicy()
        policy.version = data.get("Version", policy.DEFAULT_VERSION)
        policy.statements = data.get("Statement", [])
        return policy

    @staticmethod
    def default_template():
        """
        Return a simple bucket policy - useful for basic testing.

        Returns:
            BucketPolicy: The default bucket policy
        """
        return (
            BucketPolicyBuilder()
            .add_deny_statement()
            .add_principal("*")
            .add_action("GetObject")
            .add_resource("*")
            .build()
        )

    @staticmethod
    def get_ops_with_perm_overlap(operation):
        """
        Get the operations that have overlapping permissions with the given operation.

        Args:
            operation (str): The operation to get overlapping permissions for

        Returns:
            list: Operations that have overlapping permissions
        """
        # Map of operations that have overlapping permissions
        overlap_map = {
            "HeadObject": ["GetObject"],
            "GetObject": ["HeadObject", "CopyObject"],
            "CopyObject": ["GetObject", "PutObject"],
            "PutObject": ["CopyObject"],
        }
        # Add the operation to its own list because
        # technically it always overlaps with itself
        overlap_map[operation] = overlap_map.get(operation, []) + [operation]

        return overlap_map[operation]


class BucketPolicyBuilder:
    """
    A builder class for creating BucketPolicy objects.

    It offers a fluent interface for readability, and allows
    incremental build depending on runtime logic.
    """

    def __init__(self, policy=None):
        self.policy = policy or BucketPolicy()

    def add_allow_statement(self):
        """
        Add an allow statement to the policy.
        """
        self.policy.statements.append({"Effect": "Allow"})
        return self

    def add_deny_statement(self):
        """
        Add a deny statement to the policy.
        """
        self.policy.statements.append({"Effect": "Deny"})
        return self

    def add_principal(self, principal):
        """
        Add a principal to the last statement in the policy.
        Allows for multiple principals to be added per statement.

        Args:
            principal (str): The name of the account to add
        """
        self._update_property_on_last_statement("Principal", principal)
        return self

    def add_not_principal(self, not_principal):
        """
        Add a NotPrincipal to the last statement in the policy.
        Allows for multiple NotPrincipals to be added per statement.

        Args:
            not_principal (str): The name of the account to add
        """
        self._update_property_on_last_statement("NotPrincipal", not_principal)
        return self

    def add_action(self, action):
        """
        Add an action to the last statement in the policy.
        Allows for multiple actions to be added per statement.

        Args:
            action (str): The name of the S3 operation to add.
                        - Prefixing with "s3:" is optional.
                        - e.g "GetObject" or "s3:GetObject"
        """
        self._update_property_on_last_statement("Action", action)
        return self

    def add_not_action(self, not_action):
        """
        Add a NotAction to the last statement in the policy.
        Allows for multiple NotActions to be added per statement.

        Args:
            action (str): The name of the S3 operation to add.
                        - Prefixing with "s3:" is optional.
                        - For example: "GetObject" or "s3:GetObject"
        """
        self._update_property_on_last_statement("NotAction", not_action)
        return self

    def add_resource(self, resource):
        """
        Add a resource to the last statement in the policy.
        Allows for multiple resources to be added per statement.

        Args:
            resource (str): An S3 path to add.
                        - Prefixing with "arn:aws:s3:::" is optional.
                        - For example: "my-bucket/*" or "arn:aws:s3:::my-bucket/*"
                        - Another example: "my-bucket/my-obj", "my-bucket/my-prefix/*"
        """
        self._update_property_on_last_statement("Resource", resource)
        return self

    def add_not_resource(self, not_resource):
        """
        Add a NotResource to the last statement in the policy.
        Allows for multiple NotResources to be added per statement.

        Args:
            resource (str): An S3 path to add.
                        - Prefixing with "arn:aws:s3:::" is optional.
                        - For example: "my-bucket/*" or "arn:aws:s3:::my-bucket/*"
                        - Another example: "my-bucket/my-obj", "my-bucket/my-prefix/*"
        """
        self._update_property_on_last_statement("NotResource", not_resource)
        return self

    def _update_property_on_last_statement(self, property, value):
        """
        Update a given property on the last statement in the policy.

        Args:
            property (str): The property to update
            value (str): The value to set

        Raises:
            ValueError: If there are no statements to update

        """
        if not self.policy.statements:
            raise ValueError("No statement to update")

        value = self._assure_prefix(property, value)

        # Nest Principal/NotPrincipal values under the "AWS" key
        if "principal" in property.lower():
            d = self.policy.statements[-1].setdefault(property, {})
            property = "AWS"
        else:
            d = self.policy.statements[-1]

        # Append the value to the property
        d.setdefault(property, []).append(value)

    def _assure_prefix(self, property, value):
        """
        Add the expected prefix to Action or Resource values if not present.

        Args:
            property (str): The property to check
            value (str): The value to check

        Returns:
            str: The value with the expected prefix
        """
        prefix = ""
        if "action" in property.lower():
            prefix = BucketPolicy.ACTION_PREFIX
        elif "resource" in property.lower():
            prefix = BucketPolicy.RESOURCE_PREFIX

        return value if value.startswith(prefix) else prefix + value

    def build(self):
        """
        Initialize and return a BucketPolicy object with the built statements.

        Returns:
            BucketPolicy: The built policy
        """
        return self.policy

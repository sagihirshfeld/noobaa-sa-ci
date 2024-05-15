import json
import logging

log = logging.getLogger(__name__)


class BucketPolicy:
    DEFAULT_VERSION = "2012-10-17"
    ACTION_PREFIX = "s3:"
    RESOURCE_PREFIX = "arn:aws:s3:::"

    @staticmethod
    def from_json(json_str):
        data = json.loads(json_str)
        policy = BucketPolicy()
        policy.version = data.get("Version", policy.DEFAULT_VERSION)
        policy.statements = data.get("Statement", [])
        return policy

    def __init__(self):
        self.version = self.DEFAULT_VERSION
        self.statements = []

    def as_dict(self):
        return {"Version": self.version, "Statement": self.statements}

    def __str__(self):
        return json.dumps(self.as_dict(), indent=4)

    @staticmethod
    def default_template():
        return (
            BucketPolicyBuilder()
            .add_deny_statement()
            .add_principal("*")
            .add_action("GetObject")
            .add_resource("*")
            .build()
        )


class BucketPolicyBuilder:
    def __init__(self, policy=None):
        self.policy = policy or BucketPolicy()

    def add_allow_statement(self):
        self.policy.statements.append({"Effect": "Allow"})
        return self

    def add_deny_statement(self):
        self.policy.statements.append({"Effect": "Deny"})
        return self

    def add_principal(self, principal):
        self._update_property_on_last_statement("Principal", principal)
        return self

    def add_not_principal(self, not_principal):
        self._update_property_on_last_statement("NotPrincipal", not_principal)
        return self

    def add_action(self, action):
        self._update_property_on_last_statement("Action", action)
        return self

    def add_not_action(self, not_action):
        self._update_property_on_last_statement("NotAction", not_action)
        return self

    def add_resource(self, resource):
        self._update_property_on_last_statement("Resource", resource)
        return self

    def add_not_resource(self, not_resource):
        self._update_property_on_last_statement("NotResource", not_resource)
        return self

    def _update_property_on_last_statement(self, property, value):
        """
        Update the given property on the last statement in the policy.

        """
        if not self.policy.statements:
            raise ValueError("No statement to update")

        value = self._assure_prefix(property, value)

        # Principal and NotPrincipal formats are different
        if "principal" in property.lower():
            d = self.policy.statements[-1].setdefault(property, {})
            property = "AWS"
        else:
            d = self.policy.statements[-1]

        if property not in d:
            d[property] = value
        elif isinstance(d[property], list):
            d[property].append(value)
        else:
            d[property] = [d[property], value]

    def _assure_prefix(self, property, value):
        prefix = ""
        if "action" in property.lower():
            prefix = BucketPolicy.ACTION_PREFIX
        elif "resource" in property.lower():
            prefix = BucketPolicy.RESOURCE_PREFIX

        return value if value.startswith(prefix) else prefix + value

    def build(self):
        return self.policy

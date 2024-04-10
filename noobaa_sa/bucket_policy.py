import json
import logging

log = logging.getLogger(__name__)


class BucketPolicy:
    DEFAULT_VERSION = "2012-10-17"
    ACTION_PREFIX = "s3:"
    RESOURCE_PREFIX = "arn:aws:s3:::"

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
            .for_principal("*")
            .on_action("GetObject")
            .with_resource("*")
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

    def for_principal(self, principal):
        self._update_property_on_last_statement("Principal", principal)
        return self

    def not_for_principal(self, not_principal):
        self._update_property_on_last_statement("NotPrincipal", not_principal)
        return self

    def on_action(self, action):
        self._update_property_on_last_statement("Action", action)
        return self

    def not_for_action(self, not_action):
        self._update_property_on_last_statement("NotAction", not_action)
        return self

    def with_resource(self, resource):
        self._update_property_on_last_statement("Resource", resource)
        return self

    def not_on_resource(self, not_resource):
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
            d = self.policy.statements[-1].setdefault("Principal", {})
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
        if property == "Action":
            prefix = BucketPolicy.ACTION_PREFIX
        elif property == "Resource":
            prefix = BucketPolicy.RESOURCE_PREFIX

        return value if value.startswith(prefix) else prefix + value

    def build(self):
        return self.policy

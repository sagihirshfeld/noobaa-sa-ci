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
    def __init__(self, policy=None, autocorrect=True):
        self.policy = policy or BucketPolicy()
        self._autocorrect = autocorrect

    def add_allow_statement(self):
        self.policy.statements.append({"Effect": "Allow"})
        return self

    def add_deny_statement(self):
        self.policy.statements.append({"Effect": "Deny"})
        return self

    def for_principal(self, principal):
        if self._autocorrect:
            principal = {"AWS": principal}
        self.policy.statements[-1]["Principal"] = principal
        return self

    def not_on_principal(self, not_principal):
        self.policy.statements[-1]["NotPrincipal"] = not_principal
        return self

    def on_action(self, action):
        if not action.startswith(self.ACTION_PREFIX):
            action = self.ACTION_PREFIX + action
        self.policy.statements[-1]["Action"] = action
        return self

    def not_for_action(self, not_action):
        if self._autocorrect:
            not_action = self._assure_prefix(not_action, self.ACTION_PREFIX)
        self.policy.statements[-1]["NotAction"] = not_action
        return self

    def with_resource(self, resource):
        if self._autocorrect:
            resource = self._assure_prefix(resource, self.RESOURCE_PREFIX)
        self.policy.statements[-1]["Resource"] = resource
        return self

    def not_on_resource(self, not_resource):
        if self._autocorrect:
            not_resource = self._assure_prefix(not_resource, self.RESOURCE_PREFIX)
        self.policy.statements[-1]["NotResource"] = not_resource
        return self

    def _assure_prefix(self, str, prefix):
        return str if str.startswith(prefix) else prefix + str

    def build(self):
        return self.policy

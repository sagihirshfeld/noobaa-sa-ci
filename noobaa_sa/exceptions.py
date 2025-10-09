class AccountCreationFailed(Exception):
    pass


class AccountListFailed(Exception):
    pass


class AccountDeletionFailed(Exception):
    pass


class AccountUpdateFailed(Exception):
    pass


class AccountStatusQueryFailed(Exception):
    pass


class InvalidDeploymentType(Exception):
    pass


class AccountStatusFailed(Exception):
    pass


class BucketCreationFailed(Exception):
    pass


class BucketListFailed(Exception):
    pass


class BucketDeletionFailed(Exception):
    pass


class BucketStatusFailed(Exception):
    pass


class HealthStatusFailed(Exception):
    pass


class BucketUpdateFailed(Exception):
    pass


class MissingFileOrDirectory(Exception):
    pass


class NoSuchBucket(Exception):
    pass


class BucketNotEmpty(Exception):
    pass


class BucketAlreadyExists(Exception):
    pass


class AccessDenied(Exception):
    pass


class UnexpectedBehaviour(Exception):
    pass


class NoSuchKey(Exception):
    pass

class TimeoutExpiredError(Exception):
    message = "Timed Out"

    def __init__(self, value, custom_message=None):
        self.value = value
        self.custom_message = custom_message

    def __str__(self):
        if self.custom_message is None:
            self.message = f"{self.__class__.message}: {self.value}"
        else:
            self.message = self.custom_message
        return self.message

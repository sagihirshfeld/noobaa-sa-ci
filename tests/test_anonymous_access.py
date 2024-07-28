import logging
import random

from framework.customizations.marks import tier1
from noobaa_sa.exceptions import AccountCreationFailed

log = logging.getLogger(__name__)


class TestAnonymousAccess:
    """
    Test S3 bucket operations with anonymous access
    """

    @tier1
    def test_anonymous_account_management(self, account_manager):
        """
        Test management of the anonymous account via the NooBaa CLI
        1. Create an anonymous account
        2. Check that its uid and gid match the ones we provided
        3. Try creating a second anonymous account and check that it fails
        3. Update the anonymous account with new uid and gid
        4. Check that the uid and gid were updated
        5. Delete the anonymous account
        6. Check that the anonymous account was deleted

        """
        # 1. Create an anonymous account
        uid, gid = random.randint(1000, 2000), random.randint(1000, 2000)
        account_manager.anonymous.create(uid=uid, gid=gid)

        # 2. Check that its uid and gid match the ones we provided
        anon_acc_nsfs_config = account_manager.anonymous.status()["nsfs_account_config"]
        assert (
            anon_acc_nsfs_config["uid"] == uid and anon_acc_nsfs_config["gid"] == gid
        ), f"Expected uid: {uid}, gid: {gid}, got {anon_acc_nsfs_config}"

        # 3. Try creating a second anonymous account and check that it fails
        try:
            account_manager.anonymous.create(uid=uid, gid=gid)
        except AccountCreationFailed as e:
            if "AccountNameAlreadyExists" not in str(e):
                log.error(
                    "Creating a second anonymous account failed with unexpected error"
                )
                raise e
            else:
                log.info("Creating a second anonymous account failed as expected")

        # 4. Update the anonymous account with new uid and gid
        log.info(account_manager.anonymous.update(uid=1234, gid=5678))

        # 5. Check that the uid and gid were updated
        anon_acc_nsfs_config = account_manager.anonymous.status()["nsfs_account_config"]
        assert (
            anon_acc_nsfs_config["uid"] == 1234 and anon_acc_nsfs_config["gid"] == 5678
        ), f"Expected uid: 1234, gid: 5678, got {anon_acc_nsfs_config}"

        # 6. Delete the anonymous account
        account_manager.anonymous.delete()

        # 7. Check that the anonymous account was deleted
        assert (
            not account_manager.anonymous.status()
        ), "Anonymous account was not deleted"

import logging
import json

from framework.ssh_connection_manager import SSHConnectionManager
from noobaa_sa.defaults import MANAGE_NSFS
from noobaa_sa.exceptions import AccountCreationFailed, AccountDeletionFailed

log = logging.getLogger(__name__)


class AnonymousAccountManager:
    """
    A class to manage the anonymous account in NooBaa
    """

    NB_CLI_PATH = MANAGE_NSFS

    def __init__(self):
        self.conn = SSHConnectionManager().connection

    def create(self, uid=None, gid=None, user=None):
        """
        Create an anonymous account using the NooBaa CLI

        Args:
            uid (str|optional): uid of an account with access to the file system
            gid (str|optional): gid of an account with access to the file system
            user (str|optional): user name of an account with access to the file system

            Note that either a valid uid and gid pair or a valid user name must be provided

        Raises:
            AccountCreationFailed: If the creation of the anonymous account fails

        """

        log.info(f"Adding anonymous account: uid: {uid}, gid: {gid}, user: {user}")

        cmd = f"sudo {self.NB_CLI_PATH} account add --anonymous "
        if uid is not None and gid is not None:
            cmd += f"--uid {uid} --gid {gid}"
        elif user:
            cmd += f"--user {user}"
        else:
            raise AccountCreationFailed(
                "Please provide either a valid uid and gid pair, or a valid user name"
            )

        retcode, stdout, _ = self.conn.exec_cmd(cmd)
        if retcode != 0:
            raise AccountCreationFailed(
                f"Creation of anonymous account failed with error {stdout}"
            )
        log.info("Anonymous account created successfully")

    def delete(self):
        """
        Delete the anonymous account using the NooBaa CLI

        Raises:
            AccountDeletionFailed: If the deletion of the anonymous account fails

        """
        log.info("Deleting the anonymous account")
        cmd = f"sudo {self.NB_CLI_PATH} account delete --anonymous"
        retcode, stdout, _ = self.conn.exec_cmd(cmd)
        if retcode != 0 and "NoSuchAccount" in stdout:
            log.info("The anonymous account was already deleted")
        elif retcode != 0:
            raise AccountDeletionFailed(
                f"Deletion of the anonymous account failed with error {stdout}"
            )
        log.info("The anonymous account has been deleted successfully")

    def update(self, uid=None, gid=None, user=None):
        """
        Update the anonymous account via the NooBaa CLI

        """
        log.info(
            f"Updating the anonymous account: uid: {uid}, gid: {gid}, user: {user}"
        )
        cmd = f"sudo {self.NB_CLI_PATH} account update --anonymous "
        if uid and gid:
            cmd += f"--uid {uid} --gid {gid}"
        elif user:
            cmd += f"--user {user}"
        else:
            # TODO: Change to AccountUpdateFailed exception once PR #37 is merged
            log.error(
                "Please provide either a valid uid and gid pair, or a valid user name"
            )
        retcode, stdout, _ = self.conn.exec_cmd(cmd)
        if retcode != 0:
            # TODO: Change to AccountUpdateFailed exception once PR #37 is merged
            log.error(f"Update of the anonymous account failed with error {stdout}")
        log.info("The anonymous account has been updated successfully")

    def status(self):
        """
        Get the status of the anonymous account via the NooBaa CLI

        Returns:
            dict|None: The status of the anonymous account.
            If the anonymous account does not exist, returns None

        """
        cmd = f"sudo {self.NB_CLI_PATH} account status --anonymous"

        retcode, stdout, _ = self.conn.exec_cmd(cmd)
        if retcode != 0 and "NoSuchAccount" in stdout:
            return None
        elif retcode != 0:
            # TODO: Change to AccountStatusQueryFailed exception once PR #37 is merged
            log.error(f"Failed to get the status of the anonymous account: {stdout}")

        response_dict = json.loads(stdout)
        return response_dict["response"]["reply"]

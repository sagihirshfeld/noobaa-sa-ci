import logging

from paramiko import AutoAddPolicy
from paramiko.auth_handler import AuthenticationException, SSHException
from common_ci_utils.connection import Connection

log = logging.getLogger(__name__)


class ExtendedConnection(Connection):
    """
    Implements additional methods for SSH connection

    """

    def download_file(self, remotepath, localpath):
        """
        Download a file from a remote host

        Args:
            remotepath (str): target path on the remote host. filename should be included
            localpath (str): local file to download to

        """
        try:
            ssh = self.client
            ssh.set_missing_host_key_policy(AutoAddPolicy())

            sftp = ssh.open_sftp()
            log.info(
                f"Downloading {localpath} from {self.user}@{self.host}:{remotepath}"
            )
            sftp.get(remotepath, localpath)
            sftp.close()
        except AuthenticationException as authException:
            log.error(f"Authentication failed: {authException}")
            raise authException
        except SSHException as sshException:
            log.error(f"SSH connection failed: {sshException}")
            raise sshException

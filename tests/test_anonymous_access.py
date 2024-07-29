import logging
import os
import random
import tempfile

import boto3
import botocore
import botocore.handlers

from framework.bucket_policies.bucket_policy import BucketPolicyBuilder
from framework.customizations.marks import tier1
from framework.ssh_connection_manager import SSHConnectionManager
from noobaa_sa.exceptions import AccountCreationFailed
from noobaa_sa.s3_client import S3Client

log = logging.getLogger(__name__)


class TestAnonymousAccess:
    """
    Test S3 bucket operations with anonymous access
    """

    @tier1
    def test_anonymous_account_management(self, account_manager, linux_user_factory):
        """
        Test management of the anonymous account via the NooBaa CLI
        1. Create new Linux users on the NooBaa server
        2. Create an anonymous account by providing a uid and gid
        3. Check that its uid and gid match the ones we provided
        4. Try creating a second anonymous account and check that it fails
        5. Update the anonymous account with new uid and gid
        6. Check that the uid and gid were updated
        7. Update the anonymous account with a new user name
        8. Check that the user name was updated
        9. Delete the anonymous account
        10. Check that the anonymous account was deleted
        11. Create an anonymous account by providing a user name
        12. Check that the created account has the provided user name
        """

        def _verify_anonymous_account(uid=None, gid=None, username=None):
            """
            Verify that the anonymous account has the expected uid, gid, or user name
            """
            anon_acc_nsfs_config = account_manager.anonymous.status()[
                "nsfs_account_config"
            ]
            if username:
                assert (
                    anon_acc_nsfs_config["distinguished_name"] == username
                ), f"Expected user: {username}, got {anon_acc_nsfs_config}"
            elif uid and gid:
                assert (
                    anon_acc_nsfs_config["uid"] == uid
                    and anon_acc_nsfs_config["gid"] == gid
                ), f"Expected uid: {uid}, gid: {gid}, got {anon_acc_nsfs_config}"
            else:
                raise ValueError(
                    "Either a valid uid and gid pair or a valid user name must be provided"
                )

        # 1. Create new Linux users on the NooBaa server
        uid_a, gid_a, username_a = linux_user_factory()
        uid_b, gid_b, username_b = linux_user_factory()

        # 2. Create an anonymous account by providing a uid and gid
        account_manager.anonymous.create(uid=uid_a, gid=gid_a)

        # 3. Check that its uid and gid match the ones we provided
        _verify_anonymous_account(uid=uid_a, gid=gid_a)

        # 4. Try creating a second anonymous account and check that it fails
        try:
            account_manager.anonymous.create(uid=uid_b, gid=gid_b)
        except AccountCreationFailed as e:
            if "AccountNameAlreadyExists" in str(e):
                log.info("Creating a second anonymous account failed as expected")
            else:
                log.error(
                    "Creating a second anonymous account failed with unexpected error"
                )
                raise e

        # 5. Update the anonymous account with new uid and gid
        log.info(account_manager.anonymous.update(uid=uid_b, gid=gid_b))

        # 6. Check that the uid and gid were updated
        _verify_anonymous_account(uid=uid_b, gid=gid_b)

        # 7. Update the anonymous account with a new user name
        log.info(account_manager.anonymous.update(user=username_b))

        # 8. Check that the user name was updated
        _verify_anonymous_account(username=username_b)

        # 9. Delete the anonymous account
        account_manager.anonymous.delete()

        # 10. Check that the anonymous account was deleted
        assert (
            not account_manager.anonymous.status()
        ), "Anonymous account was not deleted"

        # 11. Create an anonymous account by providing a user name
        account_manager.anonymous.create(user=username_a)

        # 12. Check that the created account has the provided user name
        _verify_anonymous_account(username=username_a)

    @tier1
    def test_anonymous_access(self, account_manager, s3_client_factory):
        """
        Test anonymous access to a bucket

        1. Setup an anonymous account via the NooBaa CLI
        2. Check that creating a bucket without credentials fails
        3. Create a bucket using a different account
        4. Allow anonymous access to the bucket via a policy
        5. Upload files to the bucket without credentials
        6. List the files in the bucket without credentials
        7. Download the files from the bucket without credentials
        8. Delete the files from the bucket without credentials
        9. Delete the bucket without credentials

        """
        named_acc_s3_client = s3_client_factory()
        anon_s3_client = S3Client(
            endpoint=named_acc_s3_client.endpoint,
            access_key=None,
            secret_key=None,
            verify_tls=False,
        )

        anon_s3_client._boto3_resource = boto3.resource(
            "s3",
            endpoint_url=named_acc_s3_client.endpoint,
            verify=False,
        )
        anon_s3_client._boto3_resource.meta.client.meta.events.register(
            "choose-signer.s3.*", botocore.handlers.disable_signing
        )
        anon_s3_client._boto3_client = anon_s3_client._boto3_resource.meta.client

        # 1. Setup an anonymous account via the NooBaa CLI
        account_manager.anonymous.create(0, 0)

        # 2. Check that creating a bucket without credentials fails
        response = anon_s3_client.create_bucket(get_response=True)
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 403
        ), f"Expected 403, got {response['ResponseMetadata']['HTTPStatusCode']}"

        # 3. Create a bucket using a different account
        bucket_name = named_acc_s3_client.create_bucket()

        # 4. Allow anonymous access to the bucket via a policy
        bpb = BucketPolicyBuilder()
        policy = (
            bpb.add_allow_statement()
            .add_action("*")
            .add_principal("*")
            .add_resource(f"{bucket_name}/*")
            .add_resource(f"{bucket_name}")
            .build()
        )
        named_acc_s3_client.put_bucket_policy(bucket_name, str(policy))

        # 5. Upload files to the bucket without credentials
        uploaded_objs = anon_s3_client.put_random_objects(bucket_name, 3)

        # 6. List the files in the bucket without credentials
        listed_objs = anon_s3_client.list_objects(bucket_name)
        assert (
            uploaded_objs == listed_objs
        ), f"Expected {uploaded_objs}, got {listed_objs}"

        # 7. Download the files from the bucket without credentials
        with tempfile.TemporaryDirectory() as tmp_dir:
            anon_s3_client.download_bucket_contents(bucket_name, tmp_dir)
            downloaded_objs = os.listdir(tmp_dir)
            assert set(uploaded_objs) == set(
                downloaded_objs
            ), f"Expected {uploaded_objs}, got {downloaded_objs}"

        # 8. Delete the files from the bucket without credentials
        anon_s3_client.delete_objects(bucket_name, uploaded_objs)
        assert (
            anon_s3_client.list_objects(bucket_name) == []
        ), "Objects were not deleted"

        # 9. Delete the bucket without credentials
        anon_s3_client.delete_bucket(bucket_name)
        assert (
            anon_s3_client.head_bucket(bucket_name)["ResponseMetadata"][
                "HTTPStatusCode"
            ]
            == 404
        ), "Failed bucket deletion via anonymous access"

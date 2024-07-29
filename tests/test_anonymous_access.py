import logging
import os
import random
import tempfile

import boto3
import botocore
import botocore.handlers

from framework.bucket_policies.bucket_policy import BucketPolicyBuilder
from framework.customizations.marks import tier1
from noobaa_sa.exceptions import AccountCreationFailed
from noobaa_sa.s3_client import S3Client

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

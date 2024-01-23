import os
import logging
import tempfile
import boto3
from botocore.config import Config
from common_ci_utils.command_runner import exec_cmd

log = logging.getLogger(__name__)


class S3Client:
    """
    A wrapper class for S3 operations using boto3 and the AWS CLI

    """

    def __init__(self, endpoint, access_key, secret_key, tls_crt_path=None):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.tls_crt_path = tls_crt_path

        # Set the AWS_CA_BUNDLE environment variable
        if self.tls_crt_path:
            os.environ["AWS_CA_BUNDLE"] = tls_crt_path

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def exec_s3_cli_cmd(self, cmd, api=False):
        """
        Crafts the AWS CLI S3 command including the
        login credentials and command to be ran

        Args:
            cmd: The AWSCLI command to run
            api: True if the call is for s3api, false if s3

        Returns:
            The output of the command

        """
        api = "api" if api else ""
        base_command = (
            f"AWS_ACCESS_KEY_ID={self.access_key} "
            f"AWS_SECRET_ACCESS_KEY={self.secret_key} "
            f"AWS_DEFAULT_REGION=us-east-1 "  # Any value will do
            f"aws s3{api} "
            f"--endpoint={self.endpoint} "
        )

        if self.tls_crt_path:
            base_command = f"AWS_CA_BUNDLE={self.tls_crt_path} " + base_command
        else:
            base_command += " --no-verify-ssl"

        output = exec_cmd(f"bash -c '{base_command}{cmd}'")
        # TODO: raise more specific S3 related exceptions based on the content of stderr
        if output.stderr:
            raise Exception(f"Error while executing command: {output.stderr}")
        return output

    def create_bucket(self, bucket_name):
        """
        Create a bucket in an S3 account using boto3

        """
        self.s3_client.create_bucket(Bucket=bucket_name)

    def delete_bucket(self, bucket_name):
        """
        Delete a bucket in an S3 account using boto3

        """
        self.s3_client.delete_bucket(Bucket=bucket_name)

    def list_buckets(self):
        """
        List buckets in an S3 account using boto3

        """
        return self.s3_client.list_buckets()

    def list_objects(self, bucket_name):
        """
        List objects in an S3 bucket using boto3

        """
        output = self.s3_client.list_objects(Bucket=bucket_name)
        list_of_objs_metadata = output["Contents"]
        return [obj["Key"] for obj in list_of_objs_metadata]

    def put_object(self, bucket_name, object_key, object_data):
        """
        Put an object in an S3 bucket using boto3

        """
        self.s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=object_data)

    def sync(self, src, dst):
        """
        Sync files between a source and a destination using the AWS CLI

        Args:
            src: Source path - can be a local path or an S3 path
            dst: Destination path - can be a local path or an S3 path
        """
        output = self.exec_s3_cli_cmd(f"sync {src} {dst}")

    def delete_object(self, bucket_name, object_key):
        """
        Delete an object from an S3 bucket using boto3

        """

        self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)

    def rm_recursive(self, bucket_name, s3_path=""):
        """
        Delete all the objects in an s3 path recursively

        """
        self.exec_s3_cli_cmd(f"rm s3://{bucket_name}/{s3_path}")

    def get_object(self, bucket_name, object_key):
        """
        Get an object from an S3 bucket using boto3


        Returns:
        # TODO
        """
        output = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # TODO: return the object data

    def write_random_objs_to_bucket(
        self, bucket_name, amount=10, obj_size="1M", prefix="", files_dir=""
    ):
        """
        Write random objects to an S3 bucket

        Args:
            bucket_name (str): The name of the bucket to write to
            amount (int): The number of objects to write
            obj_size (str): The size of each object
            prefix (str): A prefix where the objects will be written in the bucket
            files_dir (str): A directory where the objects will be written locally.
                             If not specified, a temporary directory will be used.

        Returns:
            list: A list of the names of the objects written to the bucket

        """
        written_objs = []

        # TODO: Fix the scope issue that forced us to include prefix as an argument here
        def generate_and_upload_objects_using_local_dir(files_dir, prefix):
            for i in range(amount):
                obj_name = f"obj_{i}"
                obj_path = os.path.join(files_dir, obj_name)
                exec_cmd(
                    f"dd if=/dev/urandom of={obj_path} bs={obj_size} count=1 &> /dev/null"
                )
                written_objs.append(obj_name)
            log.info(
                f"Generated the following objects under {files_dir}: {written_objs}"
            )

            log.info(f"Uploading objects to s3://{bucket_name}/{prefix}")
            if prefix and prefix[-1] != "/":
                prefix += "/"
            self.sync(files_dir, f"s3://{prefix}{bucket_name}")

        if files_dir:
            generate_and_upload_objects_using_local_dir(files_dir, prefix)
        else:
            with tempfile.TemporaryDirectory() as tmp_dir:
                generate_and_upload_objects_using_local_dir(tmp_dir, prefix)

        return written_objs

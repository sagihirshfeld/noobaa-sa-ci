import boto3
from botocore.config import Config
from common_ci_utils.command_runner import exec_cmd


class S3Client:
    def __init__(self, endpoint, access_key, secret_key, tsl_crt_path=None):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.tls_crt_path = tsl_crt_path

        s3_client_config = Config(
            signature_version="s3v4",
            retries={"max_attempts": 10, "mode": "standard"},
            verify=tsl_crt_path or False,
        )
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=s3_client_config,
        )

    def exec_s3_cli_cmd(self, cmd, api=False):
        """
        Crafts the AWS CLI S3 command including the
        login credentials and command to be ran

        Args:
            cmd: The AWSCLI command to run
            api: True if the call is for s3api, false if s3

        Returns:
            str: The crafted command, ready to be executed on the pod

        """
        api = "api" if api else ""
        base_command = (
            f"AWS_ACCESS_KEY_ID={self.access_key} "
            f"AWS_SECRET_ACCESS_KEY={self.secret_key} "
            f"AWS_DEFAULT_REGION=us-east-1"  # Any value will do
            f"aws s3{api} "
            f"--endpoint={self.endpoint} "
        )
        if self.tls_crt_path:
            base_command = f"AWS_CA_BUNDLE={self.tls_crt_path} " + base_command
        else:
            base_command += " --no-verify-ssl"
        exec_cmd(f"{base_command}{cmd}")

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
        return self.s3_client.list_objects(Bucket=bucket_name)

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
        self.exec_s3_cli_cmd(f"sync {src} {dst}")

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

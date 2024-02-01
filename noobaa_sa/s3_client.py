import os
import logging
import tempfile
import boto3
from common_ci_utils.command_runner import exec_cmd
from noobaa_sa.exceptions import BucketCreationFailed
from utility.utils import generate_unique_resource_name

log = logging.getLogger(__name__)


# TODO: Add robust exception handling to all the methods
class S3Client:
    """
    A wrapper class for S3 operations using boto3 and the AWS CLI

    The 'access_key' and 'secret_key' are set as read-only properties.
    This allows to keep track of the buckets created by the specific account and
    to delete only them if needed.

    To use different credentials, instantiate a new S3Client object.

    """

    static_tls_crt_path = ""

    def __init__(self, endpoint, access_key, secret_key, verify_tls=True):
        self.endpoint = endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self.verify_tls = verify_tls

        # Set the AWS_CA_BUNDLE environment variable in order to
        # include the TLS certificate in the boto3 and AWS CLI calls
        if self.verify_tls:
            os.environ["AWS_CA_BUNDLE"] = S3Client.static_tls_crt_path

        self._boto3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    @property
    def access_key(self):
        return self._access_key

    @property
    def secret_key(self):
        return self._secret_key

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

        if self.verify_tls:
            base_command = (
                f"AWS_CA_BUNDLE={S3Client.static_tls_crt_path} " + base_command
            )
        else:
            base_command += " --no-verify-ssl"

        output = exec_cmd(f"bash -c '{base_command}{cmd}'")
        # TODO: raise more specific S3 related exceptions based on the content of stderr
        if output.stderr:
            raise Exception(f"Error while executing command: {output.stderr}")
        return output

    def create_bucket(self, bucket_name=""):
        """
        Create a bucket in an S3 account using boto3

        Args:
            bucket_name (str): The name of the bucket to create.
                               If not specified, a random name will be generated.

        Returns:
            str: The name of the created bucket

        """
        if bucket_name == "":
            bucket_name = generate_unique_resource_name(prefix="bucket")
        response = self._boto3_client.create_bucket(Bucket=bucket_name)
        if "Location" not in response:
            raise BucketCreationFailed(
                f"Could not create bucket {bucket_name}. Response: {response}"
            )
        return bucket_name

    def delete_bucket(self, bucket_name, empty_before_deletion=False):
        """
        Delete a bucket in an S3 account using boto3
        If the bucket is not empty, it will not be deleted unless empty_bucket is set to True

        Args:
            bucket_name (str): The name of the bucket to delete
            empty_before_deletion (bool): Whether to empty the bucket before attempting deletion

        """
        if empty_before_deletion:
            self.delete_all_objects_in_bucket(bucket_name)
        self._boto3_client.delete_bucket(Bucket=bucket_name)

    def list_buckets(self):
        """
        List buckets in an S3 account using boto3

        Returns:
            list: A list of the names of the buckets

        """
        response = self._boto3_client.list_buckets()
        return [bucket_data["Name"] for bucket_data in response["Buckets"]]

    def list_objects(self, bucket_name):
        """
        List objects in an S3 bucket using boto3

        Returns:
            list: A list of the names of the objects

        """
        output = self._boto3_client.list_objects(Bucket=bucket_name)
        if "Contents" not in output:
            return []
        else:
            return [obj["Key"] for obj in output["Contents"]]

    def put_object(self, bucket_name, object_key, object_data):
        """
        Put an object in an S3 bucket using boto3

        """
        self._boto3_client.put_object(
            Bucket=bucket_name, Key=object_key, Body=object_data
        )

    def get_text_object_str(self, bucket_name, object_key):
        """
        Get the contents of a text object in an S3 bucket using boto3

        For other types of objects, use get_object_contents

        Returns:
            str: The contents of the object

        """
        return self.get_object(bucket_name, object_key).read().decode("utf-8")

    def get_object(self, bucket_name, object_key):
        """
        Get the contents of an object in an S3 bucket using boto3

        Returns:
            A botocore.response.StreamingBody object that can be read from

        """
        output = self._boto3_client.get_object(Bucket=bucket_name, Key=object_key)
        return output["Body"]

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
        self._boto3_client.delete_object(Bucket=bucket_name, Key=object_key)

    def rm_recursive(self, bucket_name, s3_path=""):
        """
        Delete all the objects in an s3 path recursively

        """
        self.exec_s3_cli_cmd(f"rm s3://{bucket_name}/{s3_path} --recursive")

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
                exec_cmd(f"dd if=/dev/urandom of={obj_path} bs={obj_size} count=1")
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

    def delete_all_objects_in_bucket(self, bucket_name):
        """
        Delete all objects in an S3 bucket

        """
        # TODO: Add support for buckets with versioning enabled
        self.rm_recursive(bucket_name)

import logging

from utility.bucket_utils import upload_incomplete_multipart_object
from framework.customizations.marks import tier1
from utility.utils import check_data_integrity

log = logging.getLogger(__name__)


class TestMultipartOperations:
    """
    Test S3 object multipart operations on NSFS
    """

    @tier1
    def test_multipart_upload(
        self,
        c_scope_s3client,
        tmp_directories_factory,
    ):
        """
        Test basic s3 operations using a noobaa bucket:
        1. Create an account
        2. Create a bucket using S3
        3. Write multipart objects to the bucket
        4. List multipart objects from the bucket

        """
        log.info("Uploading multipart object")
        resp = upload_incomplete_multipart_object(c_scope_s3client, tmp_directories_factory)
        obj_name = resp["object_names"][0]
        log.info("Trying to complete multipart operation for the object")
        mp_response = c_scope_s3client.complete_multipart_object_upload(
            resp["bucket_name"],
            obj_name,
            resp[f"{obj_name}_upload_id"],
            resp["all_part_info"],
        )
        assert (
            mp_response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), "Failed to upload multipart object"
        log.info(mp_response)

    @tier1
    def test_multipart_download(self, c_scope_s3client, tmp_directories_factory):
        """
        Test basic s3 operations using a noobaa bucket:
        1. Write multipart objects to the bucket
        2. Download the objects from the bucket and verify data integrity

        """
        log.info("Uploading multipart object")
        resp = upload_incomplete_multipart_object(c_scope_s3client, tmp_directories_factory)
        obj_name = resp["object_names"][0]
        log.info("Completing multipart operation for the object")
        mp_response = c_scope_s3client.complete_multipart_object_upload(
            resp["bucket_name"],
            obj_name,
            resp[f"{obj_name}_upload_id"],
            resp["all_part_info"],
        )
        log.info(mp_response)
        log.info("Trying to download multipart object and validating it")
        c_scope_s3client.download_bucket_contents(
            resp["bucket_name"], resp["results_dir"]
        )
        assert check_data_integrity(resp["origin_dir"], resp["results_dir"])
        log.info("Both uploaded and downloaded data are identical")

    @tier1
    def test_list_multipart_objects(self, c_scope_s3client, tmp_directories_factory):
        """
        Test multipart object list operations using BOTO s3:
        1. Write multipart objects to the bucket
        2. List objects from the bucket and verify it

        """
        log.info("Uploading multipart object")
        resp = upload_incomplete_multipart_object(c_scope_s3client, tmp_directories_factory)
        obj_name = resp["object_names"][0]
        log.info("Completing multipart operation for the object")
        mp_response = c_scope_s3client.complete_multipart_object_upload(
            resp["bucket_name"],
            obj_name,
            resp[f"{obj_name}_upload_id"],
            resp["all_part_info"],
        )
        log.info(mp_response)
        log.info("Multipart operation is completed")
        log.info(f"Listing objects present in {resp['bucket_name']}")
        listed_objs = c_scope_s3client.list_objects(resp["bucket_name"])
        log.info(listed_objs)
        assert set(resp["object_names"]).issubset(
            set(listed_objs)
        ), "All uploaded objects are not present in bucket"
        log.info("Uploaded objects are present in bucket")

    @tier1
    def test_multipart_list_parts(self, c_scope_s3client, tmp_directories_factory):
        """
        Test multipart object list operations using BOTO s3:
        1. Write multipart objects to the bucket
        2. List parts objects from the bucket

        """
        log.info("Uploading multipart object")
        resp = upload_incomplete_multipart_object(c_scope_s3client, tmp_directories_factory)
        obj_name = resp["object_names"][0]
        log.info(f"Listing incomplete multipart uploads for the object {obj_name}")
        part_resp = c_scope_s3client.list_uploaded_parts(
            resp["bucket_name"], obj_name, resp[f"{obj_name}_upload_id"]
        )
        assert (
            len(part_resp["Parts"]) != 0
        ), f"Failed to list parts of {resp['object_names'][0]} object"
        log.info(part_resp)
        log.info(f"Listing incomplete parts for {obj_name} completed successfully")
        log.info("Completing multipart operation for the object")
        mp_response = c_scope_s3client.complete_multipart_object_upload(
            resp["bucket_name"],
            obj_name,
            resp[f"{obj_name}_upload_id"],
            resp["all_part_info"],
        )
        log.info(mp_response)
        log.info("Multipart operation is completed")

    @tier1
    def test_list_multipart_uploads(self, c_scope_s3client, tmp_directories_factory):
        """
        Test multipart object list operations using BOTO s3:
        1. Write multipart objects to the bucket
        2. List all incomplete parts from the bucket

        """
        log.info("Uploading multipart object")
        resp = upload_incomplete_multipart_object(c_scope_s3client, tmp_directories_factory)
        obj_name = resp["object_names"][0]
        log.info(
            f"Listing incomplete multipart uploads for the bucket {resp['bucket_name']}"
        )
        part_resp = c_scope_s3client.list_multipart_upload(resp["bucket_name"])
        assert (
            len(part_resp["Uploads"]) != 0
        ), f"Failed to list parts present in {resp['bucket_name']} object"
        log.info(part_resp)
        log.info(
            "Listing incomplete multipart uploads operation completed successfully"
        )
        log.info("Completing multipart operation for the object")
        mp_response = c_scope_s3client.complete_multipart_object_upload(
            resp["bucket_name"],
            obj_name,
            resp[f"{obj_name}_upload_id"],
            resp["all_part_info"],
        )
        log.info(mp_response)
        log.info("Multipart operation is completed")

    @tier1
    def test_multipart_upload_part_copy(
        self, c_scope_s3client, tmp_directories_factory
    ):
        """
        Test multipart object list operations using BOTO s3:
        1. Write multipart objects to the bucket
        2. Create new bucket
        3. Copy object data from bucket created in step 1 to new bucket

        """
        log.info("Uploading multipart object")
        resp = upload_incomplete_multipart_object(c_scope_s3client, tmp_directories_factory)
        obj_name = resp["object_names"][0]
        c_scope_s3client.complete_multipart_object_upload(
            resp["bucket_name"],
            obj_name,
            resp[f"{obj_name}_upload_id"],
            resp["all_part_info"],
        )
        log.info("Creating another bucket to copy data")
        new_bucket = c_scope_s3client.create_bucket()
        log.info("Generating upload id for the multipart object")
        get_upload_id = c_scope_s3client.initiate_multipart_object_upload(
            new_bucket,
            obj_name,
        )
        log.info("Copying data using upload_part_copy method")
        upload_part_copy = c_scope_s3client.multipart_upload_part_copy(
            new_bucket,
            obj_name,
            resp["bucket_name"] + "/" + obj_name,
            1,
            get_upload_id,
        )
        assert (
            upload_part_copy["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), f"Failed to copy data from {resp['bucket_name']} to {new_bucket}"
        log.info(upload_part_copy)
        log.info("Data copied successfully from source bucket to new bucket")
        log.info("Completing multipart operation for new object")
        all_part_info = []
        all_part_info.append(
            {"PartNumber": 1, "ETag": upload_part_copy["CopyPartResult"]["ETag"]}
        )
        c_scope_s3client.complete_multipart_object_upload(
            new_bucket, obj_name, get_upload_id, all_part_info
        )
        log.info("Multipart operation is completed using upload_part_copy method")

    @tier1
    def test_s3_multipart_abort_upload(self, c_scope_s3client, tmp_directories_factory):
        """
        Test multipart object list operations using BOTO s3:
        1. Write multipart objects to the bucket
        2. Abore multipart upload

        """
        log.info("Uploading multipart object")
        resp = upload_incomplete_multipart_object(c_scope_s3client, tmp_directories_factory)
        log.info("Aborting Multipart operation")
        obj_name = resp["object_names"][0]
        abort_resp = c_scope_s3client.abort_multipart_upload(
            resp["bucket_name"], obj_name, resp[f"{obj_name}" + "_upload_id"]
        )
        log.info(abort_resp)
        assert (
            abort_resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        ), f"Failed to abort upload operation for {obj_name}"
        log.info(abort_resp)
        log.info("Multipart operation Aborted successfully")

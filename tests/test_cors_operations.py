import logging
import string
import requests
from random import choice, choices, randint
from time import sleep
from utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


class TestCorsConfig:
    """
    Test CORS config elements along with its operation against Bucket
    """

    POSITIVE_RESPONSE = 200
    NEGATIVE_RESPONSE = 403
    NULL_RESPONSE = 204
    INVALID_RESPONSE = 400
    allowed_methods_list = ["GET", "PUT", "POST", "DELETE"]
    expose_headers_list = [
        "ETag",
        "X-Custom-Header",
        "X-Total-Count",
        "X-Request-ID",
        "Content-Disposition",
        "Link",
    ]
    default_cors_config = {
        "CORSRules": [
            {
                "AllowedHeaders": [
                    "Content-Type",
                    "Content-MD5",
                    "Authorization",
                    "X-Amz-User-Agent",
                    "X-Amz-Date",
                    "ETag",
                    "X-Amz-Content-Sha256",
                    "amz-sdk-invocation-id",
                    "amz-sdk-request",
                ],
                "AllowedMethods": ["GET", "POST", "PUT", "DELETE"],
                "AllowedOrigins": ["*"],
                "ExposeHeaders": ["ETag", "X-Amz-Version-Id"],
            }
        ]
    }

    def generate_random_domain_addresses(self, num_of_addresses=1):
        """
        Generates Random domain addresses
        Args:
            num_of_addresses (int): Number of addresses to generate
        Return:
            List
        """
        domain_addresses = []
        for _ in range(num_of_addresses):
            domain_addresses.append(
                "https://"
                + "".join(choices(string.ascii_lowercase + string.digits, k=5))
                + ".com"
            )
        return domain_addresses

    def create_custom_cors_config(
        self, no_of_config=1, method_num=2, exp_header=2, origin_num=2
    ):
        """
        Creates custom CORS config for the bucket
        Args:
            no_of_config (int): Number of CORS config to generate
            method_num (int): Number of methods CORS config should have
            exp_header (int): Number of expose headers CORS config should have
            origin_num (int): Number of origins CORS config should have
        Returns:
            Json config : Dictionary
        """
        cors_config = {"CORSRules": []}
        for _ in range(no_of_config):
            allowed_headers = ["*"]
            allowed_methods = choices(self.allowed_methods_list, k=method_num)
            expose_headers = choices(self.expose_headers_list, k=exp_header)
            allowed_origins = self.generate_random_domain_addresses(origin_num)
            max_age = randint(30, 300)
            conf = {
                "AllowedHeaders": allowed_headers,
                "AllowedMethods": allowed_methods,
                "AllowedOrigins": allowed_origins,
                "ExposeHeaders": expose_headers,
                "MaxAgeSeconds": max_age,
            }
            cors_config["CORSRules"].append(conf)
        return cors_config

    def exec_request(self, s3_endpoint, headers, verify=False):
        """
        Executes api with respective parameters and returns response code
        Args:
            s3_endpoint (String): host address with bucket name
            headers (Dict): Header options
            verify (Bool): Boolean value for SSl verification
        Returns:
            Response code (int)
        """
        response = requests.options(url=s3_endpoint, headers=headers, verify=verify)
        logger.info(response)

        return response.status_code

    def test_basic_cors_operations(
        self,
        c_scope_s3client,
    ):
        """
        Test Basic CORS operation on bucket
            step #1: Create a bucket
            step #2: Get the bucket default CORS configuration
            step #3: delete the default CORS, by running delete-bucket-cors
            step #4: Get the default bucket CORS configuration after deleting it
            step #5: set your own CORS configuration on the bucket using put-bucket-cors api
            step #6: Validate you get the correct bucket cors config
            step #7: Access bucket using supported origin
            step #8: Access bucket using non-supported origin
            step #9: Create multiple CORS config for single bucket
            step #10: Delete the assigned CORS config and try to access bucket from previously supported origin
        """

        # 1: Create a bucket
        bucket_name = c_scope_s3client.create_bucket()
        s3_endpoint = f"{c_scope_s3client.endpoint}/{bucket_name}"

        # 2: Get the bucket default CORS configuration
        response = c_scope_s3client.get_bucket_cors(bucket_name)

        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == self.POSITIVE_RESPONSE
        ), "Failed to get CORS config of the bucket"
        logger.info(response.get("CORSRules"))
        assert self.default_cors_config["CORSRules"] == response.get(
            "CORSRules"
        ), "There is mismatch in default CORS config and received CORS config"

        # 3: delete the default CORS, by running delete-bucket-core
        response = c_scope_s3client.delete_bucket_cors(bucket_name)
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == self.NULL_RESPONSE
        ), "Failed to delete CORS config of the bucket"

        # 4: Get the bucket default CORS configuration after deleting it
        response = c_scope_s3client.get_bucket_cors(bucket_name)
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 404
        ), "Failed to delete CORS config of the bucket"

        # 5: set custom CORS configuration on the bucket using put-bucket-cors api
        cors_config = self.create_custom_cors_config(no_of_config=1)
        response = c_scope_s3client.put_bucket_cors(bucket_name, cors_config)
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == self.POSITIVE_RESPONSE
        ), "Failed to apply CORS config on the bucket"

        # 6: Validate get-bucket-cors returns custom bucket cors config
        response = c_scope_s3client.get_bucket_cors(bucket_name)
        assert cors_config["CORSRules"] == response.get(
            "CORSRules"
        ), "There is mismatch in uploaded CORS config and received CORS config"
        logger.info(response.get("CORSRules"))

        # 7: Access bucket using supported origin
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        headers = {
            "Origin": allowed_origin,
            "Access-Control-Request-Method": http_method,
        }
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 8: Access bucket using non-supported origin
        incorrect_origin = self.generate_random_domain_addresses()[0]
        headers.update(Origin=incorrect_origin)
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

        # 9: Create multiple CORS config for single bucket
        cors_config = self.create_custom_cors_config(no_of_config=3)
        response = c_scope_s3client.put_bucket_cors(bucket_name, cors_config)
        response = c_scope_s3client.get_bucket_cors(bucket_name)
        assert cors_config["CORSRules"] == response.get(
            "CORSRules"
        ), "There is mismatch in uploaded CORS config and received CORS config"
        logger.info(response.get("CORSRules"))

        # 10: Delete the assigned CORS config and try to access bucket from previously supported origin
        response = c_scope_s3client.delete_bucket_cors(bucket_name)
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        headers.update(Origin=allowed_origin)
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

    def test_allowed_origin_cors_element(self, c_scope_s3client):
        """
        Test AllowedOrigins element from CORS operation on bucket
            step #1: Create bucket and apply CORS config with one allowed origin address
            step #2: Perform Allowed request from allowed origin mentioned in CORS
            step #3: Modify the existing CORS config and add different and multiple origins in it
            step #4: Perform GET request from any allowed origin mentioned in step #3
            step #5: Add wildcard(*) character in existing CORS config
            step #6: Perform GET request from any origin
            step #7: Modify exisitng CORS and add wildcard character like ""http://*.abc.com""
            step #8: Perform GET request from any origin that has address like ""http://app.abc.com""
            step #9: Perform GET request from non origin address
        """
        # 1: Create bucket and apply CORS config with one allowed origin address
        bucket_name = c_scope_s3client.create_bucket()
        s3_endpoint = f"{c_scope_s3client.endpoint}/{bucket_name}"
        cors_config = self.create_custom_cors_config(no_of_config=1)
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 2: Perform Allowed request from allowed origin mentioned in CORS
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        headers = {
            "Origin": allowed_origin,
            "Access-Control-Request-Method": http_method,
        }
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 3: Modify the existing CORS config and add different and multiple origins in it
        extra_origin = self.generate_random_domain_addresses()[0]
        cors_config["CORSRules"][0]["AllowedOrigins"].append(extra_origin)
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 4: Perform GET request from any allowed origin mentioned in step #3
        headers.update(Origin=extra_origin)
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 5: Add wildcard(*) character in existing CORS config
        wildcard_origin = ["*"]
        cors_config["CORSRules"][0]["AllowedOrigins"] = wildcard_origin
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 6: Perform GET request from any origin
        random_origin = self.generate_random_domain_addresses()[0]
        headers.update(Origin=random_origin)
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 7: Modify exisitng CORS and add wildcard character like ""http://*.abc.com""
        subdomain_origin = ["https://*.abc.com"]
        cors_config["CORSRules"][0]["AllowedOrigins"] = subdomain_origin
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 8: Perform GET request from any origin that has address like ""https://app.abc.com""
        headers.update(Origin="https://app.abc.com")
        logger.info(headers)
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 9: Perform GET request from non origin address
        invalid_origin = self.generate_random_domain_addresses()[0]
        headers.update(Origin=invalid_origin)
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

    def test_allowed_method_cors_element(self, c_scope_s3client):
        """
        Test AllowedMethods element from CORS operation on bucket
            step #1: Create bucket and apply CORS config with one allowed HTTP method
            step #2: Perform allowd method request from allowed origin mentioned in CORS
            step #3: Perform non supported request from allowed origin mentioned in CORS
            step #4: Modify the existing CORS config and add multiple HTTP method in it(GET, POST)
            step #5: Perform GET and POST request from allowed origin
            step #6: Modify the existing CORS config and add non-supported HTTP method in it(PATCH)
            step #7: Modify the existing CORS config and add all suported HTTP method along with multiple origins in it
            step #8: Perform any request mentioned in allowed HTTP method from any supported origin mentioned on CORS
        """
        # 1: Create bucket and apply CORS config with one allowed HTTP method(GET)
        bucket_name = c_scope_s3client.create_bucket()
        s3_endpoint = f"{c_scope_s3client.endpoint}/{bucket_name}"
        cors_config = self.create_custom_cors_config(no_of_config=1, method_num=1)
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 2: Perform allowed method request from allowed origin mentioned in CORS
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        headers = {
            "Origin": allowed_origin,
            "Access-Control-Request-Method": http_method,
        }
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 3: Perform non supported request from allowed origin mentioned in CORS
        non_supported_method = choice(self.allowed_methods_list)
        while True:
            if non_supported_method != http_method:
                break
            non_supported_method = choice(self.allowed_methods_list)
        headers.update({"Access-Control-Request-Method": non_supported_method})
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

        # 4: Modify the existing CORS config and add multiple HTTP method in it
        cors_config["CORSRules"][0]["AllowedMethods"].append(non_supported_method)
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 5: Perform multiple ops from allowed method request from allowed origin
        for i in cors_config["CORSRules"][0]["AllowedMethods"]:
            headers.update({"Access-Control-Request-Method": i})
            sample = TimeoutSampler(
                timeout=120,
                sleep=10,
                func=self.exec_request,
                s3_endpoint=s3_endpoint,
                headers=headers,
            )
            sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 6: Modify the existing CORS config and add non-supported HTTP method in it(PATCH)
        cors_config["CORSRules"][0]["AllowedMethods"].append("PATCH")
        response = c_scope_s3client.put_bucket_cors(bucket_name, cors_config)
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == self.INVALID_RESPONSE
        ), "CORS is applying invalid HTTP method(PATCH)"
        logger.info(response)

        # 7: Modify the existing CORS config and add all suported HTTP method along with multiple origins in it
        cors_config["CORSRules"][0]["AllowedMethods"] = []
        for val in self.allowed_methods_list:
            cors_config["CORSRules"][0]["AllowedMethods"].append(val)
        cors_config["CORSRules"][0]["AllowedOrigins"] = ["*"]
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 8: Perform any request mentioned in allowed HTTP method from any supported origin mentioned on CORS
        for i in range(3):
            method = choice(self.allowed_methods_list)
            origin = self.generate_random_domain_addresses()[0]
            headers.update({"Access-Control-Request-Method": method})
            headers.update(Origin=origin)
            sample = TimeoutSampler(
                timeout=120,
                sleep=10,
                func=self.exec_request,
                s3_endpoint=s3_endpoint,
                headers=headers,
            )
            sample.wait_for_func_value(self.POSITIVE_RESPONSE)

    def test_allowed_header_cors_element(self, c_scope_s3client):
        """
        Test AllowedHeaders element from CORS operation on bucket
            step #1: Create bucket and apply CORS config with one allowed HTTP header(x-custom-header)
            step #2: Perform allowed header request from allowed origin mentioned in CORS
            step #3: Perform non supported request from allowed origin mentioned in CORS
            step #4: Modify the existing CORS config and add multiple HTTP hraders in it
                    (x-custom-header, x-other-header)
            step #5: Perform allowed header request from allowed origin
        """
        # 1: Create bucket and apply CORS config with one allowed HTTP header(Content-Type)
        bucket_name = c_scope_s3client.create_bucket()
        s3_endpoint = f"{c_scope_s3client.endpoint}/{bucket_name}"
        cors_config = self.create_custom_cors_config(
            method_num=1, exp_header=1, origin_num=1
        )
        cors_config["CORSRules"][0]["AllowedHeaders"] = ["x-custom-header"]
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 2: Perform allowed header request from allowed origin mentioned in CORS
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        headers = {
            "Origin": allowed_origin,
            "Access-Control-Request-Method": http_method,
            "Access-Control-Request-Headers": "X-Custom-Header",
        }
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

        # 3: Perform non supported request from allowed origin mentioned in CORS
        headers.update({"Access-Control-Request-Headers": "x-other-header"})
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.NEGATIVE_RESPONSE)

        # 4: Modify the existing CORS config and add multiple HTTP hraders in it(Content-Type, Content-MD5)
        cors_config["CORSRules"][0]["AllowedHeaders"].append("x-other-header")
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 5: Perform allowed header request from allowed origin
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.exec_request,
            s3_endpoint=s3_endpoint,
            headers=headers,
        )
        sample.wait_for_func_value(self.POSITIVE_RESPONSE)

    def test_expose_header_cors_element(self, c_scope_s3client):
        """
        Test ExposeHeader element from CORS operation on bucket
            step #1: Create bucket and apply CORS config with only one ExposeHeader(x-amz-meta-custom-header)
            step #2: Perform GET request from allowed origin mentioned in CORS and validate exposed header is present
            step #3: Modify the existing CORS config and add multiple ExposeHeaders in it
                    (x-amz-meta-custom-header, x-amz-request-id)
            step #4: Perform GET request from allowed origin mentioned in CORS and validate exposed header is present
        """

        # 1: Create bucket and apply CORS config with only one ExposeHeader(x-amz-meta-custom-header)
        bucket_name = c_scope_s3client.create_bucket()
        s3_endpoint = f"{c_scope_s3client.endpoint}/{bucket_name}"
        cors_config = self.create_custom_cors_config(
            method_num=1, exp_header=1, origin_num=1
        )
        cors_config["CORSRules"][0]["ExposeHeaders"] = ["x-amz-meta-custom-header"]
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 2: Perform GET request from allowed origin mentioned in CORS and validate exposed header is present
        sleep(20)
        allowed_origin = choice(cors_config["CORSRules"][0]["AllowedOrigins"])
        http_method = choice(cors_config["CORSRules"][0]["AllowedMethods"])
        headers = {
            "Origin": allowed_origin,
            "Access-Control-Request-Method": http_method,
        }
        response = requests.options(url=s3_endpoint, headers=headers, verify=False)
        logger.info(response)
        expose_headers = response.headers.get("Access-Control-Expose-Headers")
        logger.info(expose_headers)
        assert (
            "x-amz-meta-custom-header" in expose_headers
        ), "Missing Access-Control-Expose-Headers in the response"

        # 3: Modify the existing CORS config and add multiple ExposeHeaders in it
        cors_config["CORSRules"][0]["ExposeHeaders"].append("x-amz-request-id")
        provided_expose_header = cors_config["CORSRules"][0]["ExposeHeaders"]
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 4: Perform GET request from allowed origin mentioned in CORS and validate exposed headers are present
        sleep(20)
        response = requests.options(url=s3_endpoint, headers=headers, verify=False)
        logger.info(response)
        expose_headers = response.headers.get("Access-Control-Expose-Headers")
        logger.info(expose_headers)
        received_expose_header = [i.strip() for i in expose_headers.split(",")]
        assert sorted(received_expose_header) == sorted(
            provided_expose_header
        ), "Missing expected Expose Header element from the response"

    def test_MaxAgeSeconds_and_AllowCredentials_element(self, c_scope_s3client):
        """
        Test MaxAgeSeconds and AllowCredentials element from CORS operation on bucket
            On "AllowCredentials" element part, user is not allowed to set it to false
            and this parameter is invisible from user

            step #1: Create bucket and apply basic CORS config with MaxAgeSeconds element in it
            step #2: Modify MaxAgeSeconds parameter by adding 30 secs in it and validate the same
            step #3: Apply basic CORS config on bucket by adding "AllowCredentials" parameter to "False" value
        """

        # 1: Create bucket and apply basic CORS config with MaxAgeSeconds element in it
        bucket_name = c_scope_s3client.create_bucket()
        cors_config = self.create_custom_cors_config(no_of_config=1, method_num=0)
        cors_config["CORSRules"][0]["AllowedMethods"] = ["GET"]
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)

        # 2: Modify MaxAgeSeconds parameter by adding 30 secs in it and validate the same
        new_sec = cors_config["CORSRules"][0].get("MaxAgeSeconds") + 30
        cors_config["CORSRules"][0]["MaxAgeSeconds"] = new_sec
        c_scope_s3client.put_bucket_cors(bucket_name, cors_config)
        sleep(20)
        response = c_scope_s3client.get_bucket_cors(bucket_name)
        assert (
            response["CORSRules"][0]["MaxAgeSeconds"] == new_sec
        ), "There is mismatch in uploaded CORS MaxAgeSeconds and received CORS MaxAgeSeconds"

        # 3: Apply basic CORS config on bucket by adding "AllowCredentials" parameter to "False" value
        cors_config["CORSRules"][0]["AllowCredentials"] = "False"
        try:
            response = c_scope_s3client.put_bucket_cors(bucket_name, cors_config)
            assert (
                response["ResponseMetadata"]["HTTPStatusCode"] == self.POSITIVE_RESPONSE
            ), "CORS is adding (non-supported) AllowCredentials element in it"
        except Exception as e:
            expected_err = 'Unknown parameter in CORSConfiguration.CORSRules[0]: "AllowCredentials"'
            if expected_err in str(e):
                logger.warning("Expected error")
                logger.info(
                    f"CORS config with AllowCredentials rule was rejected as expected: {e}"
                )
            else:
                raise

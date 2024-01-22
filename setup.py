from setuptools import setup

setup(
    name="noobaa-sa-ci",
    version="0.1",
    packages=[""],
    url="",
    license="MIT",
    author="Noobaa SA QE",
    author_email="ocs-ci@redhat.com",
    description="Noobaa Standalone(SA) CI is used to run test cases.",
    install_requires=[
        "common-ci-utils",
        "jinja2",
        "mergedeep",
        "pytest",
        "pynpm",
        "pyyaml",
        "requests",
        "boto3",
    ],
    entry_points={
        "console_scripts": [
            "noobaa-sa-ci=framework.main:main",
        ],
    },
)

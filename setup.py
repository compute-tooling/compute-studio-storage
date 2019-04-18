import setuptools
import os

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="s3like",
    version=os.environ.get("VERSION", "0.0.0"),
    author="Hank Doupe",
    author_email="henrymdoupe@gmail.com",
    description=(
        "A small package that is used by COMP to read and write model "
        "results to S3 like object storage systems."
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/comp-org/s3like",
    packages=setuptools.find_packages(),
    install_requires=["marshmallow>=3.*", "requests", "boto3"],
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
    ],
)

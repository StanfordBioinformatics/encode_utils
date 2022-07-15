###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

# For some useful documentation, see
# https://docs.python.org/2/distutils/setupscript.html.
# This page is useful for dependencies
# http://python-packaging.readthedocs.io/en/latest/dependencies.html.

# PSF tutorial for packaging up projects:
# https://packaging.python.org/tutorials/packaging-projects/

import glob
import os
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

SCRIPTS_DIR = os.path.join("igvf_utils", "scripts")
scripts = glob.glob(os.path.join(SCRIPTS_DIR, "*.py"))
scripts.remove(os.path.join(SCRIPTS_DIR, "__init__.py"))
scripts.append(os.path.join("igvf_utils", "MetaDataRegistration", "eu_register.py"))

setup(
  author="Nathaniel Watson",
  author_email="nathankw@stanford.edu",
  classifiers=[
      "Programming Language :: Python :: 3",
      "License :: OSI Approved :: MIT License",
      "Operating System :: OS Independent",
  ],
  description="Client and tools for IGVF data submitters.",
  install_requires=[
        "awscli",
        "boto3",
        "exifread",
        "google-api-python-client",
        "google-cloud-storage==1.28.1",
        "inflection",
        "jsonschema",
        "pillow",
        "requests",
        "urllib3",
    ],
  extras_require={
      "dev": [
          "pytest",
          "pytest-mock",
      ],
  },
  long_description=long_description,
  long_description_content_type="text/markdown",
  name="igvf-utils",
  packages=find_packages(),
  package_data={"igvf_utils": [os.path.join("tests", "data", "*")]},
  project_urls={
      "Read the Docs": "https://encode-utils.readthedocs.io/en/latest",
  },
  scripts=scripts,
  url="https://github.com/IGVF-DACC/igvf_utils",  # home page
  version="2.11.0",
)

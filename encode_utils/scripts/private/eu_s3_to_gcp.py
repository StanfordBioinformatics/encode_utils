#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Copies one or more ENCODE files from AWS S3 storage to GCP storage by using the Google Storage
Transfer Service. See :class:`encode_utils.transfer_to_gcp.Transfer` for full documentation.
"""

import argparse
import datetime
import json
import os

import encode_utils.connection as euc
from encode_utils.parent_argparser import dcc_login_parser

def get_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        parents=[dcc_login_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-f", "--file-ids", nargs="+",  required=True, help="""
      One or more ENCODE files to transfer. They can be any valid ENCODE File object identifier.
      Don't mix ENCODE files from across buckets.""")
    parser.add_argument("-gb", "--gcpbucket", required=True, help="""
        The name of the GCP bucket.""")
    parser.add_argument("-gp", "--gcpproject", required=True, help="""
        The GCP project that is associated with gcp_bucket.""")
    parser.add_argument("-d", "--description", help="""
        The description to show when querying transfers via the Google Storage Transfer API, or via
        the GCP Console. May be left empty, in which case the default description will be the value
        of the first S3 file name to transfer.""")
    parser.add_argument("-c", "--s3creds", help="""
        AWS credentials. Provide them in the form `AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY`.
        Ideally, they'll be stored in the environment. However, for additional flexability you can
        specify them here as well.""")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    desc = args.description
    aws_creds = args.s3creds
    if aws_creds:
        aws_creds = aws_creds.split(":")
    # Connect to the Portal
    dcc_mode = args.dcc_mode
    if dcc_mode:
        conn = euc.Connection(dcc_mode)
    else:
        # Default dcc_mode taken from environment variable DCC_MODE.
        conn = euc.Connection()

    file_ids = args.file_ids
    gcp_bucket = args.gcpbucket
    gcp_project = args.gcpproject
    conn.gcp_transfer(file_ids=file_ids, gcp_bucket=gcp_bucket,
                           gcp_project=gcp_project, description=desc, aws_creds=aws_creds)

if __name__ == "__main__":
    main()

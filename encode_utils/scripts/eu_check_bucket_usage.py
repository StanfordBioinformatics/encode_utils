"""
Loops over all ENCODE files and reports staus, format and size in Mb used in bucket
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Benjamin Hitz
# hitz@stanford.edu
###

import argparse
from encode_utils.connection import Connection
from encode_utils.parent_argparser import dcc_login_parser
# dcc_login_parser  contains the arguments needed for logging in to the
# ENCODE Portal, including which env.
import boto3
from urllib.parse import urlparse


def get_parser():
    parser = argparse.ArgumentParser(
        parents=[dcc_login_parser],
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)

    return parser


def main():

    """Program
    """
    parser = get_parser()
    args = parser.parse_args()
    mode = args.dcc_mode

    conn = Connection(mode)
    search_args = {
        "type": 'File',
        "format": 'json',
        "no_file_available": 'false',
        "frame": 'object'
    }
    all_files = conn.search(search_args=search_args, limit='all')

    s3 = boto3.client('s3')

    storage = {}
    for f in all_files:
        creds = conn.get_upload_credentials(f['accession'], regen=False)
        full_path = creds['upload_url']
        parse = urlparse(full_path)
        bucket = parse.netloc
        key = parse.path.lstrip('/')
        try:
            s3res = s3.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            print("S3 error:{}".format(e))
            continue
        storage[f['status']] = storage.get(f['status'], {})
        storage[f['status']][f['file_format']] = storage[f['status']].get(f['file_format'],0)
        storage[f['status']][f['file_format']] += s3res['ContentLength']

    fh = open('sizes.tsv', 'w')
    first = list(storage.values())[0]
    fh.write("\t".join(list(first.keys())+['total'])+'\n')
    for stats in storage.keys():
        tot = 0
        for size in storage[stats].values():
            tot += size
        sizes = list(storage[stats].values())+[tot]
        fh.write("\t".join([str(x) for x in sizes])+'\n')



if __name__ == "__main__":
    main()

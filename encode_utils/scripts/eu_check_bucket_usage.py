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


def add_to_storage(storage, acc, status, fmt, size):

    storage[status] = storage.get(status, {})
    storage[status][fmt] = storage[status].get(fmt, 0)
    storage[status][fmt] += size

    return storage

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
    added = {}
    logfh = open('files.log.tsv', 'r+')

    for old in logfh.readlines():
        (acc, status, fmt, size) = old.split('\t')
        storage = add_to_storage(storage, acc, status, fmt, int(size))
        added[acc] = size
    for f in all_files:
        if not f.get('accession'):
            print("FILE: {} \nhas no accession".format(f))
            # note these should be typcially counted, they are Reference file, maybe use @id as key?
            f['accession'] = f['@id']
        if added.get(f['accession'], 0):
            print("FILE: {} already counted, skipping".format(f['accession']))
            continue
        creds = conn.get_upload_credentials(f['accession'], regen=False, datastore='elasticsearch')
        if not creds or not creds.get('upload_url'):
            print("FILE {} has never been uploaded, skipping".format(f['accession']))
            continue
        full_path = creds['upload_url']
        parse = urlparse(full_path)
        bucket = parse.netloc
        key = parse.path.lstrip('/')
        try:
            s3res = s3.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            print("FILE: {}; S3 error:{}".format(f['accession'], e))
            continue

        storage = add_to_storage(storage, f['accession'], f['status'], f['file_format'], s3res['ContentLength'])

        logfh.write("\t".join([f['accession'], f['status'], f['file_format'], str(s3res['ContentLength'])]))
        logfh.write("\n")
        logfh.flush()

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

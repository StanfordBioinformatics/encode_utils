#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
# 2018-10-23
###

"""
Creates a Google Storage Transfer Service URL list file, which can be used as input into the Google
STS to transfer released ENCODE S3 files to your GCP buckets.
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
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", "--file-ids", nargs="+", help="""
      An alternative to --infile, one or more ENCODE file identifiers. Don't mix ENCODE files 
      from across buckets.""")
    group.add_argument("-i", "--infile", help="""
      An alternative to --file-ids, the path to a file containing one or more file identifiers, 
      one per line. Empty lines and lines starting with a '#' are skipped.""")
    parser.add_argument("-o", "--outfile", required=True, help="""
      The output URL list file name.""")

    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    outfile = args.outfile
    # Connect to the Portal
    dcc_mode = args.dcc_mode
    if dcc_mode:
        conn = euc.Connection(dcc_mode)
    else:
        # Default dcc_mode taken from environment variable DCC_MODE.
        conn = euc.Connection()

    file_ids = args.file_ids
    infile = args.infile
    if infile:
        fh = open(infile)
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            file_ids.append(line)
        fh.close()
            
    conn.gcp_transfer_urllist(file_ids=file_ids, filename=outfile)

if __name__ == "__main__":
    main()

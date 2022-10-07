#!/usr/bin/env python3
# -*- coding: utf-8 -*-                                                                                
                                                                                                       
###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                                
# Nathaniel Watson                                                                                     
# nathankw@stanford.edu                                                                                
### 

"""
Generates upload credentials for one or more File records on the IGVF Portal. The upload credentials store
AWS authorization tokens to allow a user to upload a file to a presigned S3 URL.

The Exception `requests.exceptions.HTTPError` will be raised if the response from the server a not a
successful status code for any File object when attempting to generate the upload credentials, and
will utlimately halt the program with an error message.

"""

import argparse
import datetime
import json
import os

import igvf_utils.connection as iuc
from igvf_utils.parent_argparser import igvf_login_parser

def get_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        parents=[igvf_login_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", "--file-ids", default=[], nargs="+", help="""
      One or more IGVF File identifiers. They can be any valid IGVF File object identifier, i.e.
      alias, uuid, accession, md5sum.""")
    group.add_argument("-i", "--infile", help="""Input file containing File object identifiers, one
        per line.""")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    file_ids = args.file_ids
    infile = args.infile
    # Connect to the Portal
    igvf_mode = args.igvf_mode
    if igvf_mode:
        conn = iuc.Connection(igvf_mode)
    else:
        # Default igvf_mode taken from environment variable IGVF_MODE.
        conn = iuc.Connection()
    if infile:
        fh = open(infile)
        for line in fh:
            line = line.strip()
            if not line:
                continue
            file_ids.append(line)
        fh.close()
    for f in file_ids:
        print("Generating upload credentials for File record '{}'.".format(f))
        conn.regenerate_aws_upload_creds(file_id=f) 


if __name__ == "__main__":
    main()

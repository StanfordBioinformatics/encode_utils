#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Provided a search URL that is specific to the IGVF Portal, saves the search results as JSON
in the specified output file. The search results are stored as a list of JSON objects.

|
"""

import argparse
import json
import os
import sys

import igvf_utils.connection as iuc
from igvf_utils.parent_argparser import igvf_login_parser

# Check that Python3 is being used
v = sys.version_info
if v < (3, 3):
    raise Exception("Requires Python 3.3 or greater.")

def get_parser():
    parser = argparse.ArgumentParser(
        description = __doc__,
        parents=[igvf_login_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-l", "--limit", type=int, help="The number of search results to get back. Leave blank if you want all.") 
    parser.add_argument("-u", "--url", required=True, help="The IGVF Portal URL that you use for searching. Wrap this in quotes to make sure its all treated as one argument.")
    parser.add_argument("-o", "--outfile", required=True, help="The JSON output file containing the search results.")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    igvf_mode = args.igvf_mode
    limit = args.limit
    outfile = args.outfile
    url = args.url

    if dcc_mode:
        conn = iuc.Connection(igvf_mode)
    else:
        # Default igvf_mode taken from environment variable IGVF_MODE.
        conn = iuc.Connection()

    fout = open(outfile, "w")
    results = conn.search(limit=limit, url=url) #returns a list of search results
    fout.write(json.dumps(results))
    fout.close()


if __name__ == "__main__":
    main()

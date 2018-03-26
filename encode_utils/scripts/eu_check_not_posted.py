#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Checks if the specified record identifiers are found on the Portal or not by doing a GET request
on each one. If a 404 (not found) response is returned, then this identifier is written to the
specified output file.
"""

import argparse

from encode_utils.connection import Connection
from encode_utils.parent_argparser import dcc_login_parser
from encode_utils.profiles import Profile

# dcc_login_parser  contains the arguments needed for logging in to the
# ENCODE Portal, including which env.


def get_parser():
    parser = argparse.ArgumentParser(
        parents=[dcc_login_parser],
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-i", "--infile", required=True, help="""
    Input file containing record identifiers, one per line. Any line starting with a
    '#' will be skipped.
    """)

    parser.add_argument("-o", "--outfile", required=True, help="""
    Output file containing a subset of the input file. Only record IDs that weren't found on the
    Portal will be written to this file, one per line.
    """)
    return parser


def main():
    """Program
    """
    parser = get_parser()
    args = parser.parse_args()
    infile = args.infile
    outfile = args.outfile
    dcc_mode = args.dcc_mode

    conn = Connection(dcc_mode)

    fh = open(infile, 'r')
    fout = open(outfile, 'w')
    for line in fh:
        rec_id = line.strip()
        if not rec_id or rec_id.startswith("#"):
            continue
        rec = conn.get(rec_id, ignore404=True)
        if not rec:
            print("'{}' not found.".format(rec_id))
            fout.write(rec_id + "\n")
    fout.close()
    fh.close()


if __name__ == "__main__":
    main()

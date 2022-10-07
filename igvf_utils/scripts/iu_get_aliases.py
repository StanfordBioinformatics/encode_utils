#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Retrieves the aliases for the given set of DCC record identifiers.
"""

import argparse
from igvf_utils.connection import Connection
from igvf_utils.parent_argparser import igvf_login_parser
# igvf_login_parser contains the arguments needed for logging in to the
# IGVF Portal, including which env.


def get_parser():
    parser = argparse.ArgumentParser(
        parents=[igvf_login_parser],
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-i", "--infile", required=True, help="""
    Input file containing IGVF object identifiers (one per line), i.e. UUID, accession, or @id.
    Empty lines and lines beginning with a '#' will be ignored.
  """)

    parser.add_argument("-o", "--outfile", required=True, help="""
    The output file, which is the same as the input file except for the addition of the
    tab-delimited columns - one for each alias.
  """)
    return parser


def main():
    """Program
    """
    parser = get_parser()
    args = parser.parse_args()
    infile = args.infile
    outfile = args.outfile
    igvf_mode = args.igvf_mode

    conn = Connection(igvf_mode=igvf_mode)

    fh = open(infile, 'r')
    fout = open(outfile, 'w')
    for line in fh:
        rec = line.strip("\n").split("\t")[0]
        if not rec or rec.startswith("#"):
            fout.write(line)
            continue
        rec = conn.get(rec_ids=rec, ignore404=False)
        aliases = rec["aliases"]
        for a in aliases:
            line = [line.strip("\n")]
            outline = line.extend(aliases)
            fout.write("\t".join(line) + "\n")
    fout.close()
    fh.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
For the provided DCC record identifiers, PATCHES each record's indicated property with the provided
value. This only makes sense to use when all records under consideration share the same property to
PATCH. But for ease of use, 422 server responses are caught; i.e. the kind with the description of

  "Additional properties are not allowed ('award' was unexpected)" 

which would mean that you are trying to PATCH a property that doesn't exist for the given record type,
and there will be no further processing of the offending record. This check is put into place rather
than just allowing the program to break execution since sometimes it's easiest to let the user provide
a mixed jumble of record identifiers where most of which need to be updated. For example, I once
POSTED a whole slew of record types with a defunct value for the award property. Some profiles don't
have this property, such as replicate. It was easy for me to just feed in the IDs for all POSTED records
and have this program fix any which one that had the award property defined and ignore the rest.
"""

import argparse
import datetime
import json
import os
import pdb

import encode_utils.connection as euc
from encode_utils.parent_argparser import dcc_login_parser

def get_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        parents=[dcc_login_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-r", "--records", default=[], nargs="+", help="""
      One or more DCC record identifiers. """)
    group.add_argument("-i", "--infile", help="""
      An input file containing one or more DCC record identifiers, one per line. Empty lines and
      lines starting with '#' are skipped. """)
    parser.add_argument("-p", "--property", required=True, help="The name of the property to PATCH.")
    parser.add_argument("-v", "--value", required=True, help="The value of the property to PATCH in.")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    rec_ids = args.records
    infile = args.infile
    prop_name = args.property
    prop_val = args.value
    # Connect to the Portal
    dcc_mode = args.dcc_mode
    if dcc_mode:
        conn = euc.Connection(dcc_mode)
    else:
        # Default dcc_mode taken from environment variable DCC_MODE.
        conn = euc.Connection()
    if not rec_ids:
        # Then get them from input file
        fh = open(infile)
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            rec_ids.append(line)
    e = ""
    patch_cnt = 0
    for i in rec_ids:
        try:
            conn.patch({prop_name: prop_val, conn.ENCID_KEY: i})
            patch_cnt += 1
        except Exception as e:
            if e.response.status_code == 422: # Unprocessable Entity
                # Then likely this property is not defined for this record.
                text = json.loads(e.response.text)
                print("Can't PATCH record {}: {}".format(i, text["errors"]))
    print("Finished: PATCHED {} records.".format(str(patch_cnt) + "/" + str(len(rec_ids))))

if __name__ == "__main__":
    main()

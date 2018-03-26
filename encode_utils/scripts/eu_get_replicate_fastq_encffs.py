#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Given an ENCODE experiment identifier, Prints the FASTQ ENCFF identifiers for the specified
replicate and technical replicates, or all replicates. Also prints the replicate numbers. For
each FASTQ identifer, the following is printed to stdout:

  $BioNum_$TechNum_$ReadNum\\t$encff

where variables are defined as:

  $BioNum  - the biological repliate number
  $TechNum - the technial replicate number
  $ReadNum - '1' for a forwards reads FASTQ file, and '2' for a reverse reads FASTQ file.
"""

import argparse

from encode_utils.connection import Connection
from encode_utils.parent_argparser import dcc_login_parser
# dcc_login_parser  contains the arguments needed for logging in to the
# ENCODE Portal, including which env.


def get_parser():
    parser = argparse.ArgumentParser(
        parents=[dcc_login_parser],
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-e", "--exp-id", required=True, help="""
    The experiment to which the replicates belong. Must be set if --all-reps is absent.
  """)
    parser.add_argument("-b", "--bio-rep-num", type=int, help="""
    Print FASTQ ENCFFs for this specified biological replicate number.
  """)

    parser.add_argument("-t", "--tech-rep-num", type=int, help="""
    Print FASTQ ENCFFs for the specified technical replicate number of the specified biological
    replicate.
  """)
    return parser


def main():
    """Program
    """
    parser = get_parser()
    args = parser.parse_args()
    mode = args.dcc_mode
    exp_id = args.exp_id
    bio_rep_num = args.bio_rep_num
    tech_rep_num = args.tech_rep_num

    conn = Connection(mode)
    rep_dico = conn.get_fastqfile_replicate_hash(exp_id)

    for b in rep_dico:
        if bio_rep_num and b != bio_rep_num:
            continue
        for t in rep_dico[b]:
            if tech_rep_num and t != tech_rep_num:
                continue
            for read_num in rep_dico[b][t]:
                for fastq_json in rep_dico[b][t][read_num]:
                    alias = fastq_json["aliases"][0]
                    print("_".join([str(b), str(t), str(read_num)]) + "\t" + alias)


if __name__ == "__main__":
    main()

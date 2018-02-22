#!/usr/bin/env python3
# -*- coding: utf-8 -*-  

###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
### 

"""Finds FASTQ files with a content error and indicates the error.

Given an input file containing experiment identifiers, one per row, looks up all FASTQ files on the
experiment to check for content errors. If any FASTQ file object has a `status` property of 
"content_error", then the content error message will be extracted from the property 
`content_error_detail`. A new file will be output containing the error details.
"""

import argparse

from encode_utils.connection import Connection
from encode_utils.parent_argparser import dcc_login_parser 
from encode_utils.profiles import Profile

#dcc_login_parser  contains the arguments needed for logging in to the ENCODE Portal, including which env.


def get_parser():
  parser = argparse.ArgumentParser(parents=[dcc_login_parser],description=__doc__,formatter_class=argparse.RawTextHelpFormatter)
  parser.add_argument("-i","--infile",required=True,help="""
    Input file containing experiment identifiers, one per row. Any line starting with a
    '#' will be skipped.
  """) 

  parser.add_argument("-o","--outfile",required=True,help="""
    Output file containing two tab-delimited columns:
    
      1. The experiment identifier (from the input file).
      2. The error message stored in the File objects `content_error_detail` property.

  """)
  return parser

def main():
  EXP_PROFILE_ID = "experiment"
  FILE_PROFILE_ID = "file"
  VALID_PROFILES = [EXP_PROFILE_ID,FILE_PROFILE_ID]
  parser = get_parser()
  args = parser.parse_args()
  infile = args.infile
  outfile = args.outfile
  dcc_mode = args.dcc_mode
  
  conn = Connection(dcc_mode)
  
  fh = open(infile,'r')
  fout = open(outfile,'w')
  for line in fh:
    rec_id = line.strip("\n").split("\t")[0]
    if not rec_id or rec_id.startswith("#"):
      fout.write(line + "\t\n")
      continue
    rec = conn.get(rec_id,ignore404=False)
    profile = Profile(rec["@id"])
    profile_id = profile.profile_id
    if profile_id not in VALID_PROFILES:
      raise Exception("Record identifier '{}' must be an identifer for an object of a type in the set {}.".format(rec_id,VALID_PROFILES))

    if profile_id == EXP_PROFILE_ID:
      fastq_recs = conn.get_fastqfiles_on_exp(rec_id) #List of FASTQ file objects in JSON format.
    else:
      fastq_recs = [conn.get(rec_id,ignore404=False)]
    for fq_rec in fastq_recs:
      status = fq_rec["status"]
      error_msg = ""
      if status == "content error":
        error_msg = fq_rec["content_error_detail"]
      fout.write("\t".join([rec_id,error_msg]) + "\n")
  fout.close()
  fh.close()
  
if __name__ == "__main__":
  main()

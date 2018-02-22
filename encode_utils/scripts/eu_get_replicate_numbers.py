#!/usr/bin/env python3
# -*- coding: utf-8 -*-  

###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
### 

"""
Given an input file with replicate IDs, one per line, retrieves the attributes 
biological_replicate_number and technical_replicate_number. An output file is created in 
tab-delimited format with these two additional columns appended to the original lines.
"""

import argparse
from encode_utils.connection import Connection
from encode_utils.parent_argparser import dcc_login_parser 
#dcc_login_parser  contains the arguments needed for logging in to the ENCODE Portal, including which env.

def get_parser()
  parser = argparse.ArgumentParser(parents=[dcc_login_parser],description=__doc__,formatter_class=argparse.RawTextHelpFormatter)
  parser.add_argument("-i","--infile",required=True,help="Input file containing in column one DCC replicate identifiers, i.e. alias, UUID, or @id. Additional columns are allowed if the file is in tab-delimited format, and such columns will be ignored. Empty lines and lines beginning with a '#' will be ignored.")
  parser.add_argument("-o","--outfile",required=True,help="The output file, which is the same as the input file except for the addition of the tab-delimited columns 'biological_replicate_number' and 'technical_replicate_number'.")
  return parser

def main():
  parser = get_parser()
  args = parser.parse_args()
  infile = args.infile
  outfile = args.outfile
  dcc_username = args.dcc_username
  dcc_mode = args.dcc_mode
  
  conn = Connection(dcc_username=dcc_username,dcc_mode=dcc_mode)
  fh = open(infile,'r')
  fout = open(outfile,'w')
  for line in fh:
  	rep_id = line.strip("\n").split("\t")[0]
  	if not rep_id or rep_id.startswith("#"):
  		fout.write(line)
  		continue
  	rep_json = conn.get(rec_ids=rep_id,ignore404=False)
  	bio,tech = rep_json["biological_replicate_number"], rep_json["technical_replicate_number"]
  	fout.write("\t".join([line.strip("\n"),str(bio),str(tech)]) + "\n")
  fh.close()
  fout.close()

if __name__ == "__main__":
  main()
  	
  	
  	
  

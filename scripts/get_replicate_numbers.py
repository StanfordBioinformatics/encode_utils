#!/bin/env python3

###
#Nathaniel Watson
#nathankw@stanford.edu
#2017-11-02
###


import argparse
from encode_utils.parent_argparser import dcc_login_parser 
#dcc_login_parser  contains the arguments needed for logging in to the ENCODE Portal, including which env.
from encode_utils.connection import Connection

description = "Given an input file with replicate IDs, one per line, retrieves the attributes biological_replicate_number and technical_replicate_number. An output file is created in tab-delimited format with these two additional columns appended to the original lines."

parser = argparse.ArgumentParser(parents=[dcc_login_parser],description=description,formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-i","--infile",required=True,help="Input file containing in column one DCC replicate identifiers, i.e. alias, UUID, or @id. Additional columns are allowed if the file is in tab-delimited format, and such columns will be ignored. Empty lines and lines beginning with a '#' will be ignored.")
parser.add_argument("-o","--outfile",required=True,help="The output file, which is the same as the input file except for the addition of the tab-delimited columns 'biological_replicate_number' and 'technical_replicate_number'.")
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
	rep_json = conn.getEncodeRecord(rec_id=rep_id,ignore404=False)
	bio,tech = conn.getReplicateNumbers(rep_json=rep_json)
	fout.write("\t".join([line.strip("\n"),str(bio),str(tech)]) + "\n")
fh.close()
fout.close()
	
	
	


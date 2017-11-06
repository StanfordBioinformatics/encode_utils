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

description = "Given an input file with ENCODE object identifiers, one per line, retrieves the aliases. An output file is created in tab-delimited format with additional tab-delimited columns appended to the original lines - one for each alias."

parser = argparse.ArgumentParser(parents=[dcc_login_parser],description=description,formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-i","--infile",required=True,help="Input file containing ENCODE object identifiers (one per line), i.e. UUID, accession, or @id. Empty lines and lines beginning with a '#' will be ignored.")
parser.add_argument("-o","--outfile",required=True,help="The output file, which is the same as the input file except for the addition of the tab-delimited columns - one for each alias.")
args = parser.parse_args()
infile = args.infile
outfile = args.outfile
dcc_username = args.dcc_username
dcc_mode = args.dcc_mode

conn = Connection(dcc_username=dcc_username,dcc_mode=dcc_mode)

fh = open(infile,'r')
fout = open(outfile,'w')
for line in fh:
	rec = line.strip("\n").split("\t")[0]
	if not rec or rec.startswith("#"):
		fout.write(line)
		continue
	rec = conn.getEncodeRecord(rec_id=rec,ignore404=False)
	aliases = rec["aliases"]
	for a in aliases:
		line = [line.strip("\n")]
		outline = line.extend(aliases)
		fout.write("\t".join(line) + "\n")
fout.close()
fh.close()

	

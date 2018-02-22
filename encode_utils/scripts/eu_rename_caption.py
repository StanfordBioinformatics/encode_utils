#!/usr/bin/env python3
# -*- coding: utf-8 -*-  

###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
### 

"""
Internal only.
"""

import argparse
from encode_utils.connection import Connection

parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-i","--infile",required=True,help="Input file with DCC record identifiers, one per line.")
args = parser.parse_args()
infile = args.infile


conn = Connection(dcc_username="nathankw",dcc_mode="prod")
fh = open(infile,'r')
for line in fh:
	line = line.strip().split("\t")
	if not line or line[0].startswith("#"):
		continue
	rec = conn.get(rec_ids=line[3])
	bio_rep_num = int(line[1])
	caption = rec["description"]
	new_cap = "Barplot for replicate {bio_rep_num}{cap}".format(bio_rep_num=bio_rep_num,cap=caption.split("Barplot")[1])
	#print(new_cap)
	payload = {}
	payload["@id"] = "document"
	payload["description"] = new_cap
	conn.patch(payload=payload,record_id=line[3],error_if_not_found=True,raise_403=True, extend_array_values=True)

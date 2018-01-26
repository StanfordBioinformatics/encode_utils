#!/bin/env python3

###                                                                                                    
# 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
### 

import argparse

from encode_utils.connection import Connection
from encode_utils.parent_argparser import dcc_login_parser 
#dcc_login_parser  contains the arguments needed for logging in to the ENCODE Portal, including which env.

description = """
Given an ENCODE experiment identifier, Prints the FASTQ ENCFF identifiers for the specified replicate and technical replicates, or all replicates. Also prints the replicate numbers. For each FASTQ identifer, the following is printed to stdout: 
		$BioNum_$TechNum_$ReadNum\\t$encff
where variables are defined as:
		$BioNum  - the biological repliate number
		$TechNum - the technial replicate number
		$ReadNum - '1' for a forwards reads FASTQ file, and '2' for a reverse reads FASTQ file.
"""
parser = argparse.ArgumentParser(parents=[dcc_login_parser],description=description,formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-e","--dcc-exp-id",required=True,help="The experiment to which the replicates belong. Must be set if --all-reps is absent.")
parser.add_argument("-b","--bio-rep-num",type=int,help="Print FASTQ ENCFFs for this specified biological replicate number.")
parser.add_argument("-t","--tech-rep-num",type=int,help="Print FASTQ ENCFFs for the specified technical replicate number of the specified biological replicate.")
args = parser.parse_args()
user_name = args.dcc_username
mode = args.dcc_mode
exp_id = args.dcc_exp_id
bio_rep_num = args.bio_rep_num
tech_rep_num = args.tech_rep_num

conn = Connection(dcc_username=user_name,dcc_mode=mode)
REP_DICO = conn.getFastqFileRepNumDico(dcc_exp_id=exp_id)

for b in REP_DICO:
	if bio_rep_num and b != bio_rep_num:
		continue
	for t in REP_DICO[b]:
		if tech_rep_num and t != tech_rep_num:
			continue
		for read_num in REP_DICO[b][t]:
			encff_json=REP_DICO[b][t][read_num]
			alias = encff_json["aliases"][0]
			print("_".join([bio_rep_num,tech_rep_num,read_num]) + "\t" + alias)



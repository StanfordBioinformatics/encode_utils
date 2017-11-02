#!/bin/env python

###
#Nathaniel Watson
#nathankw@stanford.edu
#2017-11-01
###

import argparse

import encode_utils.utils as u

description = "Hi"
parser = argparse.ArgumentParser(description=description)
parser.add_argument("-u","--dcc-username",required=True,help="The DCC user name used to log into the ENCODE Portal.")
parser.add_argument("-m","--dcc-mode",required=True,help="The ENCODE Portal site ('prod' or 'dev') to connect to.")
parser.add_argument("-e","--dcc-exp-id",required=True,help="The experiment to which the replicates belong. Must be set if --all-reps is absent.")
parser.add_argument("-b","--bio-rep-num",type=int,help="The biological replicate number")
parser.add_argument("-t","--tech-rep-num",type=int,help="The technical replicate number")
parser.add_argument("-a","--all-reps",action="store_true",help="Print FASTQ ENCFFs for all replicates on the experiment.")
args = parser.parse_args()
user_name = args.dcc_username
mode = args.dcc_mode
exp_id = args.dcc_exp_id
bio_rep_num = args.bio_rep_num
tech_rep_num = args.tech_rep_num
all_reps = args.all_reps

if not bio_rep_num and not all_reps:
	raise argparse.ArgumentTypeError("--bio-rep-num must be specified if --all-reps isn't.")

conn = u.Connection(dcc_username=user_name,dcc_mode=mode)
REP_DICO = conn.getFastqFileRepNumDico(dcc_exp_id=exp_id)

def process_tech_rep(bio_rep_num,tech_rep_num):
	for r in REP_DICO[bio_rep_num][tech_rep_num]:
		output_encff_repinfo(encff_json=REP_DICO[bio_rep_num][tech_rep_num][r])

def output_encff_repinfo(encff_json):
	bio_rep_num = encff_json["biological_replicate_number"]
	tech_rep_num = encff_json["technical_replicate_number"]
	read_num = encff_json["paired_end"]
	alias = encff_json["aliases"][0]
	print("_".join([bio_rep_num,tech_rep_num,read_num]) + "\t" + alias)

if not all_reps:
	tech_reps = REP_DICO[bio_rep_num]
	if tech_rep_num:
		process_tech_rep(bio_rep_num=bio_rep_num,tech_rep_num=tech_rep_num)
	else:
		#then all technical replicates
		for t in tech_reps:
			process_tech_rep(bio_rep_num=bio_rep_num,tech_rep_num=t)
else:
	#then all replicates of every type
	for b in REP_DICO:
		for t in REP_DICO[b]:
			process_tech_rep(bio_rep_num=bio_rep_num,tech_rep_num=t)
	



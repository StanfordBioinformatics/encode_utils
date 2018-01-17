#!/bin/env python

###
#Nathaniel Watson
#nathankw@stanford.edu
#2017-11-01
###

import argparse

from encode_utils.parent_argparser import dcc_login_parser 
#dcc_login_parser  contains the arguments needed for logging in to the ENCODE Portal, including which env.
from encode_utils.connection import Connection

description = """
Given an ENCODE experiment identifier (not a control), loops through all replicates and sets the controlled_by attribute on each FASTQ file of the replicate. This attribute will be set by looking into the FASTQs of the corresponding replicate on the control experiment, thus, the possible_controls attribute must be set already on the provided experiment ID. This script is usefuly when the experiment has the controls set but not the possible controls attribute set, or when the user changes the list of possible_controls.

In order to determine what control FASTQ file a given experiment FASTQ file is paired to, we must first determine the replicate pairings, i.e. pair a control replicate to each experiment replicate. But since there can be many controls listed in the experiment's possible_controls attribute, there can be many replicates paired to a given experiment replicate in this sense, which in turn means that a given experiment FASTQ file can have many control FASTQ files as is sometimes the case when controls are pooled together. 

For a given experimental replicate, say whose attributes biological_replicate_number is 1 and technical_replicate_number is also 1 (abbreviated as "1,1"), then the general rule of thumb is that the corresponding control experiment's replicate is the one with the same values for the two aforementioned fields. However, this isn't always the case, as the numbering of replicates can be different. For example, there may be experimental replicates "1,1" and "2,1", but the control replicates may be labelled as "2,1" and "3,1" (or even "2,1" and "4,1" ...). In such a case, the control replicate for experiment replicate "1,1" is "2,1", and the control replicate for experiment replicate "3,1" is "2,1" (or "4,1"). The way in which this script determines the pairings is by first making an ordered list of experimental replicates, and another for the control replicates. Each list is sorted by the replicate field "biological_replicate_number" as the major key, then the "technical_replicate_number" as the minor key.  Then, the experimental replicates are paired to control replicates based on identical positioning in the arrays. If the lenghts of the arrays are different, then this is currently treated as a fatal error (an Exception is raised). 

Again, because there can be multiple controls listed in the "possible_controls" fields, a given experimental replicate can have more than one control replicate pairing - there will be as many control replicates paired to it as there are controls listed in "possible_controls". When there is more than one control experiment, there must be the same number of replicates on each, otherwise this is treated as a fatal error and an Exception will be raised.
"""

parser = argparse.ArgumentParser(parents=[dcc_login_parser],description=description,formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-e","--dcc-exp-id",required=True,help="The experiment whose FASTQ file objects need to have their controlled_by field set.")
args = parser.parse_args()
user_name = args.dcc_username
mode = args.dcc_mode
exp_id = args.dcc_exp_id

conn = Connection(dcc_username=user_name,dcc_mode=mode)
exp_rep_dico = conn.getFastqFileRepNumDico(dcc_exp_id=exp_id)
exp_json = conn.getEncodeRecord(rec_id=dcc_exp_id,ignore404=True,frame="object")
num_exp_reps = len(exp_json["replicates"])
control_ids = exp_json["controlled_by"]
controls = {}
control_rep_counts = []
for c in controls_ids:
	controls[c] = {}
	c_json = conn.getEncodeRecord(rec_id=c,ignore404=True,frame="object")
	rep_count = len(c_json["replicates"])
	control[c]["count"] = rep_count
	control_rep_counts.append(rep_count)
	control[c]["rep_dico"] = conn.getFastqFileRepNumDico(dcc_exp_id=c)

#Make sure that all control experiments have the same number of replicates:
if len(set(control_rep_counts)) != 1:
	raise Exception("The controls '{controls}' have different numbers of replicates from one another '{rep_nums}'.".format(controls=control_ids,rep_nums=control_rep_counts))

#Make sure that the number of control reps equals the number of experiment reps:
if num_exp_reps != control_rep_counts[0]:
	raise Exception("The number of experiment replicates '{}' doesn't equal the number of control replicates '{}'."format(num_exp_reps,control_rep_counts[0])

for b in exp_rep_dico:
	for t in exp_rep_dico[b]:
		for read_num in exp_rep_dico[b][t]:
			encff_json=exp_rep_dico[b][t][read_num]
			alias = encff_json["aliases"][0]
			print("_".join([bio_rep_num,tech_rep_num,read_num]) + "\t" + alias)



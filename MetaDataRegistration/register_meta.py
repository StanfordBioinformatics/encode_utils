#!/bin/env python

###
#Nathaniel Watson
#2017-10-23
#nathankw@stanford.edu
###

import argparse
import pdb

import encode_utils import submit


def create_payloads(profile,infile,award=None,lab=None,):
	"""
	Function : Given a tab-delimited input file containing records belonging to one of the profiles listed on the ENCODE Portal 
						 (such as https://www.encodeproject.org/profiles/biosample.json), generates the payload that can be used to either 
						 register or patch the metadata for each row. This is a generator function.
	Args     : profile - The profile to submit to. 
						 infile  - The tab-delimited input file with a field-header line as the first line. The field names must be exactly equal
											 to the corresponding names in the profile given on the ENCODE Portal, with the exception that array fields must have
											 the suffix '[]' to signify array values. Array values within subsequent lines only need be comma-delimited and should
											 not themselves be wrapped in brackets.
					 	 award   - Equal to the 'award' field common to all profiles. This is a convenience field and is not needed if the input 
											 file provides this field.
						 lab     - Equal to the 'lab' field common to all profiles. This is a convenience field and is not needed if the input 
											 file provides this field.
	Yields  : dict. The payload that can be used to either register or patch the metadata for each row.
	"""
	field_index = {}
	fh = open(infile,'r')
	header = fh.readline().strip("\n").split("\t")
	count = -1
	for i in header:
		count += 1
		field_index[count] = i

	for line in fh:
		line = line.strip("\n").split("\t")
		if not line or line[0].startswith("#"):
			continue
		payload = {}
		payload["@id"] = "{}/".format(profile)
		if award:
			payload["award"] = award
		if lab:
			payload["lab"] = lab
		count = -1
		for val in line:
			count += 1
			val = val.strip()
			if val:
				field = field_index[count]
				if field.endswith("[]"):
					payload[field.rstrip("[]")] = val.split(",")
				else:
					payload[field] = val
		yield payload
	

if __name__ == "__main__":
	description = "Given a tab-delimited input file containing records belonging to one of the profiles listed on the ENCODE Portal (such as https://www.encodeproject.org/profiles/biosample.json), either registers or patches metadata for each record. Don't mix input files containing both new records and records to patch - in this case they should be split into separate files."
	parser = argparse.ArgumentParser(description=description)
	parser.add_argument("-p","--profile",required=True,help="The profile to submit to, i.e. put 'biosample' for https://www.encodeproject.org/profiles/biosample.json")
	parser.add_argument("-m","--dcc-mode",required=True,help="The DCC environment to submit to (either 'dev' or 'prod').")
	parser.add_argument("-i","--infile",help="The tab-delimited input file with a field-header line as the first line. The field names must be exactly equal to the corresponding names in the profile given on the ENCODE Portal, with the exception that array fields must have the suffix '[]' to signify array values. Array values within subsequent lines only need be comma-delimited and should not themselves be wrapped in brackets.")
	parser.add_argument("-a","--award",help="Equal to the 'award' field common to all profiles. This is a convenience field and is not needed if the input file provides this field.")
	parser.add_argument("-l","--lab",help="Equal to the 'lab' field common to all profiles. This is a convenience field and is not needed if the input file provides this field.")
	parser.add_argument("--patch",action="store_true",help="Presence of this option indicates to patch an existing DCC record rather than register a new one.")
	args = parser.parse_args()
	profile = args.profile
	dcc_mode = args.dcc_mode

	submit = encode.dcc_submit.submit.Submit(dnanexus_username="nathankw",dcc_username="nathankw",dcc_mode=dcc_mode)

	infile = args.infile
	award = args.award
	lab = args.lab
	patch = args.patch
	gen = create_payloads(profile=profile,infile=infile,award=award,lab=lab)
	for payload in gen:
		#pdb.set_trace()
		submit.postToDcc(payload=payload,patch=patch)

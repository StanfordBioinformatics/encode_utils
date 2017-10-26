#!/bin/env python

###
#Nathaniel Watson
#2017-10-23
#nathankw@stanford.edu
###

import argparse
import pdb

import encode_utils.utils


def create_payloads(profile,infile):
	"""
	Function : Given a tab-delimited input file containing records belonging to one of the profiles listed on the ENCODE Portal 
						 (such as https://www.encodeproject.org/profiles/biosample.json), generates the payload that can be used to either 
						 register or patch the metadata for each row. This is a generator function.
	Args     : profile - The profile to submit to. 
						 infile  - The tab-delimited input file with a field-header line as the first line. The field names must be exactly equal
											 to the corresponding names in the profile given on the ENCODE Portal, with the exception that array fields must have
											 the suffix '[]' to signify array values. Array values within subsequent lines only need be comma-delimited and should
											 not themselves be wrapped in brackets. Furthermore, non-scematic fields are allowed as long as they begin with a '#'. 
											 Such fields will be skipped. 
	Yields  : dict. The payload that can be used to either register or patch the metadata for each row.
	"""
	field_index = {}
	fh = open(infile,'r')
	header_fields = fh.readline().strip("\n").split("\t")
	count = -1
	skip_field_indices = []
	for field in header_fields:
		count += 1
		if field.startswith("#"): #non-schema field
			skip_field_indices.append(count)
			continue
		field_index[count] = field

	for line in fh:
		line = line.strip("\n").split("\t")
		if not line or line[0].startswith("#"):
			continue
		payload = {}
		payload["@id"] = "{}/".format(profile)
		if encode_utils.DCC_AWARD_ATTR not in header_fields:
			if profile not in encode_utils.AWARDLESS_PROFILES:
				payload[encode_utils.DCC_AWARD_ATTR] = encode_utils.AWARD
		if encode_utils.DCC_LAB_ATTR not in header_fields:
			if profile not in encode_utils.AWARDLESS_PROFILES:
				payload[encode_utils.DCC_LAB_ATTR] = encode_utils.LAB
		count = -1
		for val in line:
			count += 1
			if count in skip_field_indices:
				continue
			val = val.strip()
			if val:
				field = field_index[count]
				if field.endswith("[]"):
					payload[field.rstrip("[]")] = val.split(",")
				else:
					try:
						val = int(val)
					except ValueError: #not an integer field
						pass
					payload[field] = val
		yield payload
	

if __name__ == "__main__":
	description = "Given a tab-delimited input file containing records belonging to one of the profiles listed on the ENCODE Portal (such as https://www.encodeproject.org/profiles/biosample.json), either registers or patches metadata for each record. Don't mix input files containing both new records and records to patch - in this case they should be split into separate files."
	parser = argparse.ArgumentParser(description=description)
	parser.add_argument("-p","--profile",required=True,help="The profile to submit to, i.e. put 'biosample' for https://www.encodeproject.org/profiles/biosample.json")
	parser.add_argument("-m","--dcc-mode",required=True,help="The DCC environment to submit to (either 'dev' or 'prod').")
	parser.add_argument("-i","--infile",help="The tab-delimited input file with a field-header line as the first line. The field names must be exactly equal to the corresponding names in the profile given on the ENCODE Portal, with the exception that array fields must have the suffix '[]' to signify array values. Array values within subsequent lines only need be comma-delimited and should not themselves be wrapped in brackets. In addition, non-scematic fields starting with a '#' are allowed and will be skipped. Such fields may be useful to store additional metadata in the submission sheet, eventhough they can't be submitted.")
	parser.add_argument("--patch",action="store_true",help="Presence of this option indicates to patch an existing DCC record rather than register a new one.")
	parser.add_argument("-e","--error-if-not-found", action="store_true",help="If trying to PATCH a record and the record cannot be found on the ENCODE Portal, the default behavior is to then attempt a POST. Specifying this option causes an Exception to be raised.")
	args = parser.parse_args()
	profile = args.profile
	dcc_mode = args.dcc_mode
	error_if_not_found = args.error_if_not_found

	conn = encode_utils.utils.Connection(dcc_username="nathankw",dcc_mode=dcc_mode)

	infile = args.infile
	patch = args.patch
	gen = create_payloads(profile=profile,infile=infile)
	for payload in gen:
		#pdb.set_trace()
		if not patch:
			conn.postToDcc(payload=payload)
		else:
			conn.patchToDcc(payload=payload,error_if_not_found=error_if_not_found)

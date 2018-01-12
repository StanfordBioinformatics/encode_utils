# -*- coding: utf-8 -*-

###
#Nathaniel Watson
#nathankw@stanford.edu
###
import datetime
import time
import logging
import json
import requests
import sys
import subprocess
import os
import re
import urllib
import base64
import mimetypes
import pdb

import urllib3

#inhouse libraries
import encode_utils as en

#debugging imports
import time


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_profile_schema(profile):
	""" 
	Function : Retrieves the JSON schema of the specified profile from the ENCODE Portal.
	Raises   : requests.exceptions.HTTPError if the status code is something other than 200 or 404. 
	Returns  : 404 (int) if profile not found, otherwise a dict representing the profile's JSON schema. 
	"""
	url = os.path.join(PROFILES_URL,profile + ".json?format=json")
	res = requests.get(url,headers={"content-type": "application/json"})
	status_code = res.status_code
	if status_code == 404:
	  raise UnknownENCODEProfile("Please verify the profile name that you specifed.")
	res.raise_for_status()
	return res.json()

def createSubprocess(cmd,pipeStdout=False,checkRetcode=True):
	"""
	Function : Creates a subprocess via a call to subprocess.Popen with the argument 'shell=True', and pipes stdout and stderr. Stderr is always
						 piped; stdout if off by default. If the argument 'checkRetcode' is True, which it is by defualt, then for any non-zero
						 return code, an Exception is raised that will print out the the command, stdout, stderr, and the returncode.
						 Otherwise, the Popen instance will be returned, in which case the caller must call the instance's communicate() method 
						 (and not it's wait() method!!) in order to get the return code to see if the command was successful. communicate() will 
						 return a tuple containing (stdout, stderr), after that you can then check the return code with Popen instance's 'returncode' 
						 attribute.
	Args     : cmd   - str. The command line for the subprocess wrapped in the subprocess.Popen instance. If given, will be printed to stdout when there is an error in the subprocess.
						 pipeStdout - bool. True means to pipe stdout of the subprocess.
						 checkRetcode - bool. Default is True. See documentation in the description above for specifics.
	Returns  : A two-item tuple containing stdout and stderr if 'checkRetCode' is set to True and the command has a 0 exit status. If
						 'checkRetCode' is False, then a subprocess.Popen() instance is returned. 
	"""
	stdout = None
	if pipeStdout:
		stdout = subprocess.PIPE
		stderr = subprocess.PIPE
	popen = subprocess.Popen(cmd,shell=True,stdout=stdout,stderr=subprocess.PIPE)
	if checkRetcode:
		stdout,stderr = popen.communicate()
		if not stdout: #will be None if not piped
			stdout = ""
		stdout = stdout.strip()
		stderr = stderr.strip()
		retcode = popen.returncode
		if retcode:
			#below, I'd like to raise a subprocess.SubprocessError, but that doens't exist until Python 3.3.
			raise Exception("subprocess command '{cmd}' failed with returncode '{returncode}'.\n\nstdout is: '{stdout}'.\n\nstderr is: '{stderr}'.".format(cmd=cmd,returncode=retcode,stdout=stdout,stderr=stderr))
		return stdout,stderr
	else:
		return popen


class Connection():

	REQUEST_HEADERS_JSON = {'content-type': 'application/json'}
	
	DCC_PROD_MODE = "prod"
	DCC_DEV_MODE = "dev"
	DCC_MODES = {
 	 DCC_PROD_MODE: "https://www.encodeproject.org/",
 	 DCC_DEV_MODE: "https://test.encodedcc.org/"
 	 }

	LAB_PREFIX = en.LAB_PREFIX
	
	AWARD_AND_LAB = {"award": en.AWARD,"lab": en.LAB} 

	def __init__(self,dcc_username,dcc_mode):
		"""
		Function : Opens up two log files in append mode in the calling directory named ${dcc_mode}_error.txt and ${dcc_mode}_posted.txt.
		           Parses the API keys from the config file pointed to by en.DCC_API_KEYS_FILE (in __init__.py). 
		Args     : dcc_username - The user name used to log into the ENCODE Portal.
							 dcc_mode     - The ENCODE Portal site ("prod" or "dev") to connect to.
		"""

		f_formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:\t%(message)s')
		#create logger
		logger = logging.getLogger(__name__)
		logger.setLevel(logging.DEBUG)
		#create console handler
		ch = logging.StreamHandler(stream=sys.stdout)
		ch.setLevel(logging.DEBUG)
		ch.setFormatter(f_formatter)
		logger.addHandler(ch)
		#create error handler
		error_fh = logging.FileHandler(filename=dcc_mode + "_" + "error.txt",mode="a")
		error_fh.setLevel(logging.ERROR)
		error_fh.setFormatter(f_formatter)
		logger.addHandler(error_fh)

		#Create separate logger to log IDs of posted objects. These message will need to be logged at INFO level.
		post_logger = logging.getLogger("post")
		post_logger.setLevel(logging.INFO)
		#posted IDs will get writtin to a file logger
		post_logger_fh = logging.FileHandler(filename=dcc_mode + "_" + "posted.txt",mode="a")
		post_logger_fh.setLevel(logging.INFO)
		post_logger_fh.setFormatter(f_formatter)
		post_logger.addHandler(post_logger_fh)
		#Create file logger to contain the error messages that I catch before either continuing on or raising the error.
		
		self.logger = logger
		self.post_logger = post_logger

		self.dcc_username = dcc_username
		self.dcc_mode = dcc_mode
		self.dcc_url = self._setDccUrl()
		self.api_key,self.secret_key = self._setApiKeys()
		self.auth = (self.api_key,self.secret_key)

	def _setDccUrl(self):
		return self.DCC_MODES[self.dcc_mode]

	def _setApiKeys(self):
		"""
		Function : Retrieves the API key and secret key for the supplied dcc user name.
		Args     : dcc_username - The username used to log into the DCC website (prod or dev website).
		Returns  : Tuple containing the (API Key, Secret Key)
		"""
		fh = open(en.DCC_API_KEYS_FILE)
		conf = json.load(fh)
		api_key = conf[self.dcc_username]["api_key"]
		secret_key = conf[self.dcc_username]["secret_key"]
		return api_key,secret_key
		
	def _writeAliasAndDccAccessionToLog(self,alias,dcc_id=None):
		txt = alias
		if dcc_id:
			txt += " -> {dcc_id}".format(dcc_id=dcc_id)
		self.post_logger.info(txt)

	def stripDccAliasPrefix(self,alias):
		"""
		Function : Splits 'alias' on ':' to strip off any alias prefix. Aliases must have a lab-specific prefix. The ':' is the 
							 seperator between prefix and the rest of the alias, and can't appear elsewhere in the alias. 
		Returns  : str.
		"""
		return name.split(":")[-1]

	def getAliases(self,dcc_id,strip_alias_prefix=False):
		"""
		Function : Given the ENCODE ID for an object, returns the aliases for that object. 
		Args     : dcc_id - The ENCODE ID for a given object, i.e ENCSR999EHG.
							 strip_alias_prefix - bool. True means to remove the alias prefix if all return aliases. 
		Returns  : list.
		"""
		record = self.getEncodeRecord(ignore404=False,dcc_id=dcc_id)
		aliases = record["aliases"]
		for index in range(len(aliases)):
			alias = aliases[index]
			if strip_alias_prefix:
				aliases[index] =  self.stripDccAliasPrefix(alias)
		return aliases

	def searchEncode(self,search_args):
		"""
		Function : Searches the ENCODE Portal using the provided query parameters in dictionary format. 
							 The query parameters will be first URL encoded. 
		Args     : search_args - dict. of key and value query parameters. 
		Returns  : list of search results. 
		Raises   : HTTPError if the status code is not in the set [200,404].
		Example  : Given we have the following dictionary 'd' of key and value pairs:

							{"type": "experiment",
							 "searchTerm": "ENCLB336TVW",
							 "format": "json",
							 "frame": "object",
							 "datastore": "database"}
	
							We can call the function as:

								searchEncode(search_args=d)
						
		"""
		query = urllib.parse.urlencode(search_args)
		url = os.path.join(self.dcc_url,"search/?",query)
		self.logger.info("Searching DCC with query {url}.".format(url=url))
		response = requests.get(url,auth=self.auth,headers=self.REQUEST_HEADERS_JSON,verify=False)
		if response.status_code not in [200,404]: #if not ok or not found
			response.raise_for_status()
		return response.json()["@graph"] #the @graph object is a list

	def getEncodeRecord(self,rec_id,ignore404=True,frame=None):
		"""
		Function : Looks up an object in ENCODE using a unique identifier, such as the object id, an alias, uuid, or accession. 
		Args     : ignore404 - bool. True indicates to not raise an Exception if a 404 is returned. 
						 : rec_id - A unique identifier, such as the object id, an alias, uuid, or accession.
		Returns  : The JSON response. 
		Raises   : If the status code is 403 (forbidden), an Exception will be raised.
							 A 404 (not found) status code will result in an Exception only if the 'ignore404' argument
							 is set to False. 
		"""
		recordId = rec_id 
		if recordId.endswith("/"):
			recordId = recordId.lstrip("/")
		url = os.path.join(self.dcc_url,recordId,"?format=json&datastore=database")
		if frame:
			url += "&frame={frame}".format(frame=frame)
		self.logger.info(">>>>>>GETTING {recordId} From DCC with URL {url}".format(recordId=recordId,url=url))
		response = requests.get(url,auth=self.auth, headers=self.REQUEST_HEADERS_JSON, verify=False)
		if response.ok:
			#logger.info("<<<<<GET RESPONSE: ")
			#self.logger.debug(json.dumps(response.json(), indent=4, sort_keys=True))
			return response.json()
		elif response.status_code == 403: #forbidden
			raise Exception("Access to ENCODE entity {entity} is forbidden".format(entity=recordId))
		elif response.status_code == 404: #not found
			if ignore404:
				return {}
			else:
				raise Exception("ENCODE entity '{entity}' not found".format(entity=recordId))
		else:
			#if response not okay and status_code equal to something other than 404
			response.raise_for_status()


	def getRecordId(self,rec_json):
		"""
		Function : Given the JSON serialization of a DCC record, extracts an ID from it. The ID will be the value of 
							 the 'id' key if that is present in rec_json, otherwise it will be the value of the first alias in 
							 the 'aliases' key. If there isn't an alias present, an IndexError will be raised.
		Args     : rec_json - The JSON serialization of the record in question.
		Returns  : str. 
		Raises   : IndexError if a record ID can't be found (since the last attempt to find an identifier works by 
							 subsetting the first element in the 'aliases' key).
		"""

		#The '@id' key has a value in the format /profile/id, where profile is something like 'documents', 'libraries', 'antibodies', ...
		# This key also stores a record ID at the end when addressing a record belonging to a particular profile.
		if "@id" in rec_json:
			id_tokens = rec_json["@id"].strip("/").split()
			if len(id_tokens) > 1: #Then there is a record ID stored here
				return id_tokens[-1]
		else:
			return rec_json["aliases"][0]

	def patch(self,payload,record_id=None,error_if_not_found=True,raise_403=True, extend_array_values=True):
		"""
		Function : PATCH an object to the DCC. If the object doesn't exist, then this method will call self.post().
		Args     : payload - dict. containing the attribute key and value pairs to patch.
							 record_id - str. Identifier of the DCC record to patch. If not specified, will first check if it is set in the payload's 
													 'id' attribute, and if not there, the 'aliases' attribute.
							 error_if_not_found - bool. If set to True, then an Exception will be raised if the record to Patch is not found
									     	            on the ENCODE Portal. If False and the record isn't found, then a POST will be attempted by
																		calling self.PostToDcc().
							 raise_403 - bool. True means to raise an HTTPError if a 403 status (Forbidden) is returned.
							 extend_array_values - bool. Only affects keys with array values. True (default) means to extend the corresponding value on the Portal with what's specified
									in the payload. False means to replace the value on the Portal with what's in the payload. 
		Returns  : The PATCH response. 
		Raises   : requests.exceptions.HTTPError if the return status is !ok (excluding a 403 status if 'raise_403' is False, and excluding
							 a 404 status if 'error_if_not_found' is False. 
		"""
		json_payload = json.loads(json.dumps(payload)) #make sure we have a payload that can be converted to valid JSON, and tuples become arrays, ...
		self.logger.info("\nIN patch()")
		objectType = json_payload.pop("@id") #i.e. /documents/ if it doesn't have an ID, /documents/docid if it has an ID.
		if not record_id:
			record_id = self.getRecordId(json_payload) #first tries the @id field, then looks for the first alias in the 'aliases' attr.
				
		self.logger.info("Will check if {} exists in DCC with a GET request.".format(record_id))
		get_response_json = self.getEncodeRecord(ignore404=True,rec_id=record_id,frame="object")
		if not get_response_json:
			if error_if_not_found:
				raise Exception("Can't patch record '{}' since it was not found on the ENCODE Portal.".format(record_id))
			#then need to do a POST
			else:
				json_payload["@id"] = objectType
				response = self.post(payload=json_payload)
				return response

		if extend_array_values:
			for key in json_payload:
				if type(json_payload[key]) is list:
					json_payload[key].extend(get_response_json[key])
					json_payload[key] = list(set(json_payload[key]))

		url = os.path.join(self.dcc_url,record_id)
		self.logger.info("<<<<<<Attempting to PATCH {record_id} To DCC with URL {url} and this payload:\n\n{payload}\n\n".format(record_id=record_id,url=url,payload=json_payload))
		response = requests.patch(url, auth=self.auth, headers=self.REQUEST_HEADERS_JSON, data=json.dumps(json_payload), verify=False)

		self.logger.debug("<<<<<<DCC PATCH RESPONSE: ")
		self.logger.debug(json.dumps(response.json(), indent=4, sort_keys=True))
		if response.ok:
			return response.json()
		elif response.status_code == 403: #don't have permission to PATCH this object.
			if not raise_403:
				return get_response_json
		else:
			message = "Failed to PATCH {} to DCC".format(record_id)
			self.logger.error(message)
			response.raise_for_status()

	def post(self,payload):
		"""
		Function : POST an object to the DCC.
		Args     : payload - The data to submit.
		Returns  : The object's JSON sererialization from the DCC, after it is posted.
		Raises   : requests.exceptions.HTTPError if the return status is !ok. 
		"""
		json_payload = json.loads(json.dumps(payload)) #make sure we have a payload that can be converted to valid JSON, and tuples become arrays, ...
		self.logger.info("\nIN post().")
		objectType = json_payload.pop("@id")
		url = os.path.join(self.dcc_url,objectType)
		alias = json_payload["aliases"][0]
		self.logger.info("<<<<<<Attempting to POST {alias} To DCC with URL {url} and this payload:\n\n{payload}\n\n".format(alias=alias,url=url,payload=json_payload))
		response = requests.post(url, auth=self.auth, headers=self.REQUEST_HEADERS_JSON, data=json.dumps(json_payload), verify=False)
		self.logger.debug("<<<<<<DCC POST RESPONSE: ")
		self.logger.debug(json.dumps(response.json(), indent=4, sort_keys=True))
		status_code = response.status_code
		if response.ok:
			response_dcc_accession = ""
			try:
				response_dcc_accession = response.json()["@graph"][0]["accession"]
			except KeyError:
				pass #some objects don't have an accession, i.e. replicates.
			self._writeAliasAndDccAccessionToLog(alias=alias,dcc_id=response_dcc_accession)
			return response.json()
		elif status_code == 409: #conflict
			self.logger.info("Will not post {} to DCC because it already exists.".format(alias))
			rec_json = self.getEncodeRecord(rec_id=alias,ignore404=False)
			return rec_json
		else:
			message = "Failed to POST {alias} to DCC".format(alias=alias)
			self.logger.error(message)
			response.raise_for_status()

	def doesReplicateExist(self,library_alias,biologicial_replicate_number,technical_replicate_number,replicates_json_from_dcc):
		"""
		Function : Checks if a replicate exists for a specified library alias with the given biological replicate
							 number and technical replicate number. Note that this method only works on a library alias
							 and not any other form of identifier. 
		Args     : library_alias - str. Any of the associated library's aliases. i.e. michael-snyder:L-208.
							 biologicial_replicate_number - int. The biological replicate number. 
							 technical_replicate_number - int. The technical replicate number. 
							 replicates_json_from_dcc - dict. The value of the "replicates" key in the JSON of a DCC experiment.
							
		Returns  : False if the 'library_alias' doesn't exist in the nested library object of any of the replicates.
						   If the 'library_alias' is present, then True if both 'biologicial_replicate_number' and 'technical_replicate_number'
							 match for the given keys by the same name in the repliate, False otherwise. 
		"""
		biologicial_replicate_number = int(biologicial_replicate_number)
		technical_replicate_number = int(technical_replicate_number)
		for rep in replicates_json_from_dcc:
			try:
				rep_alias = rep["aliases"][0]
			except IndexError: #replicate may not have any aliases
				continue
			rep_lib_aliases = rep_lib["library"]["aliases"]
			if not rep_lib_aliases: #library may not have any aliases
				continue
			rep_bio_rep_number = rep["biological_replicate_number"]
			rep_tech_rep_number = rep["technical_replicate_number"]
			if (library_alias in rep_lib_aliases) and (biologicial_replicate_number == rep_bio_rep_number) and (technical_replicate_number == rep_tech_rep_number):
				return rep_alias
		return False


	def getReplicateNumbers(self,rep_json):
		"""
		Function : Given the replicate replicate JSON, extracts the biological and technical replicate numbers. 
		Args     : rep_json - dict. representing the JSON serialization of a replicate from the DCC.
		Returns  : tuple of the form (bio_rep_num,tech_rep_num).
		"""
		return rep_json["biological_replicate_number"],rep_json["technical_replicate_number"]
		
	def getFastqFileRepNumDico(self,dcc_exp_id):
		"""
		Function : Given a DCC experiment ID, finds the original FASTQ files that were submitted and creates
							 a dictionary with keys being the biological_replicate_number. The value of each key is another
							 dictionary having the technical_replicate_number as the single key. The value of this is another
							 dictionary with keys being file read numbers, i.e. 1 for forward reads, 2 for reverse reads.
							 The value for a give key of this most inner dictionary is the file JSON. 

		Args    : dcc_exp_id - list of DCC file IDs or aliases 
		Returns : dict. 
		"""
		exp_json = self.getEncodeRecord(ignore404=False,rec_id=dcc_exp_id)
		dcc_file_ids = exp_json["original_files"]
		dico = {}
		for i in dcc_file_ids:
			file_json = self.getEncodeRecord(ignore404=False,rec_id=i)
			if file_json["file_type"] != "fastq":
				continue #this is not a file object for a FASTQ file.
			brn,trn = self.getReplicateNumbers(file_json["replicate"])
			read_num = file_json["paired_end"] #string
			if brn not in dico:
				dico[brn] = {}
			if trn not in dico[brn]:
				dico[brn][trn] = {}
			dico[brn][trn][read_num] = file_json
		return dico


	def _setAwsUploadCredsFromResponseGraph(self,upload_credentials):
		"""
		Function : After posting the metadata for a file object to ENCODE, the response will contain the key 
							 'upload_credentials'. This method parses the document pointed to by this key, constructing
							 a dictionary of keys that will be exported as environment variables that can be used by the
							 aws CL agent. That is what self.postFileToDcc() does, indirectly. self.postFileToDcc() has
							 an argument 'aws_creds' that expects a value generated from this method.
							 This method is also called from self.regenerateAwsUploadCreds(), which produces a JSON document
							 also containing the key 'upload_credentials'. 
		Args     : dict.
		"""
		if "@graph" in response:
			response = response["@graph"][0]
		creds = graph["upload_credentials"]
		aws_creds = {}
		aws_creds["AWS_ACCESS_KEY_ID"] = creds["access_key"]
		aws_creds["AWS_ACCESS_KEY_ID"] = creds["access_key"]
		aws_creds["AWS_SECRET_ACCESS_KEY"] = creds["secret_key"]
		aws_creds["AWS_SECURITY_TOKEN"] = creds["session_token"]
		aws_creds["UPLOAD_URL"] = creds["upload_url"]
		return aws_creds
	
	def postFileMetaDataToDcc(self,payload,patch):
		"""
		Function : This is only to be used for DCC "/file/" type objects, because for these we don't have a Syapse record for them (the regular POST method called
							 post() will try to retrive the corresponding Syapse object. Before attempting a POST, will check if the file exists by doing a get on payload["aliases"][0].
							 If the GET request succeeds, nothing will be POST'd.
		Args     : payload - The data to submit.
							 patch - bool. True indicates to perform an HTTP PATCH operation rather than POST.
		"""
		self.logger.info("\nIN postFileMetaDataToDcc(), patch={patch}\n".format(patch=patch))	
		objectType = payload.pop("@id") #should be /file/
		filename = payload["submitted_file_name"]
		#alias = payload["aliases"][0]
		md5_alias = "md5:" + payload["md5sum"]
		alias = md5_alias
		
		#check if file already exists on DCC using md5sum. Useful if file exists already but under different alias.
		exists_on_dcc = self.getEncodeRecord(ignore404=True,dcc_id=alias)
		if not exists_on_dcc:
			#check with actual file alias in the payload. Useful if previously we only had part of the file by mistake (i.e incomplete downoad)
			# hence the uploaded file on DCC would have a different md5sum.
			alias = payload["aliases"][0]
			exists_on_dcc = self.getEncodeRecord(ignore404=True,dcc_id=alias)
		if not patch and exists_on_dcc:
			self.logger.info("Will not POST metadata for {filename} with alias {alias} to DCC because it already exists as {encff}.".format(filename=filename,alias=alias,encff=exists_on_dcc["accession"]))
			return exists_on_dcc
			#The JSON response may contain the AWS credentials.

		if patch:
			if not exists_on_dcc:
				#then do POST
				payload["@id"] = objectType
				response = self.postFileMetaDataToDcc(payload=payload,patch=False)
				return response
			httpMethod = "PATCH"
			url = os.path.join(self.dcc_url,alias)
			encff_id= exists_on_dcc["accession"]
			self.logger.info("<<<<<<Attempting to PATCH {filename} metadata with alias {alias} and ENCFF ID {encff_id} for replicate to DCC with URL {url} and this payload:\n{payload}".format(filename=filename,alias=alias,encff_id=encff_id,url=url,payload=payload))
			response = requests.patch(url, auth=self.auth, headers=self.REQUEST_HEADERS_JSON, data=json.dumps(payload), verify=False)
		else:
			httpMethod = "POST"
			url = os.path.join(self.dcc_url,objectType)
			self.logger.debug("<<<<<<Attempting to POST file {filename} metadata for replicate to DCC with URL {url} and this payload:\n{payload}".format(filename=filename,url=url,payload=payload))
			response = requests.post(url, auth=self.auth, headers=self.REQUEST_HEADERS_JSON, data=json.dumps(payload), verify=False)

		response_json = response.json()
		self.logger.debug("<<<<<<DCC {httpMethod} RESPONSE: ".format(httpMethod=httpMethod))
		self.logger.debug(json.dumps(response_json, indent=4, sort_keys=True))
		if "code" in response_json and response_json["code"] == 409:
			#There was a conflict when trying to complete your request
			# i.e could be trying to post the same file again and there is thus a key conflict with the md5sum key. 
			# This can happen when the alias we have isn't the alias that was posted. For example, ENCFF363RMP has this alias:
			#    michael-snyder:150612_TENNISON_0368_BC7CM3ACXX_L3_GATCAG_1 (because it was originally created using dsalins code, 
			#    but this codebase here would use this as an alias: michael-snyder:150612_TENNISON_0368_BC7CM3ACXX_L3_GATCAG_1_pf.fastq.gz
			raise Exception
				
		response.raise_for_status()
		if "@graph" in response_json:
			response_json = response_json["@graph"][0]	
		response_dcc_accession = response_json["accession"]
		if not patch:
			self._writeAliasAndDccAccessionToLog(alias=alias,dcc_id=response_dcc_accession)
		return response_json


	def regenerateAwsUploadCreds(self,encff_number):
		self.logger.info("Using curl to generate new file upload credentials")
		cmd = "curl -X POST -H 'Accept: application/json' -H 'Content-Type: application/json' https://{api_key}:{secret_key}@www.encodeproject.org/files/{encff_number}/upload -d '{{}}' | python -m json.tool".format(api_key=self.api_key,secret_key=self.secret_key,encff_number=encff_number)
		print(cmd)
		popen = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
		stdout,stderr = popen.communicate()
		retcode = popen.returncode
		if retcode:
			raise Exception("Command {cmd} failed with return code {retcode}. stdout is {stdout} and stderr is {stderr}.".format(cmd=cmd,retcode=retcode,stdout=stdout,stderr=stderr))
		response = json.loads(stdout)
		self.logger.info(response)
		if "code" in response:
			code = response["code"]
			if code == 403:
				#Access was denied to this resource.
				# File already uploaded fine.
				return
		graph = response["@graph"][0]
		aws_creds = self._setAwsUploadCredsFromResponseGraph(graph["upload_credentials"])
		return aws_creds

	def postFileToDcc(self,filepath,encff_number,aws_creds=None):
		"""
		Function :
		Args     : filepath - The local path to the file to upload.
							 upload_url - The AWS upload address (i.e. S3 bucket address).
						   aws_creds - dict. with keys AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SECURITY_TOKEN.
		Returns  :
		"""
		self.logger.info("\nIN postFileToDcc()\n")
		if not aws_creds:
			aws_creds = self.regenerateAwsUploadCreds(encff_number=encff_number)
			if not aws_creds:
				return
		cmd = "aws s3 cp {filepath} {upload_url}".format(filepath=filepath,upload_url=aws_creds["UPLOAD_URL"])
		self.logger.info("Running command {cmd}.".format(cmd=cmd))
		popen = subprocess.Popen(cmd,shell=True, env=os.environ.update(aws_creds),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
		stdout,stderr = popen.communicate()
		#print("STDOUT: {stdout}.".format(stdout=stdout))
		#print("STDERR: {stderr}.".format(stderr=stderr))
		retcode = popen.returncode
		if retcode:
			if retcode == 403:
				#HTTPForbidden; now allowed to update.
				logger.info("Will not upload file {filepath} to s3. Attempt failed with status code HTTP 403 (Forbidden). Normally, this means we shouldn't be editing this object and that all is fine.".format(filepath=filepath))
			else:
				raise Exception("Subprocess command {cmd} failed with returncode {retcode}. Stdout is {stdout}. Stderr is {stderr}.".format(cmd=cmd,retcode=retcode,stdout=stdout,stderr=stderr))
			

	def getPlatformsOnExperiment(self,rec_id):
		"""
		Function : Looks at all FASTQ files on the specified experiment, and tallies up the varying sequencing platforms that generated them. 
							 This is moreless used to verify that there aren't a mix of multiple different platforms present as normally all reads
							 should come from the same platform.
		Args     : rec_id : str. DCC identifier for an experiment. 
		Returns :  list. De-duplicated list of platforms seen on the experiment's FASTQ files. 
		"""
		exp_json = self.getEncodeRecord(rec_id=rec_id,frame=None)
		if "@graph" in exp_json:
			exp_json = exp_json["@graph"][0]
		files_json = exp_json["original_files"]
		platforms = []
		for f in files_json:
			if not f["file_format"] == "fastq":
				continue
			platforms.extend(f["platform"]["aliases"])
		return list(set(platforms))
				
	def cleanAliasName(self,alias):
		"""
		Function : Removes unwanted characters from the alias name. Only the '/' character purportedly causes issues.
							 This function replaces both '/' and '\' with '_'.
		Args     : alias - str. 
		Returns  : str.
		"""
		alias = alias.replace("/","_")	
		alias = alias.replace("\\","_")
		return alias


	def postMotifEnrichmentsFromTextFile(self,infile,patch=False):
		"""
		Function : Submits motif enrichment metadata organized in a text file to the DCC.
		Args     : The tab-delimited text file describing the motif enrichments.
		"""
		fh = open(infile,'r')
		for line in fh: 
			line = line.strip("\n")
			if not line: 
				continue
			if line.startswith("#"):
				continue
			line = line.split("\t")
			target = line[0].strip()
			accept_prob = line[1].strip()
			pos_bias_zscore = line[2].strip()
			peak_rank_bias_zscore = line[3].strip()
			global_enrichment_zscore = line[4].strip()
			encff = line[5].strip()
			encab = line[6].strip() 
			motif_analysis_file = line[8].strip()
			alias = en.LAB + encff + "_" + encab + "_" + target

			caption = "The motif for target {target} is represented by the attached position weight matrix (PWM) derived from {encff}. Motif enrichment analysis was done by Dr. Zhizhuo Zhang (Broad Institute, Kellis Lab). Accept probability score: {accept_prob}; Global enrichment Z-score: {global_enrichment_zscore}; Positional bias Z-score: {pos_bias_zscore}; Peak rank bias Z-score: {peak_rank_bias_zscore}; Enrichment rank: 1.0.".format(target=target,encff=encff,accept_prob=accept_prob,global_enrichment_zscore=global_enrichment_zscore,pos_bias_zscore=pos_bias_zscore,peak_rank_bias_zscore=peak_rank_bias_zscore)
		  
		
			payload = {} #payload will hold the secondary char submission
			payload["@id"] = "antibody_characterization/"
			payload["secondary_characterization_method"] = "motif enrichment"
			payload.update(self.AWARD_AND_LAB)
			payload["aliases"] = [alias]
			payload["characterizes"] = encab
			payload["target"] = target + "-human"
			payload["status"] = "pending dcc review"
			payload["caption"] = caption

			motif_analysis_basename= os.path.basename(motif_analysis_file)
			motif_analysis_file_mime_type = str(mimetypes.guess_type(motif_analysis_basename)[0])
			contents = str(base64.b64encode(open(motif_analysis_file,"rb").read()),"utf-8")
			pdb.set_trace()
			motif_analysis_temp_uri = 'data:' + motif_analysis_file_mime_type + ';base64,' + contents
			attachment_properties = {}
			attachment_properties["download"] = motif_analysis_basename
			attachment_properties["href"] = motif_analysis_temp_uri
			attachment_properties["type"] = motif_analysis_file_mime_type
			
			payload["attachment"] = attachment_properties
			payload["documents"] = ["encode:motif_enrichment_method","encode:TF_Antibody_Characterization_ENCODE3_May2016.pdf"]

			response = self.post(payload=payload,patch=patch)	
			if "@graph" in response:
				response = response["@graph"][0]
			self._writeAliasAndDccAccessionToLog(alias=alias,dcc_id=response["uuid"])

	def postDocument(self,download_filename,document,document_type,document_description):
		"""
		Function : The alias for the document will be the lab prefix plus the file name (minus the file extension).
		Args     : download_filename - str. The name to give the document when downloading it from the ENCODE portal.
							 document_type - str. For possible values, see https://www.encodeproject.org/profiles/document.json. It
								  appears that one should use "data QA" for analysis results documents. 
							 document_description - str. The description for the document.
							 document - str. Local filepath to the document to be submitted.
		Returns  : The DCC UUID of the new document. 
		"""
		document_filename = os.path.basename(document)
		document_alias = en.LAB + os.path.splitext(document_filename)[0]
		mime_type = mimetypes.guess_type(document_filename)[0]
		if not mime_type:
			raise Exception("Couldn't guess MIME type for {}.".format(document_filename))
		
		## Post information to DCC
		payload = {} 
		payload["@id"] = "documents/"
		payload.update(en.AWARD_AND_LAB)
		payload["aliases"] = [document_alias]
		payload["document_type"] = document_type
		payload["description"] = document_description
	
		data = base64.b64encode(open(document,'rb').read())
		temp_uri = str(data,"utf-8")
		href = "data:{mime_type};base64,{temp_uri}".format(mime_type=mime_type,temp_uri=temp_uri)
		#download_filename = library_alias.split(":")[1] + "_relative_knockdown.jpeg"
		attachment_properties = {} 
		attachment_properties["download"] = download_filename
		attachment_properties["href"] = href
		attachment_properties["type"] = mime_type
	
		payload['attachment'] = attachment_properties
		
		response = self.post(payload=payload)
		if "@graph" in response:
			response = response["@graph"][0]
		dcc_uuid = response['uuid']
		return dcc_uuid
	
	
	def linkDocument(self,rec_profile,rec_id,dcc_document_uuid):
		"""
		Function : Links an existing document on the ENCODE Portal to an existing experiment via the experiment's 'documents' attribute.
		Args     : rec_profile - An object profile name in the DCC schema, i.e. document, library, antibody, ..., signifying the
									profile of the object describing 'rec_id' that is to be linked to the document.  
							 dcc_document_uuid - The value of the document's 'uuid' attribute.
							 rec_id      - A DCC object identifier, i.e. accession, @id, UUID, ..., of the object to link the document to. 	
		Returns  : The PATCH response form self.patch().
		"""
		rec_json = self.getEncodeRecord(ignore404=False,rec_id=rec_id)
		documents_json = rec_json["documents"]
		#originally in form of [u'/documents/ba93f5cc-a470-41a2-842f-2cb3befbeb60/', u'/documents/tg81g5aa-a580-01a2-842f-2cb5iegcea03, ...]
		#strip off the /documents/ prefix from each document UUID:
		document_uuids = [x.strip("/").split("/")[-1] for x in documents_json]
		if document_uuids:
			document_uuids = self.addToSet(entries=document_uuids,new=dcc_document_uuid)
		else:
			document_uuids.append(dcc_document_uuid)
		payload = {}
		payload["@id"] = "{rec_profile}/".format(rec_profile=rec_profile)
		payload["documents"] = document_uuids
		self.patch(payload=payload,record_id=rec_id)
	
	def addToSet(self,entries,new):
		"""
		Function : Given a list of document IDs, determines whether the document to add to the list is alread in the list, and only adds it if it isn't yet a member.
							 This function has a side-affect of removing any existing duplicate documents.
		Args     : documents_list - list of document IDs in the form of UUIDs, document links (i.e. "/documents/709538e6-41a4-4dc1-a5d3-a5ee3c42413f/"), or a mix.
							 document      - A document UUID or document link.
		Returns  : list.
		"""
		entries.append(new)
		#Extract UUIDs part from each document (in the event some documents were passed in as document links).
		#documents_list = [x.strip("/").split("/")[-1] for x in documents_list]
		unique_list = list(set(entries))
		return unique_list
	
	#dcc_document_uuid = postDocument(download_filename="Snyder_RsemProtocol.txt",document="/srv/gsfs0/software/gbsc/encode/current/encode/sirna/rsem_protocol.txt",document_type="data QA",document_description="RSEM Protocol",patch=True)
	
	#exps=["ENCSR136ZPD","ENCSR045TQN","ENCSR181AXM","ENCSR542VBC","ENCSR977SOT","ENCSR509YMP","ENCSR669QED","ENCSR047MQO","ENCSR627AFW","ENCSR071JWS","ENCSR820EGA","ENCSR710CEM","ENCSR051NHG","ENCSR989NEA","ENCSR129WCZ","ENCSR261IHP","ENCSR431LBP","ENCSR067CAG","ENCSR169DSM","ENCSR965CCM","ENCSR798QCC","ENCSR793ISR","ENCSR640PVZ","ENCSR656ZOI","ENCSR174FUO","ENCSR301SWM","ENCSR626SFM","ENCSR856MQG","ENCSR205BWT","ENCSR312FIT","ENCSR555CYH","ENCSR390GWL","ENCSR345LKR","ENCSR472UFW","ENCSR133AIK","ENCSR675SDG","ENCSR874ZXG","ENCSR100ODO","ENCSR080OUZ","ENCSR336ZWX","ENCSR631RKH"]
	
	#for i in exps:
		#patchDocumentToExperiment(dcc_exp_id=i,dcc_document_uuid=dcc_document_uuid)
	#dcc_document_uuid = "455f1dae-ccec-495a-a76b-a0f7c64508b3"
	#patchDocumentToExperiment(dcc_exp_id="ENCSR092LZV",dcc_document_uuid=dcc_document_uuid)
	#patchDocumentToExperiment(dcc_exp_id="ENCSR576FFB/",dcc_document_uuid=dcc_document_uuid)



#indexing - bool. If set to True, means that the ENCODE Portal is indexing, thus newly POSTed objects may not 
# show up in search queries for several minutes. Giving an absolute resource identifier, on the other hand, seems to work
# when appending "?datastore=database" to the URL. As Esther stated: "_indexer to the end of the URL to see the status of elastic 
# search like https://www.encodeproject.org/_indexer if it's indexing it will say the status is "indexing", versus 
# waiting" and the results property will indicate the last object that was indexed."

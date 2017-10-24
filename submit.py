
# -*- coding: utf-8 -*-
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
import mimetypes

#inhouse libraries
import encode_utils as en

#debugging imports
import time
import pdb


def createSubprocess(cmd,pipeStdout=False,checkRetcode=True):
	"""
	Function : Creates a subprocess via a call to subprocess.Popen with the argument 'shell=True', and pipes stdout and stderr. Stderr is always
						 piped, but stdout can be turned off.
             If the argument checkRetcode is True, which it is by defualt, then for any non-zero return code, an Exception is
						 raised that will print out the the command, stdout, stderr, and the returncode when not caught. Otherwise, the Popen instance will be
						 returned, in which case the caller must 
					   call the instance's communicate() method (and not it's wait() method!!) in order to get the return code to see if the command was a success. communicate() will return 
						 a tuple containing (stdout, stderr). But at that point, you can then check the return code with Popen instance's 'returncode' attribute.
	Args     : cmd   - str. The command line for the subprocess wrapped in the subprocess.Popen instance. If given, will be printed to stdout when there is an error in the subprocess.
						 pipeStdout - bool. True means to pipe stdout of the subprocess.
						 checkRetcode - bool. See documentation in the description above for specifics.
	Returns  : A two-item tuple containing stdout and stderr, respectively.
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


class Submit():

	REQUEST_HEADERS_JSON = {'content-type': 'application/json'}
	SYAPSE_ENTEX_BIOSAMPLE_KBCLASS_ID = "BiosampleENTex"
	SYAPSE_ATACSEQ_KBCLASS_ID = "AtacSeq"
	SYAPSE_LIBRARY_KBCLASS_ID = "Library"
	
	DCC_PROD_MODE = "prod"
	DCC_DEV_MODE = "dev"
	DCC_MODES = {
 	 DCC_PROD_MODE: "https://www.encodeproject.org/",
 	 DCC_DEV_MODE: "https://test.encodedcc.org/"
 	 }

	DCC_ALIAS_PREFIX = en.DCC_ALIAS_PREFIX
	
	AWARD_AND_LAB = {"award": en.AWARD,"lab": DCC_ALIAS_PREFIX.rstrip(":")}

	def __init__(self,dcc_username,dcc_mode):

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
		#self.sconn.kb.getProperty("submittedToDcc"): [u'Internal Hold', u'Send to DCC', u'Registered with DCC', u'DCC Hold"]
		self.sendToDcc = {
			"SEND": "Send to DCC",
			"INTERNAL_HOLD": "Internal Hold",
			"REGISTERED": "Registered with DCC",
			"HOLD": "DCC Hold"
		}	
		
	def writeAliasAndDccAccessionToLog(self,alias,dcc_id=None):
		txt = alias
		if dcc_id:
			txt += " -> {dcc_id}".format(dcc_id=dcc_id)
		self.post_logger.info(txt)

	def stripDccAlias(self,name):
		"""
		Function : Strips off the value of self.DCC_ALIAS_PREFIX if present.
		Returns  : str.
		"""
		return name.split(":",1)[-1]

	def getAliases(self,dcc_id):
		"""
		Function : Given the ENCODE ID for an object, returns the aliases for that object. For any alias that is prefixed by
							 self.DCC_ALIAS_PREFIX, that prefix will be removed.
		Args     : dcc_id - The ENCODE ID for a given object, i.e ENCSR999EHG.
		Returns  : list.
		"""
		record = self.getDccRecord(ignore404=False,dcc_id=dcc_id)
		aliases = record["aliases"]
		for index in range(len(aliases)):
			alias = aliases[index]
			if alias.startswith(self.DCC_ALIAS_PREFIX):
				aliases[index] =  self.stripDccAlias(alias)
		return aliases

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

	def searchDcc(self,searchString):
		"""
		Example : searchDcc(searchString="search/?type=experiment&searchTerm=ENCLB336TVW&format=json&frame=object&datastore=database") #This is L-519 as the library query
		"""
		url = os.path.join(self.dcc_url,searchString)
		self.logger.info("Searching DCC with query {url}.".format(url=url))
		response = requests.get(url,auth=self.auth,headers=self.REQUEST_HEADERS_JSON,verify=False)
		if response.status_code not in [200,404]: #if not ok and not found
			response.raise_for_status()
		return response.json()["@graph"] #the @graph object is a list

	def getJsonFromDccUrl(self,url):
		response = requests.get(url=url,auth=self.auth,headers=self.REQUEST_HEADERS_JSON, verify=False)
		if response.ok:
			response = response.json()
			if "@graph" in response:
				response = response["@graph"]
			return response

		else:
			response.raise_for_status()
			

	def getDccRecord(self,identifier,ignore404=True,frame=None):
		"""
		Function : Looks up an object in ENCODE using a unique identifier, such as the object id, an alias, uuid, or accession. 
		Args     : ignore404 - bool. True indicates to not raise an Exception if a 404 is returned. 
						 : identifier - A unique identifier, such as the object id, an alias, uuid, or accession.
		"""
		recordId = identifier
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
		elif response.status_code == 403:
			return
		elif response.status_code == 404:
			if ignore404:
				return {}
			else:
				raise Exception("DCC entity {entity} not found".format(entity=recordId))
		else:
			#if response not okay and status_code equal to something other than 404
			response.raise_for_status()

	def patchToDcc(self,payload,record_id,indexing=False):
		"""
		Function : PATCH an object to the DCC. If the object doesn't exist, then this method will call self.postToDcc().
		Args     : payload - dict. containing the attribute key and value pairs to patch.
							 record_id - str. Identifier of the DCC record to patch. 
							 indexing - bool. If set to True, means that the ENCODE Portal is indexing, thus newly POSTed objects may not 
                  show up in search queries for several minutes. Giving an absolute resource identifier, on the other hand, seems to work
                  when appending "?datastore=database" to the URL. Setting this to True ultimately adds a 5 min. delay after POSTing an
                  object in the self.postToDcc() method. As Esther stated: "_indexer to the end of the URL to see the status of elastic 
                  search like https://www.encodeproject.org/_indexer  If it's indexing it will say the status is "indexing", versus 
									"waiting" and the results property will indicate the last object that was indexed."
		Raises   : requests.exceptions.HTTPError if the return status is !ok. 
		"""
		self.logger.info("\nIN patchToDcc()")
		objectType = payload.pop("@id")
		self.logger.info("Will check if {} exists in DCC with a GET request.".format(record_id))
		get_response_json = self.getDccRecord(ignore404=True,identifier=record_id)
		if not get_response_json:
			#then need to do a POST
			payload["@id"] = objectType
			response = self.postToDcc(payload=payload)
			return response

		url = os.path.join(self.dcc_url,record_id)
		self.logger.info("<<<<<<Attempting to PATCH {record_id} To DCC with URL {url} and this payload:\n\n{payload}\n\n".format(record_id=record_id,url=url,payload=payload))
		response = requests.patch(url, auth=self.auth, headers=self.REQUEST_HEADERS_JSON, data=json.dumps(payload), verify=False)

		self.logger.debug("<<<<<<DCC PATCH RESPONSE: ")
		self.logger.debug(json.dumps(response.json(), indent=4, sort_keys=True))
		if response.ok:
			return response.json()
		elif response.status_code == 403: #don't have permission to PATCH or POST to this object:
			return get_response_json
		else:
			message = "Failed to PATCH {alias} to DCC".format(alias=alias)
			self.logger.error(message)
			response.raise_for_status()
		if indexing and not patch:
			#then an object was just POSTed, and since the ENCODE portal is indexing, add a 5 min. delay:
			time.sleep(60 * 5)

	def postToDcc(self,payload,indexing=False):
		"""
		Function : POST an object to the DCC.
		Args     : payload - The data to submit.
							 record_id - str. Must be set if 'patch' is True.
							 patch - bool. True indicates to perform an HTTP PATCH operation rather than POST.
							 indexing - bool. If set to True, means that the ENCODE Portal is indexing, thus newly POSTed objects may not 
                  show up in search queries for several minutes. Giving an absolute resource identifier, on the other hand, seems to work
                  when appending "?datastore=database" to the URL. Setting this to True ultimately adds a 5 min. delay after POSTing an
                  object in the self.postToDcc() method. As Esther stated: "_indexer to the end of the URL to see the status of elastic 
                  search like https://www.encodeproject.org/_indexer  If it's indexing it will say the status is "indexing", versus 
									"waiting" and the results property will indicate the last object that was indexed."
		Raises   : requests.exceptions.HTTPError if the return status is !ok. 
		"""
		self.logger.info("\nIN postToDcc().")
		objectType = payload.pop("@id")
		url = os.path.join(self.dcc_url,objectType)
		self.logger.info("<<<<<<Attempting to POST {record_id} To DCC with URL {url} and this payload:\n\n{payload}\n\n".format(record_id=record_id,url=url,payload=payload))
		response = requests.post(url, auth=self.auth, headers=self.REQUEST_HEADERS_JSON, data=json.dumps(payload), verify=False)
		self.logger.debug("<<<<<<DCC {httpMethod} RESPONSE: ".format(httpMethod=httpMethod))
		self.logger.debug(json.dumps(response.json(), indent=4, sort_keys=True))
		status_code = response.status_code
		if response.ok:
			self.writeAliasAndDccAccessionToLog(alias=alias,dcc_id=response_dcc_accession)
			response_dcc_accession = ""
			try:
				response_dcc_accession = response.json()["@graph"][0]["accession"]
			except KeyError:
				pass #some objects don't have an accession, i.e. replicates.
			return response.json()
		elif status_code == 409: #conflict
			self.logger.info("Will not post {} to DCC because it already exists.".format(record_id))
		else:
			message = "Failed to POST {alias} to DCC".format(alias=alias)
			self.logger.error(message)
			response.raise_for_status()
		if indexing and not patch:
			#then an object was just POSTed, and since the ENCODE portal is indexing, add a 5 min. delay:
			time.sleep(60 * 5)


	def doesReplicateExist(self,library_alias,biologicial_replicate_number,technical_replicate_number,replicates_json_from_dcc):
		"""
		Function :
		Args     : library_alias - str. Any of the associated library's aliases. i.e. michael-snyder:L-208.
							 biologicial_replicate_number - int. The biological replicate number. In Syapse, see the "Replicate Number" attribute.
							 technical_replicate_number - int. The technical replicate number. 
							 replicates_json_from_dcc - dict. The value of the "replicates" key in the JSON of a DCC experiment.
							
		Returns  : str. The replicate alias if a such a replicate is already linked to the experiment in question, otherwise the empty string.
		"""
		biologicial_replicate_number = int(biologicial_replicate_number)
		technical_replicate_number = int(technical_replicate_number)
		for rep in replicates_json_from_dcc:
			rep_alias = rep["aliases"][0]
			rep_lib = rep["library"]
			rep_bio_rep_number = rep["biological_replicate_number"]
			rep_tech_rep_number = rep["technical_replicate_number"]
			if (library_alias in rep["aliases"]) and (biologicial_replicate_number == rep_bio_rep_number) and (technical_replicate_number == rep_tech_rep_number):
				return rep_alias
		return ""
		
	def get_file_rep_dico(self,dcc_exp_id):
		"""
		Function : Given a DCC experiment ID, finds the original FASTQ files that were submitted and creates
							 a dictionary with keys being the biological_replicate_number. For example, If there are three replicates on the
							 experiment, then there will be three entries in the dictionary. The value of each key is another dictonary 
							 that contains a single key being the read number describing the reads in a given FASTQ file, and the value being 
							 the FASTQ file JSON. The read number will be 1 for a FASTQ file containing forward reads, and 2 for reverse reads.

		Args : dcc_exp_id - list of DCC file IDs or aliases 
		"""
		exp_json = self.getDccRecord(ignore404=False,dcc_id=dcc_exp_id)
		dcc_file_ids = exp_json["original_files"]
		dico = {}
		for i in dcc_file_ids:
			file_json = self.getDccRecord(ignore404=False,dcc_id=i)
			brn = file_json["replicate"]["biological_replicate_number"]	#int
			read_num = file_json["paired_end"] #string
			if brn not in dico:
				dico[brn] = {}
			dico[brn][read_num] = file_json
		return dico


	def getAwsUploadCredsFromResponseGraph(self,graph):
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
							 postToDcc() will try to retrive the corresponding Syapse object. Before attempting a POST, will check if the file exists by doing a get on payload["aliases"][0].
							 If the GET request succeeds, nothing will be POSTed.
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
		exists_on_dcc = self.getDccRecord(ignore404=True,dcc_id=alias)
		if not exists_on_dcc:
			#check with actual file alias in the payload. Useful if previously we only had part of the file by mistake (i.e incomplete downoad)
			# hence the uploaded file on DCC would have a different md5sum.
			alias = payload["aliases"][0]
			exists_on_dcc = self.getDccRecord(ignore404=True,dcc_id=alias)
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
			self.writeAliasAndDccAccessionToLog(alias=alias,dcc_id=response_dcc_accession)
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
		aws_creds = self.getAwsUploadCredsFromResponseGraph(graph)
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
			

	def getPlatformsOnExperiment(self,dcc_exp_alias=False,dcc_exp_encid=False):
		if not dcc_exp_alias and not dcc_exp_encid:
			raise Exception("You must specify either dcc_exp_alias or dcc_exp_encid.")
		exp_json = self.getDccRecord(ignore404=False,dcc_alias=dcc_exp_alias,dcc_id=dcc_exp_encid,frame=None)
		if "@graph" in exp_json:
			exp_json = exp_json["@graph"][0]
		files_json = exp_json["files"]
		platforms = []
		for f in files_json:
			if "platform" not in f:
				continue #could be an analysis file created by the DCC.
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
			alias = en.DCC_ALIAS_PREFIX + encff + "_" + encab + "_" + target

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
			contents = open(motif_analysis_file,"rb").read().encode("base64").replace("\n", "")
			motif_analysis_temp_uri = 'data:' + motif_analysis_file_mime_type + ';base64,' + contents
			attachment_properties = {}
			attachment_properties["download"] = motif_analysis_basename
			attachment_properties["href"] = motif_analysis_temp_uri
			attachment_properties["type"] = motif_analysis_file_mime_type
			
			payload["attachment"] = attachment_properties
			payload["documents"] = ["encode:motif_enrichment_method","encode:TF_Antibody_Characterization_ENCODE3_May2016.pdf"]

			response = self.postToDcc(payload=payload,patch=patch)	
			if "@graph" in response:
				response = response["@graph"][0]
			self.writeAliasAndDccAccessionToLog(alias=alias,dcc_id=response["uuid"])

if __name__ == "__main__":
	syapseConfFile = syapse_scgpm.syapse_conf_file
	syapseConnConfFh = open(syapseConfFile,'r')
	SCC = json.load(syapseConnConfFh) #syapse connection conf

	from argparse import ArgumentParser
	parser = ArgumentParser()
	parser.add_argument('-u','--dcc-user-name',default="nathankw",help="The username for logging into the prod or dev encode website. This username will determine which API key to use when posting to DCC by looking in the file called dcc_submitters.json in the same directory as this script. If your username is not in that file, you'll need to add it. Default is %(default)s.")
	parser.add_argument('--syapse-mode',default=SCC["modes"]["prod"],choices=SCC["modes"].keys(),help="Determines which Syapse LIMS host URL to connect to. Default is %(default)s.")
	parser.add_argument('--dcc-mode',default="prod",choices=Submit.DCC_MODES.keys(),help="Determines which DCC URL to connect to. Default=%(default)s.")
	parser.add_argument('-p','--patch',action="store_true",help="Indicates to perform an HTTP PATCH operation rather than POST.")
	parser.add_argument('-c','--continue-on',action="store_true",help="Continue past error conditions.")

	args = parser.parse_args()
	patch = args.patch
	dcc_user_name = args.dcc_user_name
	dcc_mode = args.dcc_mode
	continueOn = args.continue_on

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
import encode_utils.utils

import time

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class UnknownDccProfile(Exception):
  pass

class Connection():
  """ENCODE Portal data submission and retrieval. 

  In order to authenticate with the DCC servers when making HTTP requests, you must have the 
  the environment variables DCC_API_KEY and DCC_SECRET_KEY set. Check with your DCC data wrangler
  if you haven't been assigned these keys. 

  Two log files will be opened in append mode in the calling directory, and named 
  ${dcc_mode}_posted.txt and ${dcc_mode}_error.txt.

  Attributes:
      dcc_mode: The environment of the ENCODE Portal site ("prod" or "dev") to connect to.
  """
  REQUEST_HEADERS_JSON = {'content-type': 'application/json'}
  
  DCC_PROD_MODE = "prod"
  DCC_DEV_MODE = "dev"
  DCC_MODES = {
    DCC_PROD_MODE: "https://www.encodeproject.org/",
    DCC_DEV_MODE: "https://test.encodedcc.org/"
    }

  def __init__(self,dcc_mode):

    f_formatter = logging.Formatter(
        '%(asctime)s:%(name)s:%(levelname)s:\t%(message)s')
    #: A logging instance with a console handler accepting DEBUG level messages.
    #: Also contains an error handler, logging error messages at the ERROR level to a file by the 
    #: name of ${dcc_mode}_error.txt that is opened in append mode in the calling directory.
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    #Create console handler
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(f_formatter)
    logger.addHandler(ch)
    #Create error handler
    error_fh = logging.FileHandler(
        filename=dcc_mode + "_" + "error.txt", mode="a")
    error_fh.setLevel(logging.ERROR)
    error_fh.setFormatter(f_formatter)
    logger.addHandler(error_fh)

    #: Another logging instance to log the IDs of posted objects. Accepts messages at the 
    #: INFO level and logs them to a file named ${dcc_mode}_posted that is opened in append mode
    #: in the calling directory. 
    post_logger = logging.getLogger("post")
    post_logger.setLevel(logging.INFO)
    post_logger_fh = logging.FileHandler(filename=dcc_mode + "_" + "posted.txt",mode="a")
    post_logger_fh.setLevel(logging.INFO)
    post_logger_fh.setFormatter(f_formatter)
    post_logger.addHandler(post_logger_fh)
    
    self.logger = logger
    self.post_logger = post_logger

    #: Stores the value of the passed in argumement by the same name.
    self.dcc_mode = dcc_mode

    #: The prod or dev DCC URL, determined by the value of the dcc_mode instance attribute.
    self.dcc_url = self._setDccUrl()

    #: The API key to use when authenticating with the DCC servers. This is set automatically
    #: to the value of the DCC_API_KEY variable in the _setDccUrl() private method. 
    self.api_key = self._setApiKeys()[0]
    #: The secret key to use when authenticating with the DCC servers. This is set automatically
    #: to the value of the DCC_SECRET_KEY variable in the _setDccUrl() private method.
    self.secret_key = self._setApiKeys()[1]
    self.auth = (self.api_key,self.secret_key)

  def _setDccUrl(self):
    return self.DCC_MODES[self.dcc_mode]

  def _setApiKeys(self):
    """
    Retrieves the API key and secret key based on the environment variables DCC_API_KEY and 
    DCC_SECRET_KEY.

    Args: 
        Returns: Tuple containing the (API Key, Secret Key)
    """
    api_key = os.environ["DCC_API_KEY"]
    secret_key = os.environ["DCC_SECRET_KEY"]
    return api_key,secret_key
    
  def _writeAliasAndDccAccessionToLog(self,alias,dcc_id=None):
    txt = alias
    if dcc_id:
      txt += " -> {dcc_id}".format(dcc_id=dcc_id)
    self.post_logger.info(txt)

  def getAliases(self,dcc_id,strip_alias_prefix=False):
    """
    Given the ENCODE ID for an object, returns the aliases for that object. 

    Args: 
        dcc_id: The ENCODE ID for a given object, i.e ENCSR999EHG.
        strip_alias_prefix: bool. True means to remove the alias prefix if all return aliases. 

    Returns:
        list.
    """
    record = self.getEncodeRecord(ignore404=False,dcc_id=dcc_id)
    aliases = record["aliases"]
    for index in range(len(aliases)):
      alias = aliases[index]
      if strip_alias_prefix:
        aliases[index] =  encode_utils.utils.stripDccAliasPrefix(alias)
    return aliases

  def searchEncode(self,search_args):
    """
    Searches the ENCODE Portal using the provided query parameters in dictionary format. The query 
    parameters will be first URL encoded. 

    Args:
        search_args - dict. of key and value query parameters. 

    Returns:
        list of search results. 

    Raises:
        HTTPError: If the status code is not in the set [200,404].

    Example
        Given we have the following dictionary *d* of key and value pairs::

            {"type": "experiment",
             "searchTerm": "ENCLB336TVW",
             "format": "json",
             "frame": "object",
             "datastore": "database"
            }
  
        We can call the function as::

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

    Looks up an object in ENCODE using a unique identifier, such as the object id, an alias, uuid, 
    or accession. 

    Args: 
        ignore404: bool. True indicates to not raise an Exception if a 404 is returned. 
        rec_id: A unique identifier, such as the object id, an alias, uuid, or accession.

    Returns:
        The JSON response. 

    Raises:
        Exception: The status code is 403 (forbidden) or 404 (not found). If the 'ignore404'
            argument is set to True, however, an Exception will not be raised in this latter case. 
    """
    recordId = rec_id 
    if recordId.endswith("/"):
      recordId = recordId.lstrip("/")
    url = os.path.join(self.dcc_url,recordId,"?format=json&datastore=database")
    if frame:
      url += "&frame={frame}".format(frame=frame)
    self.logger.info(">>>>>>GETTING {recordId} From DCC with URL {url}".format(
        recordId=recordId,url=url))
    response = requests.get( url,auth=self.auth, headers=self.REQUEST_HEADERS_JSON, verify=False)
    if response.ok:
      #logger.info("<<<<<GET RESPONSE: ")
      #self.logger.debug(json.dumps(response.json(), indent=4, sort_keys=True))
      return response.json()
    elif response.status_code == 403: #forbidden
      raise Exception("Access to ENCODE entity {entity} is forbidden".format( entity=recordId))
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

    Given the JSON serialization of a DCC record, extracts an ID from it. The ID will be the value
    of the 'id' key if that is present in rec_json, otherwise it will be the value of the first 
    alias in the 'aliases' key. If there isn't an alias present, an IndexError will be raised.

    Args:
        rec_json - The JSON serialization of the record in question.

    Returns:
        str. 

    Raises:
        IndexError: if a record ID can't be found (since the last attempt to find an identifier 
            works by subsetting the first element in the 'aliases' key).
    """

    #The '@id' key has a value in the format /profile/id, where profile is 
    #  something like 'documents', 'libraries', 'antibodies', ... This key also 
    #  stores a record ID at the end when addressing a record belonging to a 
    #  particular profile.
    if "@id" in rec_json:
      id_tokens = rec_json["@id"].strip("/").split()
      if len(id_tokens) > 1: #Then there is a record ID stored here
        return id_tokens[-1]
    else:
      return rec_json["aliases"][0]

  def patch(self,payload,record_id=None,error_if_not_found=True,raise_403=True,
            extend_array_values=True):
    """
    PATCH an object to the DCC. If the object doesn't exist, then this method will call 
    self.post(), unless the argument 'error_if_not_found' is set to True.

    Args: 
        payload: dict. containing the attribute key and value pairs to patch.
        record_id: str. Identifier of the DCC record to patch. If not specified, will first check 
            if it is set in the payload's '@id' attribute, and if not there, the 'aliases'
            attribute.
        error_if_not_found: bool. If set to True, then an Exception will be raised if the record to
            PATCH is not found on the ENCODE Portal. If False and the record isn't found, then a 
            POST will be attempted by calling self.PostToDcc().
        raise_403: bool. True means to raise an HTTPError if a 403 status (Forbidden) is returned. 
            If set to False and there still is a 403 return status, then the object you were 
            trying to PATCH will be fetched from the Portal in JSON format as this function's
            return value.
        extend_array_values: bool. Only affects keys with array values. True (default) means to 
            extend the corresponding value on the Portal with what's specified in the payload. 
            False means to replace the value on the Portal with what's in the payload. 
    Returns: 
        The PATCH response. 

    Raises: 
        requests.exceptioas.HTTPError: if the return status is not in the 200 range (excluding a 
            403 status if 'raise_403' is False, and excluding a 404 status if 'error_if_not_found' 
            is False. 
        UnknownDccProfile: can be raised if a POST is attempted and the payload does not contain 
            the profile to post to (as a value of the '@id' key).
    """
    json_payload = json.loads(json.dumps(payload)) 
      #make sure we have a payload that can be converted to valid JSON, and 
      # tuples become arrays, ...
    self.logger.info("\nIN patch()")
    if not record_id:
      record_id = self.getRecordId(json_payload) 
        #first tries the @id field, then looks for the first alias in the 'aliases' attr.
        
    self.logger.info(
        "Will check if {} exists in DCC with a GET request.".format(record_id))
    get_response_json = self.getEncodeRecord(ignore404=True,rec_id=record_id,frame="object")
    if not get_response_json:
      if error_if_not_found:
        raise Exception(("Can't patch record '{}' since it was not found on the"
                         " ENCODE Portal.").format(record_id))
      #then need to do a POST
      else:
        response = self.post(payload=json_payload)
        return response

    if "@id" in json_payload:
      #We don't submit the '@id' prop when PATCHing, only POSTing (and in this 
      # latter case it must specify the profile to POST to). 
      json_payload.pop("@id")

    if extend_array_values:
      for key in json_payload:
        if type(json_payload[key]) is list:
          json_payload[key].extend(get_response_json.get(key,[]))
          #I use get_response_json.get(key,[]) above because in a GET request, 
          # not all props are pulled back when they are empty.
          # For ex, in a file object, if the controlled_by prop isn't set, then 
          # it won't be in the response.
          json_payload[key] = list(set(json_payload[key]))

    url = os.path.join(self.dcc_url,record_id)
    self.logger.info(
        ("<<<<<<Attempting to PATCH {record_id} To DCC with URL"
         " {url} and this payload:\n\n{payload}\n\n").format(
             record_id=record_id,url=url,payload=json_payload))

    response = requests.patch(url, auth=self.auth, headers=self.REQUEST_HEADERS_JSON,
                              data=json.dumps(json_payload), verify=False)

    self.logger.debug("<<<<<<DCC PATCH RESPONSE: ")
    self.logger.debug(json.dumps(response.json(), indent=4, sort_keys=True))
    if response.ok:
      return response.json()
    elif response.status_code == 403:
      #don't have permission to PATCH this object.
      if not raise_403:
        return get_response_json
    else:
      message = "Failed to PATCH {} to DCC".format(record_id)
      self.logger.error(message)
      response.raise_for_status()

  def post(self,payload):
    """ POST an object to the DCC.

    Args:
        payload: The data to submit.

    Returns: 
        The object's JSON sererialization from the DCC, after it is posted.

    Raises:
        requests.exceptions.HTTPError: if the return status is !ok. 
    """
    #make sure we have a payload that can be converted to valid JSON, and tuples become arrays, ...
    json_payload = json.loads(json.dumps(payload)) 
    self.logger.info("\nIN post().")
    profile = encode_utls.utils.parse_profile_from_id_prop(json_payload)
    if not profile:
      raise UnknownDccProfile(
          "Invalid profile '{}' specified in the '@id' attribute.".format( profile))
    url = os.path.join(self.dcc_url,profile)
    alias = json_payload["aliases"][0]
    self.logger.info(
        ("<<<<<<Attempting to POST {alias} To DCC with URL {url} and this"
         " payload:\n\n{payload}\n\n").format( alias=alias,url=url,payload=json_payload))

    response = requests.post(url, auth=self.auth, headers=self.REQUEST_HEADERS_JSON,
                             data=json.dumps(json_payload), verify=False)
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
      self.logger.error("Will not post {} to DCC because it already exists.".format(alias))
      rec_json = self.getEncodeRecord(rec_id=alias,ignore404=False)
      return rec_json
    else:
      message = "Failed to POST {alias} to DCC".format(alias=alias)
      self.logger.error(message)
      response.raise_for_status()

  def getFastqFileRepNumDico(self,dcc_exp_id):
    """
    Given a DCC experiment ID, finds the original FASTQ files that were submitted and creates a 
    dictionary with keys being the biological_replicate_number. The value of each key is another 
    dictionary having the technical_replicate_number as the single key. The value of this is 
    another dictionary with keys being file read numbers, i.e. 1 for forward reads, 2 for reverse 
    reads.  The value for a give key of this most inner dictionary is the file JSON. 

    Args:
        dcc_exp_id - list of DCC file IDs or aliases 

    Returns:
        dict. 
    """
    exp_json = self.getEncodeRecord(ignore404=False,rec_id=dcc_exp_id)
    dcc_file_ids = exp_json["original_files"]
    dico = {}
    for i in dcc_file_ids:
      file_json = self.getEncodeRecord(ignore404=False,rec_id=i)
      if file_json["file_type"] != "fastq":
        continue #this is not a file object for a FASTQ file.
      brn,trn = file_json["replicate"]["biological_replicate_number"], file_json["replicate"]["technical_replicate_number"]
      read_num = file_json["paired_end"] #string
      if brn not in dico:
        dico[brn] = {}
      if trn not in dico[brn]:
        dico[brn][trn] = {}
      dico[brn][trn][read_num] = file_json
    return dico


  def _setAwsUploadCredsFromResponseGraph(self,upload_credentials):
    """
    After posting the metadata for a file object to ENCODE, the response will contain the key 
    'upload_credentials'. This method parses the document pointed to by this key, constructing a 
    dictionary of keys that will be exported as environment variables that can be used by the aws 
    CL agent.  That is what self.postFileToDcc() does, indirectly. self.postFileToDcc() has an 
    argument 'aws_creds' that expects a value generated from this method.  This method is also 
    called from self.regenerateAwsUploadCreds(), which produces a JSON document also containing the
    key 'upload_credentials'. 

    Returns:
        dict.
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
    This is only to be used for DCC "/file/" type objects, because for these we don't have a Syapse
    record for them (the regular POST method called post() will try to retrive the corresponding 
    Syapse object. Before attempting a POST, will check if the file exists by doing a get on 
    payload["aliases"][0].  If the GET request succeeds, nothing will be POST'd.

    Args:
        payload: The data to submit.
        patch: bool. True indicates to perform an HTTP PATCH operation rather than POST.
    """
    self.logger.info("\nIN postFileMetaDataToDcc(), patch={patch}\n".format(patch=patch))  
    objectType = payload.pop("@id") #should be /file/
    filename = payload["submitted_file_name"]
    #alias = payload["aliases"][0]
    md5_alias = "md5:" + payload["md5sum"]
    alias = md5_alias
    
    #check if file already exists on DCC using md5sum. Useful if file exists 
    # already but under different alias.
    exists_on_dcc = self.getEncodeRecord(ignore404=True,dcc_id=alias)
    if not exists_on_dcc:
      #check with actual file alias in the payload. Useful if previously we only
      # had part of the file by mistake (i.e incomplete downoad)
      # hence the uploaded file on DCC would have a different md5sum.
      alias = payload["aliases"][0]
      exists_on_dcc = self.getEncodeRecord(ignore404=True,dcc_id=alias)
    if not patch and exists_on_dcc:
      self.logger.info(
          ("Will not POST metadata for {filename} with alias {alias} to DCC"
           " because it already exists as {encff}.").format(
               filename=filename,alias=alias,encff=exists_on_dcc["accession"]))
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
      self.logger.info(
          ("<<<<<<Attempting to PATCH {filename} metadata with alias {alias} and ENCFF ID"
           " {encff_id} for replicate to DCC with URL {url} and this payload:"
           "\n{payload}").format(filename=filename,alias=alias,encff_id=encff_id,
                                 url=url,payload=payload))

      response = requests.patch(url,auth=self.auth,headers=self.REQUEST_HEADERS_JSON,
                                data=json.dumps(payload),verify=False)
    else:
      httpMethod = "POST"
      url = os.path.join(self.dcc_url,objectType)
      self.logger.debug(
          ("<<<<<<Attempting to POST file {filename} metadata for replicate to"
           " DCC with URL {url} and this payload:\n{payload}").format(
               filename=filename,url=url,payload=payload))
      response = requests.post(url, auth=self.auth, headers=self.REQUEST_HEADERS_JSON,
                               data=json.dumps(payload), verify=False)

    response_json = response.json()
    self.logger.debug(
        "<<<<<<DCC {httpMethod} RESPONSE: ".format(httpMethod=httpMethod))
    self.logger.debug(json.dumps(response_json, indent=4, sort_keys=True))
    if "code" in response_json and response_json["code"] == 409:
      #There was a conflict when trying to complete your request
      # i.e could be trying to post the same file again and there is thus a key
      # conflict with the md5sum key. This can happen when the alias we have 
      # isn't the alias that was posted. For example, ENCFF363RMP has this
      # alis: michael-snyder:150612_TENNISON_0368_BC7CM3ACXX_L3_GATCAG_1 
      #(because it was originally created using dsalins code, but this codebase
      # here would use this as an alias: 
      # michael-snyder:150612_TENNISON_0368_BC7CM3ACXX_L3_GATCAG_1_pf.fastq.gz
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
    cmd = ("curl -X POST -H 'Accept: application/json' -H 'Content-Type: application/json'"
           " https://{api_key}:{secret_key}@www.encodeproject.org/files/{encff_number}/upload -d '{{}}' | python -m json.tool").format(api_key=self.api_key,secret_key=self.secret_key,encff_number=encff_number)
    print(cmd)
    popen = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = popen.communicate()
    retcode = popen.returncode
    if retcode:
      raise Exception(("Command {cmd} failed with return code {retcode}. stdout is {stdout} and"
                       " stderr is {stderr}.").format(cmd=cmd,retcode=retcode,stdout=stdout,
                                                     stderr=stderr))
    response = json.loads(stdout)
    self.logger.info(response)
    if "code" in response:
      code = response["code"]
      if code == 403:
        #Access was denied to this resource. File already uploaded fine.
        return
    graph = response["@graph"][0]
    aws_creds = self._setAwsUploadCredsFromResponseGraph(graph["upload_credentials"])
    return aws_creds

  def postFileToDcc(self,filepath,encff_number,aws_creds=None):
    """
    Args:
        filepath: The local path to the file to upload.
        upload_url: The AWS upload address (i.e. S3 bucket address).
        aws_creds: dict. with keys AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SECURITY_TOKEN.
    Returns:
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
        logger.info(("Will not upload file {filepath} to s3. Attempt failed with status code HTTP"
                    " 403 (Forbidden). Normally, this means we shouldn't be editing this object"
                    " and that all is fine.").format(filepath=filepath))
      else:
        raise Exception(
            ("Subprocess command {cmd} failed with returncode {retcode}. Stdout is {stdout}."
             " Stderr is {stderr}.").format(cmd=cmd,retcode=retcode,stdout=stdout,stderr=stderr))
      

  def getPlatformsOnExperiment(self,rec_id):
    """
    Looks at all FASTQ files on the specified experiment, and tallies up the varying sequencing 
    platforms that generated them.  This is moreless used to verify that there aren't a mix of 
    multiple different platforms present as normally all reads should come from the same platform.

    Args:
        rec_id: str. DCC identifier for an experiment. 
    Returns:
        De-duplicated list of platforms seen on the experiment's FASTQ files. 
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
        
  def postMotifEnrichmentsFromTextFile(self,infile,patch=False):
    """
    Submits motif enrichment metadata organized in a text file to the DCC.

    Args: 
        The tab-delimited text file describing the motif enrichments.
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

      caption = ("The motif for target {target} is represented by the attached position weight"
                 " matrix (PWM) derived from {encff}. Motif enrichment analysis was done by Dr."
                 " Zhizhuo Zhang (Broad Institute, Kellis Lab). Accept probability score:"
                 " {accept_prob}; Global enrichment Z-score: {global_enrichment_zscore};"
                 " Positional bias Z-score: {pos_bias_zscore}; Peak rank bias Z-score:"
                 " {peak_rank_bias_zscore}; Enrichment rank: 1.0.").format(
                     target=target,encff=encff,accept_prob=accept_prob,
                     global_enrichment_zscore=global_enrichment_zscore,
                     pos_bias_zscore=pos_bias_zscore,peak_rank_bias_zscore=peak_rank_bias_zscore)
      
    
      payload = {} #payload will hold the secondary char submission
      payload["@id"] = "antibody_characterization/"
      payload["secondary_characterization_method"] = "motif enrichment"
      payload.update(en.AWARD_AND_LAB)
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
      payload["documents"] = [
          "encode:motif_enrichment_method",
          "encode:TF_Antibody_Characterization_ENCODE3_May2016.pdf"]

      response = self.post(payload=payload,patch=patch)  
      if "@graph" in response:
        response = response["@graph"][0]
      self._writeAliasAndDccAccessionToLog(alias=alias,dcc_id=response["uuid"])

  def postDocument(self,download_filename,document,document_type,document_description):
    """
    The alias for the document will be the lab prefix plus the file name (minus the file extension).

    Args: 
        download_filename: str. The name to give the document when downloading it from the ENCODE 
            portal.
        document_type: str. For possible values, see 
            https://www.encodeproject.org/profiles/document.json. It appears that one should use 
            "data QA" for analysis results documents. 
        document_description - str. The description for the document.
        document - str. Local filepath to the document to be submitted.

    Returns: 
        The DCC UUID of the new document. 
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
  
  
  def linkDocument(self,rec_id,dcc_document_uuid):
    """
    Links an existing document on the ENCODE Portal to another existing object on the Portal via
    the latter's "documents" property.

    Args:
         rec_id: A DCC object identifier, i.e. accession, @id, UUID, ..., of the object to link the 
             document to.   
         dcc_document_uuid: The value of the document's 'uuid' attribute.

    Returns:
        The PATCH response form self.patch().
    """
    rec_json = self.getEncodeRecord(ignore404=False,rec_id=rec_id)
    documents_json = rec_json["documents"]
    #Originally in form of [u'/documents/ba93f5cc-a470-41a2-842f-2cb3befbeb60/',
    #                       u'/documents/tg81g5aa-a580-01a2-842f-2cb5iegcea03, ...]
    #Strip off the /documents/ prefix from each document UUID:
    document_uuids = [x.strip("/").split("/")[-1] for x in documents_json]
    if document_uuids:
      document_uuids = encode_utils.utils.addToSet(entries=document_uuids,new=dcc_document_uuid)
    else:
      document_uuids.append(dcc_document_uuid)
    payload = {}
    payload["@id"] = encode_utils.utils.parse_profile_from_id_prop(rec_json["@id"])
    payload["documents"] = document_uuids
    self.patch(payload=payload,record_id=rec_id)
  
#When appending "?datastore=database" to the URL. As Esther stated: "_indexer to the end of the 
# URL to see the status of elastic search like 
# https://www.encodeproject.org/_indexer if it's indexing it will say the status is "indexing", 
# versus waiting" and the results property will indicate the last object that was indexed."

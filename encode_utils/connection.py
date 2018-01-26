# -*- coding: utf-8 -*-

###
#© 2018 The Board of Trustees of the Leland Stanford Junior University
#Nathaniel Watson
#nathankw@stanford.edu
###

import base64
import datetime
import json
import logging
import mimetypes
import os
import pdb
import re
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import subprocess
import sys
import time
import urllib
import urllib3

#inhouse libraries
import encode_utils as en
import encode_utils.utils


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class AwardPropertyMissing(Exception):
  """
  Raised when the 'award' property isn't set in the payload when doing a POST, and a default isn't
  set by the environment variable DCC_AWARD either. 
  """
  message = ("The property '{}' is missing from the payload and a default isn't set either. To"
             " store a default, set the DCC_AWARD environment variable.")
     

class LabPropertyMissing(Exception):
  """
  Raised when the 'lab' property isn't set in the payload when doing a POST, and a default isn't
  set by the environment variable DCC_AWARD either. 
  """
  message = ("The property '{}' is missing from the payload and a default isn't set either. To"
             " store a default, set the DCC_LAB environment variable.")


class ProfileNotSpecified(Exception):
  """
  Raised when the profile (object schema) to submit to isn't specifed in a payload.
  """
  pass


class RecordNotFound(Exception):
  """
  Raised when a record that should exist on the Portal can't be retrieved via a GET request.
  """
  pass


class RecordIdNotPresent(Exception):
  """
  Raised when a payload to submit to the Portal doesn't have any record identifier (either 
  a pre-existing ENCODE assigned identifier or an alias.
  """
  pass


class UnknownDccProfile(Exception):
  """
  Raised when the profile in question doesn't match any valid profile name present in 
  encode_utils.utils.PROFILE_NAMES.
  """
  pass


class Connection():
  """ENCODE Portal data submission and retrieval. 

  In order to authenticate with the DCC servers when making HTTP requests, you must have the 
  the environment variables DCC_API_KEY and DCC_SECRET_KEY set. Check with your DCC data wrangler
  if you haven't been assigned these keys. 

  Two log files will be opened in append mode in the calling directory, and named 
  ${dcc_mode}_posted.txt and ${dcc_mode}_error.txt.
  """
  REQUEST_HEADERS_JSON = {'content-type': 'application/json'}
  #: The timeout in seconds when making HTTP requests via the 'requests' module.
  TIMEOUT = 20
  
  DCC_PROD_MODE = "prod"
  DCC_DEV_MODE = "dev"
  DCC_MODES = {
    DCC_PROD_MODE: "https://www.encodeproject.org/",
    DCC_DEV_MODE: "https://test.encodedcc.org/"
    }

  #: Identifies the name of the key in the payload (dictionary) that stores a valid ENCODE-assigned
  #: identifier for a record, such as 'accession', 'uuid', 'md5sum', ... depending on the object 
  #: being submitted. 
  #: This is not a valid attribute of any ENCOCE object schema, and is only used in the patch()
  #: instance method when you need to designate the record to update and don't have an alias you 
  #: can specify in the 'aliases' attribute. 
  ENCODE_IDENTIFIER_KEY = "_enc_id"

  #: Identifies the name of the key in the payload (dictionary) that stores
  ENCODE_PROFILE_KEY = "_profile"

  def __init__(self,dcc_mode):
    if dcc_mode not in self.DCC_MODES:
      raise ValueError(
        "Invalid dcc_mode '{}' specified. Must be one of '{}'".format(
          dcc_mode,self.DCC_MODES.keys()))

    #: dcc_mode: The environment of the ENCODE Portal site ("prod" or "dev") to connect to. 
    self.dcc_mode = dcc_mode

    f_formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:\t%(message)s')
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

    #: The prod or dev DCC URL, determined by the value of the dcc_mode instance attribute.
    self.dcc_url = self._set_dcc_url()

    #: The API key to use when authenticating with the DCC servers. This is set automatically
    #: to the value of the DCC_API_KEY environment variable in the _set_dcc_url() private method. 
    self.api_key = self._set_api_keys()[0]
    #: The secret key to use when authenticating with the DCC servers. This is set automatically
    #: to the value of the DCC_SECRET_KEY environment variable in the _set_dcc_url() private method.
    self.secret_key = self._set_api_keys()[1]
    self.auth = (self.api_key,self.secret_key)

  def _set_dcc_url(self):
    return self.DCC_MODES[self.dcc_mode]

  def _set_api_keys(self):
    """
    Retrieves the API key and secret key based on the environment variables DCC_API_KEY and 
    DCC_SECRET_KEY.

    Args: 
        Returns: Tuple containing the (API Key, Secret Key)
    """
    api_key = os.environ["DCC_API_KEY"]
    secret_key = os.environ["DCC_SECRET_KEY"]
    return api_key,secret_key
    
  def _log_post(self,alias,dcc_id=None):
    """Uses self.post_logger to log the submitted object's alias and dcc_id. 
    """
    txt = alias
    if dcc_id:
      txt += " -> {dcc_id}".format(dcc_id=dcc_id)
    self.post_logger.info(txt)

  def get_aliases(self,dcc_id,strip_alias_prefix=False):
    """
    Given an ENCODE identifier for an object, performs a GET request and extracts the aliases.

    Args: 
        dcc_id: The ENCODE ID for a given object, i.e ENCSR999EHG.
        strip_alias_prefix: bool. True means to remove the alias prefix if all return aliases. 

    Returns:
        list.
    """
    record = self.get(ignore404=False,dcc_id=dcc_id)
    aliases = record["aliases"]
    for index in range(len(aliases)):
      alias = aliases[index]
      if strip_alias_prefix:
        aliases[index] =  encode_utils.utils.strip_alias_prefix(alias)
    return aliases

  def search_encode(self,search_args):
    """
    Searches the ENCODE Portal using the provided query parameters in dictionary format. The query 
    parameters will be first URL encoded. 

    Args:
        search_args - dict. of key and value query parameters. 

    Returns:
        list of search results. 

    Raises:
        requests.exceptions.HTTPError: If the status code is not in the set [200,404].

    Example
        Given we have the following dictionary *d* of key and value pairs::

            {"type": "experiment",
             "searchTerm": "ENCLB336TVW",
             "format": "json",
             "frame": "object",
             "datastore": "database"
            }
  
        We can call the function as::

            search_encode(search_args=d)
            
    """
    query = urllib.parse.urlencode(search_args)
    url = os.path.join(self.dcc_url,"search/?",query)
    self.logger.info("Searching DCC with query {url}.".format(url=url))
    response = requests.get(url,auth=self.auth,timeout=self.TIMEOUT,headers=self.REQUEST_HEADERS_JSON,verify=False)
    if response.status_code not in [requests.codes.OK,requests.codes.NOT_FOUND]:
      response.raise_for_status()
    return response.json()["@graph"] #the @graph object is a list


  def validate_profile_in_payload(self,payload):
    """
    Useful to call when doing a POST (and self.post() does call this). Ensures that the profile key
    identified by self.ENCODE_PROFILE_KEY exists in the passed-in payload and that the value is 
    a recognized ENCODE objece profile (schema).

    Args:
        payload: dict. The intended object data to POST.

    Returns:
        The name of the profile if all validations pass, otherwise.

    Raises:
        connection.ProfileNotSpecified: The key self.ENCODE_PROFILE_KEY is missing in the payload.
        connection.UnknownDccProfile: The profile isn't recognized.
    """

    #profile = encode_utls.utils.parse_profile_from_id_prop(payload)
    profile = payload.get(self.ENCODE_PROFILE_KEY)
    if not profile:
      raise ProfileNotSpecified(
        ("You need to specify the profile to submit to by using the '{}' key"
         " in the payload.").format(self.ENCODE_PROFILE_KEY))
    exists = encode_utils.utils.does_profile_exist(profile)
    if not exists:
      raise UnknownDccProfile(
          "Invalid profile '{}' specified in the payload's {} key.".format(profile,self.ENCODE_PROFILE_KEY))
    return profile

  def get_lookup_ids_from_payload(self,payload):
    """
    Given a payload to submit to the Portal, extracts the identifiers that can be used to lookup
    the record on the Portal, i.e. to see if the record already exists. Identifiers are extracted
    from the following fields:
    1) self.ENCODE_IDENTIFIER_KEY,
    2) aliases,
    3) md5sum (in the case of a file object)

    Args:
        payload: dict. The data to submit.

    Returns:
        list of possible lookup identifiers.
    """
    lookup_ids = []
    if self.ENCODE_IDENTIFIER_KEY in payload:
      lookup_ids.append(payload[self.ENCODE_IDENTIFIER_KEY])
    if "aliases" in payload:
      lookup_ids.extend(payload["aliases"])
    if "md5sum" in payload:
      #The case for file objects.
      lookup_ids.append(payload["md5sum"])

    lookup_ids = [x.strip() for x in lookup_ids]
    lookup_ids = [x for x in lookup_ids]
    if not lookup_ids:
        raise RecordIdNotPresent(
          ("The payload does not contain a recognized identifier for traceability. For example,"
           " you need to set the 'aliases' key, or specify an ENCODE assigned identifier in the"
           " non-schematic key {}.".format(self.ENCODE_IDENTIFIER_KEY)))
            
    return lookup_ids

  #def delete(self,rec_id):
  #  """Not supported at present by the DCC - Only wranglers and delete objects.
  #  """
  #  url = os.path.join(self.dcc_url,rec_id)
  #  self.logger.info(
  #    (">>>>>>DELETING {rec_id} From DCC with URL {url}").format(rec_id=rec_id,url=url))
  #  response = requests.delete(url,auth=self.auth,timeout=self.TIMEOUT,headers=self.REQUEST_HEADERS_JSON, verify=False)
  #  pdb.set_trace()
  #  if response.ok:
  #    return response.json()
  #  response.raise_for_status() 

  def get(self,rec_ids,ignore404=True,frame=None):
    """GET a record from the ENCODE Portal.

    Looks up a record in ENCODE and performs a GET request, returning the JSON serialization of 
    the object. You supply a list of identifiers for a specific record, such as the object ID, an
    alias, uuid, or accession. The ENCODE Portal will be searched for each identifier in turn 
    until one is either found or the list is exhaused.

    Args: 
        rec_ids: str. containing a single record identifier, or a list of identifiers for a 
            specific record.
        ignore404: bool. Only matters when none of the passed in record IDs were found on the 
            ENCODE Portal. In this case, If set to True, then an empty dict will be returned.
            If set to False, then an E
           

    Returns:
        dict. containing the JSON response. Will be an empty dict if no record was found 
          and ignore404=True.

    Raises:
        requests.exceptions.HTTPError: The status code is not okay (in the 200 range), and the 
            cause isn't due to a 404 (not found) status code when ignore404=True.
    """
    if isinstance(rec_ids,str):
      rec_ids = [rec_ids]
    status_codes = {} #key is return code, value is the record ID
    for r in rec_ids:
      if r.endswith("/"):
        r = r.rstrip("/")
      url = os.path.join(self.dcc_url,r,"?format=json&datastore=database")
      if frame:
        url += "&frame={frame}".format(frame=frame)
      self.logger.info(">>>>>>GETTING {rec_id} From DCC with URL {url}".format(
          rec_id=r,url=url))
      response = requests.get(url,auth=self.auth,timeout=self.TIMEOUT,headers=self.REQUEST_HEADERS_JSON, verify=False)
      if response.ok:
        return response.json()
      status_codes[response.status_code] = r

    if requests.codes.FORBIDDEN in status_codes:
      raise Exception(
        "Access to ENCODE entity {} is forbidden".format(status_codes[requests.codes.FORBIDDEN]))
    elif requests.codes.NOT_FOUND in status_codes:
      if ignore404:
        return {}
    #At this point in the code, the response is not okay.
    # Raise the error for last response we got:
    response.raise_for_status() 

  def print_format_dict(self,dico):
    """
    Formats a dictionary for printing to a stream.
    """
    #Could use pprint, but that looks too ugly with dicts due to all the extra spacing. 
    return json.dumps(dico,indent=2)

  def post(self,payload):
    """POST a record to the ENCODE Portal.

    Requires that you include in the payload the non-schematic key self.ENCODE_PROFILE_KEY to
    designate the name of the ENCODE object profile that you are submitting against.

    If the 'lab' property isn't present in the payload, then the default will be set to the value
    of the DCC_LAB environment variable. Similarly, if the 'award' property isn't present, then the
    default will be set to the value of the DCC_AWARD environment variable.

    Args:
        payload: dict. The data to submit.

    Returns: 
        The object's JSON sererialization from the DCC when the POST succeeds, or when the object
        already exists on the DCC. 

    Raises:
        AwardPropertyMissing: The 'award' property isn't present in the payload and there isn't a
          defualt set by the environment variable DCC_AWARD.
        LabPropertyMissing: The 'lab' property isn't present in the payload and there isn't a
          default set by the environment variable DCC_AWARD.
        requests.exceptions.HTTPError: The return status is not okay (not in the 200 range). 
    """
    self.logger.info("\nIN post().")
    #Make sure we have a payload that can be converted to valid JSON, and tuples become arrays, ...
    json.loads(json.dumps(payload)) 
    profile = self.validate_profile_in_payload(payload)
    payload.pop(self.ENCODE_PROFILE_KEY)
    url = os.path.join(self.dcc_url,profile)
    if profile not in encode_utils.AWARDLESS_PROFILES: #No lab prop for these profiles either.
      if en.AWARD_PROP_NAME not in payload:
        if not en.AWARD:
          raise AwardPropertyMissing
        payload.update(en.AWARD)
      if en.LAB_PROP_NAME not in payload:
        if not en.LAB:
          raise LabPropertyMissing
        payload.update(en.LAB)
    alias = payload["aliases"][0]
    self.logger.info(
        ("<<<<<<Attempting to POST {alias} To DCC with URL {url} and this"
         " payload:\n\n{payload}\n\n").format(alias=alias,url=url,payload=self.print_format_dict(payload)))

    response = requests.post(url,auth=self.auth,timeout=self.TIMEOUT,headers=self.REQUEST_HEADERS_JSON,
                             json=payload, verify=False)
    self.logger.debug("<<<<<<DCC POST RESPONSE: ")
    self.logger.debug(json.dumps(response.json(), indent=4, sort_keys=True))
    status_code = response.status_code
    if response.ok:
      response_dcc_accession = ""
      try:
        response_dcc_accession = response.json()["@graph"][0]["accession"]
      except KeyError:
        pass #some objects don't have an accession, i.e. replicates.
      self._log_post(alias=alias,dcc_id=response_dcc_accession)
      return response.json()
    elif status_code == requests.codes.CONFLICT:
      self.logger.error("Will not post {} because it already exists.".format(alias))
      rec_json = self.get(rec_ids=alias,ignore404=False)
      return rec_json
    else:
      message = "Failed to POST {alias}".format(alias=alias)
      self.logger.error(message)
      response.raise_for_status()

  def patch(self,payload,raise_403=True, extend_array_values=True):
    """PATCH a record on the ENCODE Portal.

    Args: 
        payload: dict. containing the attribute key and value pairs to patch. Must contain the key
            self.ENCODE_IDENTIFIER_KEY in order to indicate which record to PATCH.
        raise_403: bool. True means to raise a requests.exceptions.HTTPError if a 403 status
            (Forbidden) is returned. 
            If set to False and there still is a 403 return status, then the object you were 
            trying to PATCH will be fetched from the Portal in JSON format as this function's
            return value.
        extend_array_values: bool. Only affects keys with array values. True (default) means to 
            extend the corresponding value on the Portal with what's specified in the payload. 
            False means to replace the value on the Portal with what's in the payload. 
    Returns: 
        The PATCH response. 

    Raises: 
        KeyError: The payload doesn't have the key self.ENCODE_IDENTIFIER_KEY set AND there aren't 
            any aliases provided in the payload's 'aliases' key. 
        requests.exceptions.HTTPError: if the return status is not in the 200 range (excluding a 
            403 status if 'raise_403' is False.
    """
    #Make sure we have a payload that can be converted to valid JSON, and tuples become arrays, ...
    json.loads(json.dumps(payload)) 
    self.logger.info("\nIN patch()")
    encode_id = payload[self.ENCODE_IDENTIFIER_KEY]
    rec_json = self.get(rec_ids=lookup_ids,ignore404=True) 
        
    if extend_array_values:
      for key in payload:
        if isinstance(payload[key],list):
          val = payload[key]
          val.extend(rec_json.get(key,[]))
          #I use rec_json.get(key,[]) above because in a GET request, 
          # not all props are pulled back when they are empty.
          # For ex, in a file object, if the controlled_by prop isn't set, then 
          # it won't be in the response.
          payload[key] = list(set(val))

    url = os.path.join(self.dcc_url,encode_id)
    self.logger.info(
        ("<<<<<<Attempting to PATCH {encode_id} To DCC with URL"
         " {url} and this payload:\n\n{payload}\n\n").format(
             encode_id=encode_id,url=url,payload=self.print_format_dict(payload)))

    response = requests.patch(url,auth=self.auth,timeout=self.TIMEOUT,headers=self.REQUEST_HEADERS_JSON,
                              json=payload,verify=False)

    self.logger.debug("<<<<<<DCC PATCH RESPONSE: ")
    self.logger.debug(json.dumps(response.json(), indent=4, sort_keys=True))
    if response.ok:
      return response.json()
    elif response.status_code == requests.codes.FORBIDDEN:
      #Don't have permission to PATCH this object.
      if not raise_403:
        return rec_json 
    else:
      message = "Failed to PATCH {}".format(encode_id)
      self.logger.error(message)
      response.raise_for_status()


  def send(self,payload,error_if_not_found=False,extend_array_values=True,raise_403=True):
    """
    A wrapper over self.post() and self.patch() that determines which to call based on whether the
    record exists on the Portal or not. Especially useful when submitting a high-level object,
    such as an experiment which contains many dependent objects, in which case you could have a mix
    where some need to be POST'd and some PATCH'd. 

    Args:
        payload: dict. The data to submit.
        error_if_not_found: bool. If set to True, then a PATCH will be attempted and a 
            requests.exceptions.HTTPError will be raised if the record doesn't exist on the Portal.
        extend_array_values: bool. Only matters when doing a PATCH, and Only affects keys with
            array values. True (default) means to extend the corresponding value on the Portal
            with what's specified in the payload. False means to replace the value on the Portal 
            with what's in the payload. 
        raise_403: bool. Only matters when doing a PATCH. True means to raise an
            requests.exceptions.HTTPError if a 403 status (Forbidden) is returned. 
            If set to False and there still is a 403 return status, then the object you were 
            trying to PATCH will be fetched from the Portal in JSON format as this function's
            return value (as handled by self.patch()).

    Raises:
          requests.exceptions.HTTPError: You want to do a PATCH (indicated by setting 
              error_if_not_found=True) but the record isn't found.
    """
    #Check wither record already exists on the portal
    lookup_ids = self.get_lookup_ids_from_payload(payload)
    rec_json = self.get(rec_ids=lookup_ids,ignore404=not error_if_not_found) 

    if not rec_json:
      return self.post(payload=payload)
    else:
      #PATCH
      #Set self.ENCODE_IDENTIFIER_KEY, even if already set.
      encode_id = rec_json["@id"].split("/")[-1]
      payload[self.ENCODE_IDENTIFIER_KEY] = encode_id
      return self.patch(payload=payload,extend_array_values=extend_array_values,raise_403=raise_403)

  def get_fastqfile_replicate_hash(self,dcc_exp_id):
    """
    Given a DCC experiment ID, finds the original FASTQ files that were submitted and creates a 
    dictionary with keys being the biological_replicate_number. The value of each key is another 
    dictionary having the technical_replicate_number as the single key. The value of this is 
    another dictionary with keys being file read numbers, i.e. 1 for forward reads, 2 for reverse 
    reads.  The value for a given key of this most inner dictionary is the file JSON. 

    Args:
        dcc_exp_id - list of DCC file IDs or aliases 

    Returns:
        dict. 
    """
    exp_json = self.get(ignore404=False,rec_ids=dcc_exp_id)
    dcc_file_ids = exp_json["original_files"]
    dico = {}
    for i in dcc_file_ids:
      file_json = self.get(ignore404=False,rec_ids=i)
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


  def _set_aws_upload_creds_from_response(self,upload_credentials):
    """
    After posting the metadata for a file object to ENCODE, the response will contain the key 
    'upload_credentials'. This method parses the document pointed to by this key, constructing a 
    dictionary of keys that will be exported as environment variables that can be used by the aws 
    CL agent.  That is what self.post_file() does, indirectly. self.post_file() has an 
    argument 'aws_creds' that expects a value generated from this method.  This method is also 
    called from self.regenerate_aws_upload_creds(), which produces a JSON document also containing the
    key 'upload_credentials'. 

    Returns:
        dict.
    """
    if "@graph" in response:
      response = response["@graph"][0]
    creds = graph["upload_credentials"]
    aws_creds = {}
    aws_creds["AWS_ACCESS_KEY_ID"] = creds["access_key"]
    aws_creds["AWS_SECRET_ACCESS_KEY"] = creds["secret_key"]
    aws_creds["AWS_SECURITY_TOKEN"] = creds["session_token"]
    aws_creds["UPLOAD_URL"] = creds["upload_url"]
    return aws_creds
  
  def post_file_metadata(self,payload,patch):
    """
    This is only to be used for DCC "/file/" type objects, because for these we don't have a 
    Before attempting a POST, will check if the file exists by doing a get on 
    payload["aliases"][0].  If the GET request succeeds, nothing will be POST'd.

    Args:
        payload: The data to submit.
        patch: bool. True indicates to perform an HTTP PATCH operation rather than POST.
    """
    self.logger.info("\nIN post_file_metadata(), patch={patch}\n".format(patch=patch))  
    objectType = payload.pop("@id") #should be /file/
    filename = payload["submitted_file_name"]
    #alias = payload["aliases"][0]
    md5_alias = "md5:" + payload["md5sum"]
    alias = payload["aliases"][0]
    
    #Check if record exists already using actual file alias provided in the payload.
    # In addition, check on file's MD5 sum in case the former search doesn't return a hit, since 
    # if previously we only had part of the file by mistake (i.e incomplete downoad) then the 
    # uploaded file on DCC would have a different md5sum.
    exists_on_dcc = self.get(ignore404=True,rec_ids=[md5_alias,alias])
    if not patch and exists_on_dcc:
      self.logger.info(
          ("Will not POST metadata for {filename} with alias {alias}"
           " because it already exists as {encff}.").format(
               filename=filename,alias=alias,encff=exists_on_dcc["accession"]))
      return exists_on_dcc
      #The JSON response may contain the AWS credentials.

    if patch:
      if not exists_on_dcc:
        #then do POST
        payload["@id"] = objectType
        response = self.post_file_metadata(payload=payload,patch=False)
        return response
      httpMethod = "PATCH"
      url = os.path.join(self.dcc_url,alias)
      encff_id= exists_on_dcc["accession"]
      self.logger.info(
          ("<<<<<<Attempting to PATCH {filename} metadata with alias {alias} and ENCFF ID"
           " {encff_id} for replicate with URL {url} and this payload:"
           "\n{payload}").format(filename=filename,alias=alias,encff_id=encff_id,
                                 url=url,payload=self.print_format_dict(payload)))

      response = requests.patch(url,auth=self.auth,timeout=self.TIMEOUT,headers=self.REQUEST_HEADERS_JSON,
                                data=json.dumps(payload),verify=False)
    else:
      httpMethod = "POST"
      url = os.path.join(self.dcc_url,objectType)
      self.logger.debug(
          ("<<<<<<Attempting to POST file {filename} metadata for replicate to"
           " DCC with URL {url} and this payload:\n{payload}").format(
               filename=filename,url=url,payload=self.print_format_dict(payload)))
      response = requests.post(url,auth=self.auth,timeout=self.TIMEOUT,headers=self.REQUEST_HEADERS_JSON,
                               data=json.dumps(payload), verify=False)

    response_json = response.json()
    self.logger.debug(
        "<<<<<<DCC {httpMethod} RESPONSE: ".format(httpMethod=httpMethod))
    self.logger.debug(json.dumps(response_json, indent=4, sort_keys=True))
    if "code" in response_json and response_json["code"] == requests.codes.CONFLICT:
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
      self._log_post(alias=alias,dcc_id=response_dcc_accession)
    return response_json


  def regenerate_aws_upload_creds(self,encff_number):
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
      if code == requests.codes.FORBIDDEN:
        #Access was denied to this resource. File already uploaded fine.
        return
    graph = response["@graph"][0]
    aws_creds = self._set_aws_upload_creds_from_response(graph["upload_credentials"])
    return aws_creds

  def post_file(self,filepath,encff_number,aws_creds=None):
    """
    Args:
        filepath: The local path to the file to upload.
        upload_url: The AWS upload address (i.e. S3 bucket address).
        aws_creds: dict. with keys AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SECURITY_TOKEN.
    Returns:
    """
    self.logger.info("\nIN post_file()\n")
    if not aws_creds:
      aws_creds = self.regenerate_aws_upload_creds(encff_number=encff_number)
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
      if retcode == requests.codes.FORBIDDEN:
        #HTTPForbidden; now allowed to update.
        logger.info(("Will not upload file {filepath} to s3. Attempt failed with status code HTTP"
                    " 403 (Forbidden). Normally, this means we shouldn't be editing this object"
                    " and that all is fine.").format(filepath=filepath))
      else:
        raise Exception(
            ("Subprocess command {cmd} failed with returncode {retcode}. Stdout is {stdout}."
             " Stderr is {stderr}.").format(cmd=cmd,retcode=retcode,stdout=stdout,stderr=stderr))
      

  def get_platforms_on_experiment(self,rec_id):
    """
    Looks at all FASTQ files on the specified experiment, and tallies up the varying sequencing 
    platforms that generated them.  This is moreless used to verify that there aren't a mix of 
    multiple different platforms present as normally all reads should come from the same platform.

    Args:
        rec_id: str. DCC identifier for an experiment. 
    Returns:
        De-duplicated list of platforms seen on the experiment's FASTQ files. 
    """
    exp_json = self.get(rec_ids=rec_id,frame=None)
    if "@graph" in exp_json:
      exp_json = exp_json["@graph"][0]
    files_json = exp_json["original_files"]
    platforms = []
    for f in files_json:
      if not f["file_format"] == "fastq":
        continue
      platforms.extend(f["platform"]["aliases"])
    return list(set(platforms))
        
  def post_motif_enrichments_from_textfile(self,infile,patch=False):
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
      payload["aliases"] = [alias]
      payload["characterizes"] = encab
      payload["target"] = target + "-human"
      payload["status"] = "pending dcc review"
      payload["caption"] = caption

      motif_analysis_basename= os.path.basename(motif_analysis_file)
      motif_analysis_file_mime_type = str(mimetypes.guess_type(motif_analysis_basename)[0])
      contents = str(base64.b64encode(open(motif_analysis_file,"rb").read()),"utf-8")
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
      self._log_post(alias=alias,dcc_id=response["uuid"])

  def post_document(self,download_filename,document,document_type,document_description):
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
    
    ## Post information
    payload = {} 
    payload["@id"] = "documents/"
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
  
  
  def link_document(self,rec_id,dcc_document_uuid):
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
    rec_json = self.get(ignore404=False,rec_ids=rec_id)
    documents_json = rec_json["documents"]
    #Originally in form of [u'/documents/ba93f5cc-a470-41a2-842f-2cb3befbeb60/',
    #                       u'/documents/tg81g5aa-a580-01a2-842f-2cb5iegcea03, ...]
    #Strip off the /documents/ prefix from each document UUID:
    document_uuids = [x.strip("/").split("/")[-1] for x in documents_json]
    if document_uuids:
      document_uuids = encode_utils.utils.add_to_set(entries=document_uuids,new=dcc_document_uuid)
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

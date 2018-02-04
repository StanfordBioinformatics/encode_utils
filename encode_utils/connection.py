# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
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
import encode_utils as eu
import encode_utils.profiles as eup
import encode_utils.utils as euu


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#: A descendent logger of the debug logger created in `encode_utils`
#: (see the function description for `encode_utils._create_debug_logger`)
DEBUG_LOGGER = logging.getLogger(eu.DEBUG_LOGGER_NAME + "." + __name__)
#: A descendent logger of the error logger created in `encode_utils`
#: (see the function description for `encode_utils._create_error_logger`)
ERROR_LOGGER = logging.getLogger(eu.ERROR_LOGGER_NAME + "." + __name__)

class AwardPropertyMissing(Exception):
  """
  Raised when the 'award' property isn't set in the payload when doing a POST, and a default isn't
  set by the environment variable DCC_AWARD either.
  """
  message = ("The property '{}' is missing from the payload and a default isn't set either. To"
             " store a default, set the DCC_AWARD environment variable.")


class FileUploadFailed(Exception):
  """
  Raised when the AWS CLI returns a non-zero exit status.
  """


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


class RecordIdNotPresent(Exception):
  """
  Raised when a payload to submit to the Portal doesn't have any record identifier (either
  a pre-existing ENCODE assigned identifier or an alias.
  """
  pass


class RecordNotFound(Exception):
  """
  Raised when a record that should exist on the Portal can't be retrieved via a GET request.
  """
  pass


class Connection():
  """ENCODE Portal data submission and retrieval.

  In order to authenticate with the DCC servers when making HTTP requests, you must have the
  the environment variables DCC_API_KEY and DCC_SECRET_KEY set. Check with your DCC data wrangler
  if you haven't been assigned these keys.

  Two log files will be opened in append mode in the calling directory, and named
  DCC_MODE_posted.txt and DCC_MODE_error.txt, where DCC_MODE represents the value stored in
  eu.DCC_MODE.
  """

  #: Identifies the name of the key in the payload (dictionary) that stores a valid ENCODE-assigned
  #: identifier for a record, such as 'accession', 'uuid', 'md5sum', ... depending on the object
  #: being submitted.
  #: This is not a valid attribute of any ENCODE object schema, and is only used in the patch()
  #: instance method when you need to designate the record to update and don't have an alias you
  #: can specify in the 'aliases' attribute.
  ENCODE_IDENTIFIER_KEY = "_enc_id"

  #: Identifies the name of the key in the payload (dictionary) that stores the ID of the profile
  #: to submit to.
  PROFILE_KEY = "_profile"

  POST = "post"
  PATCH = "patch"

  def __init__(self):

    debug_logger = logging.getLogger(eu.DEBUG_LOGGER_NAME + "." + __name__)
    error_logger = logging.getLogger(eu.ERROR_LOGGER_NAME + "." + __name__)

    f_formatter = logging.Formatter('%(asctime)s:%(name)s:%(pathname)s:\t%(message)s')

    #: A logging instance for logging successful POST operations to a file by the name of
    #: DCC_MODE_posted, which is opened in append mode in the calling directory.
    #: Accepts messages >= logging.INFO.
    post_logger = logging.getLogger("post")
    post_logger.setLevel(logging.INFO)
    post_logger_fh = logging.FileHandler(filename=eu.DCC_MODE + "_" + "posted.txt",mode="a")
    post_logger_fh.setLevel(logging.INFO)
    post_logger_fh.setFormatter(f_formatter)
    post_logger.addHandler(post_logger_fh)

    DEBUG_LOGGER = debug_logger
    ERROR_LOGGER = error_logger
    self.post_logger = post_logger


    #: The API key to use when authenticating with the DCC servers. This is set automatically
    #: to the value of the DCC_API_KEY environment variable in the _set_api_keys() private method.
    self.api_key = self._set_api_keys()[0]
    #: The secret key to use when authenticating with the DCC servers. This is set automatically
    #: to the value of the DCC_SECRET_KEY environment variable in the _set_api_keys() private method.
    self.secret_key = self._set_api_keys()[1]
    self.auth = (self.api_key,self.secret_key)

  def _set_api_keys(self):
    """
    Retrieves the API key and secret key based on the environment variables DCC_API_KEY and
    DCC_SECRET_KEY.

    Returns:
        `tuple`: Two item tuple containing the API Key and the Secret Key
    """
    api_key = os.environ["DCC_API_KEY"]
    secret_key = os.environ["DCC_SECRET_KEY"]
    return api_key,secret_key

  def _log_post(self,alias,dcc_id):
    """Uses the self.post_logger to log the submitted object's alias and dcc_id.

    Each message is written in a two column format delimted by a tab character. The columns are:
      1) alias (the first that appeared in the 'aliases' key in the payload), and
      2) DCC identifier
    """
    entry = alias + "\t" + dcc_id
    self.post_logger.info(entry)

  def get_aliases(self,dcc_id,strip_alias_prefix=False):
    """
    Given an ENCODE identifier for an object, performs a GET request and extracts the aliases.

    Args:
        dcc_id: `str`. The ENCODE ID for a given object, i.e ENCSR999EHG.
        strip_alias_prefix: `bool`. True means to remove the alias prefix if all return aliases.

    Returns:
        `list`: The aliases.
    """
    record = self.get(ignore404=False,dcc_id=dcc_id)
    aliases = record["aliases"]
    for index in range(len(aliases)):
      alias = aliases[index]
      if strip_alias_prefix:
        aliases[index] =  euu.strip_alias_prefix(alias)
    return aliases

  def search_encode(self,search_args):
    """
    Searches the ENCODE Portal using the provided query parameters in dictionary format. The query
    parameters will be first URL encoded.

    Args:
        search_args: `dict`. The key and value query parameters.

    Returns:
        `list`: The search results.

    Raises:
        requests.exceptions.HTTPError: If the status code is not in the set [200,404].

    **Example**:
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
    url = os.path.join(eu.DCC_URL,"search/?",query)
    DEBUG_LOGGER.debug("Searching DCC with query {url}.".format(url=url))
    response = requests.get(url,
                            auth=self.auth,
                            timeout=en.TIMEOUT,
                            headers=euu.REQUEST_HEADERS_JSON,
                            verify=False)
    if response.status_code not in [requests.codes.OK,requests.codes.NOT_FOUND]:
      response.raise_for_status()
    return response.json()["@graph"] #the @graph object is a list


  def validate_profile_in_payload(self,payload):
    """
    Useful to call when doing a POST (and self.post() does call this). Ensures that the profile key
    identified by self.PROFILE_KEY exists in the passed-in payload and that the value is
    a recognized ENCODE object profile (schema).

    Args:
        payload: `dict`. The intended object data to POST.

    Returns:
        `str`: The ID of the profile if all validations pass, otherwise.

    Raises:
        encode_utils.connection.ProfileNotSpecified: The key self.PROFILE_KEY is missing in the payload.
        encode_utils.profiles.UnknownProfile: The profile ID isn't recognized by the class
            `encode_utils.profiles.Profile`.
    """

    profile_id = payload.get(self.PROFILE_KEY)
    if not profile_id:
      raise ProfileNotSpecified(
        ("You need to specify the ID of the profile to submit to by using the '{}' key"
         " in the payload.").format(self.PROFILE_KEY))
    profile = eup.Profile(profile_id) #raises euu.UnknownProfile if unknown profile ID.
    return profile.profile_id

  def get_lookup_ids_from_payload(self,payload):
    """
    Given a payload to submit to the Portal, extracts the identifiers that can be used to lookup
    the record on the Portal, i.e. to see if the record already exists. Identifiers are extracted
    from the following fields:
    1) self.ENCODE_IDENTIFIER_KEY,
    2) aliases,
    3) md5sum (in the case of a file object)

    Args:
        payload: `dict`. The data to submit.

    Returns:
        `list`: The possible lookup identifiers.
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
  #  url = os.path.join(eu.DCC_URL,rec_id)
  #  self.logger.info(
  #    (">>>>>>DELETING {rec_id} From DCC with URL {url}").format(rec_id=rec_id,url=url))
  #  response = requests.delete(url,auth=self.auth,timeout=en.TIMEOUT,headers=euu.REQUEST_HEADERS_JSON, verify=False)
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
        rec_ids: `str` containing a single record identifier, or a list of identifiers for a
            specific record. For a few example identifiers, you can be a uuid, accession, ...,  
            or even the value of a record's `@id` property.
        ignore404: `bool`. Only matters when none of the passed in record IDs were found on the
            ENCODE Portal. In this case, If set to True, then an empty dict will be returned.
            If set to False, then an E


    Returns:
        `dict`: The JSON response. Will be empty if no record was found AND ignore404=True.

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
      url = os.path.join(eu.DCC_URL,r,"?format=json&datastore=database")
      if frame:
        url += "&frame={frame}".format(frame=frame)
      DEBUG_LOGGER.debug(">>>>>>GETTING {rec_id} From DCC with URL {url}".format(
          rec_id=r,url=url))
      response = requests.get(url,
                              auth=self.auth,
                              timeout=eu.TIMEOUT,
                              headers=euu.REQUEST_HEADERS_JSON,
                              verify=False)
      if response.ok:
        return response.json()
      status_codes[response.status_code] = r

    if requests.codes.FORBIDDEN in status_codes:
      raise Exception(
        "Access to ENCODE record {} is forbidden".format(status_codes[requests.codes.FORBIDDEN]))
    elif requests.codes.NOT_FOUND in status_codes:
      if ignore404:
        return {}
    #At this point in the code, the response is not okay.
    # Raise the error for last response we got:
    response.raise_for_status()

  def set_attachment(self,document):
    """
    Sets the attachment property for any profile that supports it, such as document or 
    antibody_characterization.

    Args:
        document: `str`. A local file path.

    Returns:
        `dict`. The attachment propery value.
    """
    download_filename = os.path.basename(document)
    mime_type = mimetypes.guess_type(download_filename)[0]
    data = base64.b64encode(open(document,'rb').read())
    temp_uri = str(data,"utf-8")
    href = "data:{mime_type};base64,{temp_uri}".format(mime_type=mime_type,temp_uri=temp_uri)
    #download_filename = library_alias.split(":")[1] + "_relative_knockdown.jpeg"
    attachment = {}
    attachment["download"] = download_filename
    attachment["type"] = mime_type
    attachment["href"] = href
    return attachment

  def after_submit_file_cloud_upload(self,rec_id,profile_id):
    """An after-POST submit hook for uploading files to AWS.

    Some objects, such as Files (file.json profile) need to have a corresponding file in the cloud.
    Where in the cloud the actual file should be uploaded to is indicated in File object's
    file.upload_credentials.upload_url property. Once the File object is posted, this hook can be
    used to perform the actual cloud upload of the physical, local file reprented by the File object.

    Args:
        rec_id: `str`. An identifier for the new File object on the Portal.
        profile_id: `str`. The ID of the profile that the record belongs to.
    """
    if profile_id != eup.Profile.FILE_PROFILE_ID:
      return
    rec = self.get(rec_ids=rec_id,ignore404=False)
    if eup.Profile.SUBMITTED_FILE_PROP_NAME in rec:
      filename = rec[eup.Profile.SUBMITTED_FILE_PROP_NAME]
      if filename:
        self.upload_file(file_id=rec_id,file_path=filename)

  def after_submit_hooks(self,rec_id,profile_id,method=""):
    """
    Calls after-submission hooks for POST and PATH operations.

    Args:
        rec_id: `str`. An identifier for a record on the Portal.
        profile_id: `str`. The profile the record belongs to.
        method: str. One of self.POST or self.PATCH, or the empty string to indicate which 
            registered hooks to look through.
    """
    #Check allowed_methods. Will matter later when there are POST-specific
    # and PATCH-specific hooks.
    allowed_methods = [self.POST,self.PATCH,""]
    if not method in allowed_methods:
      raise Exception("Unknown method '{}': must be one of {}.".format(method,allowed_methods))

    #Call agnostic hooks
    #... None yet.

    #Call POST-specific hooks if POST:
    if method == self.POST:
      self.after_submit_file_cloud_upload(rec_id,profile_id)

    #Call PATCH-specific hooks if PATCH:
    #... None yet.

  def before_submit_attachment(self,payload):
    """
    A POST and PATCH pre-submit hook used to simplify the creation of an attachment in profiles 
    that support it.

    Checks the payload for the presence of the 'attachment' property that is used by certain 
    profiles, i.e. document and antibody_characterization, and then checks to see if a particular
    shortcut is being employed to indicate the attachment. That shortcut works as follows: If the 
    dictionary value of the 'attachment' key has a key named 'path' in it (case-sensitive), then 
    the value is taken to be the path to a local file. Then, the actual attachment object is 
    constructed, as defined in the document profile, by calling self.set_attachment(). Note that 
    this shortcut is particular to this `Connection` class, and when used the 'path' key should be 
    the only key in the attachment dictionary as any others would be ignored.

    Args:
        payload: `dict`. The payload to submit to the Portal.

    Returns:
        `dict`: The payload to submit to the Portal.
    """
    attachment_prop = "attachment"
    path = "path"

    if attachment_prop in payload:
      val = payload[attachment_prop] #dict
      if path in val:
        #Then set the actual attachment object:
        attachment = self.set_attachment(document=val[path])
        payload[attachment_prop] = attachment
    return payload

  def before_post_file(self,payload):
    """Calculates and sets the md5sum property for a file record.    

    Args:
        payload: `dict`. The payload to submit to the Portal.

    Returns:
        `dict`: The payload to submit to the Portal.

    Raises:
        encode_utils.utils.MD5SumError: Perculated through the function 
          `encode_utils.utils.calculate_md5sum` when it can't calculate the md5sum.
    """
    profile_id = payload[self.PROFILE_KEY]
    if profile_id != eup.Profile.FILE_PROFILE_ID:
      return payload
    try:
      file_name = payload[eup.Profile.SUBMITTED_FILE_PROP_NAME]
    except KeyError:
      return payload
    if eup.Profile.MD5SUM_NAME_PROP_NAME in payload:
      if payload[eup.Profile.MD5SUM_NAME_PROP_NAME]:
        #Already set; nothing to do.
        return payload
    md5sum = euu.calculate_md5sum(file_name)
    payload["md5sum"] = md5sum
    return payload


  def before_submit_hooks(self,payload,method=""):
    """Calls before-submission hooks for POST and PATCH operations.

    Some hooks only run if you are doing a PATCH, others if you are only doing a POST. Then there
    are some that run if you are doing either operation. Each pre-submission hook that is called
    can potentially modify the payload.

    Both self.post() and self.patch() call this method.

    Args:
        payload: `dict`. The payload to POST or PATCH.
        method: `str`. One of "post" or "patch", or the empty string to indicate which registered
            hooks to look through.

    Returns:
        `dict`: The potentially modified payload that has been passed through all applicable
            pre-submit hooks.
    """
    #Check allowed_methods. Will matter later when there are POST-specific
    # and PATCH-specific hooks.
    allowed_methods = [self.POST,self.PATCH,""]
    if not method in allowed_methods:
      raise Exception("Unknown method '{}': must be one of {}.".format(method,allowed_methods))

    #Call agnostic hooks
    payload = self.before_submit_attachment(payload)

    #Call POST-specific hooks if POST:
    if method == self.POST:
      payload = self.before_post_file(payload)

    #Call PATCH-specific hooks if PATCH:
    #... None yet.

    return payload


  def post(self,payload):
    """POST a record to the ENCODE Portal.

    Requires that you include in the payload the non-schematic key self.PROFILE_KEY to
    designate the name of the ENCODE object profile that you are submitting against.

    If the 'lab' property isn't present in the payload, then the default will be set to the value
    of the DCC_LAB environment variable. Similarly, if the 'award' property isn't present, then the
    default will be set to the value of the DCC_AWARD environment variable.

    Before the POST is attempted, any pre-submit hooks are fist called (see the method
    `self.before_submit_hooks`).

    Args:
        payload: `dict`. The data to submit.

    Returns:
        `dict`: The JSON response from the POST operation, or GET operation If the resource already
          exist on the Portal.

    Raises:
        AwardPropertyMissing: The 'award' property isn't present in the payload and there isn't a
            defualt set by the environment variable DCC_AWARD.
        LabPropertyMissing: The 'lab' property isn't present in the payload and there isn't a
            default set by the environment variable DCC_AWARD.
        requests.exceptions.HTTPError: The return status is not okay (not in the 200 range), with
            the exception of a conflict (409), which is only logged.
    """
    DEBUG_LOGGER.debug("\nIN post().")
    #Make sure we have a payload that can be converted to valid JSON, and tuples become arrays, ...
    json.loads(json.dumps(payload))
    profile_id = self.validate_profile_in_payload(payload)
    url = os.path.join(eu.DCC_URL,profile_id)
    #Check if we need to add defaults for 'award' and 'lab' properties:
    if profile_id not in eup.Profile.AWARDLESS_PROFILE_IDS: #No lab prop for these profiles either.
      if eu.AWARD_PROP_NAME not in payload:
        if not eu.AWARD:
          raise AwardPropertyMissing
        payload.update(eu.AWARD)
      if eu.LAB_PROP_NAME not in payload:
        if not eu.LAB:
          raise LabPropertyMissing
        payload.update(eu.LAB)
    alias = payload["aliases"][0]

    #Run 'before' hooks:
    payload = self.before_submit_hooks(payload,method=self.POST)
    payload.pop(self.PROFILE_KEY)

    DEBUG_LOGGER.debug(
        ("<<<<<< POSTING {alias} To DCC with URL {url} and this"
         " payload:\n\n{payload}\n\n").format(alias=alias,url=url,payload=euu.print_format_dict(payload)))

    response = requests.post(url,
                             auth=self.auth,
                             timeout=eu.TIMEOUT,
                             headers=euu.REQUEST_HEADERS_JSON,
                             json=payload, verify=False)
    #response_json = response.json()["@graph"][0]
    response_json = response.json()

    if response.ok:
      DEBUG_LOGGER.debug("Success.")
      response_json = response_json["@graph"][0]
      encid = ""
      try:
        encid = response_json["accession"]
      except KeyError:
        #Some objects don't have an accession, i.e. replicates.
        encid = response_json["uuid"]
      self._log_post(alias=alias,dcc_id=encid)
      #Run 'after' hooks:
      self.after_submit_hooks(encid,profile_id,method=self.POST)
      return response_json
    elif response.status_code == requests.codes.CONFLICT:
      log_msg = "Will not post {} because it already exists.".format(alias)
      DEBUG_LOGGER.debug(log_msg)
      ERROR_LOGGER.error(log_msg)
      rec_json = self.get(rec_ids=alias,ignore404=False)
      return rec_json
    else:
      message = "Failed to POST {alias}".format(alias=alias)
      DEBUG_LOGGER.debug(message)
      ERROR_LOGGER.error(message)
      DEBUG_LOGGER.debug("<<<<<< DCC POST RESPONSE: ")
      DEBUG_LOGGER.debug(euu.print_format_dict(response_json))
      response.raise_for_status()

  def patch(self,payload,raise_403=True, extend_array_values=True):
    """PATCH a record on the ENCODE Portal.

    Before the PATCH is attempted, any pre-submit hooks are fist called (see the method
    `self.before_submit_hooks`).

    Args:
        payload: `dict`. containing the attribute key and value pairs to patch. Must contain the key
            self.ENCODE_IDENTIFIER_KEY in order to indicate which record to PATCH.
        raise_403: `bool`. True means to raise a requests.exceptions.HTTPError if a 403 status
            (Forbidden) is returned.
            If set to False and there still is a 403 return status, then the object you were
            trying to PATCH will be fetched from the Portal in JSON format as this function's
            return value.
        extend_array_values: `bool`. Only affects keys with array values. True (default) means to
            extend the corresponding value on the Portal with what's specified in the payload.
            False means to replace the value on the Portal with what's in the payload.
    Returns:
        `dict`: The JSON response from the PATCH operation.

    Raises:
        KeyError: The payload doesn't have the key self.ENCODE_IDENTIFIER_KEY set AND there aren't
            any aliases provided in the payload's 'aliases' key.
        requests.exceptions.HTTPError: if the return status is not in the 200 range (excluding a
            403 status if 'raise_403' is False.
    """
    #Make sure we have a payload that can be converted to valid JSON, and tuples become arrays, ...
    json.loads(json.dumps(payload))
    DEBUG_LOGGER.debug("\nIN patch()")
    encode_id = payload[self.ENCODE_IDENTIFIER_KEY]
    rec_json = self.get(rec_ids=encode_id,ignore404=False)

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

    #Run 'before' hooks:
    payload = self.before_submit_hooks(payload,method=self.PATCH)

    url = os.path.join(eu.DCC_URL,encode_id)
    DEBUG_LOGGER.debug(
        ("<<<<<< PATCHING {encode_id} To DCC with URL"
         " {url} and this payload:\n\n{payload}\n\n").format(
             encode_id=encode_id,url=url,payload=euu.print_format_dict(payload)))

    response = requests.patch(url,auth=self.auth,timeout=eu.TIMEOUT,headers=euu.REQUEST_HEADERS_JSON,
                              json=payload,verify=False)
    response_json = response.json()

    if response.ok:
      DEBUG_LOGGER.debug("Success.")
      uuid = response_json["uuid"]
      profile = eup.Profile(response_json["@id"])
      #Run 'after' hooks:
      self.after_submit_hooks(uuid,profile.profile_id,method=self.PATCH)
      return response_json
    elif response.status_code == requests.codes.FORBIDDEN:
      #Don't have permission to PATCH this object.
      if not raise_403:
        return rec_json

    message = "Failed to PATCH {}".format(encode_id)
    DEBUG_LOGGER.debug(message)
    ERROR_LOGGER.error(message)
    DEBUG_LOGGER.debug("<<<<<< DCC PATCH RESPONSE: ")
    DEBUG_LOGGER.debug(euu.print_format_dict(response_json))
    response.raise_for_status()


  def send(self,payload,error_if_not_found=False,extend_array_values=True,raise_403=True):
    """
    A wrapper over self.post() and self.patch() that determines which to call based on whether the
    record exists on the Portal or not. Especially useful when submitting a high-level object,
    such as an experiment which contains many dependent objects, in which case you could have a mix
    where some need to be POST'd and some PATCH'd.

    Args:
        payload: `dict`. The data to submit.
        error_if_not_found: `bool`. If set to True, then a PATCH will be attempted and a
            requests.exceptions.HTTPError will be raised if the record doesn't exist on the Portal.
        extend_array_values: `bool`. Only matters when doing a PATCH, and Only affects keys with
            array values. True (default) means to extend the corresponding value on the Portal
            with what's specified in the payload. False means to replace the value on the Portal
            with what's in the payload.
        raise_403: `bool`. Only matters when doing a PATCH. True means to raise an
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
      if self.ENCODE_IDENTIFIER_KEY not in payload:
        encode_id = aliases[0]
        payload[self.ENCODE_IDENTIFIER_KEY] = encode_id
      return self.patch(payload=payload,extend_array_values=extend_array_values,raise_403=raise_403)

  def get_fastqfile_replicate_hash(self,dcc_exp_id):
    """
    Given a DCC experiment ID, looks in the 'original' property to find FASTQ file objects and
    creates a dict organized by replicate numbers. Keying through the dict by replicate numbers,
    you can get to a particular file object's JSON serialization.

    Args:
        dcc_exp_id: `list` of DCC file IDs or aliases
    Returns:
        `dict`: `dict` where each key is a biological_replicate_number.
            The value of each key is another dict where each key is a technical_replicate_number.
            The value of this is yet another dict with keys being file read numbers -
            1 for forward reads, 2 for reverse reads.  The value
            for a given key of this most inner dictionary is a list of file objects.
    """
    exp_json = self.get(ignore404=False,rec_ids=dcc_exp_id)
    dcc_file_ids = exp_json["original_files"]
    dico = {}
    for i in dcc_file_ids:
      file_json = self.get(ignore404=False,rec_ids=i)
      if file_json["file_type"] != "fastq":
        continue #this is not a file object for a FASTQ file.
      brn = file_json["replicate"]["biological_replicate_number"]
      trn = file_json["replicate"]["technical_replicate_number"]
      read_num = file_json["paired_end"] #string
      if brn not in dico:
        dico[brn] = {}
      if trn not in dico[brn]:
        dico[brn][trn] = {}
      if read_num not in dico[brn][trn]:
        dico[brn][trn][read_num] = []
      dico[brn][trn][read_num].append(file_json)
    return dico

  def extract_aws_upload_credentials(self,file_json):
    """
    Sets values for the AWS environment variables to the credentials found in a file record's 
    `upload_credentials` property.

    Args:
        file_json: `dict`: A file record's JSON serialization.
 
    Returns:
        `dict`: `dict` containing keys named after AWS environment variables being:
             1. AWS_ACCESS_KEY_ID,
             2. AWS_SECRET_ACCESS_KEY,
             3. AWS_SECURITY_TOKEN,
             4. UPLOAD_URL
           Will be empty if the `upload_credentials` property isn't present in `file_json`.
           
    """
    try:
      creds = file_json["upload_credentials"]
    except KeyError:
      return {}
    aws_creds = {}
    aws_creds["AWS_ACCESS_KEY_ID"] = creds["access_key"]
    aws_creds["AWS_SECRET_ACCESS_KEY"] = creds["secret_key"]
    aws_creds["AWS_SECURITY_TOKEN"] = creds["session_token"]
    aws_creds["UPLOAD_URL"] = creds["upload_url"]
    return aws_creds

  def set_aws_upload_config(self,file_id):
    """
    Sets the AWS security credentials needed to upload a file to AWS S3 by the
    AWS CLI agent. First will attempt to extract the upload credentials
    from the file record if the property `upload_credentials` is set. If not set, then an attempt
    to regenerate the upload credentials will be made. 

    Args:
        file_id: `str`. A file object identifier (i.e. accession, uuid, alias, md5sum).

    Returns:
        `dict`: See documentation for the return value for self.extract_aws_upload_credentials().
    """
    file_json = self.get(file_id,ignore404=False)
    creds = self.set_aws_upload_config(file_json)
    if not creds:
      creds = self.regenerate_aws_upload_creds(file_id)
      #Will be None if forbidden.

    if not creds:
      return {}

    #URL example from dev Portal:
    #  s3://encoded-files-dev/2018/01/28/7c5c6d58-c98a-48b4-9d4b-3296b4126b89/TSTFF334203.fastq.gz"
    #  That's the uuid after the date.
    return creds

  def post_file_metadata(self,payload,patch):
    """
    This is only to be used for DCC "/file/" type objects, because for these we don't have a
    Before attempting a POST, will check if the file exists by doing a get on
    payload["aliases"][0].  If the GET request succeeds, nothing will be POST'd.

    Args:
        payload: `dict`. The data to submit.
        patch: `bool`. True indicates to perform an HTTP PATCH operation rather than POST.
    """
    DEBUG_LOGGER.debug("\nIN post_file_metadata(), patch={patch}\n".format(patch=patch))
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
      DEBUG_LOGGER.debug(
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
      url = os.path.join(eu.DCC_URL,alias)
      encff_id= exists_on_dcc["accession"]
      DEBUG_LOGGER.debug(
          ("<<<<<<Attempting to PATCH {filename} metadata with alias {alias} and ENCFF ID"
           " {encff_id} for replicate with URL {url} and this payload:"
           "\n{payload}").format(filename=filename,alias=alias,encff_id=encff_id,
                                 url=url,payload=euu.print_format_dict(payload)))

      response = requests.patch(url,
                                auth=self.auth,
                                timeout=eu.TIMEOUT,
                                headers=euu.REQUEST_HEADERS_JSON,
                                data=json.dumps(payload),verify=False)
    else:
      httpMethod = "POST"
      url = os.path.join(eu.DCC_URL,objectType)
      DEBUG_LOGGER.debug(
          ("<<<<<<Attempting to POST file {filename} metadata for replicate to"
           " DCC with URL {url} and this payload:\n{payload}").format(
               filename=filename,url=url,payload=euu.print_format_dict(payload)))
      response = requests.post(url,
                               auth=self.auth,
                               timeout=eu.TIMEOUT,
                               headers=euu.REQUEST_HEADERS_JSON,
                               data=json.dumps(payload),
                               verify=False)

    response_json = response.json()
    DEBUG_LOGGER.debug(
        "<<<<<<DCC {httpMethod} RESPONSE: ".format(httpMethod=httpMethod))
    DEBUG_LOGGER.debug(euu.print_format_dict(response_json))
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


  def regenerate_aws_upload_creds(self,file_id):
    """Reissues AWS S3 upload credentials for the specified file object.

    Args:
        file_id: `str`. An identifier for a file object on the Portal.

    Returns:
        `dict`: `dict` containing the value of the 'upload_credentials' key in the JSON serialization
            of the file object represented by file_id. Will be empty if new upload credentials
            could not be issued.
    """
    DEBUG_LOGGER.debug("Using curl to generate new file upload credentials")
    cmd = ("curl -X POST -H 'Accept: application/json' -H 'Content-Type: application/json'"
           " https://{api_key}:{secret_key}@{host}/files/{file_id}/upload -d '{{}}'"
           " | python -m json.tool").format(api_key=self.api_key,secret_key=self.secret_key,host=eu.DCC_HOST,file_id=file_id)
    DEBUG_LOGGER.debug("curl command: '{}'".format(cmd))
    popen = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = popen.communicate() #each is a bytes object.
    stdout = stdout.decode("utf-8")
    stderr = stderr.decode("utf-8")
    retcode = popen.returncode
    if retcode:
      raise Exception(("Command {cmd} failed with return code {retcode}. stdout is {stdout} and"
                       " stderr is {stderr}.").format(cmd=cmd,retcode=retcode,stdout=stdout, stderr=stderr))
    response = json.loads(stdout)
    DEBUG_LOGGER.debug(response)
    if "code" in response:
      #Then problem occurred.
      code = response["code"]
      ERROR_LOGGER.info("Unable to reissue upload credentials for {}: Code {}.".format(file_id,code))
      return {}

      # For ex, response would look like this for a 404.

      # {
      #     "@type": [
      #         "HTTPNotFound",
      #         "Error"
      #     ],
      #     "code": 404,
      #     "description": "The resource could not be found.",
      #     "detail": "/files/michael-snyder:test_file_1/upload",
      #     "status": "error",
      #     "title": "Not Found"
      # }

      # You get a 403 when the 'status' of the file object isn't set to 'uploading'.
      # You also get this when the file object no-longer has read access (was archived by wranglers).

    graph = response["@graph"][0]
    return response["@graph"][0]["upload_credentials"]

  def upload_file(self,file_id,file_path=None):
    """Uses AWS CLI to upload a local file or S3 object to the Portal for the indicated file object.

    Unfortunately, it doesn't appear that pulling a file into S3 is supported through the AWS API;
    only existing S3 objects or local files can be copied to a S3 bucket. External files must first
    be downloaded and then pushed to the S3 bucket.

    Args:
        file_id: 'str'. An identifier of a `file` record.
        file_path: `str`. the local path to the file to upload, or an S3 object (i.e s3://mybucket/test.txt).
          If not set, defaults to None in which case the local file path will be extracted from the
          record's `submitted_file_name` property.

    Raises:
        FileUploadFailed: The return code of the AWS upload command was non-zero.
    """
    DEBUG_LOGGER.debug("\nIN upload_file()\n")
    aws_creds = self.set_aws_upload_config(file_id)
    if not aws_creds:
      msg = "Cannot upload file for {} since upload credentials could not be generated.".format(file_id)
      DEBUG_LOGGER.debug(msg)
      ERROR_LOGGER.error(msg)
      return
    if not file_path:
      file_rec = self.get(rec_ids=file_id)
    file_path = file_rec[eup.Profile.SUBMITTED_FILE_PROP_NAME]
    cmd = "aws s3 cp {file_path} {upload_url}".format(file_path=file_path,upload_url=aws_creds["UPLOAD_URL"])
    DEBUG_LOGGER.debug("Running command {cmd}.".format(cmd=cmd))
    popen = subprocess.Popen(cmd,
                             shell=True,
                             env=os.environ.update(aws_creds),
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    stdout,stderr = popen.communicate()
    stdout = stdout.decode("utf-8")
    stderr = stderr.decode("utf-8")
    retcode = popen.returncode
    if retcode:
      error_msg = "Failed to upload file '{}' for {}.".format(file_path,file_id)
      DEBUG_LOGGER.debug(error_msg)
      ERROR_LOGGER.error(error_msg)
      error_msg += (" Subprocess command '{cmd}' failed with return code '{retcode}'."
                    " Stdout is '{stdout}'.  Stderr is '{stderr}'.").format(
                      cmd=cmd,retcode=retcode,stdout=stdout,stderr=stderr)
      DEBUG_LOGGER.debug(error_msg)
      raise FileUploadFailed(error_msg)
    DEBUG_LOGGER.debug("AWS upload successful.")


  def get_platforms_on_experiment(self,rec_id):
    """
    Looks at all FASTQ files on the specified experiment, and tallies up the varying sequencing
    platforms that generated them.  This is moreless used to verify that there aren't a mix of
    multiple different platforms present as normally all reads should come from the same platform.

    Args:
        rec_id: `str`. DCC identifier for an experiment.
    Returns:
        `list`: The de-duplicated list of platforms seen on the experiment's FASTQ files.
    """
    exp_json = self.get(rec_ids=rec_id,frame=None)
    files_json = exp_json["original_files"]
    platforms = []
    for f in files_json:
      if not f["file_format"] == "fastq":
        continue
      platforms.extend(f["platform"]["aliases"])
    return list(set(platforms))

  def post_document(self,download_filename,document,document_type,description):
    """
    The alias for the document will be the lab prefix plus the file name (minus the file extension).

    Args:
        download_filename: `str`. The name to give the document when downloading it from the ENCODE
            portal.
        document_type: `str`. For possible values, see
            https://www.encodeproject.org/profiles/document.json. It appears that one should use
            "data QA" for analysis results documents.
        description: `str`. The description for the document.
        document: `str`. Local file path to the document to be submitted.

    Returns:
        `str`: The DCC UUID of the new document.
    """
    document_filename = os.path.basename(document)
    document_alias = eu.LAB + os.path.splitext(document_filename)[0]
    mime_type = mimetypes.guess_type(document_filename)[0]
    if not mime_type:
      raise Exception("Couldn't guess MIME type for {}.".format(document_filename))

    ## Post information
    payload = {}
    payload["@id"] = "documents/"
    payload["aliases"] = [document_alias]
    payload["document_type"] = document_type
    payload["description"] = document_description

    #download_filename = library_alias.split(":")[1] + "_relative_knockdown.jpeg"
    attachment = self.set_attachment(document)

    payload['attachment'] = attachment

    response = self.post(payload=payload)
    return response['uuid']

  def link_document(self,rec_id,dcc_document_uuid):
    """
    Links an existing document on the ENCODE Portal to another existing object on the Portal via
    the latter's "documents" property.

    Args:
         rec_id: `str`. A DCC object identifier, i.e. accession, @id, UUID, ..., of the object to link the
             document to.
         dcc_document_uuid: `str`. The value of the document's 'uuid' attribute.

    Returns:
        `dict`: The response form self.patch().
    """
    rec_json = self.get(ignore404=False,rec_ids=rec_id)
    documents_json = rec_json["documents"]
    #Originally in form of [u'/documents/ba93f5cc-a470-41a2-842f-2cb3befbeb60/',
    #                       u'/documents/tg81g5aa-a580-01a2-842f-2cb5iegcea03, ...]
    #Strip off the /documents/ prefix from each document UUID:
    document_uuids = [x.strip("/").split("/")[-1] for x in documents_json]
    if document_uuids:
      document_uuids = euu.add_to_set(entries=document_uuids,new=dcc_document_uuid)
    else:
      document_uuids.append(dcc_document_uuid)
    payload = {}
    payload["@id"] = euu.parse_profile_from_id_prop(rec_json["@id"])
    payload["documents"] = document_uuids
    self.patch(payload=payload,record_id=rec_id)

#When appending "?datastore=database" to the URL. As Esther stated: "_indexer to the end of the
# URL to see the status of elastic search like
# https://www.encodeproject.org/_indexer if it's indexing it will say the status is "indexing",
# versus waiting" and the results property will indicate the last object that was indexed."

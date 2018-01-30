# -*- coding: utf-8 -*-

###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
### 

"""
Contains utilities that don't require authorization on the DCC servers. 
"""

import json
import os
import requests
import subprocess
import pdb

import encode_utils as eu


REQUEST_HEADERS_JSON = {'content-type': 'application/json'}


class UnknownProfile(Exception):                                                                     
  """                                                                                                   
  Raised when the profile in question doesn't match any valid profile name present in                   
  """                                                                                                   
  pass

def get_profile_ids():
  """Creates a list of the profile IDs spanning all public profiles on the Portal.

  The profile ID for a given profile is extracted from the 'id' key. The 'profiles' prefix
  is stripped off, and the '.json' suffix is also removed. For example, the value of the 'id' 
  property for the genetic modification profile is `/profiles/genetic_modification.json`. The
  value that gets inserted into the list returned by this function is `genetic_modification`.

  Returns:
      list: list of profile IDs.
  """
  profiles = requests.get(eu.PROFILES_URL + "?format=json",
                          timeout=eu.TIMEOUT,
                          headers=REQUEST_HEADERS_JSON)
  profiles = profiles.json()
  profile_ids = []
  for profile_name in profiles:
     if profile_name.startswith("_"):
       #i.e. _subtypes
       continue
     print(profile_name)
     profile_id = profiles[profile_name]["id"].split("/")[-1].split(".json")[0]
     profile_ids.append(profile_id)
  return profile_ids


class Profile:
  """
  Encapsulates knowledge about the existing profiles on the Portal and contains useful methods
  for working with a given profile.
   
  The user supplies a profile name, typically the value of a record's '@id' attribute. It will be
  normalized to match the syntax of the profile IDs in list returned by the function 
  `get_profile_ids()`.
  """
 
  #: The list of the profile IDs spanning all public profiles on the Portal, as returned by
  #: `get_profile_ids()`.
  PROFILE_IDS = get_profile_ids()

  FILE_PROFILE_NAME = "file"
  try:
    assert(FILE_PROFILE_NAME in PROFILE_IDS)
  except AssertionError:
    print("WARNING: The profile for file.json has underwent a name change apparently and is no longer known to this package.")

  def __init__(self,profile_id):
    """
    Args:
        profile_id: str. Typically the value of a record's '@id' property.
    """

    #: The profile_id after it has become internally normalized to match the format used in
    #: Profile.PROFILE_IDS.
    self.profile_id = self.set_profile_id(profile_id)

  def _set_profile_id(self,profile_id):
    """
    Normalizes profile_id so that it matches the format of the profile IDs in the list 
    Profile.PROFILE_IDS, and ensures that the normalized profile ID is a member of this list.

    Args: 
        profile_id: str. Typeically the value of a record's '@id' property.

    Returns:
        str: The normalized profile ID.
    Raises: 
        UnknownProfile: The normalized profile ID is not a member of the list Profile.PROFILE_IDS.
    """
    orig_profile = profile_id
    profile_id = profile_id.strip("/").split("/")[0].lower()
    #Multi-word profile names are hypen-separated, i.e. genetic-modifications.
    profile_id = profile_id.replace("-","")
    if not profile_id in PROFILE_IDS:
      profile_id = profile_id.rstrip("s")
      if not profile_id in PROFILE_IDS:
        raise UnknownProfile("Unknown profile ID '{}'.".format(orig_profile))
    return profile_id

  def get_schema(self):
    """Retrieves the JSON schema of the profile from the Portal.
  
    Returns: 
        tuple: Two-item tuple where the first item is the URL used to fetch the schema, and the
            second item is a dict representing the profile's JSON schema.
  
    Raises: 
        requests.exceptions.HTTPError: The status code is not okay.
    """
    url = os.path.join(eu.PROFILES_URL,self.profile_id + ".json?format=json")
    res = requests.get(url,headers=REQUEST_HEADERS_JSON,timeout=eu.TIMEOUT)
    res.raise_for_status()
    return url, res.json()
  

def print_format_dict(dico,indent=2):                                                           
  """Formats a dictionary for printing purposes to ease visual inspection.

  Wraps the json.dumps() function.

  Args:
      indent: int. The number of spaces to indent each level of nesting. Passed directly
          to the json.dumps() method.
  """                                                                                                
  #Could use pprint, but that looks too ugly with dicts due to all the extra spacing.                
  return json.dumps(dico,indent=indent,sort_keys=True)  

def clean_alias_name(self,alias):
  """
  Removes unwanted characters from the alias name. Only the '/' character purportedly causes issues.
  This function replaces both '/' and '\' with '_'.

  Args:
      alias: str. 

  Returns: 
      str:
  """
  alias = alias.replace("/","_")
  alias = alias.replace("\\","_")
  return alias

def create_subprocess(cmd,check_retcode=True):
  """Runs a command in a subprocess and checks for any errors.

  Creates a subprocess via a call to subprocess.Popen with the argument 'shell=True', and pipes 
  stdout and stderr. Stderr is always piped; stdout if off by default. If the argument 
  'check_retcode' is True, which it is by defualt, then for any non-zero return code, an Exception 
  is raised that will print out the the command, stdout, stderr, and the returncode.  Otherwise, 
  the Popen instance will be returned, in which case the caller must call the instance's 
  communicate() method (and not it's wait() method!!) in order to get the return code to see if the 
  command was successful. communicate() will return a tuple containing (stdout, stderr), after 
  that you can then check the return code with Popen instance's 'returncode' attribute.

  Args: 
      cmd: str. The command line for the subprocess wrapped in the subprocess.Popen instance. If 
          given, will be printed to stdout when there is an error in the subprocess.
      check_retcode: bool. Default is True. See documentation in the description above for specifics.

  Returns: 
      Two-item tuple being stdout and stderr if 'checkRetCode' is set to True and the 
      command has a 0 exit status. If 'checkRetCode' is False, then a subprocess.Popen() 
      instance is instead returned. 

  Raises:
      subprocess.SubprocessError: There is a non-zero return code and check_retcode=True.
  """
  popen = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  if check_retcode:
    stdout,stderr = popen.communicate()
    stdout = stdout.strip()
    stderr = stderr.strip()
    retcode = popen.returncode
    if retcode:
      #subprocess.SubprocessError was introduced in Python 3.3.
      raise subprocess.SubprocessError(
        ("subprocess command '{cmd}' failed with returncode '{returncode}'.\n\nstdout is:"
         " '{stdout}'.\n\nstderr is: '{stderr}'.").format(cmd=cmd,returncode=retcode,stdout=stdout,stderr=stderr))
    return stdout,stderr
  else:
    return popen

def strip_alias_prefix(self,alias):
  """
  Splits 'alias' on ':' to strip off any alias prefix. Aliases must have a lab-specific prefix. 
  The ':' is the seperator between prefix and the rest of the alias, and can't appear elsewhere in 
  the alias. 

  Args:
      alias: str. The alias.

  Returns:
      str:
  """
  return name.split(":")[-1]
 
def add_to_set(self,entries,new):
  """Adds an entry to a list and makes a set for uniqueness before returning the list.

  Args:
      entries: list.
      new: A new member to add to the list.

  Returns: 
      list: A deduplicated list.
  """
  entries.append(new)
  unique_list = list(set(entries))
  return unique_list
 
def does_lib_replicate_exist(lib_accession,exp_accession,biologicial_replicate_number=False,technical_replicate_number=False):
  """
  Regarding the replicates on the specified experiment, determines whether the specified library
  belongs_to any of the replicates.  Optional constraints are the biologicial_replicate_number and 
  the technical_replicate_number props of the replicates.

  Args:
      lib_accession: str. The value of a library object's 'accession' property.
      exp_accession: str. The value of an experiment object's accession. The lib_accession
        should belong to a replicate on this experiment. 
      biologicial_replicate_number: int. The biological replicate number. 
      technical_replicate_number: int. The technical replicate number. 
        
  Returns: 
      list: The replicates that pass the search constraints.
  """
  biologicial_replicate_number = int(biologicial_replicate_number)
  technical_replicate_number = int(technical_replicate_number)
  results = []
  for rep in replicates_json_from_dcc:
    rep_id = rep["uuid"]
    if not lib_accession == rep["library"]["accession"]:
      continue
    if biologicial_replicate_number:
      if biologicial_replicate_number != rep["biological_replicate_number"]:
        continue 
    if technical_replicate_number:
      if technical_replicate_number != rep["technical_replicate_number"]:
        continue
    results.append(rep)
  return results

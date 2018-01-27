# -*- coding: utf-8 -*-

###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
### 

"""
Contains utilities that don't require authorization on the ENCODE Servers. 
"""

import json
import os
import requests
import subprocess

import encode_utils as eu

#: The lower-cased names of all ENCODE object profiles, dynamically parsed out of the result of a
#: GET request to the URL encode_utils.PROFILES_URL. Note that profile names are not hyphenated.
#: For example, the profile listed in the response as GeneticModification becomes 
#: geneticmodification in this resulting list.
PROFILE_NAMES = sorted([x.lower() for x in requests.get(eu.PROFILES_URL + "?format=json",timeout=eu.TIMEOUT,headers={"content-type": "application/json"}).json().keys()])

def does_profile_exist(profile):
  """
  Indicates whether the specified profile exists on the Portal.  
  """
  profile = profile.lower().replace("_","")
  return profile in PROFILE_NAMES

def parse_profile_from_id_prop(id_val):
  """Figures out what profile a record belongs to by looking at it's '@id' property.

  Given the value of the '@id' property of any schema that supports it (all I think?), extracts
  the profile out of it. On the portal, a record stores its ID in this field also, following the 
  profile. For example, given the file object identified by ENCFF859CWS, the value its '@id' 
  property as shown on the Portal is '/files/ENCFF859CWS/'. The profile can be extracted out of 
  this and singularized in order match the name of a profile listed in 
  https://www.encodeproject.org/profiles/.

  Args: 
      id_val: str. The value of the '@id' key in a record's JSON.

  Returns: 
      str. Will be empty if no profile could be extracted.
  """
  #i.e. /documents/ if it doesn't have an ID, /documents/docid if it has an ID.
  profile = id_val.strip("/").split("/")[0].rstrip("s").lower()
  if not profile in PROFILE_NAMES:
    return ""
  return profile

def print_format_dict(dico,indent=2):                                                           
  """                                                                                                
  Formats a dictionary for printing to a stream.                                                     
  """                                                                                                
  #Could use pprint, but that looks too ugly with dicts due to all the extra spacing.                
  return json.dumps(dico,indent=indent,sort_keys=True)  

def clean_alias_name(self,alias):
  """
  Removes unwanted characters from the alias name. Only the '/' character purportedly causes issues.
  This function replaces both '/' and '\' with '_'.

  Args:
      alias - str. 

  Returns: 
      str.
  """
  alias = alias.replace("/","_")
  alias = alias.replace("\\","_")
  return alias

def create_subprocess(cmd,checkRetcode=True):
  """Runs a command in a subprocess and checks for any errors.

  Creates a subprocess via a call to subprocess.Popen with the argument 'shell=True', and pipes 
  stdout and stderr. Stderr is always piped; stdout if off by default. If the argument 
  'checkRetcode' is True, which it is by defualt, then for any non-zero return code, an Exception 
  is raised that will print out the the command, stdout, stderr, and the returncode.  Otherwise, 
  the Popen instance will be returned, in which case the caller must call the instance's 
  communicate() method (and not it's wait() method!!) in order to get the return code to see if the 
  command was successful. communicate() will return a tuple containing (stdout, stderr), after 
  that you can then check the return code with Popen instance's 'returncode' attribute.

  Args: 
      cmd: str. The command line for the subprocess wrapped in the subprocess.Popen instance. If 
          given, will be printed to stdout when there is an error in the subprocess.
      checkRetcode: bool. Default is True. See documentation in the description above for specifics.

  Returns: 
      A two-item tuple containing stdout and stderr if 'checkRetCode' is set to True and the 
      command has a 0 exit status. If 'checkRetCode' is False, then a subprocess.Popen() 
      instance is returned. 
  """
  popen = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  if checkRetcode:
    stdout,stderr = popen.communicate()
    stdout = stdout.strip()
    stderr = stderr.strip()
    retcode = popen.returncode
    if retcode:
      #below, I'd like to raise a subprocess.SubprocessError, but that doesn't exist until Python 3.3.
      raise Exception(
          ("subprocess command '{cmd}' failed with returncode '{returncode}'.\n\nstdout is:"
           " '{stdout}'.\n\nstderr is: '{stderr}'.").format(
               cmd=cmd,returncode=retcode,stdout=stdout,stderr=stderr))
    return stdout,stderr
  else:
    return popen

def get_profile_schema(profile):
  """Retrieves the JSON schema of the specified profile from the ENCODE Portal.

  Raises: 
      requests.exceptions.HTTPError if the status code is something other than 200 or 404. 

  Returns  : 404 (int) if profile not found, otherwise a two-item tuple where item 1 is the URL
             used to fetch the schema, and item 2 is a dict representing the profile's JSON schema.
  """
  url = os.path.join(eu.PROFILES_URL,profile + ".json?format=json")
  print(url)
  res = requests.get(url,headers={"content-type": "application/json"},timeout=eu.TIMEOUT)
  status_code = res.status_code
  if status_code == 404:
    raise UnknownENCODEProfile("Please verify the profile name that you specifed.")
  res.raise_for_status()
  return url, res.json()

def strip_alias_prefix(self,alias):
  """
  Splits 'alias' on ':' to strip off any alias prefix. Aliases must have a lab-specific prefix. 
  The ':' is the seperator between prefix and the rest of the alias, and can't appear elsewhere in 
  the alias. 

  Returns:
      str.
  """
  return name.split(":")[-1]
 
def add_to_set(self,entries,new):
  """Adds an entry to a list and makes a set for uniqueness before returning the list.

  Args:
      entries: list.
      new: A new member to add to the list.

  Returns:
      list.
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


import os
import requests
import subprocess

import pdb
"""
Contains utilities that don't require authorization on the ENCODE Servers. 
"""

#: The timeout in seconds when making HTTP requests via the 'requests' module.
TIMEOUT = 20
PROFILES_URL = "https://www.encodeproject.org/profiles"
#PROFILE_NAMES are lower case.
url = PROFILES_URL + "/?format=json"
PROFILE_NAMES = [x.lower() for x in requests.get(PROFILES_URL + "/?format=json",timeout=TIMEOUT,headers={"content-type": "application/json"}).json().keys()]

def does_profile_exist(profile):
  return profile.lower() in PROFILE_NAMES

def parse_profile_from_id_prop(id_val):
  """Figures out what profile a record belongs to by looking at it's '@id' attribute.

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
  url = os.path.join(PROFILES_URL,profile + ".json?format=json")
  print(url)
  res = requests.get(url,headers={"content-type": "application/json"},timeout=TIMEOUT)
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
 
def does_replicate_exist(library_alias,biologicial_replicate_number,technical_replicate_number,replicates_json_from_dcc):
  """
  Checks if a replicate exists for a specified library alias with the given biological replicate
  number and technical replicate number. Note that this method only works on a library alias
  and not any other form of identifier. 

  Args:
      library_alias: str. Any of the associated library's aliases. i.e. michael-snyder:L-208.
      biologicial_replicate_number: int. The biological replicate number. 
      technical_replicate_number: int. The technical replicate number. 
      replicates_json_from_dcc: dict. The value of the "replicates" key in the JSON of a DCC 
          experiment.
        
  Returns: 
      False if the 'library_alias' doesn't exist in the nested library object of any of the 
      replicates. If the 'library_alias' is present, then True if both 
      'biologicial_replicate_number' and 'technical_replicate_number'
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

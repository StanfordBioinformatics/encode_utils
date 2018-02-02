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
import logging
import os
import requests
import subprocess
import pdb

import encode_utils as eu


REQUEST_HEADERS_JSON = {'content-type': 'application/json'}

#: A descendent logger of the debug logger created in `encode_utils`
#: (see the function description for `encode_utils.create_debug_logger`)
DEBUG_LOGGER = logging.getLogger(eu.DEBUG_LOGGER_NAME + "." + __name__)
#: A descendent logger of the error logger created in `encode_utils`
#: (see the function description for `encode_utils.create_error_logger`)
ERROR_LOGGER = logging.getLogger(eu.ERROR_LOGGER_NAME + "." + __name__)

class MD5SumError(Exception):
  """Raised when there is a non-zero exit status from the md5sum utility from GNU coreutils.
  """

def calculate_md5sum(file_path):
  """"Calculates the md5sum for a file using the md5sum utility from GNU coreutils.

  Args:
      file_path: str. The path to a local file.

  Returns:
      str: The md5sum.

  Raises:
      MD5SumError: There was a non-zero exit status from the md5sum command.
  """
  cmd = "md5sum {}".format(file_path)
  DEBUG_LOGGER.debug("Calculating md5sum for '{}' with command '{}'.".format(file_path,cmd))
  popen = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  stdout,stderr = popen.communicate()
  stdout = stdout.decode("utf-8")
  stderr = stderr.decode("utf-8")
  retcode = popen.returncode
  if retcode:
    error_msg = "Failed to calculate md5sum for file '{}'.".format(file_path)
    DEBUG_LOGGER.debug(error_msg)
    ERROR_LOGGER.error(error_msg)
    error_msg += (" Subprocess command '{cmd}' failed with return code '{retcode}'."
                  " Stdout is '{stdout}'.  Stderr is '{stderr}'.").format(
                    cmd=cmd,retcode=retcode,stdout=stdout,stderr=stderr)
    DEBUG_LOGGER.debug(error_msg)
    raise MD5SumError(error_msg)
  DEBUG_LOGGER.debug(stdout)
  #stdout currently equals the md5sum hash followed by a space and the name of the file.
  return stdout.split()[0] 

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

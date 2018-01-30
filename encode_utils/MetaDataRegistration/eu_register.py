#!/usr/bin/env python3
# -*- coding: utf-8 -*-  

###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
### 

import os
import json
import argparse
import requests
import re

import encode_utils.utils as euu
import encode_utils.connection as euc
from encode_utils.parent_argparser import dcc_login_parser


#: RECORD_ID_FIELD is a special field that won't be skipped in the create_payload() function.
#: It is used when patching objects to indicate the identifier of the record to patch. 
RECORD_ID_FIELD = "record_id" 

def check_valid_json(prop,val,row_count):
  """
  Runs json.loads(val) to ensure valid JSON.
 
  Args:
      val: str. A string load as JSON.
      prop: str. Name of the schema property/field that stores the passed in val. 
      row_count: int. The line number from the input file that is currently being processed.
  
  Raises:
      ValueError: The input is malformed JSON.
  """

  #Don't try to break down the individual pieces of a nested object. That will be too complext for this script, and will also
  # be too complex for the end user to try and represent in some flattened way. Thus, require the end user to supply proper JSON
  # for a nested object.
  try:
    json_val = json.loads(val)
  except ValueError:
    print("Error: Invalid JSON in field '{}', row '{}'".format(prop,row_count))
    raise
  return json_val

def typecast(value,value_type):
  """
  """
  if value_type == "integer":
    return int(value)
  return value

def create_payloads(profile,infile):
  """
  Function : Given a tab-delimited input file containing records belonging to one of the profiles listed on the ENCODE Portal 
             (such as https://www.encodeproject.org/profiles/biosample.json), generates the payload that can be used to either 
             register or patch the metadata for each row. This is a generator function. Note that all profiles are given at 
             https://www.encodeproject.org/profiles/, and that the lower-case name of the profile should be used here. 
  Args     : profile - The profile to submit to (lower-case). See details above.  
             infile  - The tab-delimited input file with a field-header line as the first line. The field names must be exactly equal
                       to the corresponding names in the profile given on the ENCODE Portal. For fields containing an array as the value,
                       values within the array must be comma-delimited and should not themselves be wrapped in brackets. 
  
                       Non-scematic fields are allowed as long as they begin with a '#'. Such fields will be skipped. 
                       Any lines after the header line that start with a '#' will be skipped, as well as any empty lines. 

                       When patching objects, you must specify the 'record_id' field to indicate the identifier of the record. 
                       Note that this a special field that is not present in the ENCODE schema, and doesn't use the '#' prefix to mark it 
                       as non-schematic. Here you can specify any valid record identifier (i.e. UUID, accession, alias). If this 
                       special field is present, it will not be skipped. 


                       Some profiles (most) require specification of the 'award' and 'lab' attributes. These may be set as fields in the
                       input file, or can be left out, in which case the default values for these attributes will be pulled from the
                       environment variables DCC_AWARD and DCC_LAB, respectively.

  Yields  : dict. The payload that can be used to either register or patch the metadata for each row.
  """
  STR_REGX = reg = re.compile(r'\'|"')
  #Fetch the schema from the ENCODE Portal so we can set attr values to the right type when generating the  payload (dict). 
  schema_url, schema = euu.get_schema(profile)
  schema_props = schema["properties"]
  schema_props.update({RECORD_ID_FIELD:1}) #Not an actual schema property.
  field_index = {}
  fh = open(infile,'r')
  header_fields = fh.readline().strip("\n").split("\t")
  skip_field_indices = []
  fi_count = -1 #field index count
  for field in header_fields:
    fi_count += 1
    if field.startswith("#"): #non-schema field
      skip_field_indices.append(fi_count)
      continue
    if field not in schema_props:
      raise Exception("Unknown field name '{}', which is not registered as a property in the specified schema at {}.".format(field,schema_url.split("?")[0]))  
    field_index[fi_count] = field

  line_count = 1 #already read header line
  for line in fh:
    line_count += 1
    line = line.strip("\n").split("\t")
    if not line or line[0].startswith("#"):
      continue
    payload = {}
    payload[conn.ENCODE_PROFILE_KEY] = profile
    fi_count = -1 
    for val in line:
      fi_count += 1
      if fi_count in skip_field_indices:
        continue
      val = val.strip()
      if not val:
        #Then skip. For ex., the biosample schema has a 'date_obtained' property, and if that is 
        # empty it'll be treated as a formatting error, and the Portal will return a a 422.
        continue
      field = field_index[fi_count]
      if field == RECORD_ID_FIELD:
        payload[field] = val
        continue
      schema_val_type = schema_props[field]["type"]
      if schema_val_type == "object":
        #Must be proper JSON
        val = check_valid_json(field,val,line_count)
      elif schema_val_type == "array":
        item_val_type = schema_props[field]["items"]["type"]
        if item_val_type == "object":
          #Must be valid JSON
          #Check if user supplied optional JSON array literal. If not, I'll add it. 
          if not val.startswith("["):
            val = "[" + val
          if not val.endswith("]"):
            val+= "]"
          val = check_valid_json(field,val,line_count)
        else:
          #User is allowed to enter values in string literals. I'll remove them if I find them,
          # since I'm splitting on the ',' to create a list of strings anyway:
          val = STR_REGX.sub("",val)
          #Remove optional JSON array literal since I'm tokenizing and then converting 
          # to an array regardless.
          if val.startswith("["):
            val = val[1:]
          if val.endswith("]"):
            val = val[:-1]
          val = [x.strip() for x in val.split(",")]
          #Type cast tokens if need be, i.e. to integers:
          val = [typecast(value=x,value_type=item_val_type) for x in val]
      else:
        val = typecast(value=val,value_type=schema_val_type)
      payload[field] = val
    yield payload


if __name__ == "__main__":


  description = """
Given a tab-delimited input file containing records belonging to one of the profiles listed on the 
ENCODE Portal (such as https://www.encodeproject.org/profiles/biosample.json), either registers or
patches metadata for each record. Don't mix input files containing both new records and records to 
patch - in this case they should be split into separate input files.

Note that there is a special 'trick' defined in the encode_utils.connection.Connection()
class that can be taken advantage of to simplify submission under certain profiles.  
It concerns the 'attachment' property in any profile that employs it, such as document. 
The trick works as follows: instead of constructing the attachment propery object value as 
defined in the schema, simply use a single-key object containing 'path', i.e.
 {"path": "/path/to/myfile"}, and it'll do the rest. 

"""
  parser = argparse.ArgumentParser(parents=[dcc_login_parser],description=description,formatter_class=argparse.RawTextHelpFormatter)

  parser.add_argument("-p","--profile",required=True,help="""
The profile to submit to, i.e. put 'biosample' for 
https://www.encodeproject.org/profiles/biosample.json. The profile will be pulled down for 
type-checking in order to type-cast any values in the input file to the proper type (i.e. some 
values need to be submitted as integers, not strings).""")

  parser.add_argument("-i","--infile",help="""
The tab-delimited input file with a field-header line as the first line. The field names must be 
exactly equal to the corresponding property names in the specified profile. Non-scematic fields 
are allowed as long as they begin with a '#'. Such fields will be skipped. If a property has an 
array value (as indicated in the profile documentation on the Portal), the array literals 
'[' and ']' are optional. Values within the array must be comma-delimited. For example, if a 
property takes an array of strings, then you can use either of these as the value:

  1) str1,str2,str3
  2) [str1,str2,str3]

On the other hand, if a property takes a JSON object as a value, then the value you enter must be 
valid JSON. This is true anytime you have to specify a JSON object.  Thus, if you are submitting a 
genetic_modification and you have two 'introduced_tags' to provide, you can supply them in either 
of the following two ways:

  1) {"name": "eGFP", "location": "C-terminal"},{"name": "FLAG","C-terminal"}
  2) [{"name": "eGFP", "location": "C-terminal"},{"name": "FLAG","C-terminal"}]
     
Any lines after the header line that start with a '#' will be skipped, as well as any empty lines. 

When patching objects, you must specify the 'record_id' field to indicate the identifier of the record. 
Note that this a special field that is not present in the ENCODE schema, and doesn't use the '#' 
prefix to mark it as non-schematic. Here you can specify any valid record identifier 
(i.e. UUID, accession, alias). If this special field is present, it will not be skipped. 

Some profiles (most) require specification of the 'award' and 'lab' attributes. These may be set 
as fields in the input file, or can be left out, in which case the default values for these
attributes will be pulled from the environment variables DCC_AWARD and DCC_LAB, respectively.""")


  parser.add_argument("--patch",action="store_true",help="""
Presence of this option indicates to patch an existing DCC record rather than register a new one.""")

  parser.add_argument("-e","--error-if-not-found", action="store_true",help="""
If trying to PATCH a record and the record cannot be found on the ENCODE Portal, the default 
behavior is to then attempt a POST. Specifying this option causes an Exception to be raised.""")

  parser.add_argument("-w","--overwrite-array-values",action="store_true",help="""
Only has meaning in combination with the --patch option. When this is specified, it means that 
any keys with array values will be overwritten on the ENCODE Portal with the corresponding value 
to patch. The default action is to extend the array value with the patch value and then to remove 
any duplicates.""")

  args = parser.parse_args()
  profile = args.profile
  dcc_mode = args.dcc_mode
  error_if_not_found = args.error_if_not_found
  overwrite_array_values = args.overwrite_array_values

  conn = euc.Connection(dcc_mode=dcc_mode)

  infile = args.infile
  patch = args.patch
  gen = create_payloads(profile=profile,infile=infile)
  for payload in gen:
    if not patch:
      conn.post(payload)
    else:
      record_id = payload.get(RECORD_ID_FIELD,False)
      if not record_id:
        raise Exception("Can't patch payload {} since there isn't a '{}' field indiciating an identifer for the record to be PATCHED.".format(euu.print_format_dict(payload),RECORD_ID_FIELD))
      payload.pop(RECORD_ID_FIELD)
      payload.update({conn.ENCODE_IDENTIFIER_KEY: record_id})
      conn.send(payload=payload,error_if_not_found=error_if_not_found,extend_array_values=not overwrite_array_values)

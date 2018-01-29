#!/usr/bin/env python3                                                                                     
# -*- coding: utf-8 -*-                                                                                
                                                                                                       
###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
###                                                                                                    
                                                                                                       
import argparse                                                                                        
from encode_utils.connection import Connection                                                         
from encode_utils.parent_argparser import dcc_login_parser                                             
#dcc_login_parser  contains the arguments needed for logging in to the ENCODE Portal, including which env.

#Declare constants for field names:
DOC_FN = "document"
TYPE_FN = "type"
DOWNLOAD_FILENAME_FN = "download_filename"
DESC_FN = "description"

#Create FN hash. Value is field index in header line.
FIELDS = {}
FIELDS[DOC_FN] = None
FIELDS[TYPE_FN] = None
FIELDS[DOWNLOAD_FILENAME_FN] = None
FIELDS[DESC_FN] = None

REQUIRED_FIELDS = [DOC_FN,TYPE_FN,DESC_FN]


description = "POSTS documents to ENCODE's document.json schema."
parser = argparse.ArgumentParser(parents=[dcc_login_parser],description=description,formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-i","--infile",required=True,help="""The tab-delimited input file with required headerline in first row containing the following fields:

    *) document - Required. The local path to the file.
    *) type - Required. See possible values for document_type at https://www.encodeproject.org/profiles/document.json.
    *) download_filename - Optional. The name of the file when a user downloads it from the Portal.
    *) description - Required. Description of the document. Shown on the Portal.

The header line is case-insensitive and the fields can be in any order. Additional fields will be ignored.

The 'download_filename' field is optional and defaults to the name given in the 'document' field, minus the directory path. """)


args = parser.parse_args()
infile = args.infile

fh = open(infile)
header = fh.readline().lower().strip().split("\t")
count = -1
for i in header:
count += 1
  if i in FIELDS:
    FIELDS[i] = count

  


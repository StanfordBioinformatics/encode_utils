#!/usr/bin/env python3
# -*- coding: utf-8 -*-                                                                                
                                                                                                       
###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                                
# Nathaniel Watson                                                                                     
# nathankw@stanford.edu                                                                                
### 

"""
Copies one or more ENCODE files from AWS S3 storage to GCP storage by using the Google Storage         
Transfer Service. The transfer is scheduled to run in upto 1 minute from the time        
this method is called.                                                                             
                                                                                                   
AWS Credentials are fetched from the environment via the variables `AWS_ACCESS_KEY_ID` and         
`AWS_SECRET_ACCESS_KEY`, unless passed explicitly to the aws_creds argument.                        
                                                                                                   
Google credentials are fetched from the environment via the variable                               
GOOGLE_APPLICATION_CREDENTIALS.  This should be set to the JSON file provided to you               
by the GCP Console when you create a service account; see                                          
https://cloud.google.com/docs/authentication/getting-started for more details. Note that           
the service account that you create must have at least the two roles below:                        
    1) Project role with access level of Editor or greater.                                        
    2) Storage role with access level of Storage Object Creator or greater.                        
                                                                                                   
Note! If this is the first time that you are using the Google Storage Transfer Service on          
your GCP bucket, it won't work just yet as you'll get an error that reads:                         
                                                                                                   
  Failed to obtain the location of the destination Google Cloud Storage (GCS) bucket due to        
  insufficient permissions.  Please verify that the necessary permissions have been granted.    
  (Google::Apis::ClientError)                                                                      
                                                                                                   
To resolve this, I recommend that you go into the GCP Console and run a manual transfer there,  
as this adds the missing permission that you need. I personaly don't know how to add it            
otherwise, or even know what it is that's being added, but there you go! 
"""

import argparse
import datetime
import json
import os

import encode_utils.connection as euc
from encode_utils.parent_argparser import dcc_login_parser

def get_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        parents=[dcc_login_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-f", "--file-ids", nargs="+",  required=True, help="""
      One or more ENCODE files to transfer. They can be any valid ENCODE File object identifier.
      Don't mix ENCODE files from across buckets.""")
    parser.add_argument("-gb", "--gcpbucket", required=True, help="""
        The name of the GCP bucket.""")
    parser.add_argument("-gp", "--gcpproject", required=True, help="""
        The GCP project that is associated with gcp_bucket.""")
    parser.add_argument("-d", "--description", help="""
        The description to show when querying transfers via the Google Storage Transfer API, or via
        the GCP Console. May be left empty, in which case the default description will be the value 
        of the first S3 file name to transfer.""")
    parser.add_argument("-c", "--s3creds", help="""
        AWS credentials. Provide them in the form `AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY`.
        Ideally, they'll be stored in the environment. However, for additional flexability you can 
        specify them here as well.""")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    desc = args.description
    aws_creds = args.s3creds
    if aws_creds:
        aws_creds = aws_creds.split(":")
    # Connect to the Portal
    dcc_mode = args.dcc_mode
    if dcc_mode:
        conn = euc.Connection(dcc_mode)
    else:
        # Default dcc_mode taken from environment variable DCC_MODE.
        conn = euc.Connection()

    file_ids = args.file_ids
    gcp_bucket = args.gcpbucket
    gcp_project = args.gcpproject
    conn.copy_files_to_gcp(file_ids=file_ids, gcp_bucket=gcp_bucket,
                           gcp_project=gcp_project, description=desc, aws_creds=aws_creds)

if __name__ == "__main__":
    main()

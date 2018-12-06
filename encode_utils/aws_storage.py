# -*- coding: utf-8 -*-                                                                             
                                                                                                       
###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                                
# Nathaniel Watson                                                                                     
# nathankw@stanford.edu                                                                                
### 

import json
import urllib

import boto3

class S3Object():
    def __init__(self, bucket_name="", key="", s3_uri=""):
        self.bucket_name = bucket_name
        self.key = self._process_key_name(key)
        if s3_uri:
            parse_result = urllib.parse.urlparse(s3_uri)
            self.bucket_name = parse_result.netloc
            self.key = self._process_key_name(parse_result.path)
        if not self.bucket_name and not self.key:
            raise Exception("Either the s3_uri parameter must be specified, or both bucket_name and key must be given.")
        s3 = boto3.resource("s3")
        self.obj = s3.Object(self.bucket_name, self.key)
        #self.bucket = self.s3.Bucket(self.bucket_name)

    @staticmethod
    def _process_key_name(key):
        """
        S3 keys shouldn't start with a "/" as boto3 doesn't work with them, so remove it found.
        """
        return key.lstrip("/")

    def md5sum(self):
        # Convert string in JSON string to Python string.
        return json.loads(self.obj.e_tag)

    def size(self):
        return self.obj.content_length

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
    """
    Represents an object in a S3 bucket. Internally used for calculating the md5sum and file size 
    when submitting files to the ENCODE Portal.

    You must set the appropriate AWS keys as documented in the wiki_.

    .. _wiki: https://github.com/StanfordBioinformatics/encode_utils/wiki/Configuration#aws-keys
    """
    def __init__(self, bucket_name="", key="", s3_uri=""):
        """
        Args:
            bucket_name: `str`. The name of the S3 bucket that contains your file of interest.
                For example, "mybucket". 
            key: `str`. The object path in the bucket.  For example, /path/to/reads.fastq.gz.
            s3_uri: Fully qualified path to the object in the bucket, i.e. 
                s3://pulsar-lims-assets/path/to/reads.fastq.gz. If this is set, then the `buket_name`
                and `key` parameters are ignored since they will be set internally by parsing the
                value of `s3_uri`.
        """
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
        """
        Retrieves the ETag as the md5sum. As explained `here`_, an object's ETag may or may not be
        equal to its MD5 digest. In most cases, however, it will be. If ETags on your objects aren't
        equal, then this method shouldn't be used. 

        .. _here: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
        """
        # Convert string in JSON string to Python string.
        return json.loads(self.obj.e_tag)

    def size(self):
        return self.obj.content_length

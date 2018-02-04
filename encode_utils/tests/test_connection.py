#!/usr/bin/env python3                                                                                 
# -*- coding: utf-8 -*-                                                                                
                                                                                                       
###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
###

"""                                                                                                    
Tests logic in the Connection class in the connection module.
"""

import unittest

from encode_utils import connection
from encode_utils import profiles

DATA_DIR = "data"

class TestConnection(unittest.TestCase):
  """Tests the connection.py module.
  """

  def setUp(self):
    self.conn = connection.Connection()

  def test_before_file_post(self):
    """
    Tests the method before_file_post() for correctly setting the `md5sum` property of a 
    file record.
    """
    payload = {
      self.conn.PROFILE_KEY: profiles.Profile.FILE_PROFILE_ID,
      profiles.Profile.SUBMITTED_FILE_PROP_NAME: "data/test_fq_40recs.fastq.gz"
    }
    res = self.conn.before_post_file(payload)
    self.assertEquals(res["md5sum"],"a3e7cb3df359d0642ab0edd33ea7e93e")

  def test_get_lookup_ids_from_payload(self):
    """
    Tests the method get_lookup_ids_from_payload() for returning the correct result.
    """
    accession = "ENCSR502NRF"
    alias = "michael-snyder:SCGPM_SReq-1103_HG7CL_L3_GGCTAC_R1.fastq.gz"
    md5 = "3fef3e25315f105b944691668838b9b5"
    payload = {
      self.conn.ENCODE_IDENTIFIER_KEY: accession,
      "aliases": [alias],
      "md5sum": md5
    }

    res = self.conn.get_lookup_ids_from_payload(payload)
    self.assertEquals(sorted(res),sorted([accession,alias,md5]))

  def test_extract_aws_upload_credentials(self):
    """ 
    Tests the method extract_aws_upload_credentials() for extracting the upload credentials 
    for from a file object's JSON.
    """
    access_key = "access_key"
    secret_key = "secret_key"
    session_token = "session_token"
    upload_url = "upload_url"

    payload = {
      "upload_credentials": {
        access_key: access_key,
        secret_key: secret_key,
        session_token: session_token,
        upload_url: upload_url
      }
    }
 
    res = self.conn.extract_aws_upload_credentials(payload)

    aws_creds = {}
    aws_creds["AWS_ACCESS_KEY_ID"] = access_key
    aws_creds["AWS_SECRET_ACCESS_KEY"] = secret_key
    aws_creds["AWS_SECURITY_TOKEN"] = session_token
    aws_creds["UPLOAD_URL"] = upload_url

    self.assertEquals(res,aws_creds)

  def test_validate_profile_in_payload(self):
    """
    Tests the method validate_profile_in_payload() for returning the correct profile ID 
    when given the value of a record's `@id` property.
    """
    payload = {
      connection.Connection.PROFILE_KEY: "/genetic-modifications/ENCGM063ASY/"
    }

    res = self.conn.validate_profile_in_payload(payload)
    self.assertEquals(res,"genetic_modification")

if __name__ == "__main__":
  unittest.main()

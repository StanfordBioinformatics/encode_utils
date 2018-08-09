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

import os
import unittest

import encode_utils as eu
import encode_utils.tests
from encode_utils import connection
from encode_utils import profiles

DATA_DIR = encode_utils.tests.DATA_DIR


class TestConnection(unittest.TestCase):
    """Tests the ``encode_utils.connection.py`` module.
    """

    def setUp(self):
        self.conn = connection.Connection(eu.DCC_DEV_MODE)

    def test_arbitrary_host(self):
        self.conn = connection.Connection(dcc_mode='test.encodedcc.org')

    def test_before_file_post(self):
        """
        Tests the method ``before_file_post()`` for correctly setting the `md5sum` property of a
        file record.
        """
        payload = {
            self.conn.PROFILE_KEY: profiles.Profile.FILE_PROFILE_ID,
            profiles.Profile.SUBMITTED_FILE_PROP_NAME: os.path.join(
                DATA_DIR, "test_fq_40recs.fastq.gz")
        }
        res = self.conn.before_post_file(payload)
        self.assertEqual(res["md5sum"], "a3e7cb3df359d0642ab0edd33ea7e93e")

    def test_get_lookup_ids_from_payload(self):
        """
        Tests the method ``get_lookup_ids_from_payload()`` for returning the correct result when
        given a variaty of identifiers (accession, alias, and md5sum).
        """
        accession = "ENCSR502NRF"
        alias = "michael-snyder:SCGPM_SReq-1103_HG7CL_L3_GGCTAC_R1.fastq.gz"
        md5 = "3fef3e25315f105b944691668838b9b5"
        payload = {
            self.conn.ENCID_KEY: accession,
            "aliases": [alias],
            "md5sum": md5
        }

        res = self.conn.get_lookup_ids_from_payload(payload)
        self.assertEqual(sorted(res), sorted([accession, alias, md5]))

    def test_get_profile_from_payload(self):
        """
        Tests the method ``get_profile_from_payload()`` for returning the correct result when only the
        key ``encode_utils.connection.Connection.PROFILE_KEY`` is set in the payload.
        """
        # Use a valid profile ID that exists as a key in profiles.Profile.PROFILES.
        profile_id = "genetic_modification"
        payload = {}
        payload[self.conn.PROFILE_KEY] = "genetic_modification"
        res = self.conn.get_profile_from_payload(payload)
        self.assertEqual(res, profile_id)

    def test_2_get_profile_from_payload(self):
        """
        Tests the method ``get_profile_from_payload()`` for returning the correct result when only the
        key for the `@id` property is set in the payload.
        """
        # Use a valid profile ID that exists as a key in profiles.Profile.PROFILES.
        profile_id = "genetic_modification"
        payload = {}
        payload["@id"] = "genetic_modification"
        res = self.conn.get_profile_from_payload(payload)
        self.assertEqual(res, profile_id)

    def test_3_get_profile_from_payload(self):
        """
        Tests the method ``get_profile_from_payload()`` for raising the exception
        ``encode_utils.connection.ProfileNotSpecified`` when neither the ``self.PROFILE_KEY`` or `@id`
        key is present in the payload.
        """
        # Use a valid profile ID that exists as a key in profiles.Profile.PROFILES.
        payload = {}
        self.assertRaises(
            connection.ProfileNotSpecified,
            self.conn.get_profile_from_payload,
            payload)

    def test_4_get_profile_from_payload(self):
        """
        Tests the method ``get_profile_from_payload()`` for raising the exception
        ``profiles.UnknownProfile`` when an unknown profile is specified in the payload.
        """
        # Use a valid profile ID that exists as a key in profiles.Profile.PROFILES.
        payload = {}
        payload[self.conn.PROFILE_KEY] = "unknown_profile"
        self.assertRaises(
            profiles.UnknownProfile,
            self.conn.get_profile_from_payload,
            payload)

    def test_extract_aws_upload_credentials(self):
        """
        Tests the ``method extract_aws_upload_credentials()`` for extracting the upload credentials
        for from a file object's JSON.
        """
        access_key = "access_key"
        secret_key = "secret_key"
        session_token = "session_token"
        upload_url = "upload_url"

        payload = {
            access_key: access_key,
            secret_key: secret_key,
            session_token: session_token,
            upload_url: upload_url
        }

        res = self.conn.extract_aws_upload_credentials(payload)

        aws_creds = {}
        aws_creds["AWS_ACCESS_KEY_ID"] = access_key
        aws_creds["AWS_SECRET_ACCESS_KEY"] = secret_key
        aws_creds["AWS_SESSION_TOKEN"] = session_token
        aws_creds["UPLOAD_URL"] = upload_url

        self.assertEqual(res, aws_creds)

    def test_make_search_url(self):
        """
        Tests the method ``make_search_url()`` for building the correct URL given the query arguments
        to find ChIP-seq assays performed on primary cells from blood.
        """
        query = {
            "assay_title": "ChIP-seq",
            "biosample_type": "primary cell",
            "organ_slims": "blood",
            "type": "Experiment"
        }

        res = self.conn.make_search_url(search_args=query)
        query = "search/?assay_title=ChIP-seq&biosample_type=primary+cell&organ_slims=blood&type=Experiment"
        self.assertEqual(res, os.path.join(self.conn.dcc_url, query))

    def test_get(self):
        res = self.conn.get('experiments/ENCSR502NRF/', frame='object')
        self.assertEqual(res.get('uuid', ""), "e44c59cc-f14a-4722-a9c5-2fe63c2b9533")

    def test_dry_run_enabled(self):
        """
        Tests the method ``check_dry_run`` for returning True when the ``Connection`` class is
        instantiated in dry-run mode.
        """
        self.conn = connection.Connection(eu.DCC_DEV_MODE,True) 
        self.assertEqual(True, self.conn.check_dry_run())

    def test_bedfile_download(self):
        """
        Tests the method ``download`` for downloading a tiny BED file record (ENCFF815QOR) of size 44 KB. 
        in this directory.
        """
        filepath = self.conn.download(rec_id="ENCFF815QOR", directory=os.getcwd())
        self.assertTrue(os.stat(filepath).st_size > 0)

    def test_doc_download(self):
        """
        Tests the method ``download`` for downloading a document record (michael-snyder:P-17) in
        this directory.
        """
        filepath = self.conn.download(rec_id="michael-snyder:P-17", directory=os.getcwd())
        self.assertTrue(os.stat(filepath).st_size > 0)
        
        
        

if __name__ == "__main__":
    unittest.main()

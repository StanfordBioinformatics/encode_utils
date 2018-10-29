#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Tests functions in the ``encode_utils.utils`` module. A few test data files are used from within the 'data' folder.
The contents of the files are explained here. When a test utilizes any of them, you can check here
to read the documentation about the contents of that file.

* replicates_for_ENCSR502NRF.json
  Contains the value of the 'replicates' property for the actual experiment ENCSR502NRF.
  There are two replicates:

  * michael-snyder:L-3525-1_ENCSR502NRF with biological_replicate_number=1 and
    technical_replicate_number=1. It belongs to library accession ENCLB690UAL.
  * michael-snyder:L-3526-2_ENCSR502NRF with biological_replicate_number=2 and
    technical_replicate_number=1. It belongs to library accession ENCLB782CIR.

* test_fq_40recs.fastq.gz
  Contains 40 FASTQ records.
"""

import json
import os
import unittest

import encode_utils.tests
from encode_utils import utils

DATA_DIR = encode_utils.tests.DATA_DIR


class TestUtils(unittest.TestCase):
    """
    Tests functions in the ``encode_utils.utils`` module.
    """

    def setUp(self):
        rep_json_file = os.path.join(DATA_DIR, "replicates_for_ENCSR502NRF.json")
        with open(rep_json_file, 'r') as fh:
            self.replicates_json = json.loads(fh.read())
        self.fqfile = os.path.join(DATA_DIR, "test_fq_40recs.fastq.gz")

    def test_add_to_set(self):
        """Tests the function ``add_to_set()`` for success when all elements are already unique.
        """
        entries = [1, 2, 3]
        new = 4
        self.assertEqual(utils.add_to_set(entries=entries, new=new), [1, 2, 3, 4])

    def test_add_to_set_2(self):
        """Tests the function ``add_to_set()`` for success when there are duplicates.
        """
        entries = [1, 2, 3]
        new = 3
        self.assertEqual(utils.add_to_set(entries=entries, new=new), [1, 2, 3])

    def test_calculate_md5_sum(self):
        """Tests the function ``calculate_md5_sum()`` for success.
        """
        infile = os.path.join(DATA_DIR, "test_fq_40recs.fastq.gz")
        md5sum = utils.calculate_md5sum(infile)
        self.assertEqual(md5sum, "a3e7cb3df359d0642ab0edd33ea7e93e")

    def test_clean_aliases(self):
        """Tests the function ``clean_aliases()`` for success.
        """
        alias = r"michael-snyder:a/troublesome\alias"
        self.assertEqual(utils.clean_aliases(aliases=[alias]), ["michael-snyder:a_troublesome_alias"])

    def test_does_lib_replicate_exist(self):
        """
        Test the function ``does_lib_replicate_exist()`` for correct result when we only care about
        whether the library has any replicates, and not any particular one.
        """
        lib_accession = "ENCLB690UAL"  # has replicate for (1,1).
        res = utils.does_lib_replicate_exist(replicates_json=self.replicates_json,
                                             lib_accession=lib_accession)
        self.assertEqual(res, ["37b3dabc-bbdc-4832-88a5-78c2a8369942"])

    def test_2_does_lib_replicate_exist(self):
        """
        Test the function ``does_lib_replicate_exist()`` for the correct result when we restrict
        the replicate search to only those with the specific `biological_replicate_number`.
        """
        lib_accession = "ENCLB690UAL"  # has replicate for (1,1).
        brn = 1
        res = utils.does_lib_replicate_exist(
            replicates_json=self.replicates_json,
            lib_accession=lib_accession,
            biological_replicate_number=brn)

        self.assertEqual(res, ["37b3dabc-bbdc-4832-88a5-78c2a8369942"])

    def test_3_does_lib_replicate_exist(self):
        """
        Test the function ``does_lib_replicate_exist()`` for the empty result when we restrict
        the replicates search to a `biological_replicate_number` that does not apply.
        """
        lib_accession = "ENCLB690UAL"  # has replicate for (1,1).
        brn = 2
        res = utils.does_lib_replicate_exist(
            replicates_json=self.replicates_json,
            lib_accession=lib_accession,
            biological_replicate_number=brn)

        self.assertEqual(res, [])

    def test_4_does_lib_replicate_exist(self):
        """
        Test the function ``does_lib_replicate_exist()`` for the empty result when we restrict
        the replicates search to a `biological_replicate_number` that does apply but a
        `technical_replicate_number` that doesn't.
        """
        lib_accession = "ENCLB690UAL"  # has replicate for (1,1).
        brn = 1
        trn = 2
        res = utils.does_lib_replicate_exist(
            replicates_json=self.replicates_json,
            lib_accession=lib_accession,
            biological_replicate_number=brn,
            technical_replicate_number=trn)

        self.assertEqual(res, [])

    def test_5_does_lib_replicate_exist(self):
        """
        Test the function ``does_lib_replicate_exist()`` for the empty result when the library
        accession doesn't belong to any of the replicates.
        """
        lib_accession = "ENCLB000000"  # Doesn't exist.
        res = utils.does_lib_replicate_exist(replicates_json=self.replicates_json,
                                             lib_accession=lib_accession)
        self.assertEqual(res, [])

    def test_strip_alias_prefix(self):
        """Tests the function ``strip_alias_prefix()`` for success.
        """
        alias = "michael-snyder:B-167"
        self.assertEqual(utils.strip_alias_prefix(alias), "B-167")


if __name__ == "__main__":
    unittest.main()

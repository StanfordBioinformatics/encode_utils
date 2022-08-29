#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
"""

import json
import os
import unittest

import igvf_utils.tests
from igvf_utils import utils

DATA_DIR = os.path.join(igvf_utils.tests.DATA_DIR)
REGISTER_DIR = os.path.join(DATA_DIR, "register")


class TestRegister(unittest.TestCase):
    """
    Tests functions in the ``igvf_utils.utils`` module.
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

    def test_strip_alias_prefix(self):
        """Tests the function ``strip_alias_prefix()`` for success.
        """
        alias = "michael-snyder:B-167"
        self.assertEqual(utils.strip_alias_prefix(alias), "B-167")


if __name__ == "__main__":
    unittest.main()

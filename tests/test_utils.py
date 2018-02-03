#!/usr/bin/env python3                                                                                 
# -*- coding: utf-8 -*-                                                                                
                                                                                                       
###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
###

import unittest

from encode_utils import utils

class TestUtils(unittest.TestCase):
  """Tests the utils.py module.
  """
  def test_calculate_md5_sum(self):
    """Test the function calculate_md5_sum() for success.
    """
    infile = "test_fq_40recs.fastq.gz"
    md5sum = utils.calculate_md5sum(infile) 
    self.assertEqual(md5sum,"dc991f01103594ef590d612e0caabf39")

  def test_clean_alias_name(self):
    """Test the function clean_alias_name() for success.
    """
    alias = r"michael-snyder:a/troublesome\alias"
    self.assertEquals(utils.clean_alias_name(alias),"michael-snyder:a_troublesome_alias")
  


if __name__ == "__main__":
  unittest.main()

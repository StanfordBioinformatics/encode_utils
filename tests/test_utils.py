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
  def test_calculate_md5_sum(self):
    infile = "test_fq_40recs.fastq.gz"
    md5sum = utils.calculate_md5sum(infile) 
    self.assertEqual(md5sum,"dc991f01103594ef590d612e0caabf39")
  


if __name__ == "__main__":
  unittest.main()

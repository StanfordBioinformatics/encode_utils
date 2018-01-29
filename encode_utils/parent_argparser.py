# -*- coding: utf-8 -*- 

###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
### 

import argparse

dcc_login_parser = argparse.ArgumentParser(add_help=False)
dcc_login_parser.add_argument("-m","--dcc-mode",required=True,help="The ENCODE Portal site ('prod' or 'dev') to connect to.")


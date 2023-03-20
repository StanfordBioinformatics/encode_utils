# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

import argparse

igvf_login_parser = argparse.ArgumentParser(add_help=False)
igvf_login_parser.add_argument(
    "-m",
    "--igvf-mode",
    help="""
    The IGVF Portal site ('prod' or 'sandbox', or an explicit host name, i.e. '*.demo.igvf.org') to connect to.""")

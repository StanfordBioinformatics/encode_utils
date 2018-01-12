import argparse

dcc_login_parser = argparse.ArgumentParser(add_help=False)
dcc_login_parser.add_argument("-m","--dcc-mode",required=True,help="The ENCODE Portal site ('prod' or 'dev') to connect to.")


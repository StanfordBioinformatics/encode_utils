from distutils.core import setup

setup(
	name = "encode utils",
	version = "1.0",
	description = "Client and tools for ENCODE data submitters.",
	author = "Nathaniel Watson",
	author_email = "nathankw@stanford.edu",
	url = "https://github.com/StanfordBioinformatics/encode_utils/wiki",
	packages =["encode_utils"],
	scripts = ["encode_utils/MetaDataRegistration/register_meta.py"],
	data_files = [ ("encode_utils",["encode_utils/conf_data.json"]) ]
)

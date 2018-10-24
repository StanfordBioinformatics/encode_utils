© 2018 The Board of Trustees of the Leland Stanford Junior University.

# encode_utils
Tools that are useful to any ENCODE Consortium submitting group, as well as the general community working with ENCODE data.  Library and scripts are coded in Python.

See the [wiki](https://github.com/StanfordBioinformatics/encode_utils/wiki) to get started. 

API and script documentation are available on [Read the Docs](http://encode-utils.readthedocs.io/en/latest/).


# Latest news

Oct. 16, 2018

Bug fixes in Master:

1. Fixed bug reported by Jennifer Jou where the `profile.Profile._set_profile_id()` method didn't properly singularize the profile ID in all cases. Fixed this by using the inflection module's singularize function.
2. Fixed bug where `eu_register.py`'s `typecast()` function didn't check for booleans to typecaset to.  That meant that the registration script didn't always handle boolean fields propertly. Thanks again to jjou for reporting.

New in Master:

1. 10/23/2018 
   Added new script eu_get_accessions.py.  Given an input list of record aliases, retrieves the DCC accession for each. 
2. Added script [``eu_create_gcp_url_list.py``](https://encode-utils.readthedocs.io/en/latest/scripts/eu_create_gcp_url_list.html). 
3. Added script [``eu_get_accessions.py``](https://encode-utils.readthedocs.io/en/latest/scripts/eu_get_accessions.html).
   
Updates in Master:

1. Renamed ``utils.clean_alias_name()`` to [``utils.clean_aliases``](https://encode-utils.readthedocs.io/en/latest/utils.html#encode_utils.utils.clean_aliases). This function now takes a list of aliases and either removes or replaces non-permitted characters, such as "/" and "#". This function is called in the pre-submit hook [``before_submit_alias``](https://encode-utils.readthedocs.io/en/latest/connection.html#encode_utils.connection.Connection.before_submit_alias).
   

***

Oct. 11, 2018

Release [2.4.0](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/2.4.0)

***

Aug 9, 2018

Release [2.3.1](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/2.3.1).
Adds support to transfer from S3 to GCP (see release notes from [2.3.0](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/2.3.0)). 

***

Jun 15, 2018

Release [1.5.2](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.5.2)

***

June 7, 2018

Release [1.5.1](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.5.1)

***

May 21, 2018

Release [1.5.0](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.5.0)

***

May 10, 2018

Release [1.4.1](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.4.1)

***

May 4, 2018

Release [1.4.0](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.4.0).

***

Apr. 5, 2018

Release [1.3.0](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.3.0). 

***

Apr. 3, 2018

Release [1.2.1](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.2.1).

***

Mar. 26, 2018

Release [1.2.0](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.2.0).

***

Feb. 23, 2018

Release [1.1.3](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.1.3).

***

Feb. 15, 2018

Release [1.1.1](https://github.com/StanfordBioinformatics/encode_utils/releases/tag/1.1.1).

***

Feb. 12, 2018

Release [1.0.0](https://github.com/StanfordBioinformatics/encode_utils/tree/1.0.0).

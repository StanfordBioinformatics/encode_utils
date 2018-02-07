Examples
========

Imports
-------

::

  import encode_utils as eu
  from encode_utils.connection import Connection

Connecting to the production and development Portals
----------------------------------------------------

::

  #prod portal:
  conn = Connection("prod")

  #dev portal
  conn = Connection("dev")

GET Request
-----------

Retrieve the JSON serialization for the Experiment record with accession ENCSR161EAA::

  conn.get("ENCSR161EAA")

Search
------

Search for ChIP-seq assays performed on primary cells from blood::

  query = {
    "assay_title": "ChIP-seq",
    "biosample_type": "primary cell",
    "organ_slims": "blood",
    "type": "Experiment"
  }

  conn.search(query)

The query will be URL encoded for you.  If you want to use the search functionality 
programmatically, you should first test your search interactively on the Portal. The result will 
be an array of record results, where each result is given in its JSON representation.

PATCH Request
-------------

Add a new alias to the GeneticModification record ENCGM063ASY. Create a payload
(`dict`) that indicates the record to PATCH and the new alias. The record to PATCH must be
indicated by using the non-schematic key `Connection.ENCID_KEY`, or `self.ENCID_KEY` from the 
perspective of a `Connection` instance, which will be removed from the payload prior to submission:

::

  payload = {
    conn.ENCID_KEY: "ENCGM063ASY",
    "aliases": ["new-alias"]
    }
    
  conn.patch(payload)

File Upload
-----------

Given the File record ENCFF852WVP, say that we need to upload the corresponding FASTQ file to AWS
(i.e. maybe the former attempt failed at the time of creating the File record). Here's how to
do it:

::

  conn.upload_fild(file_id="ENCFF852WVP")

This will only work if the File record has the `submitted_file_name` property set, which is 
interperted as the path to the local file to submit (local to the current computer running this). 
You can also explicitely set the path to the file to upload:

::

  conn.upload_fild(file_id="ENCFF852WVP",file_path="/path/to/myfile")

POST Request
------------

Let's create a new File record on the Portal that represents a FASTQ file, and automatically upload
the file to AWS once that is done:

::

  payload = {
    "aliases": ["michael-snyder:SCGPM_SReq-1103_HG7CL_L3_GGCTAC_R1.fastq.gz"],
    "dataset": "ENCSR161EAA",
    "file_format": "fastq",
    "flowcell_details": {
      "barcode": "GGCTAC",
      "flowcell": "HG7CL",
      "lane": "3",
      "machine": "COOOPER"
    },
    "output": "reads",
    "paired_end": "1",
    "platform": "encode:HiSeq4000",
    "read_length": 101,
    "replicate": "michael-snyder:GM12878_eGFP-ZBTB11_CRISPR_ChIP_input_R1",
    "submitted_file_name": "/path/to/SCGPM_SReq-1103_HG7CL_L3_GGCTAC_R1.fastq.gz"
  }

Notice that we didn't specify the required `award` and `lab` properties (required by the ENCODE
profiles). When not specified, the defaults will be taken from the environment variables 
`DCC_AWARD` and `DCC_LAB` when present. Otherwise, you will get an error when trying to submit.

Specifying the profile key
^^^^^^^^^^^^^^^^^^^^^^^^^^

We are almost ready to hand this payload over to the `post()` method, however, we need to first
indicate the profile to POST to. To do this, add a special key to your payload that is stored in 
the constant `Connection.PROFILE_KEY`.  The `post()` method depends on this key as the way of
indicating which profile to create a new record under. There are a few ways in which you can
specify the profile, but the recommended way is to use the stripped-down profile ID. If you 
look at the JSON schema for the File profile at 
https://www.encodeproject.org/profiles/file.json, you'll find that the value of it's `id` 
property is `"/profiles/file.json"`. The stripped-down value that you should use is `file`. 
Another way to say it is to use the barebones profile name that you put in the URL to get to it.
See the documentation in the `profile.Profile()` class for further details on how this works.

Without futher ado, let's now add the profile specification to the payload and POST it::

  payload[Connection.PROFILE_KEY: "file"]
  conn.post(payload)

The logging to STDOUT and your log files will indicate the progress of your request, including
the upload of your FASTQ file to AWS.


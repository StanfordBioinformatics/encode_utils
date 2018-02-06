Examples
========

Imports:

::

  import encode_utils as eu
  from encode_utils.connection import Connection

Connecting to the production and development Portals:

::

  #prod portal:
  conn = Connection("prod")

  #dev portal
  conn = Connection("dev")

PATCH operation: Add a new alias to the GeneticModification object ENCGM063ASY. Create a payload
(`dict`) that indicates the record to PATCH and the new alias. The record to PATCH must be
indicated by using the non-schematic key `Connection.ENCID_KEY`, or `self.ENCID_KEY` from the 
perspective of a `Connection` instance, which will be removed from the payload prior to submission:

::

  payload = {
    conn.ENCID_KEY: "ENCGM063ASY",
    "aliases": ["new-alias"]
    }
    
  conn.patch(payload)

Given the File object ENCFF852WVP, say that we need to upload the corresponding FASTQ file to AWS
(i.e. maybe the former attempt failed at the time of creating the File object). Here's how to
do it:

::

  conn.upload_fild(file_id="ENCFF852WVP")

This will only work if the File object has the `submitted_file_name` property set, which is 
interperted as the path to the local file to submit (local to the current computer running this). 
You can also explicitely set the path to the file to upload:

::

  conn.upload_fild(file_id="ENCFF852WVP",file_path="/path/to/myfile")



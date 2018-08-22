Transferring files to GCP
=========================

.. _transferOperation: https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations

Encapsulates working with the Google Storage Transfer Service (STS) to transfer files to GCP from either
list of public URLs, or from a list of AWS S3 URIs. The latter requires that the user has AWS keys
with permissions granted to the source S3 bucket and objects. 

The Google Storage Transfer API documentation for Python is available `here 
<https://developers.google.com/resources/api-libraries/documentation/storagetransfer/v1/python/latest/>`_ 
for more details. This package exports a few ways for interfacing with this logic.

  1. A  module named :mod:`encode_utils.transfer_to_gcp` that defines the `Transfer` class, which
     provides the ability to transfer files from an S3 bucket (even non-ENCODE S3 buckets) to a 
     GCP bucket. It does so by creating what the STS calls a transferJob. A transferJob can also
     be created by passing in a URL list, which is a public file accessible through HTTP/S; GCS
     details at https://cloud.google.com/storage-transfer/docs/create-url-list.  
  
  2. The :class:`encode_utils.connection.Connection`
     class has a method named :func:`encode_utils.connection.Connection.gcp_transfer_from_s3` that uses the above
     module and is specific towards working with ENCODE files.  Note: this requires AWS keys.  If 
     you simply want to copy released ENCODE files, you should 
     When using this method, you don't need to specify the S3 bucket or full path of an S3 object 
     to transfer. All you need to do is specify an ENCODE file identifier, such as an accession or 
     alias, and it will figure out the bucket and path to the file in the bucket for you.

  3. The method :func:`encode_utils.connection.Connection.gcp_transfer_from_urllist`, which doesn't
     require AWS keys since it only works with released ENCODE files. This method just creates the
     file that can be used directly as input to the Google Storage Transfer Service in the Google Console,
     or programatically to the method :func:`encode_utils.transfer_to_gcp.Transfer.from_urllist`. 
  
  4. The script :doc:`scripts/eu_s3_to_gcp` can be used, which calls the aforementioned
     method `gcp_transfer_from_s3` method, to transfer one or more ENCODE files to GCP.  Requires
     AWS keys. 

The Transfer class
-------------------

Create a one-off transferJob that executes immediately
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example is not ENCODE specific.

::

  import encode_utils.transfer_to_gcp as gcp

  transfer = gcp.Transfer(gcp_project="sigma-night-206802")

The :func:`encode_utils.transfer_to_gcp.Transfer.from_s3` method is used to create what the STS
calls a transferJob. A transferJob either runs once (a one-off job) or is scheduled
to run repeatedly, depending on how the job schedule is specified. However, the `from_s3` method shown below
only schedules one-off jobs at present::

  transfer_job = transfer.from_s3(s3_bucket="pulsar-encode-assets", s3_paths=["reads.fastq.gz"], gcp_bucket="nathankw1", description="test")

  # At this point you can log into the GCP Consle for a visual look at your transferJob.
  transfer_job_id = transfer_job["name"]
  print(transfer_job_id) # Looks sth. like "transferJobs/10467364435665373026".

  # Get the status of the execution of a transferJob. An execution of a transferJob is called 
  # a transferOperation in the STS lingo:
  status = gcp.get_transfer_status(transfer_job_id)
  print(status) # i.e. "SUCCESS". Other possabilities: IN_PROGRESS, PAUSED, FAILED, ABORTED.

  # Get details of the actual transferOperation.
  # The "details" variable below is a list of transferOperations. Since we created a one-off job, 
  # there will only be a single transferOperation.
  details = transfer.get_transfers_from_job(transfer_job_id)[0]
  >>> print(json.dumps(d, indent=4))
  #  {
  #      "name": "transferOperations/transferJobs-10467364435665373026-1532468269728367",
  #      "metadata": {
  #          "@type": "type.googleapis.com/google.storagetransfer.v1.TransferOperation",
  #          "name": "transferOperations/transferJobs-10467364435665373026-1532468269728367",
  #          "projectId": "sigma-night-206802",
  #          "transferSpec": {
  #              "awsS3DataSource": {
  #                  "bucketName": "pulsar-encode-assets"
  #              },
  #              "gcsDataSink": {
  #                  "bucketName": "nathankw1"
  #              },
  #              "objectConditions": {
  #                  "includePrefixes": [
  #                      "cat.png"
  #                  ]
  #              }
  #          },
  #          "startTime": "2018-07-24T21:37:49.745522946Z",
  #          "endTime": "2018-07-24T21:38:10.477273750Z",
  #          "status": "SUCCESS",
  #          "counters": {
  #              "objectsFoundFromSource": "1",
  #              "bytesFoundFromSource": "80376",
  #              "objectsCopiedToSink": "1",
  #              "bytesCopiedToSink": "80376"
  #          },
  #          "transferJobName": "transferJobs/10467364435665373026"
  #      },
  #      "done": true,
  #      "response": {
  #          "@type": "type.googleapis.com/google.protobuf.Empty"
  #      }
  #  }

The `gcp_transfer_from_s3()` method of the `encode_utils.connection.Connection` class
-----------------------------------------------------------------------------
Requires that the user has AWS key permissions on the ENCODE buckets and file objects.

::

  import encode_utils.connection as euc
  conn = euc.Connection("prod")
  # In production mode, the S3 source bucket is set to encode-files. In any other mode, the
  # bucket is set to encoded-files-dev.

  transfer_job = conn.gcp_transfer_from_s3(
      file_ids=["ENCFF270SAL", "ENCFF861EEE"], 
      gcp_bucket="nathankw1", 
      gcp_project="sigma-night-206802",
      description="test")

Copying files using a URL list
------------------------------
No AWS keys required, but all files being copied must have a status of released. 

::
  import encode_utils.transfer_to_gcp as gcp 
  import encode_utils.connection as euc
  conn = euc.Connection("prod")
  # Create URL list file
  url_file = conn.gcp_transfer_urllist(
       file_ids=["ENCFF385UTX"],
       filename="files_to_transfer.txt")

  # Upload files_to_transfer.txt to your GCS bucket, or some other public place accessible via HTTP/S.
  # Suggested to use a txt extension for your file rathar than tsv so that it can be opened in the 
  # browser (i.e. in GCP to obtain the URL). 

  transfer = gcp.Transfer(gcp_project="sigma-night-206802")
  transfer_job = transfer.from_urllist(
       urllist="https://files_to_transfer.txt",
       gcp_bucket="nathankw1", 
       description="test")


Running the script
------------------
Requires that the user has AWS key permissions on the ENCODE buckets and file objects.

::

  eu_s3_to_gcp.py --dcc-mode prod \
                  --file-ids ENCFF270SAL ENCFF861EEE \
                  --gcpbucket nathankw1 \
                  --gcpproject sigma-night-206802 \
                  --description test
 


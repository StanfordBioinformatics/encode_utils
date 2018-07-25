Transferring files to GCP
=========================

.. _transferOperation: https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations

Encapsulates working with the Google Storage Transfer Service (STS) to transfer files from AWS S3 to
GCP buckets.  The Google Storage Transfer API documentation for Python is available `here 
<https://developers.google.com/resources/api-libraries/documentation/storagetransfer/v1/python/latest/>`_ 
for more details. This package exports three ways for interfacing with this logic.

  1. A module named :mod:`encode_utils.transfer_to_gcp` that defines the `Transfer` class, which
     provides the ability to transfer files from an S3 bucket to a GCP bucket. It does so by 
     creating what the STS calls a transfer job. 
  
  2. The :class:`encode_utils.connection.Connection`
     class has a method named :func:`encode_utils.connection.Connection.gcp_transfer` that uses the above
     module and is specific towards working with ENCODE buckets.  When using this method, 
     you don't need to specify the S3 bucket or full path of an S3 object to transfer. All you need
     to do is specify an ENCODE file identifier, such as an accession or alias, and it will figure out
     the bucket and path to the file in the bucket for you.
  
  3. Lastly the script :doc:`scripts/eu_s3_to_gcp` can be used, which calls the aforementioned
     method `gcp_transfer` method, to transfer one or more ENCODE files to GCP. 
  
Any transfer event of a trasferJob is termed as a transferOperation in the STS API. There are
a few utility methods in this class that work with transferOperations.

The Transfer class
-------------------

Create a one-off transfer job that executes immediately
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example is not ENCODE specific.

::

  import encode_utils.transfer_to_gcp as gcp

  transfer = gcp.Transfer(gcp_project="sigma-night-206802")

The :func:`encode_utils.transfer_to_gcp.Transfer.create` method is used to create a transfer job.
A transfer job either runs once (a one-off job) or is scheduled
to run repeatedly, depending on how the job schedule is specified. The `create` method shown below
only schedules one-off jobs at present::

  transfer_job = transfer.create(s3_bucket="pulsar-encode-assets", s3_paths=["reads.fastq.gz"], gcp_bucket="nathankw1", description="test")

  # At this point you can log into the GCP Consle for a visual look at your transfer job.
  transfer_job_id = transfer_job["name"]
  print(transfer_job_id) # Looks sth. like "transferJobs/10467364435665373026".

  # Get the status of the transfer job. This queries the transferOperation:
  status = gcp.get_transfer_status(transfer_job_id)
  print(status) # i.e. "SUCCESS". Other possabilities: IN_PROGRESS, PAUSED, FAILED, ABORTED.

  # Get details of the actual transferOperation.
  # The "details" variable below is a list of transferOperations. Since we created a one-off job, there will only
  # be a single transferOperation.
  details = transfer.get_transfers_from_job("transferJobs/10467364435665373026")[0]
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

The `gcp_transfer()` method of the `encode_utils.connection.Connection` class
-----------------------------------------------------------------------------

::

  import encode_utils.connection as euc
  conn = euc.Connection("prod")
  transfer_job = conn.gcp_transfer(file_ids=["ENCFF270SAL", "ENCFF861EEE"], 
                    gcp_bucket="nathankw1", 
                    gcp_project="sigma-night-206802",
                    description="test")

Running the script
------------------

::

  eu_s3_to_gcp.py --dcc-mode prod \
                  --file-ids ENCFF270SAL ENCFF861EEE \
                  --gcpbucket nathankw1 \
                  --gcpproject sigma-night-206802
                  --description test
 

# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
# 2018-07-20
###

import datetime
import json
import os
import googleapiclient.discovery
#pip install google-api-python-client (details https://developers.google.com/api-client-library/python/)
# List of APIs that google-api-python-client can use at https://developers.google.com/api-client-library/python/apis/
# Storage Transfer API docs at https://developers.google.com/resources/api-libraries/documentation/storagetransfer/v1/python/latest/

storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

def copy_files_to_gcp(s3_bucket, s3_paths, gcp_bucket, gcp_project, description="", aws_creds=()):
    """
    See example at https://cloud.google.com/storage-transfer/docs/create-client and
    https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/transfer_service/aws_request.py.

    Copies one or more files from AWS S3 storage to GCP storage by using the Google Storage
    Transfer Service. The transfer is scheduled to run in upto 1 minute from the time
    this method is called.

    AWS Credentials are fetched from the environment via the variables `AWS_ACCESS_KEY_ID` and
    `AWS_SECRET_ACCESS_KEY`, unless passed explicitly to the aws_creds argument.

    Google credentials are fetched from the environment via the variable
    GOOGLE_APPLICATION_CREDENTIALS.  This should be set to the JSON file provided to you
    by the GCP Console when you create a service account; see
    https://cloud.google.com/docs/authentication/getting-started for more details. Note that
    the service account that you create must have at least the two roles below:

      1) Project role with access level of Editor or greater.
      2) Storage role with access level of Storage Object Creator or greater.

    Note1: If this is the first time that you are using the Google Storage Transfer Service on
    your GCP bucket, it won't work just yet as you'll get an error that reads:

      Failed to obtain the location of the destination Google Cloud Storage (GCS) bucket due to
      insufficient permissions.  Please verify that the necessary permissions have been granted.
      (Google::Apis::ClientError)

    To resolve this, I recommend that you go into the GCP Console and run a manual transfer there,
    as this adds the missing permission that you need. I personaly don't know how to add it
    otherwise, or even know what it is that's being added, but there you go!

    Note2: If a file transfer doens't work (i.e. it doesn't exist in source bucket or incorrect
    path provided), I'm not aware of a way to know that w/o explicitely having to inspect the GCP
    bucket for presence/absence of the file. Even in the GCP Console, the Tranfer job stil shows as green
    and doesn't indicate any sort of failure.

    Args:
        s3_bucket: `str`. The name of the AWS S3 bucket.
        s3_paths: `list`. The paths to S3 objects in s3_bucket. Don't include leading '/' (it will
            be removed if seen at the beginning anyways). Up to 1000 files can be transferred in a
            given transfer job, per the Storage Transfer API transferJobs_ documentation.
        gcp_bucket: `str`. The name of the GCP bucket.
        gcp_project: `str`. The GCP project that is associated with gcp_bucket. Can be given
            in either integer form  or the user-friendly name form (i.e. sigma-night-207122)
        description: `str`. The description to show when querying transfers via the
             Google Storage Transfer API, or via the GCP Console. May be left empty, in which
             case the default description will be the value of the first S3 file name to transfer.
        aws_creds: `tuple`. Ideally, your AWS credentials will be stored in the environment.
            For additional flexability though, you can specify them here as well in the form
            ``(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)``.

    Returns:
        `dict`: The JSON response representing the newly created transfer job.

    .. _transferJobs: https://developers.google.com/resources/api-libraries/documentation/storagetransfer/v1/python/latest/storagetransfer_v1.transferJobs.html
    """
    #See example at https://cloud.google.com/storage-transfer/docs/create-client and
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/transfer_service/aws_request.py.

    s3_paths = list(set(s3_paths))
    # The transferJobs() doc specifies not to include leading '/'.
    s3_paths = [x.lstrip("/") for x in s3_paths]

    if aws_creds:
        AWS_ACCESS_KEY_ID = aws_creds[0]
        AWS_SECRET_ACCESS_KEY = aws_creds[1]
    else:
        AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
        AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

    # Start transfer between 1 to 2 minutes from now.
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    hour = now.hour
    minute = now.minute

    if not description:
        # Default description is then first s3 object to transfer.
        description = s3_paths[0]
    params = {}
    params["description"] = description
    params["status"] = "ENABLED"
    params["projectId"] = gcp_project
    # Set transfer day (set to today). Note that if the start day is different from the end day,
    # then a daily transfer will be set that ends on the end date. If no end date is set, a daily
    # transfer job will run each day. So, need to avoid those last two scenarios and stick with a
    # one-time transfer.
    params["schedule"] = {
        "scheduleStartDate": {
            "year": now.year,
            "month": now.month,
            "day": now.day
        },
        "scheduleEndDate": {
            "year": now.year,
            "month": now.month,
            "day": now.day
        }
    }
    params["transferSpec"] = {
        "awsS3DataSource": {
            "bucketName": "pulsar-encode-assets",
            "awsAccessKey": {
                "accessKeyId": AWS_ACCESS_KEY_ID,
                "secretAccessKey": AWS_SECRET_ACCESS_KEY
            }
        },
        "gcsDataSink": {
            "bucketName": gcp_bucket
        },
        "objectConditions": {
            "includePrefixes": s3_paths
        }
    }

    job = storagetransfer.transferJobs().create(body=params).execute() #dict
    job_id = job["name"].split("/")[-1]
    print("Created transfer job with ID {}: {}".format(job_id, json.dumps(job, indent=4)))
    return job

def get_transfers_from_job(gcp_project, transferjob_name):
    """
    Fetches descriptions in JSON format of any realized transfers under the specified transferJob.
    These are called transferOperations in the Google Storage Transfer API terminology.

    See Google API example at https://cloud.google.com/storage-transfer/docs/create-manage-transfer-program?hl=ja
    in the section called "Check transfer operation status".
    See API details at https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations.

    Args:
        gcp_project: `str`. The GCP project in which the transferJob specified by transferjob_name
            was created. The underlying API call requires that this be specified, and it can be
            given in either integer form  or the user-friendly name form (i.e. sigma-night-207122).
        transferjob_name: `str`. The value of the `name` key in the dictionary that is returned by
          copy_files_to_gcp().

    Returns:
        `list` of transferOperations belonging to the specified transferJob. This will be a list
            of only a single element if the transferJob is a one-off transfer. But if this is a
            repeating transferJob, then there could be several transferOperations in the list.
    """
    filt = {}
    filt["project_id"] = gcp_project
    filt["job_names"] = [transferjob_name]
    query = storagetransfer.transferOperations().list(
        name="transferOperations",
        filter=json.dumps(filt))
    return query.execute()["operations"]

def get_transfer_status(gcp_project, transferjob_name):
    """
    Returns the transfer status of the first transferOperation that is returned for the given
    transferJob. Thus, this function really only makes sense for one-off transferJobs that don't
    repeat.

    Note: if a transferJob attempts to transfer a non-existing file from the source bucket,
    this has no effect on the transferOperation status (it will not cause a FAILED status).
    Moreover, transferOperation status doesn't look at what files were and were not transferred and is
    ony concerned with the execution status of the transferOperation job itself.

    Args:
        gcp_project: `str`. The GCP project in which the transferJob specified by transferjob_name
            was created. The underlying API call requires that this be specified, and it can be
            given in either integer form  or the user-friendly name form (i.e. sigma-night-207122).
        transferjob_name: `str`. The value of the `name` key in the dictionary that is returned by
          copy_files_to_gcp().
    """
    meta = get_transfers_from_job(gcp_project=gcp_project, transferjob_name=transferjob_name)[0]["metadata"]
    return meta["status"]


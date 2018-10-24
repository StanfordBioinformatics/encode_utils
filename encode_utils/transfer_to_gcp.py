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
import google.auth

#pip install google-api-python-client (details https://developers.google.com/api-client-library/python/)
# List of APIs that google-api-python-client can use at https://developers.google.com/api-client-library/python/apis/

"""
Contains a Transfer class that encapsulates working with the Google Storage Transfer Service to
transfer files from AWS S3 to GCP buckets. 

The Storage Transfer API documentation is available at https://developers.google.com/resources/api-libraries/documentation/storagetransfer/v1/python/latest/
"""


class AwsCredentialsMissing(Exception):
    """
    Raised when a method needs AWS credentials but can't find them.
    """
    pass


class Transfer:
    """
    See example at https://cloud.google.com/storage-transfer/docs/create-client and
    https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/transfer_service/aws_request.py.

    Encapsulates the transfer of files from AWS S3 storage to GCP storage by using the Google Storage
    Transfer Service (STS). The ``create()`` method is used to create a transfer job (termed 
    `transferJob` in the STS API). A transferJob either runs once (a one-off job) or is scheduled 
    to run repeatedly, depending on how the job schedule is specified. 

    Any transfer event of a trasferJob is termed as a transferOperation in the STS API. There are 
    a few utility methods in this class that work with transferOperations.

    AWS Credentials are fetched from the environment via the variables `AWS_ACCESS_KEY_ID` and
    `AWS_SECRET_ACCESS_KEY`, unless passed explicitly to the aws_creds argument. If no AWS credentials
    are available, then transferJobs can not be created, and only query interfacing methods will
    be available for use.

    Google credentials are fetched from the environment via the variable
    GOOGLE_APPLICATION_CREDENTIALS.  This should be set to the JSON file provided to you
    by the GCP Console when you create a service account; see
    https://cloud.google.com/docs/authentication/getting-started for more details. Note that
    the service account that you create must have at least the two roles below:

      1) Project role with access level of Editor or greater.
      2) Storage role with access level of Storage Object Creator or greater.

    Note1: If this is the first time that you are using the Google Storage Transfer Service on
    your GCP bucket, it won't work just yet as you'll get an error that reads:

      **Failed to obtain the location of the destination Google Cloud Storage (GCS) bucket due to
      insufficient permissions.  Please verify that the necessary permissions have been granted.
      (Google::Apis::ClientError)**

    To resolve this, I recommend that you go into the GCP Console and run a manual transfer there,
    as this adds the missing permission that you need. I personaly don't know how to add it
    otherwise, or even know what it is that's being added, but there you go!

    Note2: If you try to transfer a file that is mistyped or doesn't exist in the source bucket, then
    this will not set a failed status on the transferJob. If you really need to know whether a file
    was tranferred in the API, you need to query the transferOperation; see the method 
    :func:`get_transfers_from_job`.  

    .. _transferJobs: https://developers.google.com/resources/api-libraries/documentation/storagetransfer/v1/python/latest/storagetransfer_v1.transferJobs.html
    """

    def __init__(self, gcp_project, aws_creds=()):
        """
        Args:
            gcp_project: `str`. The GCP project that contains your GCP bucket(s). Can be given
                in either integer form  or the user-friendly name form (i.e. sigma-night-207122)
            aws_creds: `tuple`. Ideally, your AWS credentials will be stored in the environment.
                For additional flexability though, you can specify them here as well in the form
                ``(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)``.
        """
        self.gcp_project = gcp_project
        if aws_creds:
            aws_access_key_id = aws_creds[0]
            aws_secret_access_key = aws_creds[1]
        else:
            try:
                aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
            except KeyError:
                print("Warning: AWS_ACCESS_KEY_ID not set.")
                aws_access_key_id = ""
            try:
                aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
            except KeyError:
                print("Warning: AWS_SECRET_ACCESS_KEY not set.")
                aws_secret_access_key = ""
        self.aws_creds = (aws_access_key_id, aws_secret_access_key)
        self.storagetransfer = Transfer._build_client()

    @staticmethod
    def _build_client():
        # OAuth2 scope needed to submit transfer requests:
        # https://developers.google.com/identity/protocols/googlescopes#storagetransferv1
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        return googleapiclient.discovery.build('storagetransfer', 'v1', credentials=credentials)

    def from_s3(self, s3_bucket, s3_paths, gcp_bucket, overwrite_existing=False, description=""):
        """
        Schedules an one-off transferJob that runs immediately to copy the specified file(s) from 
        s3_bucket to gcp_bucket. 

        Args:
            s3_bucket: `str`. The name of the AWS S3 bucket.
            s3_paths: `list`. The paths to S3 objects in s3_bucket. Don't include leading '/' (it will
                be removed if seen at the beginning anyways). Up to 1000 files can be transferred in a
                given transfer job, per the Storage Transfer API transferJobs_ documentation.
                If you only need to transfer a single file, it may be given as a string.
            gcp_bucket: `str`. The name of the GCP bucket.
            overwrite_existing: `bool`. True means that files in GCP get overwritten by any files
                being transferred with the same name (key).
            description: `str`. The description to show when querying transfers via the
                 Google Storage Transfer API, or via the GCP Console. May be left empty, in which
                 case the default description will be the value of the first S3 file name to transfer.
    
        Returns:
            `dict`: The JSON response representing the newly created transferJob.
    
        """
        # See api documentation at https://developers.google.com/resources/api-libraries/documentation/storagetransfer/v1/python/latest/storagetransfer_v1.transferJobs.html.
        if not self.aws_creds[0] and not self.aws_creds[1]:
            raise AwsCredentialsMissing(("Error: In order to create a transferJob, you need to "
                                         "instantiate the {} class with AWS credentials, or have them preset "
                                         "in your environment.".format(self.__class__.__name__)))
        #See example at https://cloud.google.com/storage-transfer/docs/create-client and
        # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/transfer_service/aws_request.py.
    
        if type(s3_paths) == str:
            s3_paths = [s3_paths]

        s3_paths = list(set(s3_paths))
        # The transferJobs API doc specifies not to include leading '/'.
        s3_paths = [x.lstrip("/") for x in s3_paths]
    
        if not description:
            # Default description is the name of the first s3 object to transfer.
            description = s3_paths[0]
        params = {}
        params["description"] = description
        params["status"] = "ENABLED"
        params["projectId"] = self.gcp_project
        # Set transfer day to present day and schedule an immediate, one-off transferJob. 
        # Note that if the start day is different from the end day, then a repeating transferJob 
        # will be created that runs daily all the way through the specified end day. 
        # If no end date is set, a daily transferJob is created that runs indefinitely. 
        # So, we need to avoid these last two scenarios and stick with a one-time transferJob.
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        hour = now.hour
        minute = now.minute
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
                "bucketName": s3_bucket,
                "awsAccessKey": {
                    "accessKeyId": self.aws_creds[0],
                    "secretAccessKey": self.aws_creds[1]
                }
            },
            "gcsDataSink": {
                "bucketName": gcp_bucket
            },
            "objectConditions": {
                "includePrefixes": s3_paths
            }
        }

        if overwrite_existing:
            params["transferSpec"]["transferOptions"] = {
                "overwriteObjectsAlreadyExistingInSink": True
            }
    
        job = self.storagetransfer.transferJobs().create(body=params).execute() #dict
        job_id = job["name"].split("/")[-1]
        print("Created transfer job with ID {}\n{}".format(job_id, json.dumps(job, indent=4)))
        return job

    def from_urllist(self, urllist, gcp_bucket, overwrite_existing=False, description=""):
        """
        Schedules an one-off transferJob that runs immediately to copy the files specified in the
        URL list to GCS.

        Args:
            gcp_bucket: `str`. The name of the GCP bucket.
            overwrite_existing: `bool`. True means that files in GCP get overwritten by any files
                being transferred with the same name (key).
            description: `str`. The description to show when querying transfers via the
                 Google Storage Transfer API, or via the GCP Console. May be left empty, in which
                 case the default description will be the value of the first S3 file name to transfer.
    
        Returns:
            `dict`: The JSON response representing the newly created transferJob.
    
        """
        #See api documentation at https://developers.google.com/resources/api-libraries/documentation/storagetransfer/v1/python/latest/storagetransfer_v1.transferJobs.html.
    
        if not description:
            # Default description is the name of the first s3 object to transfer.
            description = urllist
        params = {}
        params["description"] = description
        params["status"] = "ENABLED"
        params["projectId"] = self.gcp_project
        # Set transfer day to present day and schedule an immediate, one-off transferJob. 
        # Note that if the start day is different from the end day, then a repeating transferJob 
        # will be created that runs daily all the way through the specified end day. 
        # If no end date is set, a daily transferJob is created that runs indefinitely. 
        # So, we need to avoid these last two scenarios and stick with a one-time transferJob.
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        hour = now.hour
        minute = now.minute
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
            "httpDataSource": {
                "listUrl": urllist
            },
            "gcsDataSink": {
                "bucketName": gcp_bucket
            }
        }

        if overwrite_existing:
            params["transferSpec"]["transferOptions"] = {
                "overwriteObjectsAlreadyExistingInSink": True
            }
    
        job = self.storagetransfer.transferJobs().create(body=params).execute() #dict
        job_id = job["name"].split("/")[-1]
        print("Created transfer job with ID {}\n{}".format(job_id, json.dumps(job, indent=4)))
        return job
    
    def get_transfers_from_job(self, transferjob_name):
        """
        Fetches descriptions in JSON format of any realized transfers under the specified transferJob.
        These are called transferOperations in the Google Storage Transfer API terminology.
    
        See Google API example at https://cloud.google.com/storage-transfer/docs/create-manage-transfer-program?hl=ja
        in the section called "Check transfer operation status".
        See API details at https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations.
    
        Args:
            transferjob_name: `str`. The value of the `name` key in the dictionary that is returned by
              self.from_s3 or self.from_urllist().
    
        Returns:
            `list` of transferOperations belonging to the specified transferJob. This will be a list
                of only a single element if the transferJob is a one-off transfer. But if this is a
                repeating transferJob, then there could be several transferOperations in the list.
        """
        filt = {}
        filt["project_id"] = self.gcp_project
        filt["job_names"] = [transferjob_name]
        query = self.storagetransfer.transferOperations().list(
            name="transferOperations",
            filter=json.dumps(filt))
        return query.execute()["operations"]
    
    def get_transfer_status(self, transferjob_name):
        """
        Returns the transfer status of the first transferOperation that is returned for the given
        transferJob. Thus, this function really only makes sense for one-off transferJobs that don't
        repeat.
    
        Note: if a transferJob attempts to transfer a non-existing file from the source bucket,
        this has no effect on the transferOperation status (it will not cause a FAILED status).
        Moreover, transferOperation status doesn't look at what files were and were not transferred and is
        ony concerned with the execution status of the transferOperation job itself.
    
        Args:
            transferjob_name: `str`. The value of the `name` key in the dictionary that is returned by
              create().
        """
        meta = self.get_transfers_from_job(transferjob_name=transferjob_name)[0]["metadata"]
        return meta["status"]
    

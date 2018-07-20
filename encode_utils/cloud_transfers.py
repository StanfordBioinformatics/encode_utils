# -*- coding: utf-8 -*-                                                                                 
                                                                                                        
###                                                                                                     
# © 2018 The Board of Trustees of the Leland Stanford Junior University                                 
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
# 2018-07-20
###

import googleapiclient.discovery

def copy_file_to_gcp(self, s3_bucket, s3_paths, gcp_buket, gcp_project, description="", s3_creds=()):
    """
    Copies one or more files from AWS S3 storage to GCP storage by using the Google Storage
    Transfer Service. The transfer is scheduled to run between 2 and 3 minutes from the time 
    this method is called. 

    AWS Credentials are fetched from the environment via the variables `AWS_ACCESS_KEY_ID` and
    `AWS_SECRET_ACCESS_KEY`, unless passed explicitly to the s3_creds argument. 

    Google credentials are fetched from the environment via the variable 
    GOOGLE_APPLICATION_CREDENTIALS.  This should be set to the JSON file provided to you
    by the GCP Console when you create a service account; see 
    https://cloud.google.com/docs/authentication/getting-started for more details. Note that
    the service account that you create must have at least the two roles below:
        1) Project role with access level of Editor or greater.
        2) Storage role with access level of Storage Object Creator or greater.

    Note! If this is the first time that you are using the Google Storage Transfer Service on 
    your GCP bucket, it won't work just yet as you'll get an error that reads:

      Failed to obtain the location of the destination Google Cloud Storage (GCS) bucket due to 
      insufficient permissions.  Please verify that the necessary permissions have been granted. 
      (Google::Apis::ClientError)

    To resolve this, I recommend that you go into the GCP Console and run a manual transfer there,
    as this adds the missing permission that you need. I personaly don't know how to add it
    otherwise, or even know what it is that's being added, but there you go!

    Args:
        s3_bucket: `str`. The name of the AWS S3 bucket.
        s3_paths: `str`. The paths to S3 objects in s3_bucket.
        gcp_bucket: `str`. The name of the GCP bucket.
        gcp_project: `str`. The GCP project that is associated with gcp_bucket. Can be given 
            in either integer form  or the user-friendly name form (i.e. sigma-night-207122)
        description: `str`. The description to show when querying transfers via the 
             Google Storage Transfer API, or via the GCP Console. May be left empty, in which
             case the default description will be the value of the first S3 file name to transfer.
        s3_creds: `tuple`. Ideally, your AWS credentials will be stored in the environment.
            For additional flexability though, you can specify them here as well in the form 
            ``(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)``.
    """
    #See example at https://cloud.google.com/storage-transfer/docs/create-client and
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/transfer_service/aws_request.py.
    if aws_creds:
        AWS_ACCESS_KEY_ID = aws_creds[0]
        AWS_SECRET_ACCESS_KEY = aws_creds[1]
    else:
        AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
        AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

    client = googleapiclient.discovery.build('storagetransfer', 'v1')
    # Start transfer between 2 to 3 minutes from now.
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    hour = now.hour
    minute = now.minute
    if minute > 57: #minutes go from 0 - 59.
        if hour == 23: #hours go from 0 - 23.
            hour = 1
        else:
            hour += 1
        minute = 1
    else:
        minute += 2

    if not desc:
        # Default desc is then first s3 object to transfer.
        desc = s3_paths[0]
    params = {}
    params["status"] = "ENABLED"
    params["projectId"] = gcp_project
    params["schedule"] = {
        "scheduleStartDate": {
            "year": now.year,
            "month": now.month,
            "day": now.day
        },
        "scheduleEndDate": {
            "year": now.year,
            "month": now.month,
            "day": now.day + 1
        },
        "startTimeOfDay": {
            "hours": hour,
            "minutes": minute
        }
    }
    params["transferSpec"] = {
        "awsS3DataSource": {
            "bucketName": s3_bucket,
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

    result = client.transferJobs().create(body=params).execute()
    print('Returned transferJob: {}'.format(json.dumps(result, indent=4)))

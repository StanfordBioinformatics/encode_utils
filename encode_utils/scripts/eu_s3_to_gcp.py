import datetime
import os

def copy_file_to_gcp(s3bucket):
    #See example at https://cloud.google.com/storage-transfer/docs/create-client and
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/transfer_service/aws_request.py.
    import googleapiclient.discovery
    client = googleapiclient.discovery.build('storagetransfer', 'v1')
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    hour = now.hour
    minute = now.minute
    if minute > 57:
        if hour == 23:
            hour = 1
        else:
            hour += 1
        minute = 1
    else:
        minute += 2
    params = {}
    params["status"] = "ENABLED"
    params["projectId"] = "1078465049259"
    #params["projectId"] = "sigma-night-206802"
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
            "bucketName": s3bucket,
            "awsAccessKey": {
                "accessKeyId": os.environ["AWS_ACCESS_KEY_ID"],
                "secretAccessKey": os.environ["AWS_SECRET_ACCESS_KEY"]
            }
        },
        "gcsDataSink": {
            "bucketName": "nathankw1"
        },
        "objectConditions": {
            "includePrefixes": ["cat.png"]
        }
    }
        
    result = client.transferJobs().create(body=params).execute()
    print('Returned transferJob: {}'.format(json.dumps(result, indent=4)))

if __name__ == "__main__":
    copy_file_to_gcp("pulsar-encode-assets")

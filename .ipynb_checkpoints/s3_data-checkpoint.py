import boto3
import botocore

ACCESS_KEY = AWS_CREDENTIALS['AWS_ACCESS_KEY']
SECRET_KEY = AWS_CREDENTIALS['AWS_SECRET_KEY']

def get_s3resource(service, region):
    return boto3.resource(service, region, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

# Download specific files ('ipath') from AWS s3 bucket and store in local machine ('opath')
def download_files(bucket_name, ipath, opath):
    s3 = get_s3resource()
    bucket = s3.Bucket(bucket_name)
    try:
        s3.Bucket(bucket).download_file(ipath,opath)
        message = "Downloaded %s" % opath
        #ignore empty files
        if int(os.stat(opath).st_size) < 1.1e4:
            pass
    except botocore.exceptions.ClientError as ex:
        print(ex)
        if ex.response['Error']['Code'] == "404":
            message = "File %s not found in archives" % ipath
        else:
            pass
    print(message)

# gets the earliest file date in S3 bucket based on the directory provided as prefix.
def get_earliest_s3_fileDate(bucket, prefix):
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    s3 = boto3.client('s3')
    paginator = s3.get_paginator( "list_objects" )
    page_iterator = paginator.paginate( Bucket = bucket, Prefix = prefix)
    for page in page_iterator:
        if "Contents" in page:
            last_added = [obj['Key'] for obj in sorted( page["Contents"], key=get_last_modified)][0]
    return last_added
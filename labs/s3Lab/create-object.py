import boto3, botocore, configparser, os

os.environ['AWS_PROFILE'] = "workshop"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"

def main(s3Client):
    print('\nStart of create object script\n')
    ## Initialize variables for object creation

    print('Reading configuration file for bucket name...')
    config = readConfig()
    bucket_name = config['bucket_name']
    source_file_name = config["object_name"] + config['source_file_extension']
    key_name = config['key_name']+ config['source_file_extension']
    contentType = config['source_content_type']
    metaData_key = config['metaData_key']
    metaData_value = config['metaData_value']

    #### Create object in the s3 bucket
    print('Creating Object...')
    print(uploadObject(s3Client, bucket_name, source_file_name, key_name, contentType, {metaData_key: metaData_value}))
    
    print('\nEnd of create object script\n')

def uploadObject(s3Client, bucket, name, key, contentType, metadata={}):

    ## Start TODO 5: create a object by transferring the file to the S3 bucket, 
    ## set the contentType of the file and add any metadata passed to this function.
    response = s3Client.upload_file(
        Bucket=bucket, 
        Key=key,
        Filename=name,
        ExtraArgs={
            'ContentType': contentType,
            'Metadata': metadata
        }
    )

    
    
    ## End TODO 5
    return "Finished creating object\n"
    
def readConfig():
    config = configparser.ConfigParser()
    config.read('./s3Lab/config.ini')
    
    return config['S3']

# Create an S3 client to interact with the service and pass 
# it to the main function that will create the buckets
client = boto3.client('s3')
try:
    main(client)
except botocore.exceptions.ClientError as err:
    print(err.response['Error']['Message'])
except botocore.exceptions.ParamValidationError as error:
    print(error)
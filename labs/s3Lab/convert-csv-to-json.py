import boto3, botocore, json, csv, io, configparser

def main(s3Client):
    print('\nStart of convert object script\n')

    ## Initialize variables for object creation
    print('Reading configuration file for bucket name...')
    config = readConfig()
    bucket_name = config['bucket_name']
    source_file_name = config['object_name'] + config['source_file_extension']
    key_name = config['key_name']+ config['source_file_extension']
    processed_file_name = config['key_name'] + config['processed_file_extension']
    contentType = config['processed_content_type']
    metaData_key = config['metaData_key']
    metaData_value = config['metaData_value']

    #### Get the object from S3
    print('\nGetting the CSV object from S3 bucket')
    csvStr = getCSVFile(s3Client, bucket_name, key_name)
    
    ## Convert the object to the new format
    print('\nConverting CSV string to JSON...')
    jsonStr = convertToJSON(csvStr)
    
    ## Uploaded the converted object to S3
    print('Creating the new JSON object on S3')
    print(createObject(s3Client, bucket_name, processed_file_name, jsonStr, contentType, {metaData_key: metaData_value}))

    print('\nEnd of convert object script\n')

def getCSVFile(s3Client, bucket, key):
    bytes_buffer = io.BytesIO()
    
    ## Start TODO 6: Download the file contents to the 
    ## bytes_buffer object so that it can be decoded to a string.
    
    s3Client.download_fileobj(
      Bucket=bucket, 
      Key=key, 
      Fileobj=bytes_buffer
    )


    ## End TODO 6

    byte_value = bytes_buffer.getvalue()
    return byte_value.decode('utf-8')

def createObject(s3Client, bucket, key, data, contentType, metadata={}):
    ## Start TODO 7: Create an S3 object with the converted data
    
    s3Client.put_object(
        Bucket=bucket, 
        Key=key,
        Body=data,
        ContentType=contentType,
        Metadata=metadata
    )

    # after running python s3Lab/convert-csv-to-json.py
    # aws s3 sync ~/environment/s3Lab/html/. s3://$mybucket/
    # aws s3api put-bucket-website --bucket $mybucket --website-configuration file://~/environment/s3Lab/website.json
    # sed -i "s/\[BUCKET\]/$mybucket/g" ~/environment/s3Lab/policy.json
    # cat ~/environment/s3Lab/policy.json
    # aws s3api put-bucket-policy --bucket $mybucket --policy file://~/environment/s3Lab/policy.json
    # region=$(curl http://169.254.169.254/latest/meta-data/placement/region -s)
    # printf "\nYou can now access the website at:\nhttp://$mybucket.s3-website-$region.amazonaws.com\n\n" OR
    # printf "\nYou can now access the website at:\nhttp://$mybucket.s3-website.$region.amazonaws.com\n\n"



    ## End TODO 7
    
    return 'Successfully Created Object\n'

def convertToJSON(input):
    jsonList = []
    keys = []
    
    csvReader = csv.reader(input.split('\n'), delimiter=",")

    for i, row in enumerate(csvReader):
        if i == 0:
            keys = row
        else:
            obj = {}
            for x, val in enumerate(keys):
                obj[val] = row[x]
            jsonList.append(obj)
    return json.dumps(jsonList, indent=4)

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
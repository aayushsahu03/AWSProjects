import json
import boto3
from datetime import datetime
import urllib

event_bridge_client = boto3.client('events')
github_url = ""
BUCKET_NAME = "johnhopkins-test-data"


def lambda_handler(event, context):

    """intialize boto3 for s3
    get datet time today

    write a code to create a bucket if empty or else move files to archival location and
    create a new folder for today
    """
    #Boto3 client
    s3 = boto3.client('s3')
    date = datetime.now().strftime(r'%Y%m%d')


    def get_s3_object_count(key):

        s3_object_count = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=key)['KeyCount']

        return s3_object_count
    

    covid_array = ['confirmed','deaths','recovered']

    for element in covid_array:
        obj_count = get_s3_object_count(BUCKET_NAME+'covid19/'+element+'/time_series_covid19_'+element+'_global.csv')
        if obj_count!=0:
            # Write code to archive old data and put new data there
            s3.put_object(Bucket=BUCKET_NAME, Key='covid19/archival/'+element+'/'+date+\
                          '/time_series_covid19_'+element+'_global.csv')
            copy_source = {
            'Bucket': BUCKET_NAME,
            'Key':'covid19/'+element+'/time_series_covid19_'+element+'_global.csv'
            }
            s3.copy( CopySource =copy_source, Bucket=BUCKET_NAME, Key='covid19/archival/'+element+'/'+\
                    date+'/time_series_covid19_'+element+'_global.csv')

        url = 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_'+\
        element+'_global.csv'
        #Get repsonse data from url
        with urllib.request.urlopen(url) as response:
            data = response.read()
            # put s3 object
            s3.put_object(Bucket=BUCKET_NAME, Key='covid19/'+
                        element+'/time_series_covid19_'+element+'_global.csv', Body=data)
        
    # Put an event to event bridge
    response = event_bridge_client.put_events(
        Entries=[
            {
                'Source': 'arn:aws:lambda:us-east-1:389303251243:function:copyToS3',
                'DetailType': 'Lambda Function Execution Status Change',
                'Detail': json.dumps({'status': 'success'})

            }
        ]
    )

    print(response.get('Entries'))
    # Check the response and handle any errors
    if response['FailedEntryCount'] > 0:
        print(f'Failed to publish event: {response.get("Entries")}')
    else:
        print("Successfully published event to EventBridge bus")

    return {
        'statuscode':200,
        'body':json.dumps('csv file copy from j.hopkins github successful')

    }
    

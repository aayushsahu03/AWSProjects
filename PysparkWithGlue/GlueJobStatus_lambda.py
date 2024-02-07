import boto3
import json
import time


def lambda_handler(event, context):

    glue_client = boto3.client('glue')

    job_name = 'glue-job-johnhopkins'
    
    #Initialize the latest run
    latest_run_status = 'STARTING'
    while latest_run_status not in ['SUCCEEDED', 'FAILED']:
        res = glue_client.get_job_runs(
                JobName=job_name,
                MaxResults=1)
        
        if len(res['JobRuns']>0):
            res['JobRuns'][0]
            latest_run = res['JobRuns'][0]
            # Get the status of the latest run
            latest_run_status = latest_run['JobRunState']
            print(f'Latest run status: {latest_run_status}')
            time.sleep(5)

        else:
            print('No job runs found')
            latest_run_status = 'FAILED' # exit the loop if there are no job runs found

    if latest_run_status == 'SUCCEEDED':
        print('Latest run completed successfully')
    else:
        print('Latest run failed')
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'GlueJob Status check lambda is completed with STATUS: {latest_run_status}')
    }

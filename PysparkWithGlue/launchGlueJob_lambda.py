import json
import boto3
import numpy as np

eventbridge = boto3.client('events')

def lambda_handler(event, context):
    
    glue = boto3.client('glue')

    # Trigger the Glue job
    job_name = 'glue-job-johnhopkins'
    response = glue.start_job_run(JobName=job_name)

    # Print the response
    print(response)
    
    response1 = eventbridge.put_events(
        Entries=[
            {
                'Source': 'arn:aws:lambda:us-east-1:389303251243:function:launchGlueJob',
                'DetailType': 'Lambda Function Execution Glue Job Status Check',
                'Detail': json.dumps({'status': 'success'})
            }
        ]
    )
    print(response1['Entries'])
    
    # Return a message indicating that the job was started
    return {
        'statusCode': 200,
        'body': 'Glue job {} started'.format(job_name)
    }


    ##Delete after this

epx_num = np.random.default_rng(seed=1).exponential(scale=0.667,size=1000)

plt.hist(epx_num, bins=40)
plt.show()
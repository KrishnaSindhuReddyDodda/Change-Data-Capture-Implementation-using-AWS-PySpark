import json
import boto3 #If you want a service or stop the service/if get the data/put the data we'll use boto3 service

def lambda_handler(event, context):
    
    
    bucketName = event["Records"][0]["s3"]["bucket"]["name"]
    fileName = event["Records"][0]["s3"]["object"]["key"]
    
    print(bucketName, fileName)
        
    glue = boto3.client('glue')    #Here I want to manipulate a "glue", so am creating a client for glue

    response = glue.start_job_run(      #start_job_run states that I want to start run the job inside the glue 
        JobName = 'glueCDC-pyspark',    # This jobname indicates the name of the job created in the glue
        Arguments = {                   # we'll refer these arguments in the glue job to invoke the filename and bucketname
            '--s3_target_path_key': fileName,
            '--s3_target_path_bucket': bucketName
        } 
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

import json
import time
import os
import math
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
import boto3
s3_resource = boto3.resource("s3")

dynamodb = boto3.client('dynamodb')
DynamoTableName = 'GlueCapacityAudit'


def lambda_handler(event, context):


    ## Assigning env variables
    def get_env_var(name):
        return os.environ[name] if name in os.environ else None
    
    account_max_glue_worker = int(get_env_var('ACCOUNT_MAX_GLUE_WORKER'))
    additional_ip_per_job = int(get_env_var('ADDITIONAL_IP_PER_JOB'))
    max_job_concurrency = int(get_env_var('MAX_JOB_CONCURRENCY'))
    max_job_workers = int(get_env_var('MAX_JOB_WORKERS'))
    max_subnet_ip = int(get_env_var('MAX_SUBNET_IP'))
    
    

    bucket = event["body"]["bucket"]
    key = event['body']['key'] ##changing variable group_key to key
    print(bucket, key)
    payer_id_list = read_object(bucket, key)
    response = event
    message_processed_date = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")
    extract_start_time = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")
    extract_end_time = datetime.now().strftime("%Y%m%d")

    # Querying dynamo to get list of jobs & status
    initiated_jobs=[]
    active_jobs=0
    pending_jobs=[]
    
    resp_items = get_payer_details('2021-07-26')
    for x in resp_items:
        if x['payer']['S'] not in initiated_jobs:
            initiated_jobs.append( x['payer']['S'])
            
        if x['active']['BOOL'] == 1:
            active_jobs=active_jobs+1
            
    print(initiated_jobs)
    print(active_jobs)
    
    # Applying capacity based on limits
    total_running_jobs = active_jobs
    total_job_workers = total_running_jobs * max_job_workers
    total_used_ip = total_job_workers + (additional_ip_per_job * total_running_jobs)
    available_ip = max_subnet_ip - total_used_ip
    
    if available_ip < account_max_glue_worker :
        available_workers = available_ip
    else:
        available_workers = account_max_glue_worker
        
    
# calculating fresh instance of jobs
    
    available_job_instance = math.floor(available_workers / max_job_workers)
    recommended_job_instance = available_job_instance
    
    # tuning job counts to include additional ip
    while (recommended_job_instance >= available_job_instance):
        recommended_job_instance = recommended_job_instance - (math.ceil((additional_ip_per_job * available_job_instance)/max_job_workers))
        
    print('available_ip :'+ str(available_ip) + '; available_workers :'+ str(available_workers) + ' ; '+'available_job_instance '+ str(available_job_instance) + ' ; '+'recommended_job_instance '+ str(recommended_job_instance))
    
    
    # getting un processed jobs to trigger
    for i in payer_id_list:
        if i not in initiated_jobs:
            pending_jobs.append(i)
    
    print(pending_jobs)
    
    # restricting based on recommended job instance
    
    pending_jobs=pending_jobs[:recommended_job_instance]
    
    

    response["body"]["message_processed_date"] = message_processed_date
    response["body"]["extract_start_time"] = extract_start_time
    response["body"]["extract_end_time"] = extract_end_time
    response["body"]["payer_count"] = len(pending_jobs)
    response["body"]["payer_id"] = pending_jobs

    return response


def get_payer_details(exec_date):
        response=dynamodb.query(
        TableName=DynamoTableName,
        IndexName='execution_date-index',
        KeyConditionExpression="execution_date = :X",
        ExpressionAttributeValues={":X" : {"S" : exec_date}}
        )
        return response["Items"]

def read_object(bucket, key):

    key = unquote_plus(key)
    data = []
    try:
        obj = s3_resource.Object(bucket, key)
        for line in obj.get()["Body"].iter_lines():
            data.append(line.decode("utf-8"))

    except ClientError:
        msg = "Error reading object: {}/{}".format(bucket, key)
        raise
    return data

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import boto3
import logging
from urllib.request import Request, urlopen
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'hannover-orchestration-dag',
    default_args=default_args,
    description='A DAG to enable end-to-end parsing of Hannover JSON data',
    schedule_interval=None
)

date = Variable.get('PARSING_EXECUTION_DATE')
def fetch_prefix_parameter(**context):
    
    s3 = boto3.client('s3',aws_access_key_id=Variable.get("AWS_SECRET_ACCESS_ID_PREPRD_HANNOVER"),aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY_PREPRD_HANNOVER"))
    logging.info(Variable.get('HANNOVER_PARSER_INPUT')+date.replace('-',''))
    response = s3.list_objects_v2(Bucket=Variable.get('AWS_S3_HANNOVER_LANDING_BUCKET'), Prefix=Variable.get('HANNOVER_PARSER_INPUT')+date.replace('-',''))
    logging.info('Got response: '+ str(response))
    if 'Contents' in response:
        logging.info('Returning contents of the response..')
        keys_list=[]
        distinct_dates=[]
        for obj in response['Contents']:
            keys_list.append(obj['Key'])
            distinct_dates.append(obj['Key'].split('/')[2].split(' ')[0])
        logging.info(str(len(keys_list)) + ' files fetched from prefix..')
        distinct_dates_set = sorted(set(distinct_dates))
        logging.info(str(len(distinct_dates_set)) + ' distinct dates fetched from source')
        logging.info('Distinct Dates: '+str(distinct_dates_set))
        return distinct_dates_set
    logging.info('No content found in the response..')
    return []


def check_folder_in_s3():
    bucket_name = Variable.get('AWS_S3_HANNOVER_LANDING_BUCKET')
    folder_name = Variable.get('HANNOVER_PARSER_INPUT')
    date_folder = Variable.get('PARSING_EXECUTION_DATE').replace('-', '')
    access_key_id = Variable.get("AWS_SECRET_ACCESS_ID_PREPRD_HANNOVER")
    secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY_PREPRD_HANNOVER")
    env_account_name = Variable.get("env_account_name")

    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key)

    folder_path = folder_name + date_folder + '/'

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

        if 'Contents' in response and len(response['Contents']) > 0:
            num_files = len(response['Contents'])
            message = f"{env_account_name} - Folder {date_folder} exists in bucket '{bucket_name}' under {folder_name} and contains {num_files} file(s)."
            return 'success', message
        else:
            message = f"{env_account_name} - Sub-folder {date_folder} does not exists in bucket '{bucket_name}' under {folder_name} or is empty."
            return 'failure', message
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            message = f"{env_account_name} - Folder {date_folder} does not exist."
        elif e.response["Error"]["Code"] == "NoSuchBucket":
            message = f"{env_account_name} - Bucket {bucket_name} does not exist."
        else:
            message = f"{env_account_name} - Error checking folder in S3 bucket: {str(e)}"
        return 'failure', message

def send_slack_notification(**context):
    status = context['task_instance'].xcom_pull(task_ids='check_folder_in_s3', key='return_value')[0]
    message = context['task_instance'].xcom_pull(task_ids='check_folder_in_s3', key='return_value')[1]
    success_emoji = " :large_green_circle: Success Report:\n"
    failure_emoji = " :red_circle: Failure Report:\n"

    if status == 'success':
        emoji = success_emoji
        message_with_emoji = f"{emoji} {message}"

    else:
        emoji = failure_emoji
        message_with_emoji = f"{emoji} {message}"

    slack_webhook_url = Variable.get('SLACK_WEBHOOK_URL')

    slack_data = {
        'channel': Variable.get('SLACK_ALERT_CHANNEL'),
        'text': message_with_emoji
    }

    req = Request(slack_webhook_url)
    req.add_header('Content-Type', 'application/json')

    response = urlopen(req, json.dumps(slack_data).encode())

    if response.status != 200:
        raise Exception(f"Failed to send Slack notification. Status code: {response.status}, Response: {response.read().decode()}")

    if status == 'success':
        return 'fetch_execution_prefixes'
    


check_folder_task = PythonOperator(
    task_id='check_folder_in_s3',
    python_callable=check_folder_in_s3,
    provide_context=True,
    dag=dag
)

send_slack_notification_task = BranchPythonOperator(
    task_id='send_slack_notification',
    python_callable=send_slack_notification,
    provide_context=True,
    dag=dag
)

fetch_execution_prefixes = PythonOperator(
    task_id='fetch_execution_prefixes',
    python_callable=fetch_prefix_parameter,
    provide_context=True,
    dag=dag
)

trigger_batch_json_parser = TriggerDagRunOperator(
    trigger_dag_id='batch-processing-hannover',
    task_id="trigger_batch_json_parser",
    conf={'date': date.replace('-', '')},
    wait_for_completion=True,
    dag=dag,
    trigger_rule="all_done"
)

trigger_merge_operation = TriggerDagRunOperator(
    trigger_dag_id='merge_task_dag',
    task_id="trigger_merge_operation",
    conf={'dates': fetch_execution_prefixes.output},
    wait_for_completion=True,
    dag=dag,
    trigger_rule="all_done"
)

trigger_redshift_load_operation = TriggerDagRunOperator(
    trigger_dag_id='incremental-load-to-redshift',
    task_id="trigger_redshift_load_operation",
    conf={'dates': fetch_execution_prefixes.output},
    wait_for_completion=True,
    dag=dag,
    trigger_rule="all_done"
)

check_folder_task >> send_slack_notification_task

send_slack_notification_task >> fetch_execution_prefixes

fetch_execution_prefixes >> trigger_batch_json_parser >> trigger_merge_operation >> trigger_redshift_load_operation

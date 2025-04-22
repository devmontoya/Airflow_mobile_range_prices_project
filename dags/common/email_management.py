import os
from datetime import datetime, timedelta
from airflow.utils.email import send_email

to_email_env = os.getenv("AIRFLOW__SMTP__SMTP_MAIL_TO")

def success_email(context):
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    task_status = 'Success'
    subject = f'Airflow DAG {dag.dag_id} - {task_status}'
    body = f"""<p>The dag <b>{dag.dag_id}</b> completed with status : <b>{task_status}</b>.</p>
                <p>The dag execution date is: {dag_run.execution_date}</p>
                <p><b>Log url of one of the tasks involved:</b> {context['task_instance'].log_url}</p>"""
    to_email = to_email_env #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

def failure_email(context):
    task_instance = context['task_instance']
    task_status = 'Failed'
    subject = f'Airflow DAG {dag.dag_id} - {task_status}'
    body = f"""<p>The dag <b>{dag.dag_id}</b> completed with status : <b>{task_status}</b>.</p>
                <p>The dag execution date is: {dag_run.execution_date}</p>
                <p><b>Log url of one of the tasks involved:</b> {context['task_instance'].log_url}</p>"""
    to_email = to_email_env #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2025, 4, 1),
    'schedule_interval' : 'None',
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
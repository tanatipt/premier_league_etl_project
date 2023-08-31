from time import sleep
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor, EmrJobFlowSensor
from airflow.providers.amazon.aws.transfers.mongo_to_s3 import MongoToS3Operator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

JOB_FLOW_OVERRIDES = {
    'Name': 'Project',
    'ReleaseLabel': 'emr-6.12.0',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        "Ec2KeyName": "flower-project",
        "Ec2SubnetId": "subnet-0d174bd9d2f479874",
        'InstanceGroups': [{
            'Name': 'MyInstanceGroup',
            'InstanceRole': 'MASTER',
            'InstanceType': 'm5.xlarge',
            'InstanceCount': 1,
        }],
        "TerminationProtected": False,
        "KeepJobFlowAliveWhenNoSteps": True
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    "VisibleToAllUsers": True,
}


TRANSFORM_STEP = [
    {
        'Name': 'Transform_Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ["spark-submit", "--master", "yarn", "--deploy-mode", "cluster", "--packages", "org.apache.hadoop:hadoop-aws:3.3.3",  "s3://iris-data-tanatip/scripts/transform.py"],
        },
    }
]

SQL_STATEMENTS = ["ALTER EXTERNAL TABLE TANATIP_DATA.PUBLIC.PL_2019_2023 REFRESH", "ALTER EXTERNAL TABLE TANATIP_DATA.PUBLIC.PL_2014_2018 REFRESH",
                  "ALTER EXTERNAL TABLE TANATIP_DATA.PUBLIC.PL_2009_2013 REFRESH", "ALTER EXTERNAL TABLE TANATIP_DATA.PUBLIC.PL_2004_2008 REFRESH", "ALTER EXTERNAL TABLE TANATIP_DATA.PUBLIC.PL_1999_2003 REFRESH",  "ALTER EXTERNAL TABLE TANATIP_DATA.PUBLIC.PL_1994_1998 REFRESH"]

JOIN_STATEMENTS = "; ".join(SQL_STATEMENTS)


@task
def get_step_id(step_ids):
    return step_ids[0]


with DAG(dag_id="emr_orchestrator", schedule="@daily", start_date=datetime(2023, 7, 27), catchup=False) as dag:
    create_job_flow = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_connection',
        region_name="ap-southeast-1"
    )

    wait_job_flow = EmrJobFlowSensor(
        task_id="wait_job_flow", job_flow_id=create_job_flow.output, target_states=["RUNNING", "WAITING"])

    """
    # Why can we access create_job_flow.output? What does EmrCreateJobFlowOperator return?
    # ValueError: XComArg only supports str lookup, received int when using step_id = run_ingest.output[0] -> Why do we have to use a function that is a task
    )"""

    run_ingest = MongoToS3Operator(task_id="run_ingest",
                                   mongo_conn_id="mongo_connection", aws_conn_id="aws_connection", mongo_collection="premier_league_statistics", mongo_db="football_data", mongo_query={}, mongo_projection={"_id": 0}, s3_bucket="iris-data-tanatip", s3_key="input_folder/premier-league-tables.txt", replace=True)

    wait_ingest = S3KeySensor(
        task_id="wait_ingest", bucket_name="iris-data-tanatip", bucket_key="input_folder/premier-league-tables.txt", aws_conn_id="aws_connection")

    run_transform = EmrAddStepsOperator(
        task_id='run_transform',
        job_flow_id=create_job_flow.output,
        aws_conn_id='aws_connection',
        steps=TRANSFORM_STEP,
    )

    wait_transform = EmrStepSensor(
        task_id="wait_transform",
        job_flow_id=create_job_flow.output,
        step_id=get_step_id(run_transform.output)
    )

    remove_job_flow = EmrTerminateJobFlowOperator(
        task_id="remove_job_flow", job_flow_id=create_job_flow.output, aws_conn_id="aws_connection")

    load_snowflake = SnowflakeOperator(
        task_id="load_snowflake", sql=JOIN_STATEMENTS, snowflake_conn_id="snowflake_connection")

    create_job_flow >> wait_job_flow >> run_ingest >> wait_ingest >> run_transform >> wait_transform >> remove_job_flow >> load_snowflake

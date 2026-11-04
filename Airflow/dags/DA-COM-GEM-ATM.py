import sys
import os
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.models import Variable
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
# from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.sensors.weekday import DayOfWeekSensor
import pytz

from airflow.exceptions import AirflowSensorTimeout, AirflowException
from botocore.exceptions import ClientError
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import fnmatch

from iics_plugin import IICSRunJobOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow_servicenow_handler import notify_api_on_failure
from airflow.utils.email import send_email
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "parameters"))

from DA_COM_GEM_ATM_PARAMETERS import (
    PARAMS_DICT,
    DATABRICKS_CONN_ID,
    SENSOR_POKE_INTERVAL_SECONDS,
    SENSOR_TIMEOUT_SECONDS,
    SNOW_ATTRIBUTES, EMAIL_RECIPIENTS
)

# import tak_events

LOG_PFX = "TAKEDA: "

snow_attributes = {
    'configuration_item': SNOW_ATTRIBUTES['configuration_item'],
    'severity':           SNOW_ATTRIBUTES['severity'],
    'ci_apms_id':         SNOW_ATTRIBUTES['ci_apms_id'],
    'ci_bsn_id':          SNOW_ATTRIBUTES['ci_bsn_id'],
    'apms_id':            'APMS-93057',
    'bsn_id':             'BSN0020621',
    'type':               'AWS-Airflow-EU',
    'env':                'PROD',
}


def svcnow_create_p3_incident(context):
    # pass
    # TO DO implement logic for Servicenow P3 Incident
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    logging.info(f"{LOG_PFX}Servicenow P3 Incident being created for {task_id} job part {dag_id} DAG usting CI {snow_attributes.get('configuration_item')}")

    notify_api_on_failure(
        context,
        payload=snow_attributes,
    )

    print("svcnow_create_p3_incident - completed")

def svcnow_create_p3_incident_err(context):
    # pass
    # TO DO implement logic for Servicenow P3 Incident - Error
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    logging.info(f"{LOG_PFX}Servicenow P3 Incident Error being created for {task_id} job part {dag_id} DAG")
    print("svcnow_create_p3_incident_err - in progress")

def globalscape_job_failure_abnormal(context):
    # pass
    # TO DO implement logic for Globalscape Job Failure
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    logging.info(f"{LOG_PFX}Globalscape Job Failure email being created for {task_id} job part {dag_id} DAG")
    print("globalscape_job_failure_abnormal - in progress")

def globalscape_job_failure_error_occurred(context):
    # pass
    # TO DO implement logic for Globalscape Job Failure - Error
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    logging.info(f"{LOG_PFX}Globalscape Job Failure Error email being created for {task_id} job part {dag_id} DAG")
    print("globalscape_job_failure_error_occurred - in progress")

def ATM_COMBINED_EXPORTS_TAKEDA_GLOBAL_SUCCESS_NEW(context):

    email_to = EMAIL_RECIPIENTS.get(af_env, [])
    email_subject = "Combined Exports Takeda Global Data refresh completed for ATM"
    email_message = """Dear All,\n\nThe Combined Exports Takeda Global data has been refreshed successfully for ATM.\n\nThanks"""

    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message

    send_email_notification(context)

def ATM_COMBINED_EXPORTS_TAKEDA_GLOBAL_FAILURE_NEW(context):

    email_to = EMAIL_RECIPIENTS.get(af_env, [])
    email_subject = "Combined Exports Takeda Global Data refresh failure for ATM"
    email_message = """Dear All,\n\nAttempts to refresh Combined Exports Takeda Global data for ATM Failed and the team is working to resolve.\n\nThanks"""

    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message

    send_email_notification(context)

def ATM_HSS_DATA_SUCCESS(context):

    email_to = EMAIL_RECIPIENTS.get(af_env, [])
    email_subject = "HSS Data refresh completed for ATM"
    email_message = """Dear All,\n\nThe Healthcare System Strengthening data has been refreshed successfully for ATM.\n\nThanks"""

    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message

    send_email_notification(context)

def ATM_HSS_DATA_FAILURE(context):

    email_to = EMAIL_RECIPIENTS.get(af_env, [])
    email_subject = "HSS Data refresh failed for ATM"
    email_message = """Dear All,\n\nAttempts to refresh Healthcare System Strengthening data  for ATM failed and the team is working to resolve.\n\nThanks"""

    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message

    send_email_notification(context)

def send_email_notification(context):
    try:
        email_to = context.get("email_to")
        email_subject = context.get("email_subject")
        email_message = context.get("email_message")
        send_email(
            to = email_to,
            subject = email_subject,
            html_content = email_message
        )
    except Exception as e:
        print(f"Exception received while sending email notification : {e}" )

def send_failure_email_notification(context):
    
    ti = context["task_instance"]
    dag_run = context["dag_run"]
    exception = context.get("exception")

    dag_id = ti.dag_id
    job_name =  ti.task_display_name         # "job name"
    # print(f"dag_id is {dag_id}")
    # print(f"job_name is {job_name}")
    task_id = ti.task_id                    # task id
    run_id =  ti.run_id                      # "job id" - identifies the run
    end_time = ti.end_date
    end_time_str_format = end_time.strftime("%B %d, %Y at %I:%M:%S %p %Z")
    error_message = str(exception) if exception else context.get("reason")

    af_env = Variable.get("af_env")  # "DEV"
    email_to = EMAIL_RECIPIENTS.get(af_env, [])  # EMAIL_RECIPIENTS defined in parameter file and imported
    email_subject = "Job Failure:  " + dag_id + " " + job_name
    email_message = ""
    email_message +=  job_name +  " Failed at " + end_time_str_format
    email_message += "<br>"
    email_message += "DAG Name: " + dag_id
    email_message += "<br>"
    email_message += "DAG Run Id: " + run_id
    email_message += "<br>"
    email_message += "Error:" + "<br>"
    email_message += error_message + "<br>"

    print(f"email_message for Job Failure notfication is {email_message}")
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

####======================================================================================

####==============On Success/ On Failure Handlers for each Task Type======================

on_success_completion_handlers = {
    '617561_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG': ['ATM-COMBINED-EXPORTS-TAKEDA-GLOBAL-SUCCESS_NEW'],
    '621923_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA': ['ATM-HSS-DATA-SUCCESS']
}

on_failure_abnormal_completion_handlers = {
'617561_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG': ['ATM-COMBINED-EXPORTS-TAKEDA-GLOBAL-FAILURE_NEW'],
'621923_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA': ['ATM-HSS-DATA-FAILURE']
}

on_failure_error_handlers = {

}

####======================================================================================

####==============Handler name to Handler callback function mappint=======================
# TO DO - try read this from a config file or from databse. Remove this section from here

handler_callback_fun_map = {
    'SVCNOW-CREATE-P3-INCIDENT': svcnow_create_p3_incident,
    'SVCNOW-CREATE-P3-INCIDENT-ERR': svcnow_create_p3_incident_err,
    'Globalscape Job Failure - Abnormal':globalscape_job_failure_abnormal,
    'Globalscape Job Failure - Error Occurred':globalscape_job_failure_error_occurred,
    'ATM-COMBINED-EXPORTS-TAKEDA-GLOBAL-SUCCESS_NEW': ATM_COMBINED_EXPORTS_TAKEDA_GLOBAL_SUCCESS_NEW,
    'ATM-COMBINED-EXPORTS-TAKEDA-GLOBAL-FAILURE_NEW': ATM_COMBINED_EXPORTS_TAKEDA_GLOBAL_FAILURE_NEW,
    'ATM-HSS-DATA-SUCCESS': ATM_HSS_DATA_SUCCESS,
    'ATM-HSS-DATA-FAILURE': ATM_HSS_DATA_FAILURE
}

####======================================================================================

def on_success_callback_S3(context):

    task_id = context["task_instance"].task_id
    ti = context["ti"]

    try:
        bucket_name = params[af_env][task_id]["bucket_name"]
        key_pattern = params[af_env][task_id]["bucket_key"]

        logging.info(f"{LOG_PFX}on_success_callback_S3 firing for {task_id} - bucket={bucket_name} pattern={key_pattern}")

        if "*" not in key_pattern:
            # Not a wildcard pattern on this sensor - nothing to resolve.
            logging.info(f"{LOG_PFX}{task_id}: bucket_key has no wildcard, nothing to resolve")
            return

        prefix = key_pattern.split("*")[0]
        hook = S3Hook(aws_conn_id="aws_default")
        candidate_keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix) or []
        matched_keys = [k for k in candidate_keys if fnmatch.fnmatch(k, key_pattern)]

        if not matched_keys:
            raise AirflowException(
                f"No S3 key under s3://{bucket_name}/{prefix} matches pattern '{key_pattern}' - "
                f"sensor {task_id} said one existed, but it's gone by the time this ran."
            )

        if len(matched_keys) == 1:
            resolved_key = matched_keys[0]
        else:
            # More than one file currently matches the wildcard - take the most
            # recently modified one rather than guessing from list order, and
            # log the rest so this is easy to spot if it wasn't expected.
            resolved_key = max(
                matched_keys,
                key=lambda k: hook.get_key(k, bucket_name=bucket_name).last_modified,
            )
            logging.warning(
                f"{LOG_PFX}{len(matched_keys)} S3 keys matched pattern '{key_pattern}' under "
                f"s3://{bucket_name}/{prefix} - using the most recently modified one: "
                f"{resolved_key}. All matches: {matched_keys}"
            )

        logging.info(f"{LOG_PFX}Resolved wildcard bucket_key for {task_id}: {resolved_key}")
        ti.xcom_push(key="resolved_key", value=resolved_key)
        logging.info(f"{LOG_PFX}Pushed resolved_key={resolved_key!r} to XCom for {task_id}")

    except Exception as e:
        logging.error(f"{LOG_PFX}on_success_callback_S3 failed to resolve wildcard key for {task_id}: {e}")
        raise

def on_failure_callback_S3(context):
    # pass
    task_id = context["task_instance"].task_id
    ti = context["ti"]

    exception = context.get("exception")

    if isinstance(exception, AirflowSensorTimeout):

        message = "Files did not arrive, skipping the task"
        logging.info(f"{LOG_PFX}{task_id}: {message}")
        print(message)


        ti.set_state(State.SUCCESS)

        dag_run = context["dag_run"]
        for downstream_task in context["task"].downstream_list:
            if downstream_task.task_id.endswith(".SUCCESS"):
                continue
            downstream_ti = dag_run.get_task_instance(downstream_task.task_id)
            if downstream_ti:
                downstream_ti.set_state(State.SKIPPED)
                logging.info(f"{LOG_PFX}{task_id}: marked downstream task {downstream_task.task_id} as SKIPPED (no file)")

    elif isinstance(exception, ClientError):
        error_code = exception.response["Error"]["Code"]
        logging.info(f"{LOG_PFX}AWS ClientError received for {task_id} job: {error_code}")
        print(f"AWS ClientError received for {task_id} job: {error_code}")

        # Sending email failure notification
        logging.info(f"{LOG_PFX}Sending Job Failure Email Notification")
        send_failure_email_notification(context)

        # Get handlers for on_failure_error_handlers
        handlers = on_success_completion_handlers.get(task_id, [])

        for handler in handlers:
            try:
                handler_callback_fun_map.get(handler)(context)
            except Exception as e:
                print(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")
                logging.info(f"{LOG_PFX}{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")

    else:
        print("Unknown failure")
        # TO DO - enhance this section

def on_success_callback_python(context):
    pass

def on_failure_callback_python(context):
    pass

def on_success_callback_lambda(context):
    pass

def on_failure_callback_lambda(context):
    pass

def on_success_callback_iics(context):
    task_id = context["task_instance"].task_id
    handlers = on_success_completion_handlers.get(task_id, [])

    for handler in handlers:
        try:
            handler_callback_fun_map.get(handler)(context)
        except Exception as e:
            print(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")
            logging.info(f"{LOG_PFX}{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_failure_callback_iics(context):
    task_id = context["task_instance"].task_id

    logging.info(f"{LOG_PFX}Creating SNOW Incident for failed task {task_id}")
    svcnow_create_p3_incident(context)
    
    # Sending email failure notification
    logging.info(f"{LOG_PFX}Sending Job Failure Email Notification")
    send_failure_email_notification(context)

    handlers = on_failure_abnormal_completion_handlers.get(task_id, [])

    for handler in handlers:
        try:
            handler_callback_fun_map.get(handler)(context)
        except Exception as e:
            print(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")
            logging.info(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")

    handlers = on_failure_error_handlers.get(task_id, [])

    for handler in handlers:
        if handler not in ['SVCNOW-CREATE-P3-INCIDENT-ERR']:
            try:
                handler_callback_fun_map.get(handler)(context)
            except Exception as e:
                print(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")
                logging.info(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_success_callback_dbx_lf_job(context):
    pass

def on_failure_callback_dbx_lf_job(context):
    task_id = context["task_instance"].task_id

    logging.info(f"{LOG_PFX}Creating SNOW Incident for failed task {task_id}")
    svcnow_create_p3_incident(context)
    
    # Sending email failure notification
    logging.info(f"{LOG_PFX}Sending Job Failure Email Notification")
    send_failure_email_notification(context)

    handlers = on_failure_abnormal_completion_handlers.get(task_id, [])

    for handler in handlers:
        try:
            handler_callback_fun_map.get(handler)(context)
        except Exception as e:
            print(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")
            logging.info(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")

    handlers = on_failure_error_handlers.get(task_id, [])

    for handler in handlers:
        if handler not in ['SVCNOW-CREATE-P3-INCIDENT-ERR']:
            try:
                handler_callback_fun_map.get(handler)(context)
            except Exception as e:
                print(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")
                logging.info(f"{LOG_PFX}{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")

params = PARAMS_DICT
af_env = Variable.get("af_env")  # "DEV"

batch_override = None


default_args = {
    'owner':          'airflow',
    'email' : '',
    "priority_weight": 1, # TO DO - to check this later
    "weight_rule": "absolute"
}

CET = pytz.timezone("Europe/Berlin")

dag = DAG(
    dag_id = "DA-COM-GEM-ATM",
    description = "Was moved by EU-PROD-TO-EU-PROD-REPO from EU-PROD-CONN (20.25.2) to EU-PROD-REPOSITORY (Latest) by onetakeda\cef9990 on 04/22/2026 06:45:57 PM -- Deployed from USVGATAGAD001 by onetakeda\MJQ2584 on 08/06/2020 02:03:13 PM -- Deployed from DEFRATAGAD001 by ONETAKEDA\sagpatil on 10/21/2020 11:25:48 PM -- Updated from DEFRATAGAD001 by ONETAKEDA\sagpatil on 02/25/2021 07:00:39 AM -- Deployed from DEFRATAGAPT001 by onetakeda\sagpatil on 08/02/2022 05:24:03 AM",
    default_args = default_args,
    schedule_interval = "*/30 * * * *",
    concurrency = 16,
    start_date = CET.localize(datetime(2026,6,24)),
    catchup = False,
    max_active_runs = 1
)



def make_s3_filewatch(task_id):
    return S3KeySensor(
        task_id=task_id,
        task_display_name=task_id,
        bucket_name=params[af_env][task_id]["bucket_name"],
        bucket_key=params[af_env][task_id]["bucket_key"],
        aws_conn_id="aws_default",
        wildcard_match=True,
        poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3,
        on_success_callback=on_success_callback_S3,
        dag=dag,
    )


def make_lakeload(task_id, filewatch_task_id):
    job_parameters = dict(params[af_env][task_id]["job_parameters"])
    if "*" in params[af_env][filewatch_task_id]["bucket_key"]:
        job_parameters["bucket_key"] = (
            f"{{{{ ti.xcom_pull(task_ids='{filewatch_task_id}', key='resolved_key') }}}}"
        )
    return DatabricksRunNowOperator(
        task_id=task_id,
        task_display_name=task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][task_id]["job_id"],
        json={"job_parameters": job_parameters},
        execution_timeout=timedelta(minutes=60),
        on_failure_callback=on_failure_callback_dbx_lf_job,
        dag=dag,
    )


def make_dbx_job(task_id, task_display_name):
    return DatabricksRunNowOperator(
        task_id=task_id,
        task_display_name=task_display_name,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][task_id]["job_id"],
        json={"job_parameters": params[af_env][task_id]["job_parameters"]},
        execution_timeout=timedelta(minutes=60),
        on_failure_callback=on_failure_callback_dbx_lf_job,
        dag=dag,
    )

ASTART = DummyOperator (task_id = "START", execution_timeout = timedelta(minutes=60), dag = dag)

A592597 = DummyOperator (task_id = "592597_DA-COM-GEM-ATM", task_display_name = "48861_DA-COM-GEM-ATM", execution_timeout = timedelta(minutes=60), dag = dag)

with TaskGroup(
    group_id="592598_DA-COM-GEM-ATM-COMMON-LAMBDA",
    tooltip="48862_DA-COM-GEM-ATM-COMMON-LAMBDA",
    prefix_group_id=False,
    dag=dag,
) as A592598:
 
    A592599 = make_s3_filewatch("ATM-HSS-DATA-FILEWATCH")
    A605627 = make_s3_filewatch("ATM-DATA-ENTRY-FILEWATCH")
    A607370 = make_s3_filewatch("ATM-MONTHLY-PROGRAM-UPDATE-FILEWATCH")
    A611392 = make_s3_filewatch("ATM-COUNTRY-MAPPING-FILEWATCH")
    A618339 = make_s3_filewatch("ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-FILEWATCH")
    A618418 = make_s3_filewatch("ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-FILEWATCH")
    A621879 = make_s3_filewatch("ATM-COMBINED-EXPORTS-FILEWATCH")
    A627194 = make_s3_filewatch("ATM-BRAND-MAPPING-FILEWATCH")
 
    A592600 = make_lakeload("ATM-HSS-DATA-LAKELOAD", "ATM-HSS-DATA-FILEWATCH")
    A605628 = make_lakeload("ATM-DATA-ENTRY-LAKELOAD", "ATM-DATA-ENTRY-FILEWATCH")
    A607371 = make_lakeload("ATM-MONTHLY-PROGRAM-UPDATE-LAKELOAD", "ATM-MONTHLY-PROGRAM-UPDATE-FILEWATCH")
    A611393 = make_lakeload("ATM-COUNTRY-MAPPING-LAKELOAD", "ATM-COUNTRY-MAPPING-FILEWATCH")
    A619058 = make_lakeload("ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-LAKELOAD", "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-FILEWATCH")
    A618419 = make_lakeload("ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-LAKELOAD", "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-FILEWATCH")
    A623230 = make_lakeload("ATM-COMBINED-EXPORTS-LAKELOAD", "ATM-COMBINED-EXPORTS-FILEWATCH")
    A627632 = make_lakeload("ATM-BRAND-MAPPING-LAKELOAD", "ATM-BRAND-MAPPING-FILEWATCH")
 
    A592598_SUCCESS = DummyOperator(
        task_id="592598.SUCCESS",
        task_display_name="48862.SUCCESS",
        execution_timeout=timedelta(minutes=60),
        dag=dag,
    )
 
    A592599 >> [A592600, A592598_SUCCESS]
    A605627 >> [A605628, A592598_SUCCESS]
    A607370 >> [A607371, A592598_SUCCESS]
    A611392 >> [A611393, A592598_SUCCESS]
    A618339 >> [A619058, A592598_SUCCESS]
    A618418 >> [A618419, A592598_SUCCESS]
    A621879 >> [A623230, A592598_SUCCESS]
    A627194 >> [A627632, A592598_SUCCESS]
 
    A605627_SUCCESS = DummyOperator(
        task_id="605627.SUCCESS",
        task_display_name="48869.SUCCESS",
        execution_timeout=timedelta(minutes=60),
        dag=dag,
    )

    A618418_SUCCESS = DummyOperator (
        task_id = "618418.SUCCESS",
        task_display_name = "50808.SUCCESS",
        execution_timeout = timedelta(minutes=60),
        dag = dag
    )

    A618339_SUCCESS = DummyOperator (
        task_id = "618339.SUCCESS",
        task_display_name = "50800.SUCCESS",
        execution_timeout = timedelta(minutes=60),
        dag = dag
    )

    A607370_SUCCESS = DummyOperator (
        task_id = "607370.SUCCESS",
        task_display_name = "50804.SUCCESS",
        execution_timeout = timedelta(minutes=60),
        dag = dag
    )

    A592599_SUCCESS = DummyOperator (
        task_id = "592599.SUCCESS",
        task_display_name = "62277.SUCCESS",
        execution_timeout = timedelta(minutes=60),
        dag = dag
    )

    A611392_SUCCESS = DummyOperator (
        task_id = "611392.SUCCESS",
        task_display_name = "48877.SUCCESS",
        execution_timeout = timedelta(minutes=60),
        dag = dag
    )

    A621879_SUCCESS = DummyOperator (
        task_id = "621879.SUCCESS",
        task_display_name = "48865.SUCCESS",
        execution_timeout = timedelta(minutes=60),
        dag = dag
    )

    A627194_SUCCESS = DummyOperator (
        task_id = "627194.SUCCESS",
        task_display_name = "48873.SUCCESS",
        execution_timeout = timedelta(minutes=60),
        dag = dag
    )

    A605628 >> [A605627_SUCCESS]
    A618419 >> [A618418_SUCCESS]
    A619058 >> [A618339_SUCCESS]
    A607371 >> [A607370_SUCCESS]
    A592600 >> [A592599_SUCCESS]
    A611393 >> [A611392_SUCCESS]
    A623230 >> [A621879_SUCCESS]
    A627632 >> [A627194_SUCCESS]



with TaskGroup(
    group_id="597445_DA-COM-GEM-ATM-LAKE-TO-LAKE-BDM-JOBS",
    tooltip="48879_DA-COM-GEM-ATM-LAKE-TO-LAKE-BDM-JOBS",
    prefix_group_id=False,
    dag=dag,
) as A597445:
 
    A597446 = make_dbx_job(
        "597446_005-BDM-LAKETOLAKE_ATM_DATA_ENTRY_STG",
        "56836_005-BDM-LAKETOLAKE_ATM_DATA_ENTRY_STG",
    )
    A626540 = make_dbx_job(
        "626540_003-BDM-LAKETOLAKE-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-STG",
        "50811_003-BDM-LAKETOLAKE-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-STG",
    )
    A624966 = make_dbx_job(
        "624966_004-BDM-LAKETOLAKE-SUSTAINIBILITY-STG",
        "56835_004-BDM-LAKETOLAKE-SUSTAINIBILITY-STG",
    )
    A616712 = make_dbx_job(
        "616712_002-BDM-LAKETOLAKE-ICLUSIG-MONTHLY-PROGRAM-UPDATE-STG",
        "50810_002-BDM-LAKETOLAKE-ICLUSIG-MONTHLY-PROGRAM-UPDATE-STG",
    )
 
    A597445_SUCCESS = DummyOperator(
        task_id="597445.SUCCESS",
        task_display_name="48879.SUCCESS",
        execution_timeout=timedelta(minutes=60),
        dag=dag,
    )
 
    [A597446, A626540, A624966, A616712] >> A597445_SUCCESS


ASTART >> [A592597]
A592597 >> [A592598, A597445]
A605627_SUCCESS >> [A597446]  
A618418_SUCCESS >> [A626540]
A618339_SUCCESS >> [A624966]
A607370_SUCCESS >> [A616712]
A626540 >> [A597445_SUCCESS]
A624966 >> [A597445_SUCCESS]
A616712 >> [A597445_SUCCESS]
A597446 >> [A597445_SUCCESS]


A616644 = DummyOperator (task_id = "616644_DA-COM-GEM-ATM-LAKE-TO-LAKE-IICS-JOBS", task_display_name = "62279_DA-COM-GEM-ATM-LAKE-TO-LAKE-IICS-JOBS", execution_timeout = timedelta(minutes=60), dag = dag)

A616644_SUCCESS = DummyOperator (task_id = "616644.SUCCESS", task_display_name = "62279.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "621923_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA"
A621923 = IICSRunJobOperator (task_id = _task_id,
                              job_args = params[af_env][_task_id]["job_args"],
                              task_display_name = "62296_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA",
                              execution_timeout = timedelta(minutes=60),
                              on_failure_callback = on_failure_callback_iics,
                              on_success_callback = on_success_callback_iics,
                              dag = dag)

_task_id = "617561_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG"
A617561 = IICSRunJobOperator(task_id = _task_id,
                             job_args = params[af_env][_task_id]["job_args"],
                             task_display_name = "69254_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG",
                             execution_timeout = timedelta(minutes=60),
                             on_failure_callback = on_failure_callback_iics,
                             on_success_callback = on_success_callback_iics,
                             dag = dag)

A592597 >> [A616644]
A616644 >> [A617561, A621923]
A592600 >> [A621923]
A623230 >> [A617561]
A621923 >> [A616644_SUCCESS]
A617561 >> [A616644_SUCCESS]

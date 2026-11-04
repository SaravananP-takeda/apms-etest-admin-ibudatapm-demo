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

# PARAMETERS module now lives in its own "parameters" subfolder under the DAGs
# folder (alongside this file) instead of next to the DAG .py files - add it to
# sys.path so the plain "from DA_COM_GEM_ATM_PARAMETERS_LAMBDA_v5 import ..." below still
# resolves, regardless of how/whether Airflow puts the dags_folder itself on
# sys.path. Resolved relative to this file's own directory so it works the
# same locally and once synced into the Airflow DAGs folder from S3.
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

####======================================================================================
# TO DO - Later the below code for event handlers will be called directly from tak_events

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

    email_to = ["premkumar.duraisamy@takeda.com;udayan.awasthi@takeda.com;arjun.dalal@takeda.com;dheeraj.jaiman@takeda.com;DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com"]
    email_subject = "Combined Exports Takeda Global Data refresh completed for ATM"
    email_message = """Dear All,\n\nThe Combined Exports Takeda Global data has been refreshed successfully for ATM.\n\nThanks"""

    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message

    send_email_notification(context)

def ATM_COMBINED_EXPORTS_TAKEDA_GLOBAL_FAILURE_NEW(context):

    email_to = ["premkumar.duraisamy@takeda.com;udayan.awasthi@takeda.com;arjun.dalal@takeda.com;dheeraj.jaiman@takeda.com;DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com"]
    email_subject = "Combined Exports Takeda Global Data refresh failure for ATM"
    email_message = """Dear All,\n\nAttempts to refresh Combined Exports Takeda Global data for ATM Failed and the team is working to resolve.\n\nThanks"""

    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message

    send_email_notification(context)

def ATM_HSS_DATA_SUCCESS(context):

    email_to = ["premkumar.duraisamy@takeda.com;udayan.awasthi@takeda.com;arjun.dalal@takeda.com;dheeraj.jaiman@takeda.com;DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com"]
    email_subject = "HSS Data refresh completed for ATM"
    email_message = """Dear All,\n\nThe Healthcare System Strengthening data has been refreshed successfully for ATM.\n\nThanks"""

    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message

    send_email_notification(context)

def ATM_HSS_DATA_FAILURE(context):

    email_to = ["premkumar.duraisamy@takeda.com;udayan.awasthi@takeda.com;arjun.dalal@takeda.com;dheeraj.jaiman@takeda.com;DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com"]
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
    # Kept here for parity with the original DAG's callback set, even though
    # this split (DA-COM-GEM-ATM-REST) has no S3KeySensor tasks of its own -
    # all FILEWATCH sensors live in DA-COM-GEM-ATM.py.
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
    dag_id = "DA-COM-GEM-ATM-REST",
    description = "Split out of DA-COM-GEM-ATM on 2026-10-14: carries every task from the "
                   "original DAG EXCEPT the FILEWATCH sensors and their directly-connected "
                   "LAKELOAD/IICS tasks, which now live in DA-COM-GEM-ATM.py "
                   "(which kept the original dag_id \"DA-COM-GEM-ATM\"). "
                   "Was moved by EU-PROD-TO-EU-PROD-REPO from EU-PROD-CONN (20.25.2) to EU-PROD-REPOSITORY (Latest) by onetakeda\\cef9990 on 04/22/2026 06:45:57 PM -- Deployed from USVGATAGAD001 by onetakeda\\MJQ2584 on 08/06/2020 02:03:13 PM -- Deployed from DEFRATAGAD001 by ONETAKEDA\\sagpatil on 10/21/2020 11:25:48 PM -- Updated from DEFRATAGAD001 by ONETAKEDA\\sagpatil on 02/25/2021 07:00:39 AM -- Deployed from DEFRATAGAPT001 by onetakeda\\sagpatil on 08/02/2022 05:24:03 AM",
    default_args = default_args,
    schedule_interval = "0 0 * * *",
    concurrency = 16,
    start_date = CET.localize(datetime(2026,6,24)),
    catchup = False,
    max_active_runs = 1
)

def test_function():
    logging.info(f"{LOG_PFX}Tidal To Airflow Migration Testing...")
    return True

# Jobs

ASTART = DummyOperator (task_id = "START", execution_timeout = timedelta(minutes=60), dag = dag)

A592597 = DummyOperator (task_id = "592597_DA-COM-GEM-ATM", task_display_name = "48861_DA-COM-GEM-ATM", execution_timeout = timedelta(minutes=60), dag = dag)

# --- ATM-ARCHIVE pipeline -----------------------------------------------------------------

A598617 = DummyOperator (task_id = "598617_GEM-DATABRICKS-NOTEBOOK-ATM-ARCHIVE", task_display_name = "51212_GEM-DATABRICKS-NOTEBOOK-ATM-ARCHIVE", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "598618_010-DATABRICKS-RUN-NOTEBOOK-ATM-ARCHIVE"
A598618 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "51213_010-DATABRICKS-RUN-NOTEBOOK-ATM-ARCHIVE",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A600843 = PythonOperator (task_id = "600843_020-ASSOCIATED-CALLBACK-JOB-MONITOR-ATM-ARCHIVE", python_callable = test_function, task_display_name = "51214_020-ASSOCIATED-CALLBACK-JOB-MONITOR-ATM-ARCHIVE", execution_timeout = timedelta(minutes=60), dag = dag)

A598617_SUCCESS = DummyOperator (task_id = "598617.SUCCESS", task_display_name = "51212.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A598370 = DummyOperator (task_id = "598370_GEM-DATABRICKS-CALLBACK-ATM-ARCHIVE", task_display_name = "51143_GEM-DATABRICKS-CALLBACK-ATM-ARCHIVE", execution_timeout = timedelta(minutes=60), dag = dag)

A598371 = PythonOperator (task_id = "598371_010-SHOW-DATABRICKS-JOB-STATUS-ATM-ARCHIVE", python_callable = test_function, task_display_name = "51144_010-SHOW-DATABRICKS-JOB-STATUS-ATM-ARCHIVE", execution_timeout = timedelta(minutes=60), dag = dag)

A598370_SUCCESS = DummyOperator (task_id = "598370.SUCCESS", task_display_name = "51143.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# --- DATA-COLLECTION-TEMPLATE pipeline -----------------------------------------------------

A617521 = DummyOperator (task_id = "617521_GEM-DATABRICKS-NOTEBOOK-ATM-DATA-COLLECTION-TEMPLATE", task_display_name = "51215_GEM-DATABRICKS-NOTEBOOK-ATM-DATA-COLLECTION-TEMPLATE", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "627861_010-DATABRICKS-RUN-NOTEBOOK-DATA-COLLECTION-TEMPLATE"
A627861 = DatabricksRunNowOperator (task_id = _task_id,
                          task_display_name = "51216_010-DATABRICKS-RUN-NOTEBOOK-DATA-COLLECTION-TEMPLATE",
                          databricks_conn_id = DATABRICKS_CONN_ID,
                          job_id = params[af_env][_task_id]["job_id"],
                          json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                          execution_timeout = timedelta(minutes=60),
                          on_failure_callback = on_failure_callback_dbx_lf_job,
                          dag = dag)

A625610 = PythonOperator (task_id = "625610_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DATA-COLLECTION-TEMPLATE", python_callable = test_function, task_display_name = "51217_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DATA-COLLECTION-TEMPLATE", execution_timeout = timedelta(minutes=60), dag = dag)

A617521_SUCCESS = DummyOperator (task_id = "617521.SUCCESS", task_display_name = "51215.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A627738 = DummyOperator (task_id = "627738_GEM-DATABRICKS-CALLBACK-DATA-COLLECTION-TEMPLATE", task_display_name = "51145_GEM-DATABRICKS-CALLBACK-DATA-COLLECTION-TEMPLATE", execution_timeout = timedelta(minutes=60), dag = dag)

A628749 = PythonOperator (task_id = "628749_010-SHOW-DATABRICKS-JOB-STATUS-DATA-COLLECTION-TEMPLATE", python_callable = test_function, task_display_name = "51146_010-SHOW-DATABRICKS-JOB-STATUS-DATA-COLLECTION-TEMPLATE", execution_timeout = timedelta(minutes=60), dag = dag)

A627738_SUCCESS = DummyOperator (task_id = "627738.SUCCESS", task_display_name = "51145.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

ASTART >> [A592597]
A592597 >> [A598370, A598617, A617521, A627738]

A598617 >> [A598618]
A598618 >> [A600843]
A598370 >> [A598371]
A598371 >> [A598370_SUCCESS]
A598370_SUCCESS >> [A600843]
A600843 >> [A598617_SUCCESS]

A617521 >> [A627861]
A627861 >> [A625610]
A627738 >> [A628749]
A628749 >> [A627738_SUCCESS]
A627738_SUCCESS >> [A625610]
A625610 >> [A617521_SUCCESS]

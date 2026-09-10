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
    SNOW_ATTRIBUTES,
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
    # S3KeySensor's poke() only ever returns True/False - even with
    # wildcard_match=True, it never records WHICH key actually matched
    # "<file_name>_*.xlsx". This resolves the real key right when the sensor
    # succeeds and pushes it to XCom (under a named key, not the default
    # return_value, since a sensor's own return value is just the poke bool)
    # so the Databricks job triggered right after this sensor can pull the
    # real file name instead of the still-wildcarded value from PARAMS_DICT.
    #
    # Deliberately done here rather than as a separate resolver task: a
    # PythonOperator between the sensor and the Databricks job is a new node
    # the scheduler has to pick up for every DAG run, and if that task
    # instance isn't created in time (e.g. a run already in flight when this
    # was deployed), its downstream sees "dependencies not met" and the
    # Databricks job never fires. A callback runs synchronously as part of
    # the sensor's own task instance - there's no separate node to schedule.
    #
    # NOTE: Airflow does not fail (or even flag) a task if its
    # on_success_callback raises - the task instance still shows SUCCESS.
    # That means a bug in here fails *silently*: the Databricks job just
    # gets a "None" bucket_key with nothing visibly wrong upstream. So this
    # logs at every step and re-raises on failure - the sensor task's own
    # log tab is where to look if bucket_key ever comes back empty again.
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
        # Files for this DAG land at ad hoc times during the day - the sensor is
        # given until the SENSOR_TIMEOUT cutoff (23:59 CET/CEST) to see the file
        # show up. Timing out just means nothing arrived *today* - not a real
        # failure, and NOT an incident, so the on_failure_abnormal_completion_handlers
        # (SNOW incident / email) are deliberately skipped for this branch.
        message = "Files did not arrive, skipping the task"
        logging.info(f"{LOG_PFX}{task_id}: {message}")
        print(message)

        # Sensor itself is forced to SUCCESS (not FAILED) so this shows as a
        # normal daily outcome rather than a failure.
        ti.set_state(State.SUCCESS)

        # There's nothing for the real processing task downstream of this sensor
        # to do without a file, so mark it SKIPPED explicitly rather than letting
        # it run (it would otherwise run anyway, since its trigger rule only
        # sees the sensor's now-SUCCESS state). The shared "*.SUCCESS" checkpoint
        # markers are left alone - those just record "this branch was attempted",
        # not "a file arrived" - so they still proceed normally.
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

# default_args = {
#     'owner':          'airflow',
#     'email' : '',
#     "priority_weight": 1, # TO DO - to check this later
#     "weight_rule": "absolute",
#     'on_failure_callback': lambda context: notify_api_on_failure(
#         context,
#         payload=snow_attributes,
#     ),
# }

default_args = {
    'owner':          'airflow',
    'email' : '',
    "priority_weight": 1, # TO DO - to check this later
    "weight_rule": "absolute"
}

# Files this DAG waits on (via the S3KeySensors below) land at ad hoc times during
# the day rather than on a fixed schedule of their own, so the DAG itself now runs
# once daily and each sensor pokes throughout the day for its file. "CET" below is
# implemented as the Europe/Berlin zone (not pytz's fixed-offset "CET") so both the
# DAG's daily kick-off and the sensors' 23:59 cutoff (SENSOR_TIMEOUT_SECONDS, from the
# parameters module) shift correctly across the CET/CEST DST change instead of
# drifting by an hour twice a year.
CET = pytz.timezone("Europe/Berlin")

dag = DAG(
    dag_id = "DA-COM-GEM-ATM",
    description = "Was moved by EU-PROD-TO-EU-PROD-REPO from EU-PROD-CONN (20.25.2) to EU-PROD-REPOSITORY (Latest) by onetakeda\cef9990 on 04/22/2026 06:45:57 PM -- Deployed from USVGATAGAD001 by onetakeda\MJQ2584 on 08/06/2020 02:03:13 PM -- Deployed from DEFRATAGAD001 by ONETAKEDA\sagpatil on 10/21/2020 11:25:48 PM -- Updated from DEFRATAGAD001 by ONETAKEDA\sagpatil on 02/25/2021 07:00:39 AM -- Deployed from DEFRATAGAPT001 by onetakeda\sagpatil on 08/02/2022 05:24:03 AM",
    default_args = default_args,
    # Runs once daily at 00:00 CET/CEST - files can arrive any time after that, and the
    # sensors below poke until the 23:50 CET/CEST cutoff waiting for them.
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

# A598618 = PythonOperator (task_id = "598618_010-DATABRICKS-RUN-NOTEBOOK-ATM-ARCHIVE", python_callable = test_function, task_display_name = "51213_010-DATABRICKS-RUN-NOTEBOOK-ATM-ARCHIVE", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "598618_010-DATABRICKS-RUN-NOTEBOOK-ATM-ARCHIVE"
A598618 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "51213_010-DATABRICKS-RUN-NOTEBOOK-ATM-ARCHIVE",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A618418_SUCCESS = DummyOperator (task_id = "618418.SUCCESS", task_display_name = "50808.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A626540 = PythonOperator (task_id = "626540_003-BDM-LAKETOLAKE-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-STG", python_callable = test_function, task_display_name = "50811_003-BDM-LAKETOLAKE-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-STG", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "626540_003-BDM-LAKETOLAKE-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-STG"
A626540 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50811_003-BDM-LAKETOLAKE-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-STG",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A627194_SUCCESS = DummyOperator (task_id = "627194.SUCCESS", task_display_name = "48873.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A600843 = PythonOperator (task_id = "600843_020-ASSOCIATED-CALLBACK-JOB-MONITOR-ATM-ARCHIVE", python_callable = test_function, task_display_name = "51214_020-ASSOCIATED-CALLBACK-JOB-MONITOR-ATM-ARCHIVE", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-COMBINED-EXPORTS-FILEWATCH"
A621879 = S3KeySensor (task_id = _task_id,
                        task_display_name = "ATM-COMBINED-EXPORTS-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

# A624966 = PythonOperator (task_id = "624966_004-BDM-LAKETOLAKE-SUSTAINIBILITY-STG", python_callable = test_function, task_display_name = "56835_004-BDM-LAKETOLAKE-SUSTAINIBILITY-STG", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "624966_004-BDM-LAKETOLAKE-SUSTAINIBILITY-STG"
A624966 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56835_004-BDM-LAKETOLAKE-SUSTAINIBILITY-STG",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A616644 = DummyOperator (task_id = "616644_DA-COM-GEM-ATM-LAKE-TO-LAKE-IICS-JOBS", task_display_name = "62279_DA-COM-GEM-ATM-LAKE-TO-LAKE-IICS-JOBS", execution_timeout = timedelta(minutes=60), dag = dag)

A598370 = DummyOperator (task_id = "598370_GEM-DATABRICKS-CALLBACK-ATM-ARCHIVE", task_display_name = "51143_GEM-DATABRICKS-CALLBACK-ATM-ARCHIVE", execution_timeout = timedelta(minutes=60), dag = dag)

A605627_SUCCESS = DummyOperator (task_id = "605627.SUCCESS", task_display_name = "48869.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-DATA-ENTRY-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["ATM-DATA-ENTRY-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ATM-DATA-ENTRY-FILEWATCH', key='resolved_key') }}"
A605628 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "ATM-DATA-ENTRY-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A598617_SUCCESS = DummyOperator (task_id = "598617.SUCCESS", task_display_name = "51212.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-FILEWATCH"
A618418 = S3KeySensor (task_id = _task_id,
                        task_display_name = "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A592599_SUCCESS = DummyOperator (task_id = "592599.SUCCESS", task_display_name = "62277.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-DATA-ENTRY-FILEWATCH"
A605627 = S3KeySensor (task_id = _task_id,
                        task_display_name = "ATM-DATA-ENTRY-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A598371 = PythonOperator (task_id = "598371_010-SHOW-DATABRICKS-JOB-STATUS-ATM-ARCHIVE", python_callable = test_function, task_display_name = "51144_010-SHOW-DATABRICKS-JOB-STATUS-ATM-ARCHIVE", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-COUNTRY-MAPPING-FILEWATCH"
A611392 = S3KeySensor (task_id = _task_id,
                        task_display_name = "ATM-COUNTRY-MAPPING-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A627738_SUCCESS = DummyOperator (task_id = "627738.SUCCESS", task_display_name = "51145.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A616712 = PythonOperator (task_id = "616712_002-BDM-LAKETOLAKE-ICLUSIG-MONTHLY-PROGRAM-UPDATE-STG", python_callable = test_function, task_display_name = "50810_002-BDM-LAKETOLAKE-ICLUSIG-MONTHLY-PROGRAM-UPDATE-STG", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "616712_002-BDM-LAKETOLAKE-ICLUSIG-MONTHLY-PROGRAM-UPDATE-STG"
A616712 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50810_002-BDM-LAKETOLAKE-ICLUSIG-MONTHLY-PROGRAM-UPDATE-STG",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-FILEWATCH', key='resolved_key') }}"
A619058 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-FILEWATCH"
A618339 = S3KeySensor (task_id = _task_id,
                        task_display_name = "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A592598_SUCCESS = DummyOperator (task_id = "592598.SUCCESS", task_display_name = "48862.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-MONTHLY-PROGRAM-UPDATE-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["ATM-MONTHLY-PROGRAM-UPDATE-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ATM-MONTHLY-PROGRAM-UPDATE-FILEWATCH', key='resolved_key') }}"
A607371 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "ATM-MONTHLY-PROGRAM-UPDATE-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "ATM-COUNTRY-MAPPING-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["ATM-COUNTRY-MAPPING-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ATM-COUNTRY-MAPPING-FILEWATCH', key='resolved_key') }}"
A611393 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "ATM-COUNTRY-MAPPING-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A592597 = DummyOperator (task_id = "592597_DA-COM-GEM-ATM", task_display_name = "48861_DA-COM-GEM-ATM", execution_timeout = timedelta(minutes=60), dag = dag)

A618339_SUCCESS = DummyOperator (task_id = "618339.SUCCESS", task_display_name = "50800.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-MONTHLY-PROGRAM-UPDATE-FILEWATCH"
A607370 = S3KeySensor (task_id = _task_id,
                        task_display_name = "ATM-MONTHLY-PROGRAM-UPDATE-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A607370_SUCCESS = DummyOperator (task_id = "607370.SUCCESS", task_display_name = "50804.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-BRAND-MAPPING-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["ATM-BRAND-MAPPING-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ATM-BRAND-MAPPING-FILEWATCH', key='resolved_key') }}"
A627632 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "ATM-BRAND-MAPPING-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A616644_SUCCESS = DummyOperator (task_id = "616644.SUCCESS", task_display_name = "62279.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-BRAND-MAPPING-FILEWATCH"
A627194 = S3KeySensor (task_id = _task_id,
                        task_display_name = "ATM-BRAND-MAPPING-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

_task_id = "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-FILEWATCH', key='resolved_key') }}"
A618419 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A617521 = DummyOperator (task_id = "617521_GEM-DATABRICKS-NOTEBOOK-ATM-DATA-COLLECTION-TEMPLATE", task_display_name = "51215_GEM-DATABRICKS-NOTEBOOK-ATM-DATA-COLLECTION-TEMPLATE", execution_timeout = timedelta(minutes=60), dag = dag)

ASTART = DummyOperator (task_id = "START", execution_timeout = timedelta(minutes=60), dag = dag)

A621879_SUCCESS = DummyOperator (task_id = "621879.SUCCESS", task_display_name = "48865.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-COMBINED-EXPORTS-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["ATM-COMBINED-EXPORTS-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ATM-COMBINED-EXPORTS-FILEWATCH', key='resolved_key') }}"
A623230 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "ATM-COMBINED-EXPORTS-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A628749 = PythonOperator (task_id = "628749_010-SHOW-DATABRICKS-JOB-STATUS-DATA-COLLECTION-TEMPLATE", python_callable = test_function, task_display_name = "51146_010-SHOW-DATABRICKS-JOB-STATUS-DATA-COLLECTION-TEMPLATE", execution_timeout = timedelta(minutes=60), dag = dag)

# A627861 = PythonOperator (task_id = "627861_010-DATABRICKS-RUN-NOTEBOOK-DATA-COLLECTION-TEMPLATE", python_callable = test_function, task_display_name = "51216_010-DATABRICKS-RUN-NOTEBOOK-DATA-COLLECTION-TEMPLATE", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "627861_010-DATABRICKS-RUN-NOTEBOOK-DATA-COLLECTION-TEMPLATE"
A627861 = DatabricksRunNowOperator (task_id = _task_id,
                          task_display_name = "51216_010-DATABRICKS-RUN-NOTEBOOK-DATA-COLLECTION-TEMPLATE",
                          databricks_conn_id = DATABRICKS_CONN_ID,
                          job_id = params[af_env][_task_id]["job_id"],
                          json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                          execution_timeout = timedelta(minutes=60),
                          on_failure_callback = on_failure_callback_dbx_lf_job,
                          dag = dag)

A611392_SUCCESS = DummyOperator (task_id = "611392.SUCCESS", task_display_name = "48877.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A597446 = PythonOperator (task_id = "597446_005-BDM-LAKETOLAKE_ATM_DATA_ENTRY_STG", python_callable = test_function, task_display_name = "56836_005-BDM-LAKETOLAKE_ATM_DATA_ENTRY_STG", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "597446_005-BDM-LAKETOLAKE_ATM_DATA_ENTRY_STG"
A597446 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56836_005-BDM-LAKETOLAKE_ATM_DATA_ENTRY_STG",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

A598370_SUCCESS = DummyOperator (task_id = "598370.SUCCESS", task_display_name = "51143.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A625610 = PythonOperator (task_id = "625610_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DATA-COLLECTION-TEMPLATE", python_callable = test_function, task_display_name = "51217_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DATA-COLLECTION-TEMPLATE", execution_timeout = timedelta(minutes=60), dag = dag)

A627738 = DummyOperator (task_id = "627738_GEM-DATABRICKS-CALLBACK-DATA-COLLECTION-TEMPLATE", task_display_name = "51145_GEM-DATABRICKS-CALLBACK-DATA-COLLECTION-TEMPLATE", execution_timeout = timedelta(minutes=60), dag = dag)

A597445_SUCCESS = DummyOperator (task_id = "597445.SUCCESS", task_display_name = "48879.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A617521_SUCCESS = DummyOperator (task_id = "617521.SUCCESS", task_display_name = "51215.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A597445 = DummyOperator (task_id = "597445_DA-COM-GEM-ATM-LAKE-TO-LAKE-BDM-JOBS", task_display_name = "48879_DA-COM-GEM-ATM-LAKE-TO-LAKE-BDM-JOBS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "ATM-HSS-DATA-FILEWATCH"
A592599 = S3KeySensor (task_id = _task_id,
                        task_display_name = "ATM-HSS-DATA-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A598617 = DummyOperator (task_id = "598617_GEM-DATABRICKS-NOTEBOOK-ATM-ARCHIVE", task_display_name = "51212_GEM-DATABRICKS-NOTEBOOK-ATM-ARCHIVE", execution_timeout = timedelta(minutes=60), dag = dag)

A592598 = DummyOperator (task_id = "592598_DA-COM-GEM-ATM-COMMON-LAMBDA", task_display_name = "48862_DA-COM-GEM-ATM-COMMON-LAMBDA", execution_timeout = timedelta(minutes=60), dag = dag)

# A617561 = PythonOperator (task_id = "617561_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG", python_callable = test_function, task_display_name = "69254_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "617561_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG"
A617561 = IICSRunJobOperator(task_id = _task_id,
                             job_args = params[af_env][_task_id]["job_args"],
                             task_display_name = "69254_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG",
                             execution_timeout = timedelta(minutes=60),
                             on_failure_callback = on_failure_callback_iics,
                             on_success_callback = on_success_callback_iics,
                             dag = dag)

_task_id = "ATM-HSS-DATA-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["ATM-HSS-DATA-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ATM-HSS-DATA-FILEWATCH', key='resolved_key') }}"
A592600 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "ATM-HSS-DATA-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    dag = dag)

# A621923 = PythonOperator (task_id = "621923_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA", python_callable = test_function, task_display_name = "62296_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "621923_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA"
A621923 = IICSRunJobOperator (task_id = _task_id,
                              job_args = params[af_env][_task_id]["job_args"],
                              task_display_name = "62296_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA",
                              execution_timeout = timedelta(minutes=60),
                              on_failure_callback = on_failure_callback_iics,
                              on_success_callback = on_success_callback_iics,
                              dag = dag)

# Dependency Management

A598618 >> [A600843]
A618418_SUCCESS >> [A626540]
A626540 >> [A597445_SUCCESS]
A600843 >> [A598617_SUCCESS]
A621879 >> [A592598_SUCCESS, A623230]
A624966 >> [A597445_SUCCESS]
A616644 >> [A617561, A621923]
A598370 >> [A598371]
A605627_SUCCESS >> [A597446]
A605628 >> [A605627_SUCCESS]
A618418 >> [A592598_SUCCESS, A618419]
A605627 >> [A592598_SUCCESS, A605628]
A598371 >> [A598370_SUCCESS]
A611392 >> [A592598_SUCCESS, A611393]
A627738_SUCCESS >> [A625610]
A616712 >> [A597445_SUCCESS]
A619058 >> [A618339_SUCCESS]
A618339 >> [A592598_SUCCESS, A619058]
A607371 >> [A607370_SUCCESS]
A611393 >> [A611392_SUCCESS]
A592597 >> [A592598, A597445, A598370, A598617, A616644, A617521, A627738]
A618339_SUCCESS >> [A624966]
A607370 >> [A592598_SUCCESS, A607371]
A607370_SUCCESS >> [A616712]
A627632 >> [A627194_SUCCESS]
A627194 >> [A592598_SUCCESS, A627632]
A618419 >> [A618418_SUCCESS]
A617521 >> [A627861]
ASTART >> [A592597]
A623230 >> [A621879_SUCCESS]
A623230 >> [A617561]
A628749 >> [A627738_SUCCESS]
A627861 >> [A625610]
A597446 >> [A597445_SUCCESS]
A598370_SUCCESS >> [A600843]
A625610 >> [A617521_SUCCESS]
A627738 >> [A628749]
A597445 >> [A597446, A616712, A624966, A626540]
A592599 >> [A592598_SUCCESS, A592600]
A598617 >> [A598618]
A592598 >> [A592599, A605627, A607370, A611392, A618339, A618418, A621879, A627194]
A617561 >> [A616644_SUCCESS]
A592600 >> [A592599_SUCCESS]
A592600 >> [A621923]
A621923 >> [A616644_SUCCESS]

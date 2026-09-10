import sys
import os
import logging
import fnmatch
from airflow.models import Variable
from airflow.utils.state import State
from airflow.exceptions import AirflowSensorTimeout, AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

from airflow_servicenow_handler import notify_api_on_failure
from airflow.utils.email import send_email

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "parameters"))

from DA_COM_GEM_VN_PARAMETERS import EMAIL_RECIPIENTS, PARAMS_DICT, SNOW_ATTRIBUTES_DEFAULTS

DATABRICKS_CONN_ID = "Databricks_BDMToDBX_Default"

LOG_PFX = "TAKEDA: "

params = PARAMS_DICT
af_env = Variable.get("af_env")  # "DEV"
vn_email_recipients = EMAIL_RECIPIENTS
VN_DBX_EMAIL_TO = [vn_email_recipients.get(af_env, "")]

snow_attributes = {
    'configuration_item': SNOW_ATTRIBUTES_DEFAULTS['configuration_item'],
    'severity':           SNOW_ATTRIBUTES_DEFAULTS['severity'],
    'ci_apms_id':         SNOW_ATTRIBUTES_DEFAULTS['ci_apms_id'],
    'ci_bsn_id':          SNOW_ATTRIBUTES_DEFAULTS['ci_bsn_id'],
    'apms_id':            'APMS-93057',
    'bsn_id':             'BSN0020621',
    'type':               'AWS-Airflow-EU',
    'env':                'PROD',
}

####======================================================================================
# TO DO - Later the below code for event handlers will be called directly from tak_events

def svcnow_create_p3_incident(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    logging.info(f"{LOG_PFX}Servicenow P3 Incident being created for {task_id} job part {dag_id} DAG usting CI {snow_attributes.get('configuration_item')}")

    notify_api_on_failure(
        context,
        payload=snow_attributes,
    )

    print("svcnow_create_p3_incident - completed")

def svcnow_create_p3_incident_err(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    logging.info(f"{LOG_PFX}Servicenow P3 Incident Error being created for {task_id} job part {dag_id} DAG")
    print("svcnow_create_p3_incident_err - in progress")

def VN_WHS_SALES_DETAIL_LOAD_SUCCESS(context):
    email_to = VN_DBX_EMAIL_TO
    email_subject = "Wholesaler Sales Detail Data Refresh Completion for Vietnam"
    email_message = """Dear All,\n\nThe Wholesale Sales Detail data has been refreshed successfully for Vietnam.\n\nThanks"""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def VN_WHS_SALES_DETAIL_LOAD_FAILURE(context):
    email_to = VN_DBX_EMAIL_TO
    email_subject = "Wholesaler Sales Detail Data Refresh Failure for Vietnam"
    email_message = """Dear All,\n\nAttempts to refresh Wholesaler Sales Detail data for Vietnam failed and the team is working to resolve. \n\nThanks"""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def VN_TGT_SALES_LOAD_SUCCESS(context):
    email_to = VN_DBX_EMAIL_TO
    email_subject = "Sales Target Data Refresh Completion for  Vietnam"
    email_message = """Dear All,\n\nThe Sales Target data has been refreshed successfully for Vietnam.\n\nThanka"""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def VN_TGT_SALES_LOAD_FAILURE(context):
    email_to = VN_DBX_EMAIL_TO
    email_subject = "Sales Target Data Refresh Failure for Vietnam"
    email_message = """Dear All,\n\nAttempts to refresh Sales Target data for Vietnam failed and the team is working to resolve."""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def VN_EPLUS_TXN_WHS_SALES_LOAD_SUCCESS(context):
    email_to = VN_DBX_EMAIL_TO
    email_subject = "Eplus Wholesaler Sales data refresh successful for Vietnam"
    email_message = """Dear All,\n\nEplus Wholesaler Sales data has been refreshed successfully for Vietnam.\n\nThanks"""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def VN_EPLUS_TXN_WHS_SALES_LOAD_FAILURE(context):
    email_to = VN_DBX_EMAIL_TO
    email_subject = "Eplus Wholesaler Sales data refresh failure for Vietnam"
    email_message = """Dear All,\n\nAttempts to refresh Eplus Wholesaler Sales data for Vietnam failed and the team is working to resolve. \n\nThanks"""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def VN_TERR_ALIGNMENT_LOAD_SUCCESS(context):
    email_to = VN_DBX_EMAIL_TO
    email_subject = "Territory Alignment Data Refresh Completion for  Vietnam"
    email_message = """Dear All,\n\nThe Territory Alignment data has been refreshed successfully for Vietnam.\n\nThanks"""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def VN_TERR_ALIGNMENT_LOAD_FAILURE(context):
    email_to = VN_DBX_EMAIL_TO
    email_subject = "Territory Alignment Data Refresh Failure for Vietnam"
    email_message = """Dear All,\n\nAttempts to refresh Territory Alignment data for Vietnam failed and the team is working to resolve. \n\nThanks"""
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
# TO DO - try read this from a config file or from databse. Remove this section from here

on_success_completion_handlers = {
    '599619_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL': ['VN_WHS_SALES_DETAIL_LOAD_SUCCESS'],
    '599961_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL': ['VN_WHS_SALES_DETAIL_LOAD_SUCCESS'],
    '604687_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL': ['VN_WHS_SALES_DETAIL_LOAD_SUCCESS'],
    '608175_003-VN-HUBTOMART-TXN_TGT_SALES': ['VN_TGT_SALES_LOAD_SUCCESS'],
    '624973_010-DATABRICKS-RUN-NOTEBOOK-HUBTOMART-TXN-EPLUS-WHS-SALES': ['RUN-VN-EPLUS-TXN-WHS-SALES-LOAD-SUCCESS'],
    '624482_002-VN-HUBTOMART-REF_TERR_ALIGNMENT': ['VN_TERR_ALIGNMENT_LOAD_SUCCESS']
}

on_failure_abnormal_completion_handlers = {
    '599619_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL': ['VN_WHS_SALES_DETAIL_LOAD_FAILURE'],
    '599961_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL': ['VN_WHS_SALES_DETAIL_LOAD_FAILURE'],
    '604687_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL': ['VN_WHS_SALES_DETAIL_LOAD_FAILURE'],
    '608175_003-VN-HUBTOMART-TXN_TGT_SALES': ['VN_TGT_SALES_LOAD_FAILURE'],
    '624973_010-DATABRICKS-RUN-NOTEBOOK-HUBTOMART-TXN-EPLUS-WHS-SALES': ['RUN-VN-EPLUS-TXN-WHS-SALES-LOAD-FAILURE'],
    '624482_002-VN-HUBTOMART-REF_TERR_ALIGNMENT': ['VN_TERR_ALIGNMENT_LOAD_FAILURE']
}

on_failure_error_handlers = {

}

####======================================================================================

####==============Handler name to Handler callback function mappint=======================

handler_callback_fun_map = {
    'VN_WHS_SALES_DETAIL_LOAD_SUCCESS': VN_WHS_SALES_DETAIL_LOAD_SUCCESS,
    'VN_WHS_SALES_DETAIL_LOAD_FAILURE': VN_WHS_SALES_DETAIL_LOAD_FAILURE,
    'VN_TGT_SALES_LOAD_SUCCESS': VN_TGT_SALES_LOAD_SUCCESS,
    'VN_TGT_SALES_LOAD_FAILURE': VN_TGT_SALES_LOAD_FAILURE,
    'RUN-VN-EPLUS-TXN-WHS-SALES-LOAD-SUCCESS': VN_EPLUS_TXN_WHS_SALES_LOAD_SUCCESS,
    'RUN-VN-EPLUS-TXN-WHS-SALES-LOAD-FAILURE': VN_EPLUS_TXN_WHS_SALES_LOAD_FAILURE,
    'VN_TERR_ALIGNMENT_LOAD_SUCCESS': VN_TERR_ALIGNMENT_LOAD_SUCCESS,
    'VN_TERR_ALIGNMENT_LOAD_FAILURE': VN_TERR_ALIGNMENT_LOAD_FAILURE
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
    task_id = context["task_instance"].task_id
    ti = context["ti"]

    exception = context.get("exception")

    if isinstance(exception, AirflowSensorTimeout):
        # Files land at ad hoc times during the day - the sensor is given until the
        # SENSOR_TIMEOUT cutoff to see the file show up. Timing out just means nothing
        # arrived *today* - not a real failure, and NOT an incident, so
        # on_failure_abnormal_completion_handlers (SNOW incident / email) are
        # deliberately skipped for this branch.
        message = "Files did not arrive, skipping the task"
        logging.info(f"{LOG_PFX}{task_id}: {message}")
        print(message)

        # Sensor itself is forced to SUCCESS (not FAILED) so this shows as a normal
        # daily outcome rather than a failure.
        ti.set_state(State.SUCCESS)

        # There's nothing for the real processing task downstream of this sensor to do
        # without a file, so mark it SKIPPED explicitly rather than letting it run (it
        # would otherwise run anyway, since its trigger rule only sees the sensor's
        # now-SUCCESS state). The shared "*.SUCCESS" checkpoint markers are left alone -
        # those just record "this branch was attempted", not "a file arrived" - so they
        # still proceed normally.
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
    task_id = context["task_instance"].task_id
    handlers = on_success_completion_handlers.get(task_id, [])

    for handler in handlers:
        try:
            handler_callback_fun_map.get(handler)(context)
        except Exception as e:
            print(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")
            logging.info(f"{LOG_PFX}{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")

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

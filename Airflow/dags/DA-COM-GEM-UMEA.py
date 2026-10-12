import sys
import os
import logging
import fnmatch
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
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.sensors.weekday import DayOfWeekSensor
import pytz

from airflow.exceptions import AirflowSensorTimeout, AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow_servicenow_handler import notify_api_on_failure
from airflow.utils.email import send_email


LOG_PFX = "TAKEDA: "

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "parameters"))

from DA_COM_GEM_UMEA_PARAMETERS import PARAMS_DICT, EMAIL_RECIPIENTS, S3_SENSOR_POKE_INTERVAL_SECONDS, S3_SENSOR_TIMEOUT_SECONDS, PYTHON_SENSOR_POKE_INTERVAL_SECONDS, PYTHON_SENSOR_TIMEOUT_SECONDS, SNOW_ATTRIBUTES_DEFAULTS

DATABRICKS_CONN_ID = "Databricks_BDMToDBX_Default"

params = PARAMS_DICT
af_env = Variable.get("af_env")  # "DEV"
umea_email_recipients = EMAIL_RECIPIENTS
UMEA_DBX_EMAIL_TO = [umea_email_recipients.get(af_env, "")]

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


def update_af_variable(context, key, new_value):
    try:
        curr_value = Variable.get(key)
        logging.info(f"{LOG_PFX} Current value of AF Variable: {key} is {curr_value}")
        logging.info(f"{LOG_PFX} Updating value of AF Variable: {key} to {new_value}")
        Variable.set(key = key, value = new_value)
        logging.info(f"{LOG_PFX} Value of AF Variable: {key} is updated")
    except Exception as e:
        logging.error(f"{LOG_PFX} Error received while updating value of AF Variable: {key} : {e}")

# ---- Variable-event functions ----

def UMEA_HUBTOMART_REF_PRODUCT_SET_FALSE(context):
    update_af_variable(context, 'UMEA-DISTRIBUTOR-PRODUCT-MAPPING', False)
    update_af_variable(context, 'UMEA-LAKETOHUB-REF-PROD-INTL', False)

def RUN_JOB_UMEA_HUBTOMART_REF_PRODUCT(context):
    update_af_variable(context, 'UMEA-LAKETOHUB-REF-PROD-INTL', True)

def UMEA_HUBTOMART_REF_COUNTRY_SET_FALSE(context):
    update_af_variable(context, 'UMEA-COUNTRY-MAPPING', False)
    update_af_variable(context, 'UMEA-LAKETOHUB-REF-COUNTRY-UMEA', False)

def RUN_JOB_UMEA_HUBTOMART_REF_CUSTOMER_MAP(context):
    update_af_variable(context, 'UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA', True)

def RUN_JOB_UMEA_HUBTOMART_TXN_DISTRIBUTOR_SALES(context):
    update_af_variable(context, 'UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA', True)

def RUN_JOB_UMEA_HUBTOMART_REF_COUNTRY(context):
    update_af_variable(context, 'UMEA-LAKETOHUB-REF-COUNTRY-UMEA', True)

def UMEA_HUBTOMART_TXN_DISTRIBUTOR_SALES_SET_FALSE(context):
    update_af_variable(context, 'UMEA-DISTRIBUTOR-SALES', False)
    update_af_variable(context, 'UMEA-ACCOUNT-LEAD-CONTRIBUTION', False)
    update_af_variable(context, 'UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA', False)

def RUN_JOB_UMEA_HUBTOMART_TXN_TARGET_SALES(context):
    update_af_variable(context, 'UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA', True)

def UMEA_HUBTOMART_TXN_TARGET_SALES_SET_FALSE(context):
    update_af_variable(context, 'UMEA-TARGET-SALES', False)
    update_af_variable(context, 'UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA', False)

def RUN_JOB_UMEA_HUBTOMART_REF_PRICE(context):
    update_af_variable(context, 'UMEA-LAKETOHUB-REF-PRICE-UMEA', True)

def RUN_JOB_UMEA_LAKETOHUB_REF_CUSTOMER_MAP_UMEA(context):
    update_af_variable(context, 'UMEA-ACCOUNT-MAPPING', True)

def RUN_JOB_UMEA_LAKETOHUB_REF_COUNTRY_UMEA(context):
    update_af_variable(context, 'UMEA-COUNTRY-MAPPING', True)

def RUN_JOB_UMEA_LAKETOHUB_REF_PROD_INTL(context):
    update_af_variable(context, 'UMEA-DISTRIBUTOR-PRODUCT-MAPPING', True)

def UMEA_HUBTOMART_REF_CUSTOMER_MAP_SET_FALSE(context):
    update_af_variable(context, 'UMEA-ACCOUNT-MAPPING', False)
    update_af_variable(context, 'UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA', False)

def RUN_JOB_UMEA_LAKETOLAKE_PRICE_STG(context):
    update_af_variable(context, 'UMEA-PRICE', True)

def UMEA_HUBTOMART_REF_PRICE_SET_FALSE(context):
    update_af_variable(context, 'UMEA-PRICE', False)
    update_af_variable(context, 'UMEA-LAKETOHUB-REF-PRICE-UMEA', False)

def RUN_JOB_UMEA_LAKETOLAKE_TARGET_SALES_STG(context):
    update_af_variable(context, 'UMEA-TARGET-SALES', True)

def RUN_JOB_UMEA_LAKETOLAKE_DISTRIBUTOR_SALES_STG(context):
    update_af_variable(context, 'UMEA-DISTRIBUTOR-SALES', True)


# ---- Variable-dependency pre-checks ----

VARIABLE_DEPENDENCIES = {
    "UMEA-ACCOUNT-MAPPING-LAKELOAD": {
        "type": "simple",
        "variable_name": "UMEA-ACCOUNT-MAPPING",
        "expected_value": False,
    },
    "UMEA-COUNTRY-MAPPING-LAKELOAD": {
        "type": "simple",
        "variable_name": "UMEA-COUNTRY-MAPPING",
        "expected_value": False,
    },
    "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-LAKELOAD": {
        "type": "simple",
        "variable_name": "UMEA-DISTRIBUTOR-PRODUCT-MAPPING",
        "expected_value": False,
    },
    "UMEA-DISTRIBUTOR-SALES-LAKELOAD": {
        "type": "simple",
        "variable_name": "UMEA-DISTRIBUTOR-SALES",
        "expected_value": False,
    },
    "UMEA-PRICE-LAKELOAD": {
        "type": "simple",
        "variable_name": "UMEA-PRICE",
        "expected_value": False,
    },
    "UMEA-TARGET-SALES-LAKELOAD": {
        "type": "simple",
        "variable_name": "UMEA-TARGET-SALES",
        "expected_value": False,
    },
    "UMEA-ACCOUNT-LEAD-CONTRIBUTION-LAKELOAD": {
        "type": "simple",
        "variable_name": "UMEA-ACCOUNT-LEAD-CONTRIBUTION",
        "expected_value": False,
    },
    "609983_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA": {
        "type": "compound",
        "formula": "([62796] OR [62793] OR [62795]) AND ([62794] OR [62793]) AND ([62797] OR [62796])",
        "atomic": {
            62796: ("UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA", True),
            62795: ("UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA", False),
            62793: ("UMEA-LAKETOHUB-REF-PRICE-UMEA", True),
            62797: ("UMEA-ACCOUNT-MAPPING", False),
            62794: ("UMEA-PRICE", False),
        },
    },
    "607462_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA": {
        "type": "compound",
        "formula": "([62787] OR [62785] OR [62783] OR [62786] OR [62782] OR [62791] OR [62784]) AND ([62788] OR [62785]) AND ([62789] OR [62787]) AND ([62790] OR [62786]) AND ([62792] OR [62791])",
        "atomic": {
            62787: ("UMEA-LAKETOHUB-REF-COUNTRY-UMEA", True),
            62785: ("UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA", True),
            62783: ("UMEA-LAKETOHUB-REF-PRICE-UMEA", True),
            62786: ("UMEA-LAKETOHUB-REF-PROD-INTL", True),
            62782: ("UMEA-LAKETOLAKE-TARGET-SALES-STG", True),
            62791: ("UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA", False),
            62784: ("UMEA-ACCOUNT-LEAD-CONTRIBUTION", True),
            62788: ("UMEA-ACCOUNT-MAPPING", False),
            62789: ("UMEA-COUNTRY-MAPPING", False),
            62790: ("UMEA-DISTRIBUTOR-PRODUCT-MAPPING", False),
            62792: ("UMEA-DISTRIBUTOR-SALES", False),
        },
    },
}

def _variable_matches(var_name, expected_value):
    actual = Variable.get(var_name, default_var="False")
    result = (str(actual) == str(expected_value))
    logging.info(f"{LOG_PFX}[dependency check] {var_name} == {expected_value}? actual={actual!r} -> {result}")
    return result

def check_variable_dependency(job_task_id):
    """Returns a PythonSensor-compatible callable that checks job_task_id's dependency
    (simple or compound) against VARIABLE_DEPENDENCIES, evaluating live Variable values."""
    def _check(**context):
        dep = VARIABLE_DEPENDENCIES[job_task_id]

        if dep["type"] == "simple":
            return _variable_matches(dep["variable_name"], dep["expected_value"])

        expr = dep["formula"]
        for dep_tes_id, (var_name, expected) in dep["atomic"].items():
            expr = expr.replace(f"[{dep_tes_id}]", str(_variable_matches(var_name, expected)))
        expr_py = expr.replace(" OR ", " or ").replace(" AND ", " and ")
        result = eval(expr_py, {"__builtins__": {}}, {})
        logging.info(f"{LOG_PFX}[dependency check] {job_task_id}: formula resolved to {expr} -> {result}")
        return result

    return _check


# ---- Email-event functions ----

def EMAIL_UMEA_HUBTOMART_TXN_DISTRIBUTOR_SALES_SUCCESS(context):
    email_to = UMEA_DBX_EMAIL_TO
    email_subject = "TXN Distributor Sales refresh success for UMEA"
    email_message = """Dear All,\n\nTXN Distributor sales data has been refreshed successfully for UMEA. \n\nThanks"""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def EMAIL_UMEA_HUBTOMART_TXN_DISTRIBUTOR_SALES_FAILURE(context):
    email_to = UMEA_DBX_EMAIL_TO
    email_subject = "TXN Distributor Sales refresh failure for UMEA"
    email_message = """Dear All,\n\nAttempts to refresh TXN Distributor sales data for UMEA failed and the team is working to resolve.  \n\n\nThanks"""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def EMAIL_UMEA_HUBTOMART_TXN_TARGET_SALES_SUCCESS(context):
    email_to = UMEA_DBX_EMAIL_TO
    email_subject = "TXN Target Sales refresh success for UMEA"
    email_message = """Dear All,\n\nTXN Target sales data has been refreshed successfully for UMEA. \n\nThanks"""
    context["email_to"] = email_to
    context["email_subject"] = email_subject
    context["email_message"] = email_message
    send_email_notification(context)

def EMAIL_UMEA_HUBTOMART_TXN_TARGET_SALES_FAILURE(context):
    email_to = UMEA_DBX_EMAIL_TO
    email_subject = "TXN Target Sales refresh failure for UMEA"
    email_message = """Dear All,\n\nAttempts to refresh TXN Target sales data for UMEA failed and the team is working to resolve.  \n\n\nThanks"""
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
    '598135_001-GEM-DATABRICKS-NOTEBOOK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL': ['RUN-JOB-UMEA-LAKETOLAKE-DISTRIBUTOR-SALES-STG'],
    '601308_002-UMEA-HUBTOMART-REF-PRODUCT': ['UMEA-HUBTOMART-REF-PRODUCT-SET-FALSE'],
    '602672_003-UMEA-LAKETOHUB-REF-PROD-INTL': ['RUN-JOB-UMEA-HUBTOMART-REF-PRODUCT'],
    '603722_002-UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA': ['RUN-JOB-UMEA-HUBTOMART-REF-CUSTOMER-MAP'],
    '604835_001-UMEA-HUBTOMART-REF-COUNTRY': ['UMEA-HUBTOMART-REF-COUNTRY-SET-FALSE'],
    '607462_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA': ['RUN-JOB-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES'],
    '609983_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA': ['RUN-JOB-UMEA-HUBTOMART-TXN-TARGET-SALES'],
    '614727_005-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES': ['UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES-SET-FALSE', 'EMAIL-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES-SUCCESS'],
    '614911_001-GEM-DATABRICKS-NOTEBOOK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL': ['RUN-JOB-UMEA-LAKETOLAKE-TARGET-SALES-STG'],
    '616931_004-UMEA-LAKETOHUB-REF-PRICE-UMEA': ['RUN-JOB-UMEA-HUBTOMART-REF-PRICE'],
    '617175_001-UMEA-LAKETOHUB-REF-COUNTRY-UMEA': ['RUN-JOB-UMEA-HUBTOMART-REF-COUNTRY'],
    '617593_006-UMEA-HUBTOMART-TXN-TARGET-SALES': ['UMEA-HUBTOMART-TXN-TARGET-SALES-SET-FALSE', 'EMAIL-UMEA-HUBTOMART-TXN-TARGET-SALES-SUCCESS'],
    '618585_004-UMEA-HUBTOMART-REF-PRICE': ['UMEA-HUBTOMART-REF-PRICE-SET-FALSE'],
    'UMEA-PRICE-LAKELOAD': ['RUN-JOB-UMEA-LAKETOLAKE-PRICE-STG'],
    'UMEA-DISTRIBUTOR-PRODUCT-MAPPING-LAKELOAD': ['RUN-JOB-UMEA-LAKETOHUB-REF-PROD-INTL'],
    '629600_003-UMEA-HUBTOMART-REF-CUSTOMER-MAP': ['UMEA-HUBTOMART-REF-CUSTOMER-MAP-SET-FALSE'],
    'UMEA-COUNTRY-MAPPING-LAKELOAD': ['RUN-JOB-UMEA-LAKETOHUB-REF-COUNTRY-UMEA'],
    'UMEA-ACCOUNT-MAPPING-LAKELOAD': ['RUN-JOB-UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA'],
}

on_failure_abnormal_completion_handlers = {
    '614727_005-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES': ['EMAIL-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES-FAILURE'],
    '617593_006-UMEA-HUBTOMART-TXN-TARGET-SALES': ['EMAIL-UMEA-HUBTOMART-TXN-TARGET-SALES-FAILURE'],
}

on_failure_error_handlers = {

}

####======================================================================================

####==============Handler name to Handler callback function mappint=======================

handler_callback_fun_map = {
    'SVCNOW-CREATE-P3-INCIDENT': svcnow_create_p3_incident,
    'SVCNOW-CREATE-P3-INCIDENT-ERR': svcnow_create_p3_incident_err,
    'Globalscape Job Failure - Abnormal': globalscape_job_failure_abnormal,
    'Globalscape Job Failure - Error Occurred': globalscape_job_failure_error_occurred,
    # Variable events (18)
    'RUN-JOB-UMEA-LAKETOLAKE-DISTRIBUTOR-SALES-STG': RUN_JOB_UMEA_LAKETOLAKE_DISTRIBUTOR_SALES_STG,
    'UMEA-HUBTOMART-REF-PRODUCT-SET-FALSE': UMEA_HUBTOMART_REF_PRODUCT_SET_FALSE,
    'RUN-JOB-UMEA-HUBTOMART-REF-PRODUCT': RUN_JOB_UMEA_HUBTOMART_REF_PRODUCT,
    'RUN-JOB-UMEA-HUBTOMART-REF-CUSTOMER-MAP': RUN_JOB_UMEA_HUBTOMART_REF_CUSTOMER_MAP,
    'UMEA-HUBTOMART-REF-COUNTRY-SET-FALSE': UMEA_HUBTOMART_REF_COUNTRY_SET_FALSE,
    'RUN-JOB-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES': RUN_JOB_UMEA_HUBTOMART_TXN_DISTRIBUTOR_SALES,
    'RUN-JOB-UMEA-HUBTOMART-TXN-TARGET-SALES': RUN_JOB_UMEA_HUBTOMART_TXN_TARGET_SALES,
    'UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES-SET-FALSE': UMEA_HUBTOMART_TXN_DISTRIBUTOR_SALES_SET_FALSE,
    'RUN-JOB-UMEA-LAKETOLAKE-TARGET-SALES-STG': RUN_JOB_UMEA_LAKETOLAKE_TARGET_SALES_STG,
    'RUN-JOB-UMEA-HUBTOMART-REF-PRICE': RUN_JOB_UMEA_HUBTOMART_REF_PRICE,
    'RUN-JOB-UMEA-HUBTOMART-REF-COUNTRY': RUN_JOB_UMEA_HUBTOMART_REF_COUNTRY,
    'UMEA-HUBTOMART-TXN-TARGET-SALES-SET-FALSE': UMEA_HUBTOMART_TXN_TARGET_SALES_SET_FALSE,
    'UMEA-HUBTOMART-REF-PRICE-SET-FALSE': UMEA_HUBTOMART_REF_PRICE_SET_FALSE,
    'RUN-JOB-UMEA-LAKETOLAKE-PRICE-STG': RUN_JOB_UMEA_LAKETOLAKE_PRICE_STG,
    'RUN-JOB-UMEA-LAKETOHUB-REF-PROD-INTL': RUN_JOB_UMEA_LAKETOHUB_REF_PROD_INTL,
    'UMEA-HUBTOMART-REF-CUSTOMER-MAP-SET-FALSE': UMEA_HUBTOMART_REF_CUSTOMER_MAP_SET_FALSE,
    'RUN-JOB-UMEA-LAKETOHUB-REF-COUNTRY-UMEA': RUN_JOB_UMEA_LAKETOHUB_REF_COUNTRY_UMEA,
    'RUN-JOB-UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA': RUN_JOB_UMEA_LAKETOHUB_REF_CUSTOMER_MAP_UMEA,
    # Email events (4)
    'EMAIL-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES-SUCCESS': EMAIL_UMEA_HUBTOMART_TXN_DISTRIBUTOR_SALES_SUCCESS,
    'EMAIL-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES-FAILURE': EMAIL_UMEA_HUBTOMART_TXN_DISTRIBUTOR_SALES_FAILURE,
    'EMAIL-UMEA-HUBTOMART-TXN-TARGET-SALES-SUCCESS': EMAIL_UMEA_HUBTOMART_TXN_TARGET_SALES_SUCCESS,
    'EMAIL-UMEA-HUBTOMART-TXN-TARGET-SALES-FAILURE': EMAIL_UMEA_HUBTOMART_TXN_TARGET_SALES_FAILURE,
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
        else:
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

    handlers = on_success_completion_handlers.get(task_id, [])
    for handler in handlers:
        try:
            handler_callback_fun_map.get(handler)(context)
        except Exception as e:
            print(f"{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")
            logging.info(f"{LOG_PFX}{LOG_PFX}{handler_callback_fun_map.handler.__name__}failed with error : {e}")

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
        # without a file, so mark it SKIPPED explicitly rather than letting it run. The
        # shared "*.SUCCESS" checkpoint markers are left alone - those just record "this
        # branch was attempted", not "a file arrived" - so they still proceed normally.
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


batch_override = None

def_args = {
    "owner": "airflow",
    "priority_weight": 1, # TO DO - to check this later
    "weight_rule": "absolute"
}

CET = pytz.timezone("Europe/Berlin")

dag = DAG(
    dag_id = "DA-COM-GEM-UMEA",
    description = "Was moved by EU-PROD-TO-EU-PROD-REPO from EU-PROD-CONN (20.25.2) to EU-PROD-REPOSITORY (Latest) by onetakeda\cef9990 on 04/22/2026 06:45:57 PM -- Deployed from DEFRATAGAPT001 by zye7443 on 10/04/2023 06:53:29 AM -- Updated from DEFRATAGAPT001 by zye7443 on 10/04/2023 07:31:15 AM",
    default_args = def_args,
    schedule_interval = "0 0 * * *",
    concurrency = 16,
    start_date = CET.localize(datetime(2026,6,29)),
    catchup = False,
    max_active_runs = 1
)

# ---- Variable-dependency check sensors (9) ----

A630252_DEP_CHECK = PythonSensor (task_id = "UMEA-ACCOUNT-MAPPING-DEPCHECK",
                                  task_display_name = "UMEA-ACCOUNT-MAPPING-DEPCHECK",
                                  python_callable = check_variable_dependency("UMEA-ACCOUNT-MAPPING-LAKELOAD"),
                                  poke_interval = PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
                                  timeout = PYTHON_SENSOR_TIMEOUT_SECONDS,
                                  mode = "reschedule",
                                  dag = dag)

A629657_DEP_CHECK = PythonSensor (task_id = "UMEA-COUNTRY-MAPPING-DEPCHECK",
                                  task_display_name = "UMEA-COUNTRY-MAPPING-DEPCHECK",
                                  python_callable = check_variable_dependency("UMEA-COUNTRY-MAPPING-LAKELOAD"),
                                  poke_interval = PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
                                  timeout = PYTHON_SENSOR_TIMEOUT_SECONDS,
                                  mode = "reschedule",
                                  dag = dag)

A628321_DEP_CHECK = PythonSensor (task_id = "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-DEPCHECK",
                                  task_display_name = "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-DEPCHECK",
                                  python_callable = check_variable_dependency("UMEA-DISTRIBUTOR-PRODUCT-MAPPING-LAKELOAD"),
                                  poke_interval = PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
                                  timeout = PYTHON_SENSOR_TIMEOUT_SECONDS,
                                  mode = "reschedule",
                                  dag = dag)

A598925_DEP_CHECK = PythonSensor (task_id = "UMEA-DISTRIBUTOR-SALES-DEPCHECK",
                                  task_display_name = "UMEA-DISTRIBUTOR-SALES-DEPCHECK",
                                  python_callable = check_variable_dependency("UMEA-DISTRIBUTOR-SALES-LAKELOAD"),
                                  poke_interval = PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
                                  timeout = PYTHON_SENSOR_TIMEOUT_SECONDS,
                                  mode = "reschedule",
                                  dag = dag)

A625269_DEP_CHECK = PythonSensor (task_id = "UMEA-PRICE-DEPCHECK",
                                  task_display_name = "UMEA-PRICE-DEPCHECK",
                                  python_callable = check_variable_dependency("UMEA-PRICE-LAKELOAD"),
                                  poke_interval = PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
                                  timeout = PYTHON_SENSOR_TIMEOUT_SECONDS,
                                  mode = "reschedule",
                                  dag = dag)

A612098_DEP_CHECK = PythonSensor (task_id = "UMEA-TARGET-SALES-DEPCHECK",
                                  task_display_name = "UMEA-TARGET-SALES-DEPCHECK",
                                  python_callable = check_variable_dependency("UMEA-TARGET-SALES-LAKELOAD"),
                                  poke_interval = PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
                                  timeout = PYTHON_SENSOR_TIMEOUT_SECONDS,
                                  mode = "reschedule",
                                  dag = dag)

A614276_DEP_CHECK = PythonSensor (task_id = "UMEA-ACCOUNT-LEAD-CONTRIBUTION-DEPCHECK",
                                  task_display_name = "UMEA-ACCOUNT-LEAD-CONTRIBUTION-DEPCHECK",
                                  python_callable = check_variable_dependency("UMEA-ACCOUNT-LEAD-CONTRIBUTION-LAKELOAD"),
                                  poke_interval = PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
                                  timeout = PYTHON_SENSOR_TIMEOUT_SECONDS,
                                  mode = "reschedule",
                                  dag = dag)

A609983_DEP_CHECK = PythonSensor (task_id = "DEP_CHECK_609983_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA",
                                  task_display_name = "Dependency Check - 006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA",
                                  python_callable = check_variable_dependency("609983_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA"),
                                  poke_interval = PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
                                  timeout = PYTHON_SENSOR_TIMEOUT_SECONDS,
                                  mode = "reschedule",
                                  dag = dag)

A607462_DEP_CHECK = PythonSensor (task_id = "DEP_CHECK_607462_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA",
                                  task_display_name = "Dependency Check - 005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA",
                                  python_callable = check_variable_dependency("607462_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA"),
                                  poke_interval = PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
                                  timeout = PYTHON_SENSOR_TIMEOUT_SECONDS,
                                  mode = "reschedule",
                                  dag = dag)


def test_function():
    logging.info(f"{LOG_PFX}Tidal To Airflow Migration Testing...")
    return True


# Jobs
A593330_SUCCESS = DummyOperator (task_id = "593330.SUCCESS",
                                 task_display_name = "61156.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A602671_SUCCESS = DummyOperator (task_id = "602671.SUCCESS",
                                 task_display_name = "56409.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A605672 = PythonOperator (task_id = "605672_020-ASSOCIATED-CALLBACK-JOB-MONITOR-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL",
                          python_callable = test_function, task_display_name = "56455_020-ASSOCIATED-CALLBACK-JOB-MONITOR-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL",
                          execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "UMEA-TARGET-SALES-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["UMEA-TARGET-SALES-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='UMEA-TARGET-SALES-FILEWATCH', key='resolved_key') }}"
A612098 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "UMEA-TARGET-SALES-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "UMEA-DISTRIBUTOR-SALES-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["UMEA-DISTRIBUTOR-SALES-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='UMEA-DISTRIBUTOR-SALES-FILEWATCH', key='resolved_key') }}"
A598925 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "UMEA-DISTRIBUTOR-SALES-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# A629600 = PythonOperator (task_id = "629600_003-UMEA-HUBTOMART-REF-CUSTOMER-MAP", python_callable = test_function, task_display_name = "56402_003-UMEA-HUBTOMART-REF-CUSTOMER-MAP", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "629600_003-UMEA-HUBTOMART-REF-CUSTOMER-MAP"
A629600 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56402_003-UMEA-HUBTOMART-REF-CUSTOMER-MAP",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)




A613709_SUCCESS = DummyOperator (task_id = "613709.SUCCESS",
                                 task_display_name = "56451.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A611564_SUCCESS = DummyOperator (task_id = "611564.SUCCESS",
                                 task_display_name = "56447.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A610880 = PythonOperator (task_id = "610880_010-SHOW-DATABRICKS-JOB-STATUS-UMEA-FILE-VALIDATION-EMAIL",
                          python_callable = test_function,
                          task_display_name = "56456_010-SHOW-DATABRICKS-JOB-STATUS-UMEA-FILE-VALIDATION-EMAIL",
                          execution_timeout = timedelta(minutes=60), dag = dag)


_task_id = "UMEA-ACCOUNT-LEAD-CONTRIBUTION-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["UMEA-ACCOUNT-LEAD-CONTRIBUTION-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='UMEA-ACCOUNT-LEAD-CONTRIBUTION-FILEWATCH', key='resolved_key') }}"
A614276 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "UMEA-ACCOUNT-LEAD-CONTRIBUTION-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A600679 = DummyOperator (task_id = "600679_DA-COM-GEM-UMEA-HUB-TO-MART-BDM-JOBS",
                         task_display_name = "56398_DA-COM-GEM-UMEA-HUB-TO-MART-BDM-JOBS",
                         execution_timeout = timedelta(minutes=60), dag = dag)


A613143 = PythonOperator (task_id = "613143_010-SHOW-DATABRICKS-JOB-STATUS-UMEA-FILE-VALIDATION-EMAIL",
                          python_callable = test_function,
                          task_display_name = "56460_010-SHOW-DATABRICKS-JOB-STATUS-UMEA-FILE-VALIDATION-EMAIL",
                          execution_timeout = timedelta(minutes=60), dag = dag)

# A629725 = PythonOperator (task_id = "629725_001-UMEA-LAKETOLAKE-PRICE-STG", python_callable = test_function, task_display_name = "56408_001-UMEA-LAKETOLAKE-PRICE-STG", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "629725_001-UMEA-LAKETOLAKE-PRICE-STG"
A629725 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56408_001-UMEA-LAKETOLAKE-PRICE-STG",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A610879 = DummyOperator (task_id = "610879_002-GEM-DATABRICKS-CALLBACK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL",
                        task_display_name = "56454_002-GEM-DATABRICKS-CALLBACK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL",
                        execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "UMEA-PRICE-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["UMEA-PRICE-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='UMEA-PRICE-FILEWATCH', key='resolved_key') }}"
A625269 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "UMEA-PRICE-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A617836 = PythonOperator (task_id = "617836_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DMP-EXCEPTION-SYNC",
                          python_callable = test_function,
                          task_display_name = "61157_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DMP-EXCEPTION-SYNC",
                          execution_timeout = timedelta(minutes=60), dag = dag)



A630251_SUCCESS = DummyOperator (task_id = "630251.SUCCESS",
                                 task_display_name = "56423.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "UMEA-ACCOUNT-LEAD-CONTRIBUTION-FILEWATCH"
A613709 = S3KeySensor (task_id = _task_id,
                        task_display_name = "UMEA-ACCOUNT-LEAD-CONTRIBUTION-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = S3_SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)


A598109_SUCCESS = DummyOperator (task_id = "598109.SUCCESS",
                                 task_display_name = "56420.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A598110_SUCCESS = DummyOperator (task_id = "598110.SUCCESS",
                                 task_display_name = "56435.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A614911 = DummyOperator (task_id = "614911_001-GEM-DATABRICKS-NOTEBOOK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL",
                         task_display_name = "56419_001-GEM-DATABRICKS-NOTEBOOK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL",
                         execution_timeout = timedelta(minutes=60),
                         on_success_callback = on_success_callback_dbx_lf_job,
                         dag = dag)

A598109 = DummyOperator (task_id = "598109_DA-COM-GEM-UMEA-COMMON-LAMBDA",
                         task_display_name = "56420_DA-COM-GEM-UMEA-COMMON-LAMBDA",
                         execution_timeout = timedelta(minutes=60), dag = dag)


A593329_SUCCESS = DummyOperator (task_id = "593329.SUCCESS",
                                 task_display_name = "61153.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "UMEA-ACCOUNT-MAPPING-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["UMEA-ACCOUNT-MAPPING-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='UMEA-ACCOUNT-MAPPING-FILEWATCH', key='resolved_key') }}"
A630252 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "UMEA-ACCOUNT-MAPPING-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)


# A607462 = PythonOperator (task_id = "607462_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA", python_callable = test_function, task_display_name = "56411_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "607462_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA"
A607462 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56411_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "UMEA-DISTRIBUTOR-SALES-FILEWATCH"
A598110 = S3KeySensor (task_id = _task_id,
                        task_display_name = "UMEA-DISTRIBUTOR-SALES-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = S3_SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

_task_id = "UMEA-TARGET-SALES-FILEWATCH"
A611564 = S3KeySensor (task_id = _task_id,
                        task_display_name = "UMEA-TARGET-SALES-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = S3_SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)



# A614621 = PythonOperator (task_id = "614621_003-UMEA-LAKETOLAKE-DISTRIBUTOR-SALES-STG", python_callable = test_function, task_display_name = "56406_003-UMEA-LAKETOLAKE-DISTRIBUTOR-SALES-STG", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "614621_003-UMEA-LAKETOLAKE-DISTRIBUTOR-SALES-STG"
A614621 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56406_003-UMEA-LAKETOLAKE-DISTRIBUTOR-SALES-STG",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# A617593 = PythonOperator (task_id = "617593_006-UMEA-HUBTOMART-TXN-TARGET-SALES", python_callable = test_function, task_display_name = "56399_006-UMEA-HUBTOMART-TXN-TARGET-SALES", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "617593_006-UMEA-HUBTOMART-TXN-TARGET-SALES"
A617593 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56399_006-UMEA-HUBTOMART-TXN-TARGET-SALES",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# A628884 = PythonOperator (task_id = "628884_002-UMEA-LAKETOLAKE-TARGET-SALES-STG", python_callable = test_function, task_display_name = "56407_002-UMEA-LAKETOLAKE-TARGET-SALES-STG", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "628884_002-UMEA-LAKETOLAKE-TARGET-SALES-STG"
A628884 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56407_002-UMEA-LAKETOLAKE-TARGET-SALES-STG",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# A614727 = PythonOperator (task_id = "614727_005-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES", python_callable = test_function, task_display_name = "56400_005-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "614727_005-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES"
A614727 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56400_005-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# A603722 = PythonOperator (task_id = "603722_002-UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA", python_callable = test_function, task_display_name = "56414_002-UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "603722_002-UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA"
A603722 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56414_002-UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A603088_SUCCESS = DummyOperator (task_id = "603088.SUCCESS",
                                 task_display_name = "56439.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A598135 = DummyOperator (task_id = "598135_001-GEM-DATABRICKS-NOTEBOOK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL",
                         task_display_name = "56417_001-GEM-DATABRICKS-NOTEBOOK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL",
                         execution_timeout = timedelta(minutes=60),
                         on_success_callback = on_success_callback_dbx_lf_job,
                         dag = dag)

A613141 = DummyOperator (task_id = "613141_DA-COM-GEM-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL",
                         task_display_name = "56418_DA-COM-GEM-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL",
                         execution_timeout = timedelta(minutes=60), dag = dag)

A593330 = DummyOperator (task_id = "593330_002-GEM-DATABRICKS-CALLBACK-DMP-EXCEPTION-SYNC-UMEA",
                         task_display_name = "61156_002-GEM-DATABRICKS-CALLBACK-DMP-EXCEPTION-SYNC-UMEA",
                         execution_timeout = timedelta(minutes=60), dag = dag)

A613141_SUCCESS = DummyOperator (task_id = "613141.SUCCESS",
                                 task_display_name = "56418.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "UMEA-COUNTRY-MAPPING-FILEWATCH"
A629057 = S3KeySensor (task_id = _task_id,
                        task_display_name = "UMEA-COUNTRY-MAPPING-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = S3_SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A613142_SUCCESS = DummyOperator (task_id = "613142.SUCCESS",
                                 task_display_name = "56458.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "UMEA-ACCOUNT-MAPPING-FILEWATCH"
A630251 = S3KeySensor (task_id = _task_id,
                        task_display_name = "UMEA-ACCOUNT-MAPPING-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = S3_SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

# A614349 = PythonOperator (task_id = "614349_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC", python_callable = test_function, task_display_name = "61155_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "614349_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC"
A614349 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "61155_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "UMEA-PRICE-FILEWATCH"
A603088 = S3KeySensor (task_id = _task_id,
                        task_display_name = "UMEA-PRICE-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = S3_SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

# A614752 = PythonOperator (task_id = "614752_010-DATABRICKS-RUN-NOTEBOOK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL", python_callable = test_function, task_display_name = "56453_010-DATABRICKS-RUN-NOTEBOOK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "614752_010-DATABRICKS-RUN-NOTEBOOK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL"
A614752 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56453_010-DATABRICKS-RUN-NOTEBOOK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A593328 = DummyOperator (task_id = "593328_DA-COM-GEM-UMEA",
                         task_display_name = "56397_DA-COM-GEM-UMEA",
                         execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "UMEA-COUNTRY-MAPPING-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["UMEA-COUNTRY-MAPPING-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='UMEA-COUNTRY-MAPPING-FILEWATCH', key='resolved_key') }}"
A629657 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "UMEA-COUNTRY-MAPPING-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-FILEWATCH"
A628320 = S3KeySensor (task_id = _task_id,
                        task_display_name = "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = S3_SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)


# A618585 = PythonOperator (task_id = "618585_004-UMEA-HUBTOMART-REF-PRICE", python_callable = test_function, task_display_name = "56401_004-UMEA-HUBTOMART-REF-PRICE", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "618585_004-UMEA-HUBTOMART-REF-PRICE"
A618585 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56401_004-UMEA-HUBTOMART-REF-PRICE",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)


ASTART = DummyOperator (task_id = "START", execution_timeout = timedelta(minutes=60), dag = dag)

A600679_SUCCESS = DummyOperator (task_id = "600679.SUCCESS",
                                 task_display_name = "56398.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A614620 = DummyOperator (task_id = "614620_DA-COM-GEM-UMEA-LAKE-TO-LAKE-BDM-JOBS",
                         task_display_name = "56405_DA-COM-GEM-UMEA-LAKE-TO-LAKE-BDM-JOBS",
                         execution_timeout = timedelta(minutes=60), dag = dag)

A598134 = DummyOperator (task_id = "598134_DA-COM-GEM-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL",
                         task_display_name = "56416_DA-COM-GEM-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL",
                         execution_timeout = timedelta(minutes=60), dag = dag)


A614348 = DummyOperator (task_id = "614348_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-UMEA",
                         task_display_name = "61154_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-UMEA",
                         execution_timeout = timedelta(minutes=60), dag = dag)

# A629958 = PythonOperator (task_id = "629958_010-DATABRICKS-RUN-NOTEBOOK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL", python_callable = test_function, task_display_name = "56457_010-DATABRICKS-RUN-NOTEBOOK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "629958_010-DATABRICKS-RUN-NOTEBOOK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL"
A629958 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56457_010-DATABRICKS-RUN-NOTEBOOK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)


A614620_SUCCESS = DummyOperator (task_id = "614620.SUCCESS",
                                 task_display_name = "56405.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A593329 = DummyOperator (task_id = "593329_DA-COM-GEM-UMEA-DMP-EXCEPTION-SYNC",
                         task_display_name = "61153_DA-COM-GEM-UMEA-DMP-EXCEPTION-SYNC",
                         execution_timeout = timedelta(minutes=60), dag = dag)

# A617175 = PythonOperator (task_id = "617175_001-UMEA-LAKETOHUB-REF-COUNTRY-UMEA", python_callable = test_function, task_display_name = "56415_001-UMEA-LAKETOHUB-REF-COUNTRY-UMEA", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "617175_001-UMEA-LAKETOHUB-REF-COUNTRY-UMEA"
A617175 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56415_001-UMEA-LAKETOHUB-REF-COUNTRY-UMEA",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A598135_SUCCESS = DummyOperator (task_id = "598135.SUCCESS",
                                 task_display_name = "56417.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A614911_SUCCESS = DummyOperator (task_id = "614911.SUCCESS",
                                 task_display_name = "56419.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A593331 = PythonOperator (task_id = "593331_010-SHOW-DATABRICKS-JOB-STATUS-DMP-EXCEPTION-SYNC",
                          python_callable = test_function,
                          task_display_name = "61158_010-SHOW-DATABRICKS-JOB-STATUS-DMP-EXCEPTION-SYNC",
                          execution_timeout = timedelta(minutes=60), dag = dag)

# A616931 = PythonOperator (task_id = "616931_004-UMEA-LAKETOHUB-REF-PRICE-UMEA", python_callable = test_function, task_display_name = "56412_004-UMEA-LAKETOHUB-REF-PRICE-UMEA", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "616931_004-UMEA-LAKETOHUB-REF-PRICE-UMEA"
A616931 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56412_004-UMEA-LAKETOHUB-REF-PRICE-UMEA",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A610879_SUCCESS = DummyOperator (task_id = "610879.SUCCESS",
                                 task_display_name = "56454.SUCCESS",
                                 execution_timeout = timedelta(minutes=60),dag = dag)

A614348_SUCCESS = DummyOperator (task_id = "614348.SUCCESS",
                                 task_display_name = "61154.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

# A601308 = PythonOperator (task_id = "601308_002-UMEA-HUBTOMART-REF-PRODUCT", python_callable = test_function, task_display_name = "56403_002-UMEA-HUBTOMART-REF-PRODUCT", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "601308_002-UMEA-HUBTOMART-REF-PRODUCT"
A601308 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56403_002-UMEA-HUBTOMART-REF-PRODUCT",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A628320_SUCCESS = DummyOperator (task_id = "628320.SUCCESS",
                                 task_display_name = "56431.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A629057_SUCCESS = DummyOperator (task_id = "629057.SUCCESS",
                                 task_display_name = "56427.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)


# A609983 = PythonOperator (task_id = "609983_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA", python_callable = test_function, task_display_name = "56410_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "609983_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA"
A609983 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56410_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A598134_SUCCESS = DummyOperator (task_id = "598134.SUCCESS",
                                 task_display_name = "56416.SUCCESS",
                                 execution_timeout = timedelta(minutes=60), dag = dag)

A602671 = DummyOperator (task_id = "602671_DA-COM-GEM-UMEA-LAKE-TO-HUB-BDM-JOBS",
                         task_display_name = "56409_DA-COM-GEM-UMEA-LAKE-TO-HUB-BDM-JOBS",
                         execution_timeout = timedelta(minutes=60), dag = dag)

A617037 = PythonOperator (task_id = "617037_020-ASSOCIATED-CALLBACK-JOB-MONITOR-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL",
                          python_callable = test_function,
                          task_display_name = "56459_020-ASSOCIATED-CALLBACK-JOB-MONITOR-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL",
                          execution_timeout = timedelta(minutes=60), dag = dag)


# A604835 = PythonOperator (task_id = "604835_001-UMEA-HUBTOMART-REF-COUNTRY", python_callable = test_function, task_display_name = "56404_001-UMEA-HUBTOMART-REF-COUNTRY", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "604835_001-UMEA-HUBTOMART-REF-COUNTRY"
A604835 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56404_001-UMEA-HUBTOMART-REF-COUNTRY",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["UMEA-DISTRIBUTOR-PRODUCT-MAPPING-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='UMEA-DISTRIBUTOR-PRODUCT-MAPPING-FILEWATCH', key='resolved_key') }}"
A628321 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A613142 = DummyOperator (task_id = "613142_002-GEM-DATABRICKS-CALLBACK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL",
                         task_display_name = "56458_002-GEM-DATABRICKS-CALLBACK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL",
                         execution_timeout = timedelta(minutes=60), dag = dag)




# A602672 = PythonOperator (task_id = "602672_003-UMEA-LAKETOHUB-REF-PROD-INTL", python_callable = test_function, task_display_name = "56413_003-UMEA-LAKETOHUB-REF-PROD-INTL", execution_timeout = timedelta(minutes=60), dag = dag)
_task_id = "602672_003-UMEA-LAKETOHUB-REF-PROD-INTL"
A602672 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "56413_003-UMEA-LAKETOHUB-REF-PROD-INTL",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# Dependency Management

A593330_SUCCESS >> [A617836]
A605672 >> [A598135_SUCCESS]
A612098 >> [A611564_SUCCESS]
A598925 >> [A598110_SUCCESS]
A629600 >> [A600679_SUCCESS]
A610880 >> [A610879_SUCCESS]
A614276 >> [A613709_SUCCESS]
A600679 >> [A601308, A604835, A614727, A617593, A618585, A629600]
A613143 >> [A613142_SUCCESS]
A629725 >> [A614620_SUCCESS]
A610879 >> [A598134_SUCCESS, A610880]
A625269 >> [A603088_SUCCESS]
A617836 >> [A614348_SUCCESS]
A613709 >> [A598109_SUCCESS, A614276_DEP_CHECK]
A614276_DEP_CHECK >> [A614276]
A614911 >> [A613141_SUCCESS, A629958]
A598109 >> [A598110, A603088, A611564, A613709, A628320, A629057, A630251]
A630252 >> [A630251_SUCCESS]
A607462 >> [A602671_SUCCESS]
A598110 >> [A598109_SUCCESS, A598925_DEP_CHECK]
A598925_DEP_CHECK >> [A598925]
A611564 >> [A598109_SUCCESS, A612098_DEP_CHECK]
A612098_DEP_CHECK >> [A612098]
A614621 >> [A614620_SUCCESS]
A617593 >> [A600679_SUCCESS]
A628884 >> [A614620_SUCCESS]
A614727 >> [A600679_SUCCESS]
A603722 >> [A602671_SUCCESS]
A598135 >> [A598134_SUCCESS, A614752]
A613141 >> [A613142, A614911]
A593330 >> [A593329_SUCCESS, A593331]
A629057 >> [A598109_SUCCESS, A629657_DEP_CHECK]
A629657_DEP_CHECK >> [A629657]
A613142_SUCCESS >> [A617037]
A630251 >> [A598109_SUCCESS, A630252_DEP_CHECK]
A630252_DEP_CHECK >> [A630252]
A614349 >> [A617836]
A603088 >> [A598109_SUCCESS, A625269_DEP_CHECK]
A625269_DEP_CHECK >> [A625269]
A614752 >> [A605672]
A593328 >> [A593329, A598109, A598134, A600679, A602671, A613141, A614620]
A629657 >> [A629057_SUCCESS]
A628320 >> [A598109_SUCCESS, A628321_DEP_CHECK]
A628321_DEP_CHECK >> [A628321]
A618585 >> [A600679_SUCCESS]
ASTART >> [A593328]
A614620 >> [A614621, A628884, A629725]
A598134 >> [A598135, A610879]
A614348 >> [A593329_SUCCESS, A614349]
A629958 >> [A617037]
A593329 >> [A593330, A614348]
A617175 >> [A602671_SUCCESS]
A593331 >> [A593330_SUCCESS]
A616931 >> [A602671_SUCCESS]
A610879_SUCCESS >> [A605672]
A601308 >> [A600679_SUCCESS]
A609983 >> [A602671_SUCCESS]
A602671 >> [A602672, A603722, A607462_DEP_CHECK, A609983_DEP_CHECK, A616931, A617175]
A607462_DEP_CHECK >> [A607462]
A609983_DEP_CHECK >> [A609983]
A617037 >> [A614911_SUCCESS]
A604835 >> [A600679_SUCCESS]
A628321 >> [A628320_SUCCESS]
A613142 >> [A613141_SUCCESS, A613143]
A602672 >> [A602671_SUCCESS]
# =============================================================================
# JOB-EVENT edges (action_type=4)
# =============================================================================
A598135 >> [A614621]
A598925 >> [A598135]
A602672 >> [A601308]
A603722 >> [A629600]
A607462 >> [A614727, A614348]
A609983 >> [A617593]
A612098 >> [A614911]
A614621 >> [A607462]
A614911 >> [A628884]
A616931 >> [A618585]
A617175 >> [A604835]
A625269 >> [A629725]
A628321 >> [A602672]
A628884 >> [A609983, A614348]
A629657 >> [A617175]
A629725 >> [A616931]
A630252 >> [A603722]

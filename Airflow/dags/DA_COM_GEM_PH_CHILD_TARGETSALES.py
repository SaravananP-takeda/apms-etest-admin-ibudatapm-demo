import os
import sys
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.utils.email import send_email

from tak_airflow_callbacks_utils import (
    on_success_callback_dbx_lf_job,
    on_failure_callback_dbx_lf_job,
)

from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator
)


# ============================================================
# PH PARAMETERS
# ============================================================

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "parameters"
    )
)

from DA_COM_GEM_PH_PARAMETERS import (
    PARAMS_DICT,
    DATABRICKS_CONN_ID,
    SNOW_ATTRIBUTES_DEFAULTS,
    EMAIL_RECIPIENTS,
)


# ============================================================
# ENVIRONMENT
# ============================================================

af_env = Variable.get("af_env", default_var="DEV")
params = PARAMS_DICT


# ============================================================
# CALLBACKS (success / failure mail) - same as DA-COM-GEM-PH parent
# ============================================================

snow_attributes = {
    'configuration_item': SNOW_ATTRIBUTES_DEFAULTS['configuration_item'],
    'severity':           SNOW_ATTRIBUTES_DEFAULTS['severity'],
    'ci_apms_id':         SNOW_ATTRIBUTES_DEFAULTS['ci_apms_id'],
    'ci_bsn_id':          SNOW_ATTRIBUTES_DEFAULTS['ci_bsn_id'],
    'apms_id':            'APMS-92144',
    'bsn_id':             'BSN0020621',
    'type':               'AWS-Airflow-EU',
    'env':                'PROD',
}


class DAGCallbackAction():

    def __init__(self):

        # On Success / On Failure handlers for this DAG's Hub-to-Mart task
        self.on_success_completion_handlers = {
            '728889_003-PH-HUBTOMART-TXN-PH-TGT-SALES': ['PH_TARGET_SALES_LOAD_SUCCESS'],
        }

        self.on_failure_abnormal_completion_handlers = {
            '728889_003-PH-HUBTOMART-TXN-PH-TGT-SALES': ['PH_TARGET_SALES_LOAD_FAILURE'],
        }

        self.on_failure_error_handlers = {

        }

        self.handler_callback_fun_map = {
            'PH_TARGET_SALES_LOAD_SUCCESS': self.PH_TARGET_SALES_LOAD_SUCCESS,
            'PH_TARGET_SALES_LOAD_FAILURE': self.PH_TARGET_SALES_LOAD_FAILURE,
        }

        self.snow_attributes = snow_attributes

        self.EMAIL_TO = EMAIL_RECIPIENTS.get(af_env)

    def PH_TARGET_SALES_LOAD_SUCCESS(self,context):
        email_to =self.EMAIL_TO

        email_subject = "Target Sales data refresh completion for Philippines"

        email_message = """Dear All,

The Target Sales data has been refreshed successfully for Philippines.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )

    def PH_TARGET_SALES_LOAD_FAILURE(self,context):
        email_to = self.EMAIL_TO

        email_subject = "Target Sales data refresh failure for Philippines"

        email_message = """Dear All,

Attempts to refresh Target Sales data for Philippines failed and the team is working to resolve.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


def on_success_callback_dbx_lf_job_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_success_callback_dbx_lf_job(context, dag_callback_action_obj)


def on_failure_callback_dbx_lf_job_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_failure_callback_dbx_lf_job(context, dag_callback_action_obj)



# ============================================================
# DAG
# ============================================================

DAG_ID = "DA-COM-GEM-PH-CHILD-TARGETSALES"

default_args = {
    "owner": "airflow",
    "priority_weight": 1,
    "weight_rule": "absolute",
}

dag = DAG(
    dag_id=DAG_ID,
    description="Child DAG for PH TARGETSALES",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 9, 2),
    catchup=False,
    max_active_runs=1,
)


# ============================================================
# START / END
# ============================================================

START = DummyOperator(
    task_id="START",
    dag=dag
)

END = DummyOperator(
    task_id="END",
    dag=dag
)


# ============================================================
# 714317
# 001-PH-LAKETOLAKE-TXN-PH-TGT-SALES-STG
# ============================================================

_task_id = "714317_001-PH-LAKETOLAKE-TXN-PH-TGT-SALES-STG"

A714317 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name=_task_id,
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={
        "job_parameters":
            params[af_env][_task_id]["job_parameters"]
    },
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)


# ============================================================
# 723138
# 002-PH-LAKETOHUB-TXN-PH-TGT-SALES
# ============================================================

_task_id = "723138_002-PH-LAKETOHUB-TXN-PH-TGT-SALES"

A723138 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name=_task_id,
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={
        "job_parameters":
            params[af_env][_task_id]["job_parameters"]
    },
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)


# ============================================================
# 728889
# 003-PH-HUBTOMART-TXN-PH-TGT-SALES
# ============================================================

_task_id = "728889_003-PH-HUBTOMART-TXN-PH-TGT-SALES"

A728889 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name=_task_id,
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={
        "job_parameters":
            params[af_env][_task_id]["job_parameters"]
    },
    execution_timeout=timedelta(minutes=60),
    on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
    on_success_callback=on_success_callback_dbx_lf_job_wrapper,
    dag=dag,
)


# ============================================================
# DEPENDENCIES
# ============================================================

START >> A714317
A714317 >> A723138
A723138 >> A728889
A728889 >> END



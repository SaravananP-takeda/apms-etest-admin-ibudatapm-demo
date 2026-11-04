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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
            '725593_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES': ['PH_GLOBO_WHOLESALER_SALES_LOAD_SUCCESS'],
        }

        self.on_failure_abnormal_completion_handlers = {
            '725593_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES': ['PH_GLOBO_WHOLESALER_SALES_LOAD_FAILURE'],
        }

        self.on_failure_error_handlers = {

        }

        self.handler_callback_fun_map = {
            'PH_GLOBO_WHOLESALER_SALES_LOAD_SUCCESS': self.PH_GLOBO_WHOLESALER_SALES_LOAD_SUCCESS,
            'PH_GLOBO_WHOLESALER_SALES_LOAD_FAILURE': self.PH_GLOBO_WHOLESALER_SALES_LOAD_FAILURE,
        }

        self.snow_attributes = snow_attributes

        self.EMAIL_TO = EMAIL_RECIPIENTS.get(af_env)

    def PH_GLOBO_WHOLESALER_SALES_LOAD_SUCCESS(self,context):
        email_to = self.EMAIL_TO

        email_subject = "Globo wholesaler Data refresh completion for Philippines"

        email_message = """Dear All,

The Globo wholesaler data has been refreshed successfully for Philippines.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )

    def PH_GLOBO_WHOLESALER_SALES_LOAD_FAILURE(self,context):
        email_to = self.EMAIL_TO
        email_subject = "Globo wholesaler data refresh failure  for Philippines"

        email_message = """Dear All,

Attempts to refresh Globo wholesaler data for Philippines failed and the team is working to resolve.

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

DAG_ID = "DA-COM-GEM-PH-CHILD-SRC-GLOBO"

default_args = {
    "owner": "airflow",
    "priority_weight": 1,
    "weight_rule": "absolute",
}


dag = DAG(
    dag_id=DAG_ID,
    description="Child DAG for PH SRC-GLOBO",
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
# 730157
# 001-PH-LAKETOLAKE-PH-GLOBO-STG
# ============================================================

_task_id = "730157_001-PH-LAKETOLAKE-PH-GLOBO-STG"

A730157 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="48978_001-PH-LAKETOLAKE-PH-GLOBO-STG",
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
# 734220
# 002-PH-LAKETOHUB-REF-CUST-INTL
# ============================================================

_task_id = "734220_002-PH-LAKETOHUB-REF-CUST-INTL"

A734220 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="48979_002-PH-LAKETOHUB-REF-CUST-INTL",
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
# 737130
# 003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES
# ============================================================

_task_id = "737130_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES"

A737130 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="48990_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES",
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
# 725593
# 004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES
# ============================================================

_task_id = "725593_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES"

A725593 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="48985_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES",
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
# DMP EXCEPTION SYNC
# ============================================================

# ============================================================
# DMP EXCEPTION SYNC (separate DAG: DA-COM-GEM-PH-CHILD-DMP-EXCEPTION-SYNC-GRP)
# ============================================================

A709365 = TriggerDagRunOperator(
    task_id="709365_DA-COM-GEM-PH-DMP-EXCEPTION-SYNC",
    task_display_name="61123_DA-COM-GEM-PH-DMP-EXCEPTION-SYNC",
    trigger_dag_id="DA-COM-GEM-PH-CHILD-DMP-EXCEPTION-SYNC",
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=False,
    execution_timeout=timedelta(minutes=180),
    dag=dag,
)


# ============================================================
# DEPENDENCIES
# ============================================================

START >> A730157

A730157 >> A734220

A734220 >> A737130

# Main GLOBO flow
A737130 >> A725593

# DMP flow
A737130 >> A709365



# END
[
    A725593,
    A709365,
] >> END
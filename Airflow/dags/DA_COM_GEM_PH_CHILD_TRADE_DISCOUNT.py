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
            '743912_002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT': ['PH_TRADE_DISCOUNT_LOAD_SUCCESS'],
        }

        self.on_failure_abnormal_completion_handlers = {
            '743912_002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT': ['PH_TRADE_DISCOUNT_LOAD_FAILURE'],
        }

        self.on_failure_error_handlers = {

        }

        self.handler_callback_fun_map = {
            'PH_TRADE_DISCOUNT_LOAD_SUCCESS': self.PH_TRADE_DISCOUNT_LOAD_SUCCESS,
            'PH_TRADE_DISCOUNT_LOAD_FAILURE': self.PH_TRADE_DISCOUNT_LOAD_FAILURE,
        }

        self.snow_attributes = snow_attributes

        self.EMAIL_TO = EMAIL_RECIPIENTS.get(af_env)

    def PH_TRADE_DISCOUNT_LOAD_SUCCESS(self,context):
        email_to = self.EMAIL_TO

        email_subject = "Trade Discount data refresh completion for Philippines"

        email_message = """Dear All,

The Trade Discount data has been refreshed successfully for Philippines.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )

    def PH_TRADE_DISCOUNT_LOAD_FAILURE(self, context):
        email_to = self.EMAIL_TO

        email_subject = "Trade Discount data refresh failure for Philippines"

        email_message = """Dear All,

Attempts to refresh Trade Discount data for Philippines failed and the team is working to resolve.

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

DAG_ID = "DA-COM-GEM-PH-CHILD-TRADE-DISCOUNT"

default_args = {
    "owner": "airflow",
    "priority_weight": 1,
    "weight_rule": "absolute",
}

dag = DAG(
    dag_id=DAG_ID,
    description="Child DAG for PH TRADE-DISCOUNT",
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
# 731310
# 001-PH-LAKETOHUB-TXN-PH-TRADE-DISCOUNT
# ============================================================

_task_id = "731310_001-PH-LAKETOHUB-TXN-PH-TRADE-DISCOUNT"

A731310 = DatabricksRunNowOperator(
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
# 743912
# 002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT
# ============================================================

_task_id = "743912_002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT"

A743912 = DatabricksRunNowOperator(
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

START >> A731310
A731310 >> A743912
A743912 >> END


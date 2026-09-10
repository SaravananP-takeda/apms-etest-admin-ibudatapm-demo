import sys
import os
from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
import pytz
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "parameters"))

from DA_COM_GEM_VN_PARAMETERS import PARAMS_DICT
from DA_COM_GEM_VN_COMMON import (
    DATABRICKS_CONN_ID,
    on_success_callback_dbx_lf_job,
    on_failure_callback_dbx_lf_job,
)

params = PARAMS_DICT
af_env = Variable.get("af_env")  # "DEV"

CET = pytz.timezone("Europe/Berlin")

default_args = {
    'owner':          'airflow',
    'email' : '',
    "priority_weight": 1,
    "weight_rule": "absolute"
}

dag = DAG(
    dag_id = "DA-COM-GEM-VN-CHILD-DKSH-SHIRE",
    description = "DKSH-SHIRE group child DAG - split out from DA-COM-GEM-VN as part of the Set 1 mutex-pool refactor.",
    default_args = default_args,
    schedule_interval = None,
    is_paused_upon_creation = True,
    concurrency = 16,
    start_date = CET.localize(datetime(2026, 7, 30)),
    catchup = False,
    max_active_runs = 1
)

# ---- 001: LAKETOLAKE VN_DKSH_SHIRE_STG-LOAD ----
_task_id = "601185_001-VN-LAKETOLAKE-VN_DKSH_SHIRE_STG-LOAD"
A601185 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50771_001-VN-LAKETOLAKE-VN_DKSH_SHIRE_STG-LOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# ---- 002: LAKETOHUB REF_CUST_INTL (terminal in this chain) ----
_task_id = "620394_002-VN-LAKETOHUB-REF_CUST_INTL"
A620394 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50773_002-VN-LAKETOHUB-REF_CUST_INTL",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# ---- 003: LAKETOHUB TXN_WHOLESALER_SALES ----
_task_id = "619558_003-VN-LAKETOHUB-TXN_WHOLESALER_SALES"
A619558 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50770_003-VN-LAKETOHUB-TXN_WHOLESALER_SALES",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# ---- 004: HUBTOMART TXN_WHS_SALES_DETAIL ----
_task_id = "604687_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL"
A604687 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50772_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# 001 fans out to 002 (terminal) and 003; 003 feeds 004
A601185 >> [A620394, A619558]
A619558 >> [A604687]

_task_id = "626879_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC"
A626879 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "61131_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)
A604817 = DummyOperator (task_id = "604817_DA-COM-GEM-VN-DMP-EXCEPTION-SYNC", task_display_name = "61129_DA-COM-GEM-VN-DMP-EXCEPTION-SYNC", execution_timeout = timedelta(minutes=60), dag = dag)
A604818 = DummyOperator (task_id = "604818_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-VN", task_display_name = "61130_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-VN", execution_timeout = timedelta(minutes=60), dag = dag)

A619558 >> [A604817]
A604817 >> [A604818]
A604818 >> [A626879]

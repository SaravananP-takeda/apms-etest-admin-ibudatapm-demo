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
    dag_id = "DA-COM-GEM-VN-CHILD-ZP",
    description = "ZP group child DAG - split out from DA-COM-GEM-VN as part of the Set 1 mutex-pool refactor. "
                  "NOTE: in the original single DAG, 628013 (002) also fed 619951 (an IICS job belonging to the "
                  "unrelated EPLUS/COMMON chain, task_id '619846_007-...' family - NOT one of the 6 target groups). "
                  "That cross-domain edge can no longer be a direct task link since 628013 now lives here. In the "
                  "parent DAG, this is approximated by making 619951 depend on the ZP TriggerDagRunOperator's "
                  "completion instead (waits for all of ZP's chain, not just step 002) - flag if a tighter signal is needed.",
    default_args = default_args,
    schedule_interval = None,
    is_paused_upon_creation = True,
    concurrency = 16,
    start_date = CET.localize(datetime(2026, 7, 30)),
    catchup = False,
    max_active_runs = 1
)

# ---- 001: LAKETOLAKE VN_ZP_STG-LOAD ----
_task_id = "620826_001-VN-LAKETOLAKE-VN_ZP_STG-LOAD"
A620826 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50756_001-VN-LAKETOLAKE-VN_ZP_STG-LOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# ---- 002: LAKETOHUB REF_CUST_INTL ----
_task_id = "628013_002-VN-LAKETOHUB-REF_CUST_INTL"
A628013 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50755_002-VN-LAKETOHUB-REF_CUST_INTL",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# ---- 003: LAKETOHUB TXN_WHOLESALER_SALES ----
_task_id = "619028_003-VN-LAKETOHUB-TXN_WHOLESALER_SALES"
A619028 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50769_003-VN-LAKETOHUB-TXN_WHOLESALER_SALES",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# ---- 004: HUBTOMART TXN_WHS_SALES_DETAIL ----
_task_id = "599619_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL"
A599619 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50765_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

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

# 001 fans out to 002 and 003; 003 feeds 004
A620826 >> [A628013, A619028]
A619028 >> [A599619]

A604817 = DummyOperator (task_id = "604817_DA-COM-GEM-VN-DMP-EXCEPTION-SYNC", task_display_name = "61129_DA-COM-GEM-VN-DMP-EXCEPTION-SYNC", execution_timeout = timedelta(minutes=60), dag = dag)
A604818 = DummyOperator (task_id = "604818_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-VN", task_display_name = "61130_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-VN", execution_timeout = timedelta(minutes=60), dag = dag)

A619028 >> [A604817]
A604817 >> [A604818]
A604818 >> [A626879]

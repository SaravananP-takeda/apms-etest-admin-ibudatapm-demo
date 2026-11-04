import sys
import os
from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
import pytz
from airflow.models import Variable
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
    dag_id = "DA-COM-GEM-VN-CHILD-TERRITORY-ALIGNMENT",
    description = "TERRITORY-ALIGNMENT group child DAG - split out from DA-COM-GEM-VN as part of the Set 2 mutex-pool refactor.",
    default_args = default_args,
    schedule_interval = None,
    is_paused_upon_creation = True,
    concurrency = 16,
    start_date = CET.localize(datetime(2026, 7, 30)),
    catchup = False,
    max_active_runs = 1
)

# ---- 001: LAKETOHUB REF_TERR_VN ----
_task_id = "621835_001-VN-LAKETOHUB-REF_TERR_VN"
A621835 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50760_001-VN-LAKETOHUB-REF_TERR_VN",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

# ---- 002: HUBTOMART REF_TERR_ALIGNMENT ----
_task_id = "624482_002-VN-HUBTOMART-REF_TERR_ALIGNMENT"
A624482 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50761_002-VN-HUBTOMART-REF_TERR_ALIGNMENT",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A621835 >> [A624482]

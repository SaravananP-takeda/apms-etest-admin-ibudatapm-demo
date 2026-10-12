import os
import sys
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

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
)


# ============================================================
# ENVIRONMENT
# ============================================================

af_env = Variable.get("af_env", default_var="DEV")
params = PARAMS_DICT


# ============================================================
# DAG
# ============================================================

DAG_ID = "DA-COM-GEM-PH-CHILD-CUSTOMER-MASTER"

default_args = {
    "owner": "airflow",
    "priority_weight": 1,
    "weight_rule": "absolute",
}

dag = DAG(
    dag_id=DAG_ID,
    description="Child DAG for PH CUSTOMER-MASTER",
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
# 731820
# 001-PH-LAKETOHUB-REF-CUST-INTL-MASTER
# ============================================================

_task_id = "731820_001-PH-LAKETOHUB-REF-CUST-INTL-MASTER"

A731820 = DatabricksRunNowOperator(
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
# DEPENDENCIES
# ============================================================
# NOTE:
# Dependencies will be finalized after verifying Tidal UI.

START >> A731820

A731820 >> END
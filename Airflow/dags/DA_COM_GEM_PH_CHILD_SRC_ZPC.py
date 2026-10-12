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

DAG_ID = "DA-COM-GEM-PH-CHILD-SRC-ZPC"

default_args = {
    "owner": "airflow",
    "priority_weight": 1,
    "weight_rule": "absolute",
}

dag = DAG(
    dag_id=DAG_ID,
    description="Child DAG for PH SRC-ZPC",
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
# 747809
# 001-PH-LAKETOLAKE-PH-ZPC-STG
# ============================================================

_task_id = "747809_001-PH-LAKETOLAKE-PH-ZPC-STG"

A747809 = DatabricksRunNowOperator(
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
# 736318
# 002-PH-LAKETOHUB-REF-CUST-INTL
# ============================================================

_task_id = "736318_002-PH-LAKETOHUB-REF-CUST-INTL"

A736318 = DatabricksRunNowOperator(
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
# 735954
# 003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES
# ============================================================

_task_id = "735954_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES"

A735954 = DatabricksRunNowOperator(
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



A709365 = DummyOperator(
    task_id="709365_DA-COM-GEM-PH-DMP-EXCEPTION-SYNC",
    task_display_name="61123_DA-COM-GEM-PH-DMP-EXCEPTION-SYNC",
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

A709366 = DummyOperator(
    task_id="709366_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-PH",
    task_display_name="61124_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-PH",
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)



# ============================================================
# DMP EXCEPTION SYNC
# ============================================================

_task_id = "748097_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC"

A748097 = DatabricksRunNowOperator(
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

A709366_SUCCESS = DummyOperator(
    task_id="709366.SUCCESS",
    task_display_name="61124.SUCCESS",
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)


# ============================================================
# 726984
# 004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES
# ============================================================

_task_id = "726984_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES"

A726984 = DatabricksRunNowOperator(
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



START >> A747809
A747809 >> A736318
A736318 >> A735954
A735954 >> A726984

A735954 >> A709365
A709365 >> A709366

A709366 >> [
    A748097,
    A709366_SUCCESS,
]

[
    A726984,
    A748097,
    A709366_SUCCESS,
] >> END
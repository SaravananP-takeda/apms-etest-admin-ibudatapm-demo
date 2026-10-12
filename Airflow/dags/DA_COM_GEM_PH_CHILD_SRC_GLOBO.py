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
    dag=dag,
)


# ============================================================
# DMP EXCEPTION SYNC
# ============================================================

# 61123
A709365 = DummyOperator(
    task_id="709365_DA-COM-GEM-PH-DMP-EXCEPTION-SYNC",
    task_display_name="61123_DA-COM-GEM-PH-DMP-EXCEPTION-SYNC",
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)


# 61124
A709366 = DummyOperator(
    task_id="709366_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-PH",
    task_display_name="61124_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-PH",
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)


# 61125
_task_id = "748097_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC"

A748097 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="61125_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={
        "job_parameters":
            params[af_env][_task_id]["job_parameters"]
    },
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)


# 61124.SUCCESS
A709366_SUCCESS = DummyOperator(
    task_id="709366.SUCCESS",
    task_display_name="61124.SUCCESS",
    execution_timeout=timedelta(minutes=60),
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

A709365 >> A709366

A709366 >> [
    A748097,
    A709366_SUCCESS,
]

# END
[
    A725593,
    A748097,
    A709366_SUCCESS,
] >> END
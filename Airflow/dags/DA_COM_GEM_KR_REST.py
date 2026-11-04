import sys
import os

from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator
)

from iics_plugin import IICSRunJobOperator


# ============================================================
# PARAMETERS
# ============================================================

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "parameters"
    )
)

from DA_COM_GEM_KR_PARAMETERS import (
    PARAMS_DICT,
    DATABRICKS_CONN_ID
)


params = PARAMS_DICT

af_env = Variable.get(
    "af_env",
    default_var="DEV"
)


# ============================================================
# DEFAULT ARGS
# ============================================================

default_args = {
    "owner": "airflow",
    "email": "",
    "priority_weight": 1,
    "weight_rule": "absolute"
}


# ============================================================
# DAG
# Jobs that sit directly under the root and do NOT depend on
# COMMON-LAMBDA. Kept separate so triggering the lambda flow
# (DA-COM-GEM-KR-GRP) does not trigger these.
# ============================================================

dag = DAG(
    dag_id="DA-COM-GEM-KR-REST",
    description="Tidal to Airflow Migration - KR (jobs independent of COMMON-LAMBDA)",
    default_args=default_args,
    schedule_interval=None,
    concurrency=16,
    start_date=datetime(2026, 9, 13),
    catchup=False,
    max_active_runs=1
)


# ============================================================
# START
# ============================================================

ASTART = DummyOperator(
    task_id="START",
    execution_timeout=timedelta(minutes=60),
    dag=dag
)


# ============================================================
# ROOT DAG
# ============================================================

A709988 = DummyOperator(
    task_id="709988_DA-COM-GEM-KR",
    task_display_name="50812_DA-COM-GEM-KR",
    execution_timeout=timedelta(minutes=60),
    dag=dag
)


# ============================================================
# IICS TASKS (directly under root, not part of any group)
# ============================================================

_task_id = "728171_001-KR-MTASK-REF-MSG-KAKAO-TALK"

A728171 = IICSRunJobOperator(
    task_id=_task_id,

    job_args=params[af_env][_task_id]["job_args"],

    task_display_name="58827_001-KR-MTASK-REF-MSG-KAKAO-TALK",

    execution_timeout=timedelta(minutes=60),

    dag=dag
)


# _task_id = "732769_004-IICS-KR-REF-CUST-CRM-OUTBOUND"

# A732769 = IICSRunJobOperator(
#     task_id=_task_id,

#     job_args=params[af_env][_task_id]["job_args"],

#     task_display_name="73106_004-IICS-KR-REF-CUST-CRM-OUTBOUND",

#     execution_timeout=timedelta(minutes=60),

#     dag=dag
# )


# ============================================================
# 132887 - DATABRICKS REF CUST CRM OUTBOUND
# ============================================================

with TaskGroup(
    group_id="DA-COM-GEM-KR-DATABRICKS-NOTEBOOK-REF-CUST-CRM-OUTBOUND",
    tooltip="DA-COM-GEM-KR-DATABRICKS-NOTEBOOK-REF-CUST-CRM-OUTBOUND",
    prefix_group_id=False,
    dag=dag
) as GNB:

    _task_id = "132887_010-DATABRICKS-RUN-NOTEBOOK-REF-CUST-CRM-OUTBOUND"

    _job_parameters = dict(
        params[af_env][_task_id]["job_parameters"]
    )

    A132887 = DatabricksRunNowOperator(
        task_id=_task_id,

        task_display_name="132887_010-DATABRICKS-RUN-NOTEBOOK-REF-CUST-CRM-OUTBOUND",

        databricks_conn_id=DATABRICKS_CONN_ID,

        job_id=params[af_env][_task_id]["job_id"],

        json={
            "job_parameters": _job_parameters
        },

        execution_timeout=timedelta(minutes=60),

        dag=dag
    )

_task_id = "738952_003-IICS-KR-TXN-CRM-EM-EVENT-OUTBOUND"

A738952 = IICSRunJobOperator(
    task_id=_task_id,

    job_args=params[af_env][_task_id]["job_args"],

    task_display_name="73103_003-IICS-KR-TXN-CRM-EM-EVENT-OUTBOUND",

    execution_timeout=timedelta(minutes=60),

    dag=dag
)


_task_id = "743086_002-IICS-KR-TXN-CRM-EM-EVENT-SPEAKER-OUTBOUND"

A743086 = IICSRunJobOperator(
    task_id=_task_id,

    job_args=params[af_env][_task_id]["job_args"],

    task_display_name="73102_002-IICS-KR-TXN-CRM-EM-EVENT-SPEAKER-OUTBOUND",

    execution_timeout=timedelta(minutes=60),

    dag=dag
)


_task_id = "132869_005-IICS-KR-CRM-MCCP-VW-OUTBOUND"

A132869 = IICSRunJobOperator(
    task_id=_task_id,

    job_args=params[af_env][_task_id]["job_args"],

    task_display_name="132869_005-IICS-KR-CRM-MCCP-VW-OUTBOUND",

    execution_timeout=timedelta(minutes=60),

    dag=dag
)


# ============================================================
# DEPENDENCIES
# ============================================================

ASTART >> [
    A709988
]

# Jobs directly under the root (same as Tidal)
A709988 >> [
    A728171,
    A132869,
    A738952,
    A743086
]


# ============================================================
# GROUP FAN-OUT
# Kept LAST so TaskGroup roots are computed on the complete graph.
# ============================================================

A709988 >> [
    GNB
]

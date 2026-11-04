import sys
import os
import logging

from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator
)

from iics_plugin import IICSRunJobOperator

from airflow.utils.email import send_email
from airflow.utils.task_group import TaskGroup

from tak_airflow_callbacks_utils import (
    send_email_notification,
    on_success_callback_dbx_lf_job,
    on_failure_callback_dbx_lf_job
)


# ============================================================
# PARAMETERS
# ============================================================

# sys.path.insert(
#     0,
#     os.path.join(
#         os.path.dirname(os.path.abspath(__file__)),
#         "parameters"
#     )
# )

from DA_COM_GEM_KR_PARAMETERS import (
    PARAMS_DICT,
    SNOW_ATTRIBUTES_DEFAULTS,
    EMAIL_RECIPIENTS,
    DATABRICKS_CONN_ID,
    AWS_CONN_ID,
    S3_SENSOR_POKE_INTERVAL_SECONDS, S3_SENSOR_TIMEOUT_SECONDS
)


LOG_PFX = "TAKEDA: "

params = PARAMS_DICT

af_env = Variable.get(
    "af_env",
    default_var="DEV"
)


# ============================================================
# SNOW ATTRIBUTES
# ============================================================

snow_attributes = {
    "configuration_item": SNOW_ATTRIBUTES_DEFAULTS["configuration_item"],
    "severity": SNOW_ATTRIBUTES_DEFAULTS["severity"],
    "ci_apms_id": SNOW_ATTRIBUTES_DEFAULTS["ci_apms_id"],
    "ci_bsn_id": SNOW_ATTRIBUTES_DEFAULTS["ci_bsn_id"],
    "apms_id": "APMS-92116",
    "bsn_id": "BSN0020621",
    "type": "AWS-Airflow-EU",
    "env": "PROD",
}


# ============================================================
# DAG CALLBACK ACTION
# ============================================================

class DAGCallbackAction():

    def __init__(self):

        self.on_success_completion_handlers = {
            "743321_001-KR-MART-REF-MSG-KAKAO-TALK": [
                "KR_REF_MSG_KAKAO_TALK_SUCCESS"
            ]
        }

        self.on_failure_abnormal_completion_handlers = {
            "743321_001-KR-MART-REF-MSG-KAKAO-TALK": [
                "KR_REF_MSG_KAKAO_TALK_FAILURE"
            ]
        }

        self.on_failure_error_handlers = {}

        self.handler_callback_fun_map = {
            "KR_REF_MSG_KAKAO_TALK_SUCCESS":
                self.KR_REF_MSG_KAKAO_TALK_SUCCESS,

            "KR_REF_MSG_KAKAO_TALK_FAILURE":
                self.KR_REF_MSG_KAKAO_TALK_FAILURE
        }

        self.snow_attributes = snow_attributes

        self.EMAIL_TO = EMAIL_RECIPIENTS.get(af_env)

    def KR_REF_MSG_KAKAO_TALK_FAILURE(self, context):

        email_subject = (
            "Message Kakao Talk Data Refresh Failure for Korea"
        )

        email_message = (
            "'Dear All,\n\n"
            "Attempts to refresh message kakao talk data for Korea "
            "failed and the team is working to resolve. \n\n"
            "Thanks'"
        )

        context["email_to"] = self.EMAIL_TO
        context["email_subject"] = email_subject
        context["email_message"] = email_message

        send_email_notification(context)

    def KR_REF_MSG_KAKAO_TALK_SUCCESS(self, context):

        email_subject = (
            "Message Kakao Talk Data Refresh Completion for Korea"
        )

        email_message = (
            "'Dear All,\n\n"
            "The message kakao talk data has been refreshed "
            "successfully for Korea. \n\n"
            "Thanks'"
        )

        context["email_to"] = self.EMAIL_TO
        context["email_subject"] = email_subject
        context["email_message"] = email_message

        send_email_notification(context)


# ============================================================
# DBX CALLBACK WRAPPERS
# ============================================================

def on_success_callback_dbx_lf_job_wrapper(context):

    dag_callback_action_obj = DAGCallbackAction()

    on_success_callback_dbx_lf_job(
        context,
        dag_callback_action_obj
    )


def on_failure_callback_dbx_lf_job_wrapper(context):

    dag_callback_action_obj = DAGCallbackAction()

    on_failure_callback_dbx_lf_job(
        context,
        dag_callback_action_obj
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
# ============================================================

dag = DAG(
    dag_id="DA-COM-GEM-KR",
    description="-- Deployed from DEFRATAGAPT001 by onetakeda\\sagpatil on 12/12/2022 11:34:14 AM",
    default_args=default_args,
    schedule_interval = "*/30 * * * *",
    concurrency=16,
    start_date=datetime(2026, 9, 13),
    catchup=False,
    max_active_runs=1
)

# Layout mirrors the Tidal hierarchy (same pattern as DA-COM-GEM-ID-GRP / DA-COM-GEM-PH-GRP):
#   START -> 709988_DA-COM-GEM-KR (root) -> one TaskGroup per Tidal job group.
# COMMON-LAMBDA holds the two Tidal sub-folders (CONTRACT-LAMBDA / LAKE-LAMBDA) as nested groups.
# prefix_group_id=False keeps every task_id unchanged (the xcom_pull on
# 'MSG-KAKAO-TALK-FILEWATCH' below depends on this).
# The 5 jobs that sit directly under the root and do not depend on COMMON-LAMBDA
# (4 IICS + 1 Databricks notebook) live in DAG DA-COM-GEM-KR-REST.


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
# COMMON LAMBDA (group)
# All lambda jobs: filewatch + lakeload + their SUCCESS markers
# ============================================================

with TaskGroup(
    group_id="716516_DA-COM-GEM-KR-COMMON-LAMBDA",
    tooltip="50813_DA-COM-GEM-KR-COMMON-LAMBDA",
    prefix_group_id=False,
    dag=dag
) as G716516:

    A716516_SUCCESS = DummyOperator(
        task_id="716516.SUCCESS",
        task_display_name="50813.SUCCESS",
        execution_timeout=timedelta(minutes=60),
        dag=dag
    )


    with TaskGroup(
        group_id="716517_001-CONTRACT-LAMBDA-PROCESS-MSG-KAKAO-TALK",
        tooltip="50814_001-CONTRACT-LAMBDA-PROCESS-MSG-KAKAO-TALK",
        prefix_group_id=False,
        dag=dag
    ) as G716517:

        # --------------------------------------------------------
        # CONTRACT LAMBDA PROCESS
        # --------------------------------------------------------

        A716517 = DummyOperator(
            task_id="PROCESS-MSG-KAKAO-TALK-FILEWATCH",
            task_display_name="PROCESS-MSG-KAKAO-TALK-FILEWATCH",
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        A716517_SUCCESS = DummyOperator(
            task_id="716517.SUCCESS",
            task_display_name="50814.SUCCESS",
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )


        # --------------------------------------------------------
        # FILEWATCH
        #
        # Airflow task_id is changed to FILEWATCH.
        # Parameter file still contains the old key, so we use
        # PARAMETER_TASK_ID for parameter lookup.
        # --------------------------------------------------------

        PARAMETER_TASK_ID_FILEWATCH = (
            "MSG-KAKAO-TALK-FILEWATCH"
        )

        A716518 = S3KeySensor(
            task_id="MSG-KAKAO-TALK-FILEWATCH",

            task_display_name="MSG-KAKAO-TALK-FILEWATCH",

            bucket_name=params[af_env][
                PARAMETER_TASK_ID_FILEWATCH
            ]["bucket_name"],

            bucket_key=params[af_env][
                PARAMETER_TASK_ID_FILEWATCH
            ]["bucket_key"],

            aws_conn_id=AWS_CONN_ID,

            wildcard_match=True,

            poke_interval=S3_SENSOR_POKE_INTERVAL_SECONDS,

            timeout=S3_SENSOR_TIMEOUT_SECONDS,

            mode="reschedule",

            dag=dag
        )


    with TaskGroup(
        group_id="737711_001-LAKE-LAMBDA-PROCESSING-MSG-KAKAO-TALK",
        tooltip="50816_001-LAKE-LAMBDA-PROCESSING-MSG-KAKAO-TALK",
        prefix_group_id=False,
        dag=dag
    ) as G737711:

        # --------------------------------------------------------
        # LAKE LAMBDA PROCESSING
        # --------------------------------------------------------

        A737711 = DummyOperator(
            task_id="PROCESSING-MSG-KAKAO-TALK_LAKELOAD",
            task_display_name="PROCESSING-MSG-KAKAO-TALK-LAKELOAD",
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        A737711_SUCCESS = DummyOperator(
            task_id="737711.SUCCESS",
            task_display_name="50816.SUCCESS",
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )


        # --------------------------------------------------------
        # LAKELOAD
        #
        # Airflow task_id is changed to LAKELOAD.
        # Parameter file still contains the old key.
        # --------------------------------------------------------

        PARAMETER_TASK_ID_LAKELOAD = (
            "MSG-KAKAO-TALK-LAKELOAD"
        )

        _job_parameters = dict(
            params[af_env][
                PARAMETER_TASK_ID_LAKELOAD
            ]["job_parameters"]
        )


        if "*" in params[af_env][
            PARAMETER_TASK_ID_FILEWATCH
        ]["bucket_key"]:

            _job_parameters["bucket_key"] = (
                "{{ ti.xcom_pull("
                "task_ids='MSG-KAKAO-TALK-FILEWATCH', "
                "key='resolved_key') }}"
            )


        A737712 = DatabricksRunNowOperator(
            task_id="MSG-KAKAO-TALK-LAKELOAD",

            task_display_name="MSG-KAKAO-TALK-LAKELOAD",

            databricks_conn_id=DATABRICKS_CONN_ID,

            job_id=params[af_env][
                PARAMETER_TASK_ID_LAKELOAD
            ]["job_id"],

            json={
                "job_parameters": _job_parameters
            },

            execution_timeout=timedelta(minutes=60),

            dag=dag
        )


# ============================================================
# LAKE TO HUB (group)
# ============================================================

with TaskGroup(
    group_id="739681_DA-COM-GEM-KR-LAKE-TO-HUB-BDM",
    tooltip="50819_DA-COM-GEM-KR-LAKE-TO-HUB-BDM",
    prefix_group_id=False,
    dag=dag
) as G739681:

    A739681_SUCCESS = DummyOperator(
        task_id="739681.SUCCESS",
        task_display_name="50819.SUCCESS",
        execution_timeout=timedelta(minutes=60),
        dag=dag
    )


    # --------------------------------------------------------
    # 742679 - KR HUB REF MSG KAKAO TALK
    # --------------------------------------------------------

    _task_id = "742679_001-KR-HUB-REF-MSG-KAKAO-TALK"

    _job_parameters = dict(
        params[af_env][_task_id]["job_parameters"]
    )

    A742679 = DatabricksRunNowOperator(
        task_id=_task_id,

        task_display_name="50820_001-KR-HUB-REF-MSG-KAKAO-TALK",

        databricks_conn_id=DATABRICKS_CONN_ID,

        job_id=params[af_env][_task_id]["job_id"],

        json={
            "job_parameters": _job_parameters
        },

        execution_timeout=timedelta(minutes=60),

        dag=dag
    )


# ============================================================
# HUB TO MART (group)
# ============================================================

with TaskGroup(
    group_id="737469_DA-COM-GEM-KR-HUB-TO-MART-BDM",
    tooltip="50818_DA-COM-GEM-KR-HUB-TO-MART-BDM",
    prefix_group_id=False,
    dag=dag
) as G737469:

    A737469_SUCCESS = DummyOperator(
        task_id="737469.SUCCESS",
        task_display_name="50818.SUCCESS",
        execution_timeout=timedelta(minutes=60),
        dag=dag
    )


    # --------------------------------------------------------
    # 743321 - KR MART REF MSG KAKAO TALK
    # --------------------------------------------------------

    _task_id = "743321_001-KR-MART-REF-MSG-KAKAO-TALK"

    _job_parameters = dict(
        params[af_env][_task_id]["job_parameters"]
    )

    A743321 = DatabricksRunNowOperator(
        task_id=_task_id,

        task_display_name="50821_001-KR-MART-REF-MSG-KAKAO-TALK",

        databricks_conn_id=DATABRICKS_CONN_ID,

        job_id=params[af_env][_task_id]["job_id"],

        json={
            "job_parameters": _job_parameters
        },

        execution_timeout=timedelta(minutes=60),

        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,

        on_success_callback=on_success_callback_dbx_lf_job_wrapper,

        dag=dag
    )


# ============================================================
# DEPENDENCIES
# ============================================================

ASTART >> [
    A709988
]

# ---- COMMON-LAMBDA group (internal chain) ----
A716517 >> [
    A716517_SUCCESS,
    A716518
]

A716518 >> [
    A716517_SUCCESS,
    A737712
]

A737711 >> [
    A716516_SUCCESS,
    A737712
]

A737712 >> [
    A737711_SUCCESS
]

# ---- cross-group job events ----
A737711_SUCCESS >> [
    A742679
]

A742679 >> [
    A743321
]

A743321 >> [
    A737469_SUCCESS
]


# ============================================================
# GROUP FAN-OUT
# Kept LAST so TaskGroup roots are computed on the complete graph.
# ============================================================

A709988 >> [
    G716516,
    G739681,
    G737469
]

import sys
import os
import logging
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.models import Variable
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.sensors.weekday import DayOfWeekSensor
import pytz

from airflow.exceptions import AirflowSensorTimeout
from botocore.exceptions import ClientError

from iics_plugin import IICSRunJobOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow_servicenow_handler import notify_api_on_failure
from airflow.utils.email import send_email

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "parameters"))

from DA_COM_GEM_VN_PARAMETERS import PARAMS_DICT, EMAIL_RECIPIENTS, CHILD_DAG_IDS, POOLS, SENSOR_POKE_INTERVAL_SECONDS, SENSOR_TIMEOUT_SECONDS
from DA_COM_GEM_VN_COMMON import (
    DATABRICKS_CONN_ID,
    on_success_callback_S3,
    on_failure_callback_S3,
    on_success_callback_python,
    on_failure_callback_python,
    on_success_callback_lambda,
    on_failure_callback_lambda,
    on_success_callback_iics,
    on_failure_callback_iics,
    on_success_callback_dbx_lf_job,
    on_failure_callback_dbx_lf_job,
)


LOG_PFX = "TAKEDA: "

params = PARAMS_DICT
af_env = Variable.get("af_env")
vn_email_recipients = EMAIL_RECIPIENTS
VN_DBX_EMAIL_TO = [vn_email_recipients.get(af_env, "")]

batch_override = None

default_args = {
    'owner':          'airflow',
    'email' : '',
    "priority_weight": 1,
    "weight_rule": "absolute"
}

SCHEDULE_TZ_NAME = "Europe/Berlin"
CET = pytz.timezone(SCHEDULE_TZ_NAME)

SCHEDULE_TZ = CET
SCHEDULE_CRON = "0 0,5,14 * * *"
LONG_CHAU_HOURS = {0}
EPLUS_HOURS = {5, 14}
SCHEDULE_HOURS = LONG_CHAU_HOURS | EPLUS_HOURS

LONG_CHAU_ENTRY = "720968_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-SRC-TO-LAKE"
EPLUS_ENTRY = "592186_DA-COM-GEM-DATABRICKS-NOTEBOOK-VN-EPLUS-SRC-TO-LAKE"

dag = DAG(
    dag_id = "DA-COM-GEM-VN-REST",
    description = r"Was moved by EU-PROD-TO-EU-PROD-REPO from EU-PROD-CONN (20.25.2) to EU-PROD-REPOSITORY (Latest) by onetakeda\cef9990 on 04/22/2026 06:46:07 PM -- Deployed from DEFRATAGAD001 by onetakeda\sagpatil on 12/17/2021 07:39:50 AM -- Deployed from DEFRATAGAD001 by onetakeda\sagpatil on 12/17/2021 07:39:50 AM -- Deployed from DEFRATAGAD001 by onetakeda\sagpatil on 12/17/2021 07:39:50 AM -- Deployed from DEFRATAGAPT001 by onetakeda\zym3710 on 12/09/2022 09:40:50 AM -- Updated from DEFRATAGAPT001 by onetakeda\zym3710 on 12/09/2022 09:48:05 AM -- Updated from DEFRATAGAPT001 by onetakeda\zym3710 on 12/09/2022 10:00:31 AM",
    default_args = default_args,
    schedule = CronTriggerTimetable(SCHEDULE_CRON, timezone = SCHEDULE_TZ_NAME),
    concurrency = 16,
    start_date = SCHEDULE_TZ.localize(datetime(2026,7,30)),
    catchup = False,
    max_active_runs = 1
)


def test_function():
    logging.info(f"{LOG_PFX}Tidal To Airflow Migration Testing...")
    return True


ASTART = DummyOperator (task_id = "START", execution_timeout = timedelta(minutes=60), dag = dag)

A592184 = DummyOperator (task_id = "592184_DA-COM-GEM-VN", task_display_name = "50703_DA-COM-GEM-VN", execution_timeout = timedelta(minutes=60), dag = dag)


def choose_group(data_interval_end, **_):
    hour = data_interval_end.astimezone(SCHEDULE_TZ).hour
    slot = max((h for h in SCHEDULE_HOURS if h <= hour), default=None)
    logging.info(f"{LOG_PFX}Run hour ({SCHEDULE_TZ.zone}): {hour}, slot: {slot}")
    if slot in LONG_CHAU_HOURS:
        return [LONG_CHAU_ENTRY]
    if slot in EPLUS_HOURS:
        return [EPLUS_ENTRY]
    return []


AROUTE = BranchPythonOperator(
    task_id = "ROUTE_BY_SCHEDULE",
    python_callable = choose_group,
    execution_timeout = timedelta(minutes=5),
    dag = dag,
)


with TaskGroup(
    group_id="592185_DA-COM-GEM-VN-SRC-EPLUS",
    tooltip="65690_DA-COM-GEM-VN-SRC-EPLUS",
    prefix_group_id=False,
    dag=dag,
) as A592185:

    A592186 = DummyOperator (task_id = "592186_DA-COM-GEM-DATABRICKS-NOTEBOOK-VN-EPLUS-SRC-TO-LAKE", task_display_name = "65693_DA-COM-GEM-DATABRICKS-NOTEBOOK-VN-EPLUS-SRC-TO-LAKE", execution_timeout = timedelta(minutes=60), dag = dag)

    _task_id = "593423_010-DATABRICKS-RUN-NOTEBOOK-VN-EPLUS-SRC-TO-LAKE"
    A593423 = DatabricksRunNowOperator (task_id = _task_id,
                                        task_display_name = "65706_010-DATABRICKS-RUN-NOTEBOOK-VN-EPLUS-SRC-TO-LAKE",
                                        databricks_conn_id = DATABRICKS_CONN_ID,
                                        job_id = params[af_env][_task_id]["job_id"],
                                        json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                        execution_timeout = timedelta(minutes=60),
                                        on_failure_callback = on_failure_callback_dbx_lf_job,
                                        on_success_callback = on_success_callback_dbx_lf_job,
                                        dag = dag)

    A618765 = DummyOperator (task_id = "618765_DA-COM-GEM-VN-LAKETOLAKE", task_display_name = "65701_DA-COM-GEM-VN-LAKETOLAKE", execution_timeout = timedelta(minutes=60), dag = dag)

    _task_id = "619971_005-VN-COM-DE-LAKETOLAKE-EPLUS-ITEMS-BY-LOCATION-STG"
    A619971 = IICSRunJobOperator(task_id = _task_id,
                                 job_args = params[af_env][_task_id]["job_args"],
                                 task_display_name = "65699_005-VN-COM-DE-LAKETOLAKE-EPLUS-ITEMS-BY-LOCATION-STG",
                                 execution_timeout = timedelta(minutes=60),
                                 on_failure_callback = on_failure_callback_iics,
                                 on_success_callback = on_success_callback_iics,
                                 dag = dag)

    _task_id = "622159_006-VN-COM-DE-LAKETOLAKE-EPLUS-MSTOCK-STG"
    A622159 = IICSRunJobOperator(task_id = _task_id,
                                 job_args = params[af_env][_task_id]["job_args"],
                                 task_display_name = "65695_006-VN-COM-DE-LAKETOLAKE-EPLUS-MSTOCK-STG",
                                 execution_timeout = timedelta(minutes=60),
                                 on_failure_callback = on_failure_callback_iics,
                                 on_success_callback = on_success_callback_iics,
                                 dag = dag)

    _task_id = "619846_007-VN-COM-DE-LAKETOLAKE-EPLUS-PENDING-PO-STG"
    A619846 = IICSRunJobOperator(task_id = _task_id,
                                 job_args = params[af_env][_task_id]["job_args"],
                                 task_display_name = "65697_007-VN-COM-DE-LAKETOLAKE-EPLUS-PENDING-PO-STG",
                                 execution_timeout = timedelta(minutes=60),
                                 on_failure_callback = on_failure_callback_iics,
                                 on_success_callback = on_success_callback_iics,
                                 dag = dag)

    _task_id = "606336_001-VN-COM-DE-LAKETOLAKE-EPLUS-SALES-STG"
    A606336 = IICSRunJobOperator(task_id = _task_id,
                                 job_args = params[af_env][_task_id]["job_args"],
                                 task_display_name = "65692_001-VN-COM-DE-LAKETOLAKE-EPLUS-SALES-STG",
                                 execution_timeout = timedelta(minutes=60),
                                 on_failure_callback = on_failure_callback_iics,
                                 on_success_callback = on_success_callback_iics,
                                 dag = dag)

    _task_id = "619951_002-VN-COM-DE-LAKETOHUB-EPLUS-REF-CUST-INTL"
    A619951 = IICSRunJobOperator(task_id = _task_id,
                                 job_args = params[af_env][_task_id]["job_args"],
                                 task_display_name = "65704_002-VN-COM-DE-LAKETOHUB-EPLUS-REF-CUST-INTL",
                                 execution_timeout = timedelta(minutes=60),
                                 on_failure_callback = on_failure_callback_iics,
                                 on_success_callback = on_success_callback_iics,
                                 pool = POOLS["SET1"],
                                 dag = dag)

    _task_id = "628671_003-VN-COM-DE-LAKETOHUB-TXN-EPLUS-WHS-SALES"
    A628671 = IICSRunJobOperator(task_id = _task_id,
                                 job_args = params[af_env][_task_id]["job_args"],
                                 task_display_name = "65696_003-VN-COM-DE-LAKETOHUB-TXN-EPLUS-WHS-SALES",
                                 execution_timeout = timedelta(minutes=60),
                                 on_failure_callback = on_failure_callback_iics,
                                 on_success_callback = on_success_callback_iics,
                                 dag = dag)

    A611982 = DummyOperator (task_id = "611982_DA-COM-GEM-DATABRICKS-NOTEBOOK-HUBTOMART-TXN-EPLUS-WHS-SALES", task_display_name = "65691_DA-COM-GEM-DATABRICKS-NOTEBOOK-HUBTOMART-TXN-EPLUS-WHS-SALES", execution_timeout = timedelta(minutes=60), dag = dag)

    _task_id = "624973_010-DATABRICKS-RUN-NOTEBOOK-HUBTOMART-TXN-EPLUS-WHS-SALES"
    A624973 = DatabricksRunNowOperator (task_id = _task_id,
                                        task_display_name = "65698_010-DATABRICKS-RUN-NOTEBOOK-HUBTOMART-TXN-EPLUS-WHS-SALES",
                                        databricks_conn_id = DATABRICKS_CONN_ID,
                                        job_id = params[af_env][_task_id]["job_id"],
                                        json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                        execution_timeout = timedelta(minutes=60),
                                        on_failure_callback = on_failure_callback_dbx_lf_job,
                                        on_success_callback = on_success_callback_dbx_lf_job,
                                        dag = dag)

    A592185_SUCCESS = DummyOperator (task_id = "592185.SUCCESS", task_display_name = "65690.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

    A592186 >> [A593423]
    A592186 >> [A619971, A622159, A619846, A606336]
    A606336 >> [A619951]
    A619951 >> [A628671]
    A628671 >> [A611982]
    A611982 >> [A624973]
    A592186 >> [A592185_SUCCESS]
    A606336 >> [A592185_SUCCESS]
    A619951 >> [A592185_SUCCESS]
    A628671 >> [A592185_SUCCESS]
    A611982 >> [A592185_SUCCESS]
    A618765 >> [A592185_SUCCESS]
    A619971 >> [A592185_SUCCESS]
    A622159 >> [A592185_SUCCESS]
    A619846 >> [A592185_SUCCESS]


with TaskGroup(
    group_id="719418_DA-COM-GEM-VN-SRC-LONG-CHAU",
    tooltip="73157_DA-COM-GEM-VN-SRC-LONG-CHAU",
    prefix_group_id=False,
    dag=dag,
) as A719418:

    _task_id = "720968_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-SRC-TO-LAKE"
    A720968 = DatabricksRunNowOperator (task_id = _task_id,
                                        task_display_name = "73160_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-SRC-TO-LAKE",
                                        databricks_conn_id = DATABRICKS_CONN_ID,
                                        job_id = params[af_env][_task_id]["job_id"],
                                        json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                        execution_timeout = timedelta(minutes=60),
                                        on_failure_callback = on_failure_callback_dbx_lf_job,
                                        on_success_callback = on_success_callback_dbx_lf_job,
                                        dag = dag)

    _task_id = "728219_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-LAKE-TO-STG"
    A728219 = DatabricksRunNowOperator (task_id = _task_id,
                                        task_display_name = "73166_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-LAKE-TO-STG",
                                        databricks_conn_id = DATABRICKS_CONN_ID,
                                        job_id = params[af_env][_task_id]["job_id"],
                                        json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                        execution_timeout = timedelta(minutes=60),
                                        on_failure_callback = on_failure_callback_dbx_lf_job,
                                        on_success_callback = on_success_callback_dbx_lf_job,
                                        dag = dag)

    _task_id = "739887_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-STG-TO-MART"
    A739887 = DatabricksRunNowOperator (task_id = _task_id,
                                        task_display_name = "73171_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-STG-TO-MART",
                                        databricks_conn_id = DATABRICKS_CONN_ID,
                                        job_id = params[af_env][_task_id]["job_id"],
                                        json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                        execution_timeout = timedelta(minutes=60),
                                        on_failure_callback = on_failure_callback_dbx_lf_job,
                                        on_success_callback = on_success_callback_dbx_lf_job,
                                        dag = dag)

    A720968 >> [A728219]
    A728219 >> [A739887]


ASTART >> A592184 >> AROUTE
AROUTE >> [A592186, A720968]

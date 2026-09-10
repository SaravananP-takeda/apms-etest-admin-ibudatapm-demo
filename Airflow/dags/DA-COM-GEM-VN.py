import sys
import os
import logging
from airflow import DAG
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

# import tak_events

LOG_PFX = "TAKEDA: "

params = PARAMS_DICT
af_env = Variable.get("af_env")  # "DEV"
vn_email_recipients = EMAIL_RECIPIENTS
VN_DBX_EMAIL_TO = [vn_email_recipients.get(af_env, "")]

batch_override = None

default_args = {
    'owner':          'airflow',
    'email' : '',
    "priority_weight": 1, # TO DO - to check this later
    "weight_rule": "absolute"
}

CET = pytz.timezone("Europe/Berlin")

dag = DAG(
    dag_id = "DA-COM-GEM-VN",
    description = "Was moved by EU-PROD-TO-EU-PROD-REPO from EU-PROD-CONN (20.25.2) to EU-PROD-REPOSITORY (Latest) by onetakeda\cef9990 on 04/22/2026 06:46:07 PM -- Deployed from DEFRATAGAD001 by onetakeda\sagpatil on 12/17/2021 07:39:50 AM -- Deployed from DEFRATAGAD001 by onetakeda\sagpatil on 12/17/2021 07:39:50 AM -- Deployed from DEFRATAGAD001 by onetakeda\sagpatil on 12/17/2021 07:39:50 AM -- Deployed from DEFRATAGAPT001 by onetakeda\zym3710 on 12/09/2022 09:40:50 AM -- Updated from DEFRATAGAPT001 by onetakeda\zym3710 on 12/09/2022 09:48:05 AM -- Updated from DEFRATAGAPT001 by onetakeda\zym3710 on 12/09/2022 10:00:31 AM",
    default_args = default_args,
    schedule_interval = "0 0 * * *",
    concurrency = 16,
    start_date = CET.localize(datetime(2026,7,30)),
    catchup = False,
    max_active_runs = 1
)


def test_function():
    logging.info(f"{LOG_PFX}Tidal To Airflow Migration Testing...")
    return True

# Jobs



A623252_SUCCESS = DummyOperator (task_id = "623252.SUCCESS", task_display_name = "50731.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)


A608174 = TriggerDagRunOperator (task_id = "608174_DA-COM-GEM-VN-TARGETSALES",
                                 task_display_name = "50751_DA-COM-GEM-VN-TARGETSALES",
                                 trigger_dag_id = CHILD_DAG_IDS["TARGETSALES"],
                                 wait_for_completion = True,
                                 poke_interval = 60,
                                 reset_dag_run = True,
                                 execution_timeout = timedelta(minutes=180),
                                 pool = POOLS["SET2"],
                                 dag = dag)

A596787_SUCCESS = DummyOperator (task_id = "596787.SUCCESS", task_display_name = "50735.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "VN-CHANNEL-MAPPING-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-CHANNEL-MAPPING-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-CHANNEL-MAPPING-FILEWATCH', key='resolved_key') }}"
A624634 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-CHANNEL-MAPPING-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A599618 = TriggerDagRunOperator (task_id = "599618_DA-COM-GEM-VN-SRC-ZP",
                                 task_display_name = "50749_DA-COM-GEM-VN-SRC-ZP",
                                 trigger_dag_id = CHILD_DAG_IDS["ZP"],
                                 wait_for_completion = True,
                                 poke_interval = 60,
                                 reset_dag_run = True,
                                 execution_timeout = timedelta(minutes=180),
                                 pool = POOLS["SET1"],
                                 dag = dag)


A621834_SUCCESS = DummyOperator (task_id = "621834.SUCCESS", task_display_name = "50752.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "VN-NONCHARM-USER-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-NONCHARM-USER-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-NONCHARM-USER-FILEWATCH', key='resolved_key') }}"
A596788 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-NONCHARM-USER-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)


A621834 = TriggerDagRunOperator (task_id = "621834_DA-COM-GEM-VN-TERRITORY-ALIGNMENT",
                                 task_display_name = "50752_DA-COM-GEM-VN-TERRITORY-ALIGNMENT",
                                 trigger_dag_id = CHILD_DAG_IDS["TERRITORY-ALIGNMENT"],
                                 wait_for_completion = True,
                                 poke_interval = 60,
                                 reset_dag_run = True,
                                 execution_timeout = timedelta(minutes=180),
                                 pool = POOLS["SET2"],
                                 dag = dag)

_task_id = "622159_006-VN-COM-DE-LAKETOLAKE-EPLUS-MSTOCK-STG"
A622159 = IICSRunJobOperator(task_id = _task_id,
                             job_args = params[af_env][_task_id]["job_args"],
                             task_display_name = "65695_006-VN-COM-DE-LAKETOLAKE-EPLUS-MSTOCK-STG",
                             execution_timeout = timedelta(minutes=60),
                             on_failure_callback = on_failure_callback_iics,
                             on_success_callback = on_success_callback_iics,
                             dag = dag)

A592186 = DummyOperator (task_id = "592186_DA-COM-GEM-DATABRICKS-NOTEBOOK-VN-EPLUS-SRC-TO-LAKE", task_display_name = "65693_DA-COM-GEM-DATABRICKS-NOTEBOOK-VN-EPLUS-SRC-TO-LAKE", execution_timeout = timedelta(minutes=60), dag = dag)

A619519_SUCCESS = DummyOperator (task_id = "619519.SUCCESS", task_display_name = "50763.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "VN-CUSTOMER-EXCLUDE-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-CUSTOMER-EXCLUDE-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-CUSTOMER-EXCLUDE-FILEWATCH', key='resolved_key') }}"
A629626 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-CUSTOMER-EXCLUDE-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "VN-ADMIN-MAPTERRITORY-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-ADMIN-MAPTERRITORY-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-ADMIN-MAPTERRITORY-FILEWATCH', key='resolved_key') }}"
A623253 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-ADMIN-MAPTERRITORY-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "VN-ADMIN-CUSTOMER-FILEWATCH"
A612091 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-ADMIN-CUSTOMER-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)


A594895_SUCCESS = DummyOperator (task_id = "594895.SUCCESS", task_display_name = "50704.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A597402_SUCCESS = DummyOperator (task_id = "597402.SUCCESS", task_display_name = "50764.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "VN-ADMIN-CUSTOMER-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-ADMIN-CUSTOMER-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-ADMIN-CUSTOMER-FILEWATCH', key='resolved_key') }}"
A612757 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-ADMIN-CUSTOMER-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "628671_003-VN-COM-DE-LAKETOHUB-TXN-EPLUS-WHS-SALES"
A628671 = IICSRunJobOperator(task_id = _task_id,
                             job_args = params[af_env][_task_id]["job_args"],
                             task_display_name = "65696_003-VN-COM-DE-LAKETOHUB-TXN-EPLUS-WHS-SALES",
                             execution_timeout = timedelta(minutes=60),
                             on_failure_callback = on_failure_callback_iics,
                             on_success_callback = on_success_callback_iics,
                             dag = dag)

_task_id = "619520_001-VN-LAKETOHUB-REF_CUST_INTL_MASTER"
A619520 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "50775_001-VN-LAKETOHUB-REF_CUST_INTL_MASTER",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": params[af_env][_task_id]["job_parameters"]},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    pool = POOLS["SET1"],
                                    dag = dag)

A613196_SUCCESS = DummyOperator (task_id = "613196.SUCCESS", task_display_name = "50727.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

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

_task_id = "VN-TARGETSALES-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-TARGETSALES-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-TARGETSALES-FILEWATCH', key='resolved_key') }}"
A624848 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-TARGETSALES-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

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


A719418 = DummyOperator (task_id = "719418_DA-COM-GEM-VN-SRC-LONG-CHAU", task_display_name = "73157_DA-COM-GEM-VN-SRC-LONG-CHAU", execution_timeout = timedelta(minutes=60), dag = dag)

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

_task_id = "619971_005-VN-COM-DE-LAKETOLAKE-EPLUS-ITEMS-BY-LOCATION-STG"
A619971 = IICSRunJobOperator(task_id = _task_id,
                             job_args = params[af_env][_task_id]["job_args"],
                             task_display_name = "65699_005-VN-COM-DE-LAKETOLAKE-EPLUS-ITEMS-BY-LOCATION-STG",
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



_task_id = "VN-DKSH-SHIRE-FILEWATCH"
A624845 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-DKSH-SHIRE-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

_task_id = "VN-CHANNEL-MAPPING-FILEWATCH"
A624633 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-CHANNEL-MAPPING-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A629625_SUCCESS = DummyOperator (task_id = "629625.SUCCESS", task_display_name = "50715.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)


A627839_SUCCESS = DummyOperator (task_id = "627839.SUCCESS", task_display_name = "50739.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A602823_SUCCESS = DummyOperator (task_id = "602823.SUCCESS", task_display_name = "50719.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)



_task_id = "619951_002-VN-COM-DE-LAKETOHUB-EPLUS-REF-CUST-INTL"
A619951 = IICSRunJobOperator(task_id = _task_id,
                             job_args = params[af_env][_task_id]["job_args"],
                             task_display_name = "65704_002-VN-COM-DE-LAKETOHUB-EPLUS-REF-CUST-INTL",
                             execution_timeout = timedelta(minutes=60),
                             on_failure_callback = on_failure_callback_iics,
                             on_success_callback = on_success_callback_iics,
                             pool = POOLS["SET1"],
                             dag = dag)

A599618_SUCCESS = DummyOperator (task_id = "599618.SUCCESS", task_display_name = "50749.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)



A624847_SUCCESS = DummyOperator (task_id = "624847.SUCCESS", task_display_name = "50743.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A592185_SUCCESS = DummyOperator (task_id = "592185.SUCCESS", task_display_name = "65690.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "VN-DKSH-TAKEDA-FILEWATCH"
A602823 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-DKSH-TAKEDA-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

_task_id = "VN-TARGETSALES-FILEWATCH"
A624847 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-TARGETSALES-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A608174_SUCCESS = DummyOperator (task_id = "608174.SUCCESS", task_display_name = "50751.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A597402 = TriggerDagRunOperator (task_id = "597402_DA-COM-GEM-VN-SRC-DKSH",
                                 task_display_name = "50764_DA-COM-GEM-VN-SRC-DKSH",
                                 trigger_dag_id = CHILD_DAG_IDS["DKSH"],
                                 wait_for_completion = True,
                                 poke_interval = 60,
                                 reset_dag_run = True,
                                 execution_timeout = timedelta(minutes=180),
                                 pool = POOLS["SET1"],
                                 dag = dag)

_task_id = "VN-DKSH-TAKEDA-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-DKSH-TAKEDA-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-DKSH-TAKEDA-FILEWATCH', key='resolved_key') }}"
A603726 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-DKSH-TAKEDA-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)


_task_id = "VN-ADMIN-MAPTERRITORY-FILEWATCH"
A623252 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-ADMIN-MAPTERRITORY-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A611377 = TriggerDagRunOperator (task_id = "611377_DA-COM-GEM-VN-PRODUCT",
                                 task_display_name = "50750_DA-COM-GEM-VN-PRODUCT",
                                 trigger_dag_id = CHILD_DAG_IDS["PRODUCT"],
                                 wait_for_completion = True,
                                 poke_interval = 60,
                                 reset_dag_run = True,
                                 execution_timeout = timedelta(minutes=180),
                                 pool = POOLS["SET2"],
                                 dag = dag)

_task_id = "VN-DKSH-SHIRE-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-DKSH-SHIRE-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-DKSH-SHIRE-FILEWATCH', key='resolved_key') }}"
A625294 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-DKSH-SHIRE-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A611982 = DummyOperator (task_id = "611982_DA-COM-GEM-DATABRICKS-NOTEBOOK-HUBTOMART-TXN-EPLUS-WHS-SALES", task_display_name = "65691_DA-COM-GEM-DATABRICKS-NOTEBOOK-HUBTOMART-TXN-EPLUS-WHS-SALES", execution_timeout = timedelta(minutes=60), dag = dag)


A601184_SUCCESS = DummyOperator (task_id = "601184.SUCCESS", task_display_name = "50762.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A612091_SUCCESS = DummyOperator (task_id = "612091.SUCCESS", task_display_name = "50711.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A594895 = DummyOperator (task_id = "594895_DA-COM-GEM-VN-COMMON-LAMBDA", task_display_name = "50704_DA-COM-GEM-VN-COMMON-LAMBDA", execution_timeout = timedelta(minutes=60), dag = dag)

A592185 = DummyOperator (task_id = "592185_DA-COM-GEM-VN-SRC-EPLUS", task_display_name = "65690_DA-COM-GEM-VN-SRC-EPLUS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "VN-DAILYNETSALESDATA-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-DAILYNETSALESDATA-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-DAILYNETSALESDATA-FILEWATCH', key='resolved_key') }}"
A619392 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-DAILYNETSALESDATA-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)


A592184 = DummyOperator (task_id = "592184_DA-COM-GEM-VN", task_display_name = "50703_DA-COM-GEM-VN", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "VN-NONCHARM-USER-FILEWATCH"
A596787 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-NONCHARM-USER-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A618857_SUCCESS = DummyOperator (task_id = "618857.SUCCESS", task_display_name = "50747.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "VN-ADMIN-MAPCALENDAR-FILEWATCH"
A613196 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-ADMIN-MAPCALENDAR-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

_task_id = "VN-ADMIN-MAPCALENDAR-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-ADMIN-MAPCALENDAR-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-ADMIN-MAPCALENDAR-FILEWATCH', key='resolved_key') }}"
A613824 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-ADMIN-MAPCALENDAR-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

A619519 = DummyOperator (task_id = "619519_DA-COM-GEM-VN-CUSTOMER-MASTER", task_display_name = "50763_DA-COM-GEM-VN-CUSTOMER-MASTER", execution_timeout = timedelta(minutes=60), dag = dag)


A601184 = TriggerDagRunOperator (task_id = "601184_DA-COM-GEM-VN-SRC-DKSH-SHIRE",
                                 task_display_name = "50762_DA-COM-GEM-VN-SRC-DKSH-SHIRE",
                                 trigger_dag_id = CHILD_DAG_IDS["DKSH-SHIRE"],
                                 wait_for_completion = True,
                                 poke_interval = 60,
                                 reset_dag_run = True,
                                 execution_timeout = timedelta(minutes=180),
                                 pool = POOLS["SET1"],
                                 dag = dag)

_task_id = "619846_007-VN-COM-DE-LAKETOLAKE-EPLUS-PENDING-PO-STG"
A619846 = IICSRunJobOperator(task_id = _task_id,
                             job_args = params[af_env][_task_id]["job_args"],
                             task_display_name = "65697_007-VN-COM-DE-LAKETOLAKE-EPLUS-PENDING-PO-STG",
                             execution_timeout = timedelta(minutes=60),
                             on_failure_callback = on_failure_callback_iics,
                             on_success_callback = on_success_callback_iics,
                             dag = dag)



ASTART = DummyOperator (task_id = "START", execution_timeout = timedelta(minutes=60), dag = dag)



_task_id = "VN-GEM-PRODUCT-LAKELOAD"
_job_parameters = dict(params[af_env][_task_id]["job_parameters"])
if "*" in params[af_env]["VN-GEM-PRODUCT-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='VN-GEM-PRODUCT-FILEWATCH', key='resolved_key') }}"
A627840 = DatabricksRunNowOperator (task_id = _task_id,
                                    task_display_name = "VN-GEM-PRODUCT-LAKELOAD",
                                    databricks_conn_id = DATABRICKS_CONN_ID,
                                    job_id = params[af_env][_task_id]["job_id"],
                                    json = {"job_parameters": _job_parameters},
                                    execution_timeout = timedelta(minutes=60),
                                    on_failure_callback = on_failure_callback_dbx_lf_job,
                                    on_success_callback = on_success_callback_dbx_lf_job,
                                    dag = dag)

_task_id = "VN-GEM-PRODUCT-FILEWATCH"
A627839 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-GEM-PRODUCT-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A624633_SUCCESS = DummyOperator (task_id = "624633.SUCCESS", task_display_name = "50707.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A624845_SUCCESS = DummyOperator (task_id = "624845.SUCCESS", task_display_name = "50723.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "VN-DAILYNETSALESDATA-FILEWATCH"
A618857 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-DAILYNETSALESDATA-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A611377_SUCCESS = DummyOperator (task_id = "611377.SUCCESS", task_display_name = "50750.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)


A618765 = DummyOperator (task_id = "618765_DA-COM-GEM-VN-LAKETOLAKE", task_display_name = "65701_DA-COM-GEM-VN-LAKETOLAKE", execution_timeout = timedelta(minutes=60), dag = dag)


_task_id = "VN-CUSTOMER-EXCLUDE-FILEWATCH"
A629625 = S3KeySensor (task_id = _task_id,
                        task_display_name = "VN-CUSTOMER-EXCLUDE-FILEWATCH",
                        bucket_name = params[af_env][_task_id]["bucket_name"],
                        bucket_key = params[af_env][_task_id]["bucket_key"],
                        aws_conn_id = "aws_default",
                        wildcard_match = True,
                        poke_interval = SENSOR_POKE_INTERVAL_SECONDS,
                        timeout = SENSOR_TIMEOUT_SECONDS,
                        mode = "reschedule",
                        on_failure_callback = on_failure_callback_S3,
                        on_success_callback = on_success_callback_S3,
                        dag = dag)

A591212_613808_SUCCESS = DummyOperator (task_id = "591212.613808.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)


A611377 >> [A611377_SUCCESS]
A624634 >> [A624633_SUCCESS]
A597402 >> [A597402_SUCCESS]
A601184 >> [A601184_SUCCESS]
A599618 >> [A599618_SUCCESS]
A608174 >> [A608174_SUCCESS]
A621834 >> [A621834_SUCCESS]
A596788 >> [A596787_SUCCESS]
A622159 >> [A592185_SUCCESS]
A592186 >> [A592185_SUCCESS, A593423]
A629626 >> [A629625_SUCCESS]
A623253 >> [A623252_SUCCESS, A621834]
A612091 >> [A594895_SUCCESS, A612757]
A612757 >> [A612091_SUCCESS]
A628671 >> [A592185_SUCCESS]
A619520 >> [A619519_SUCCESS]
A624848 >> [A624847_SUCCESS]
A619971 >> [A592185_SUCCESS]
A606336 >> [A592185_SUCCESS]
A624845 >> [A594895_SUCCESS, A625294]
A624633 >> [A594895_SUCCESS, A624634]
A627839_SUCCESS >> [A611377]
A602823_SUCCESS >> [A597402]
A619951 >> [A592185_SUCCESS]
A624847_SUCCESS >> [A608174]
A602823 >> [A594895_SUCCESS, A603726]
A624847 >> [A594895_SUCCESS, A624848]
A603726 >> [A602823_SUCCESS]
A623252 >> [A594895_SUCCESS, A623253]
A625294 >> [A624845_SUCCESS]
A611982 >> [A592185_SUCCESS, A624973]
A612091_SUCCESS >> [A619519]
A594895 >> [A596787, A602823, A612091, A613196, A618857, A623252, A624633, A624845, A624847, A627839, A629625]
A592185 >> [A592186, A606336, A611982, A618765, A619846, A619951, A619971, A622159, A628671]
A619392 >> [A618857_SUCCESS]
A592184 >> [A592185, A594895, A719418]
A719418 >> [A720968]
A720968 >> [A728219]
A728219 >> [A739887]
A596787 >> [A594895_SUCCESS, A596788]
A618857_SUCCESS >> [A599618]
A613196 >> [A594895_SUCCESS, A613824]
A613824 >> [A613196_SUCCESS]
A619519 >> [A619520]
A619846 >> [A592185_SUCCESS]
ASTART >> [A591212_613808_SUCCESS, A592184]
A627840 >> [A627839_SUCCESS]
A627839 >> [A594895_SUCCESS, A627840]
A624845_SUCCESS >> [A601184]
A618857 >> [A594895_SUCCESS, A619392]
A618765 >> [A592185_SUCCESS]
A629625 >> [A594895_SUCCESS, A629626]
A591212_613808_SUCCESS >> [A599618]
A592186 >> [A619971, A622159, A619846, A606336]
A606336 >> [A619951]
A619951 >> [A628671]
A628671 >> [A611982]

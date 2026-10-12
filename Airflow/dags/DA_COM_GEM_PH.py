import sys
import os
import logging
import fnmatch
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
# from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.sensors.weekday import DayOfWeekSensor
import pytz

from airflow.exceptions import AirflowSensorTimeout, AirflowException
from botocore.exceptions import ClientError
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from iics_plugin import IICSRunJobOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow_servicenow_handler import notify_api_on_failure
from airflow.utils.email import send_email
from globalscape_plugin.eft_operator import GlobalscapeEFTOperator
from tak_airflow_databricks_cluster_manager import create_dbx_cluster
from tak_airflow_databricks_cluster_manager import terminate_dbx_cluster
from airflow.providers.ssh.operators.ssh import SSHOperator

from tak_airflow_callbacks_utils import (
send_email_notification, Globalscape_Job_Failure,
on_success_callback_S3, on_failure_callback_S3, 
on_success_callback_python, on_failure_callback_python,
on_success_callback_iics, on_failure_callback_iics,
on_success_callback_dbx_lf_job, on_failure_callback_dbx_lf_job,
on_success_callback_globalscape, on_failure_callback_globalscape,
on_success_callback_ssh, on_failure_callback_ssh
)

from DA_COM_GEM_PH_PARAMETERS import (
PARAMS_DICT, SNOW_ATTRIBUTES_DEFAULTS, EMAIL_RECIPIENTS,
DATABRICKS_CONN_ID, AWS_CONN_ID, GLOBALSCAPE_CONN_ID, SSH_CONN_ID,POOLS,SENSOR_POKE_INTERVAL_SECONDS, SENSOR_TIMEOUT_SECONDS
)

####======= Parameters and Variables ==========================================================

LOG_PFX = "TAKEDA: "
# DATABRICKS_CONN_ID = "Databricks_BDMToDBX_Default"
params = PARAMS_DICT
af_env = Variable.get("af_env", default_var="DEV")  # "DEV", "TEST", "PROD"

####======= ServiceNow : Snow Attributes ========================================================

snow_attributes = {
    'configuration_item': SNOW_ATTRIBUTES_DEFAULTS['configuration_item'],
    'severity':           SNOW_ATTRIBUTES_DEFAULTS['severity'],
    'ci_apms_id':         SNOW_ATTRIBUTES_DEFAULTS['ci_apms_id'],
    'ci_bsn_id':          SNOW_ATTRIBUTES_DEFAULTS['ci_bsn_id'],
    'apms_id':            'APMS-92144',
    'bsn_id':             'BSN0020621',
    'type':               'AWS-Airflow-EU',
    'env':                'PROD',
}

####======= Class to handle Callback Action Functions ==========================================

####======= Class to handle Callback Action Functions ==========================================

class DAGCallbackAction():

    def __init__(self):

        # On Success/ On Failure Callback Handlers List for each Task
        self.on_success_completion_handlers = {
            '737130_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES': ['RUN-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES-GLOBO'],
            '729603_002-PH-HUBTOMART-TXN-PH-ZPC-CSMO-SALES': ['PH_ZPC_CSMO_SALES_LOAD_SUCCESS'],
            '743912_002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT': ['PH_TRADE_DISCOUNT_LOAD_SUCCESS'],
            '726984_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES': ['PH_ZPC_WHOLESALER_SALES_LOAD_SUCCESS'],
            '740911_002-PH-HUBTOMART-REF-PH-TERR-ALIGNMENT': ['PH_TERRITORY-ALIGNMENT_LOAD_SUCCESS'],
            '728889_003-PH-HUBTOMART-TXN-PH-TGT-SALES': ['PH_TARGET_SALES_LOAD_SUCCESS'],
            '735954_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES': ['RUN-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES-ZPC'],
            '724945_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES': ['PH_PCSO_WHOLESALER_SALES_LOAD_SUCCESS'],
            '736563_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES': ['RUN-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES-GLOBO-PCSO'],
            '725593_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES': ['PH_GLOBO_WHOLESALER_SALES_LOAD_SUCCESS']
        }

        self.on_failure_abnormal_completion_handlers = {
            '729603_002-PH-HUBTOMART-TXN-PH-ZPC-CSMO-SALES': ['PH_ZPC_CSMO_SALES_LOAD_FAILURE'],
            '743912_002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT': ['PH_TRADE_DISCOUNT_LOAD_FAILURE'],
            '726984_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES': ['PH_ZPC_WHOLESALER_SALES_LOAD_FAILURE'],
            '740911_002-PH-HUBTOMART-REF-PH-TERR-ALIGNMENT': ['PH_TERRITORY_ALIGNMENT_LOAD_FAILURE'],
            '728889_003-PH-HUBTOMART-TXN-PH-TGT-SALES': ['PH_TARGET_SALES_LOAD_FAILURE'],
            '724945_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES': ['PH_PCSO_WHOLESALER_SALES_LOAD_FAILURE'],
            '725593_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES': ['PH_GLOBO_WHOLESALER_SALES_LOAD_FAILURE']
        }

        self.on_failure_error_handlers = {

        }

        # On Success/ On Failure Callback Handler name to Callback function mapping
        self.handler_callback_fun_map = {
            'RUN-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES-GLOBO': self.RUN_PH_LAKETOHUB_TXN_PH_WHOLESALER_SALES_GLOBO,
            'PH_ZPC_CSMO_SALES_LOAD_FAILURE': self.PH_ZPC_CSMO_SALES_LOAD_FAILURE,
            'PH_ZPC_CSMO_SALES_LOAD_SUCCESS': self.PH_ZPC_CSMO_SALES_LOAD_SUCCESS,
            'PH_TRADE_DISCOUNT_LOAD_SUCCESS': self.PH_TRADE_DISCOUNT_LOAD_SUCCESS,
            'PH_TRADE_DISCOUNT_LOAD_FAILURE': self.PH_TRADE_DISCOUNT_LOAD_FAILURE,
            'PH_ZPC_WHOLESALER_SALES_LOAD_FAILURE': self.PH_ZPC_WHOLESALER_SALES_LOAD_FAILURE,
            'PH_ZPC_WHOLESALER_SALES_LOAD_SUCCESS': self.PH_ZPC_WHOLESALER_SALES_LOAD_SUCCESS,
            'PH_TERRITORY_ALIGNMENT_LOAD_FAILURE': self.PH_TERRITORY_ALIGNMENT_LOAD_FAILURE,
            'PH_TERRITORY-ALIGNMENT_LOAD_SUCCESS': self.PH_TERRITORY_ALIGNMENT_LOAD_SUCCESS,
            'PH_TARGET_SALES_LOAD_SUCCESS': self.PH_TARGET_SALES_LOAD_SUCCESS,
            'PH_TARGET_SALES_LOAD_FAILURE': self.PH_TARGET_SALES_LOAD_FAILURE,
            'RUN-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES-ZPC': self.RUN_PH_LAKETOHUB_TXN_PH_WHOLESALER_SALES_ZPC,
            'PH_PCSO_WHOLESALER_SALES_LOAD_SUCCESS': self.PH_PCSO_WHOLESALER_SALES_LOAD_SUCCESS,
            'PH_PCSO_WHOLESALER_SALES_LOAD_FAILURE': self.PH_PCSO_WHOLESALER_SALES_LOAD_FAILURE,
            'RUN-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES-GLOBO-PCSO': self.RUN_PH_LAKETOHUB_TXN_PH_WHOLESALER_SALES_GLOBO_PCSO,
            'PH_GLOBO_WHOLESALER_SALES_LOAD_SUCCESS': self.PH_GLOBO_WHOLESALER_SALES_LOAD_SUCCESS,
            'PH_GLOBO_WHOLESALER_SALES_LOAD_FAILURE': self.PH_GLOBO_WHOLESALER_SALES_LOAD_FAILURE
        }

        self.snow_attributes = snow_attributes

        self.EMAIL_TO = EMAIL_RECIPIENTS.get(af_env)

    ####========On Success/ On Failure Callback functions for Tasks ============

    
    def PH_TERRITORY_ALIGNMENT_LOAD_SUCCESS(self, context):
        email_to = self.EMAIL_TO

        email_subject = "PH Territory Alignment Load - SUCCESS"

        email_message = """
        <html>
        <body>
            <p>Hi Team,</p>
            <p>PH Territory Alignment Load has completed successfully.</p>
            <p>Thanks,<br>
            Airflow</p>
        </body>
        </html>
        """

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )  

    def PH_ZPC_CSMO_SALES_LOAD_SUCCESS(self, context):
        email_to = self.EMAIL_TO

        email_subject = "PH ZPC CSMO Sales Load - SUCCESS"

        email_message = """
        <html>
        <body>
            <p>Hi Team,</p>
            <p>PH ZPC CSMO Sales Load has completed successfully.</p>
            <p>Thanks,<br>
            Airflow</p>
        </body>
        </html>
        """

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )

    
    def PH_GLOBO_WHOLESALER_SALES_LOAD_SUCCESS(self,context):
        email_to = self.EMAIL_TO

        email_subject = "Globo wholesaler Data refresh completion for Philippines"

        email_message = """Dear All,

The Globo wholesaler data has been refreshed successfully for Philippines.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


   
    def PH_TARGET_SALES_LOAD_SUCCESS(self,context):
        email_to =self.EMAIL_TO

        email_subject = "Target Sales data refresh completion for Philippines"

        email_message = """Dear All,

The Target Sales data has been refreshed successfully for Philippines.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


    
    def PH_PCSO_WHOLESALER_SALES_LOAD_FAILURE(self,context):
        email_to = self.EMAIL_TO

        email_subject = "PCSO wholesaler data refresh failure for Philippines"

        email_message = """Dear All,

Attempts to refresh Pcso wholesaler data for Philippines failed and the team is working to resolve.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


    
    def PH_ZPC_WHOLESALER_SALES_LOAD_SUCCESS(self,context):
        email_to = [
            "DL.EDB.GEM.VMAPS.COMM@TAKEDA.COM",
            "DL.EDB.COMM.UAMS@takeda.com"
        ]

        email_subject = "ZPC wholesaler Data refresh completion for Philippines"

        email_message = """Dear All,

The Zpc wholesaler data has been refreshed successfully for Philippines.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


    
    def PH_PCSO_WHOLESALER_SALES_LOAD_SUCCESS(self,context):
        email_to = self.EMAIL_TO

        email_subject = "PCSO wholesaler data refresh completion for Philippines"

        email_message = """Dear All,

The Pcso wholesaler data has been refreshed successfully for Philippines.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


    
    def PH_TARGET_SALES_LOAD_FAILURE(self,context):
        email_to = self.EMAIL_TO

        email_subject = "Target Sales data refresh failure for Philippines"

        email_message = """Dear All,

Attempts to refresh Target Sales data for Philippines failed and the team is working to resolve.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


   
    def PH_TRADE_DISCOUNT_LOAD_SUCCESS(self,context):
        email_to = self.EMAIL_TO

        email_subject = "Trade Discount data refresh completion for Philippines"

        email_message = """Dear All,

The Trade Discount data has been refreshed successfully for Philippines.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


    
    def PH_GLOBO_WHOLESALER_SALES_LOAD_FAILURE(self,context):
        email_to = self.EMAIL_TO
        email_subject = "Globo wholesaler data refresh failure  for Philippines"

        email_message = """Dear All,

Attempts to refresh Globo wholesaler data for Philippines failed and the team is working to resolve.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


    # Jobaction function - keep unchanged for now
    def RUN_PH_LAKETOHUB_TXN_PH_WHOLESALER_SALES_GLOBO(self, context):
        pass


    
    def PH_TERRITORY_ALIGNMENT_LOAD_FAILURE(self,context):
        email_to = self.EMAIL_TO

        email_subject = "Territory Alignment data refresh failure for Philippines"

        email_message = """Dear All,

Attempts to refresh Territory Alignment data for Philippines failed and the team is working to resolve.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


    
    def PH_ZPC_WHOLESALER_SALES_LOAD_FAILURE(self, context):
        email_to =self.EMAIL_TO
        email_subject = "ZPC wholesaler data refresh failure for Philippines"

        email_message = """Dear All,

Attempts to refresh Zpc wholesaler data for Philippines failed and the team is working to resolve.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


   
    def PH_ZPC_CSMO_SALES_LOAD_FAILURE(self, context):
        email_to = self.EMAIL_TO

        email_subject = "Zpc csmo sales data refresh failure for Philippines"

        email_message = """Dear All,

Attempts to refresh zpc csmo sales data for Philippines failed and the team is working to resolve.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


    # Jobaction function - keep unchanged for now
    def RUN_PH_LAKETOHUB_TXN_PH_WHOLESALER_SALES_GLOBO_PCSO(self, context):
        pass


    
    def PH_TRADE_DISCOUNT_LOAD_FAILURE(self, context):
        email_to = self.EMAIL_TO

        email_subject = "Trade Discount data refresh failure for Philippines"

        email_message = """Dear All,

Attempts to refresh Trade Discount data for Philippines failed and the team is working to resolve.

Thanks"""

        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


    # Jobaction function - keep unchanged for now
    def RUN_PH_LAKETOHUB_TXN_PH_WHOLESALER_SALES_ZPC(self, context):
        pass

####=============Callback Wrapper Functions===============================================

def on_success_callback_S3_wrapper(context):
    on_success_callback_S3(context, af_env, params)
    
def on_failure_callback_S3_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_failure_callback_S3(context, dag_callback_action_obj)

def on_success_callback_python_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_success_callback_python(context, dag_callback_action_obj)

def on_failure_callback_python_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_failure_callback_python(context, dag_callback_action_obj)

def on_success_callback_lambda(context):
    pass

def on_failure_callback_lambda(context):
    pass

def on_success_callback_iics_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_success_callback_iics(context, dag_callback_action_obj)

def on_failure_callback_iics_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_failure_callback_iics(context, dag_callback_action_obj)

def on_success_callback_dbx_lf_job_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_success_callback_dbx_lf_job(context, dag_callback_action_obj)
    
def on_failure_callback_dbx_lf_job_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_failure_callback_dbx_lf_job(context, dag_callback_action_obj)

def on_success_callback_globalscape_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_success_callback_globalscape(context, dag_callback_action_obj)
    
def on_failure_callback_globalscape_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_failure_callback_globalscape(context, dag_callback_action_obj)

def on_success_callback_ssh_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_success_callback_ssh(context, dag_callback_action_obj)
    
def on_failure_callback_ssh_wrapper(context):
    dag_callback_action_obj = DAGCallbackAction()
    on_failure_callback_ssh(context, dag_callback_action_obj)

####======================================================================================

batch_override = None

default_args = {
    'owner':          'airflow',
    'email' : '',
    "priority_weight": 1, # TO DO - to check this later
    "weight_rule": "absolute"
}

####========= Timezone ===================================================================


dag = DAG(
    dag_id = "DA-COM-GEM-PH",
    default_args = default_args,
    description="Tidal to Airflow Migration - PH",
    # schedule_interval = None, # TO DO - to check this later
    schedule_interval = None,
    concurrency = 16,
    start_date = datetime(2026,9,2),
    catchup = False,
    max_active_runs = 1
)

def test_function():
    logging.info(f"{LOG_PFX}Tidal To Airflow Migration...")
    return True

####================== Jobs: Task Definitions ===================================


_task_id = "PH-CUSTOMER-FILEWATCH"

A723398 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-CUSTOMER-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

# A720191_SUCCESS = DummyOperator (task_id = "720191.SUCCESS", task_display_name = "48992.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A725592 = TriggerDagRunOperator(
    task_id="725592_DA-COM-GEM-PH-SRC-GLOBO",
    task_display_name="48977_DA-COM-GEM-PH-SRC-GLOBO",
    trigger_dag_id="DA-COM-GEM-PH-CHILD-SRC-GLOBO",
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    execution_timeout=timedelta(minutes=180),
    pool=POOLS["PH_POOL_1"],
    dag=dag,
)

# A732409 = DummyOperator (task_id = "732409_002-GEM-DATABRICKS-CALLBACK-DMP-EXCEPTION-SYNC-PH", task_display_name = "61126_002-GEM-DATABRICKS-CALLBACK-DMP-EXCEPTION-SYNC-PH", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "PH-ADMIN-MAPPRODCODE-LAKELOAD"
A740740 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-ADMIN-MAPPRODCODE-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

# A732781 = DummyOperator (task_id = "732781_003-RUN-LAKE-LAMBDA-PH-MAP-TERRITORY", task_display_name = "48922_003-RUN-LAKE-LAMBDA-PH-MAP-TERRITORY", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "PH-MAP-TERRITORY-FILEWATCH"

A721469 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-MAP-TERRITORY-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

# A746315 = DummyOperator (task_id = "746315_002-RUN-LAKE-LAMBDA-PH-CUSTOMER-EXCLUDE", task_display_name = "48918_002-RUN-LAKE-LAMBDA-PH-CUSTOMER-EXCLUDE", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "735246_002-PH-LAKETOHUB-REF-CUST-INTL"
# A735246 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48981_002-PH-LAKETOHUB-REF-CUST-INTL",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

# _task_id = "714317_001-PH-LAKETOLAKE-TXN-PH-TGT-SALES-STG"
# A714317 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48998_001-PH-LAKETOLAKE-TXN-PH-TGT-SALES-STG",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

# A741356_SUCCESS = DummyOperator (task_id = "741356.SUCCESS", task_display_name = "48924.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A709364 = DummyOperator (task_id = "709364_DA-COM-GEM-PH", task_display_name = "48910_DA-COM-GEM-PH", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "PH-GLOBO-LAKELOAD"
A735224 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-GLOBO-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

# A733682 = DummyOperator (task_id = "733682_002-PH-CUSTOMER-FILEWATCH-EXCLUDE", task_display_name = "48916_002-PH-CUSTOMER-FILEWATCH-EXCLUDE", execution_timeout = timedelta(minutes=60), dag = dag)

# A732409_SUCCESS = DummyOperator (task_id = "732409.SUCCESS", task_display_name = "61126.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "747809_001-PH-LAKETOLAKE-PH-ZPC-STG"
# A747809 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48982_001-PH-LAKETOLAKE-PH-ZPC-STG",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )


_task_id = "PH-TRADE-DISCOUNT-LAKELOAD"

_job_parameters = dict(params[af_env][_task_id]["job_parameters"])

if "*" in params[af_env]["PH-TRADE-DISCOUNT-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='PH-TRADE-DISCOUNT-FILEWATCH', key='resolved_key') }}"

A729613 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-TRADE-DISCOUNT-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": _job_parameters},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

_task_id = "739231_001-PH-LAKETOHUB-TXN-PH-ZPC-CSMO-SALES"
A739231 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="48971_001-PH-LAKETOHUB-TXN-PH-ZPC-CSMO-SALES",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

_task_id = "PH-ZPC-CSMO-SALES-FILEWATCH"

A745279 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-ZPC-CSMO-SALES-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

A732511_SUCCESS = DummyOperator (task_id = "732511.SUCCESS", task_display_name = "48958.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A729602 = DummyOperator (task_id = "729602_DA-COM-GEM-PH-ZPC-CSMO-SALES", task_display_name = "48970_DA-COM-GEM-PH-ZPC-CSMO-SALES", execution_timeout = timedelta(minutes=60), dag = dag)

# A709366 = DummyOperator (task_id = "709366_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-PH", task_display_name = "61124_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-PH", execution_timeout = timedelta(minutes=60), dag = dag)

A727171_SUCCESS = DummyOperator (task_id = "727171.SUCCESS", task_display_name = "48938.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A714316_SUCCESS = DummyOperator (task_id = "714316.SUCCESS", task_display_name = "48994.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A719255_SUCCESS = DummyOperator (task_id = "719255.SUCCESS", task_display_name = "48993.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "748097_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC"
# A748097 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="61125_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

# A722769_SUCCESS = DummyOperator (task_id = "722769.SUCCESS", task_display_name = "48912.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A728791_SUCCESS = DummyOperator (task_id = "728791.SUCCESS", task_display_name = "48926.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A722769 = DummyOperator (task_id = "722769_001-PH-CUSTOMER-FILEWATCH", task_display_name = "48912_001-PH-CUSTOMER-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "PH-TRADE-DISCOUNT-FILEWATCH"

A722662 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-TRADE-DISCOUNT-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

A720191 = TriggerDagRunOperator(
    task_id="720191_DA-COM-GEM-PH-PRODUCT",
    task_display_name="48992_DA-COM-GEM-PH-PRODUCT",
    trigger_dag_id="DA-COM-GEM-PH-CHILD-PRODUCT",
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    execution_timeout=timedelta(minutes=180),
    pool=POOLS["PH_POOL_2"],
    dag=dag,
)

# A715344 = DummyOperator (task_id = "715344_010-PH-GLOBO-PCSO-FILEWATCH, task_display_name = "48948_010-PH-GLOBO-PCSO-FILEWATCH, execution_timeout = timedelta(minutes=60), dag = dag)

A714316 = TriggerDagRunOperator(
    task_id="714316_DA-COM-GEM-PH-TARGETSALES",
    task_display_name="48994_DA-COM-GEM-PH-TARGETSALES",
    trigger_dag_id="DA-COM-GEM-PH-CHILD-TARGETSALES",
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    execution_timeout=timedelta(minutes=180),
    pool=POOLS["PH_POOL_2"],
    dag=dag,
)

_task_id = "PH-ADMIN-TARGETSALES-LAKELOAD"
A727771 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-ADMIN-TARGETSALES-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

# _task_id = "730724_002-PH-LAKETOHUB-REF-PROD-INTL"
# A730724 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48997_002-PH-LAKETOHUB-REF-PROD-INTL",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

_task_id = "PH-ZPC-LAKELOAD"

_job_parameters = dict(params[af_env][_task_id]["job_parameters"])

if "*" in params[af_env]["PH-ZPC-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='PH-ZPC-FILEWATCH', key='resolved_key') }}"

A733156 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-ZPC-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": _job_parameters},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

# _task_id = "731820_001-PH-LAKETOHUB-REF-CUST-INTL-MASTER"
# A731820 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48984_001-PH-LAKETOHUB-REF-CUST-INTL-MASTER",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

_task_id = "PH-GLOBO-PCSO-FILEWATCH"

A715345 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-GLOBO-PCSO-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

# _task_id = "721787_001-PH-LAKETOHUB-REF-TERR-PH"
# A721787 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="49001_001-PH-LAKETOHUB-REF-TERR-PH",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

_task_id = "PH-CUSTOMER-LAKELOAD"

A742788 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-CUSTOMER-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

# A722673 = DummyOperator (task_id = "722673_001-RUN-LAKE-LAMBDA-PH-CUSTOMER", task_display_name = "48914_001-RUN-LAKE-LAMBDA-PH-CUSTOMER", execution_timeout = timedelta(minutes=60), dag = dag)

# A740209 = DummyOperator (task_id = "740209_006-PH-ADMIN-MAPPRODCODE-LAKELOAD, task_display_name = "48934_006-PH-ADMIN-MAPPRODCODE-LAKELOAD, execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "740906_001-PH-LAKETOLAKE-GLOBO-PCSO-STG"
# A740906 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48980_001-PH-LAKETOLAKE-GLOBO-PCSO-STG",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

# A735593 = DummyOperator (task_id = "735593_005-PH-PRODUCT-FILEWATCH", task_display_name = "48928_005-PH-PRODUCT-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

A716410_SUCCESS = DummyOperator (task_id = "716410.SUCCESS", task_display_name = "48995.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A721943 = DummyOperator (task_id = "721943_011-PH-TRADE-DISCOUNT-FILEWATCH", task_display_name = "48952_011-PH-TRADE-DISCOUNT-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "PH-CHANNEL-MAPPING-LAKELOAD"
A724219 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-CHANNEL-MAPPING-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

# A740337 = DummyOperator (task_id = "740337_014-PH-MAP-CALENDAR-FILEWATCH, task_display_name = "48964_014-PH-MAP-CALENDAR-FILEWATCH, execution_timeout = timedelta(minutes=60), dag = dag)


_task_id = "PH-MAP-CALENDAR-FILEWATCH"

A741716 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-MAP-CALENDAR-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)


_task_id = "PH-ZPC-CSMO-SALES-LAKELOAD"

_job_parameters = dict(params[af_env][_task_id]["job_parameters"])

if "*" in params[af_env]["PH-ZPC-CSMO-SALES-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='PH-ZPC-CSMO-SALES-FILEWATCH', key='resolved_key') }}"

A743574 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-ZPC-CSMO-SALES-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": _job_parameters},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

ASTART = DummyOperator (task_id = "START", execution_timeout = timedelta(minutes=60), dag = dag)

# A709367 = DummyOperator (task_id = "709367_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DMP-EXCEPTION-SYNC", task_display_name = "61127_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DMP-EXCEPTION-SYNC", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "737130_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES"
# A737130 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48990_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     on_success_callback=on_success_callback_dbx_lf_job_wrapper,
#     dag=dag,
# )


_task_id = "PH-NONCHARM-USER-FILEWATCH"

A741827 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-NONCHARM-USER-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

# _task_id = "736318_002-PH-LAKETOHUB-REF-CUST-INTL"
# A736318 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48983_002-PH-LAKETOHUB-REF-CUST-INTL",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

_task_id = "PH-CUSTOMER-EXCLUDE-FILEWATCH"

A741509 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-CUSTOMER-EXCLUDE-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

# A740337_SUCCESS = DummyOperator (task_id = "740337.SUCCESS", task_display_name = "48964.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "729603_002-PH-HUBTOMART-TXN-PH-ZPC-CSMO-SALES"
A729603 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="48972_002-PH-HUBTOMART-TXN-PH-ZPC-CSMO-SALES",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
    on_success_callback=on_success_callback_dbx_lf_job_wrapper,
    dag=dag,
)

# _task_id = "743912_002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT"
# A743912 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="49004_002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
#     on_success_callback=on_success_callback_dbx_lf_job_wrapper,
#     dag=dag,
# )

# _task_id = "731310_001-PH-LAKETOHUB-TXN-PH-TRADE-DISCOUNT"
# A731310 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="49003_001-PH-LAKETOHUB-TXN-PH-TRADE-DISCOUNT",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

# A720621 = DummyOperator (task_id = "720621_003-PH-MAP-TERRITORY-FILEWATCH", task_display_name = "48920_003-PH-MAP-TERRITORY-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "726984_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES"
# A726984 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48987_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
#     on_success_callback=on_success_callback_dbx_lf_job_wrapper,
#     dag=dag,
# )

_task_id = "PH-CHANNEL-MAPPING-FILEWATCH"

A744749 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-CHANNEL-MAPPING-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

# A741356 = DummyOperator (task_id = "741356_004-PH-NONCHARM-USER-FILEWATCH", task_display_name = "48924_004-PH-NONCHARM-USER-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

A740209_SUCCESS = DummyOperator (task_id = "740209.SUCCESS", task_display_name = "48934.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A720951_SUCCESS = DummyOperator (task_id = "720951.SUCCESS", task_display_name = "48932.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A709365 = DummyOperator (task_id = "709365_DA-COM-GEM-PH-DMP-EXCEPTION-SYNC", task_display_name = "61123_DA-COM-GEM-PH-DMP-EXCEPTION-SYNC", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "740911_002-PH-HUBTOMART-REF-PH-TERR-ALIGNMENT"
# A740911 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="49002_002-PH-HUBTOMART-REF-PH-TERR-ALIGNMENT",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
#     on_success_callback=on_success_callback_dbx_lf_job_wrapper,
#     dag=dag,
# )

_task_id = "PH-PRODUCT-LAKELOAD"
A742097 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-PRODUCT-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

_task_id = "PH-NONCHARM-USER-LAKELOAD"
A728792 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-NONCHARM-USER-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

_task_id = "PH-ZPC-FILEWATCH"

A738233 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-ZPC-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

_task_id = "PH-MAP-TERRITORY-LAKELOAD"

A733309 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-MAP-TERRITORY-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

# _task_id = "742431_001-PH-LAKETOLAKE-GEM-PRODUCT"
# A742431 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48996_001-PH-LAKETOLAKE-GEM-PRODUCT",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

_task_id = "PH-GLOBO-FILEWATCH"

A747137 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-GLOBO-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

# A746704 = DummyOperator (task_id = "746704_009-PH-GLOBO-FILEWATCH", task_display_name = "48944_009-PH-GLOBO-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "734220_002-PH-LAKETOHUB-REF-CUST-INTL"
# A734220 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48979_002-PH-LAKETOHUB-REF-CUST-INTL",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

# A744819_SUCCESS = DummyOperator (task_id = "744819.SUCCESS", task_display_name = "48960.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A742096 = DummyOperator (task_id = "742096_005-RUN-LAKE-LAMBDA-PH-PRODUCT", task_display_name = "48930_005-RUN-LAKE-LAMBDA-PH-PRODUCT", execution_timeout = timedelta(minutes=60), dag = dag)

A715343 = DummyOperator (task_id = "715343_DA-COM-GEM-PH-COMMON-LAMBDA", task_display_name = "48911_DA-COM-GEM-PH-COMMON-LAMBDA", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "723138_002-PH-LAKETOHUB-TXN-PH-TGT-SALES"
# A723138 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48999_002-PH-LAKETOHUB-TXN-PH-TGT-SALES",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

# A735593_SUCCESS = DummyOperator (task_id = "735593.SUCCESS", task_display_name = "48928.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A729612 = DummyOperator (task_id = "729612_011-RUN-LAKE-LAMBDA-PH-TRADE-DISCOUNT", task_display_name = "48954_011-RUN-LAKE-LAMBDA-PH-TRADE-DISCOUNT", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "728889_003-PH-HUBTOMART-TXN-PH-TGT-SALES"
# A728889 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="49000_003-PH-HUBTOMART-TXN-PH-TGT-SALES",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
#     on_success_callback=on_success_callback_dbx_lf_job_wrapper,
#     dag=dag,
# )

# _task_id = "735954_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES"
# A735954 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48988_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     on_success_callback=on_success_callback_dbx_lf_job_wrapper,
#     dag=dag,
# )

A729736_SUCCESS = DummyOperator (task_id = "729736.SUCCESS", task_display_name = "48976.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A724218 = DummyOperator (task_id = "724218_008-RUN-LAKE-LAMBDA-PH-CHANNEL-MAPPING", task_display_name = "48942_008-RUN-LAKE-LAMBDA-PH-CHANNEL-MAPPING", execution_timeout = timedelta(minutes=60), dag = dag)

# A715343_SUCCESS = DummyOperator (task_id = "715343.SUCCESS", task_display_name = "48911.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A725493_SUCCESS = DummyOperator (task_id = "725493.SUCCESS", task_display_name = "48966.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A746704_SUCCESS = DummyOperator (task_id = "746704.SUCCESS", task_display_name = "48944.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A744286_SUCCESS = DummyOperator (task_id = "744286.SUCCESS", task_display_name = "48940.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A724218_SUCCESS = DummyOperator (task_id = "724218.SUCCESS", task_display_name = "48942.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)


_task_id = "PH-MAP-CALENDAR-LAKELOAD"

_job_parameters = dict(params[af_env][_task_id]["job_parameters"])

if "*" in params[af_env]["PH-MAP-CALENDAR-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='PH-MAP-CALENDAR-FILEWATCH', key='resolved_key') }}"

A725494 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-MAP-CALENDAR-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": _job_parameters},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

# A738232 = DummyOperator (task_id = "738232_012-PH-ZPC-FILEWATCH", task_display_name = "48956_012-PH-ZPC-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "724945_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES"
# A724945 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48986_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
#     on_success_callback=on_success_callback_dbx_lf_job_wrapper,
#     dag=dag,
# )

A726983_SUCCESS = DummyOperator (task_id = "726983.SUCCESS", task_display_name = "48975.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A722673_SUCCESS = DummyOperator (task_id = "722673.SUCCESS", task_display_name = "48914.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A719255 = TriggerDagRunOperator(
    task_id="719255_DA-COM-GEM-PH-TERRITORY-ALIGNMENT",
    task_display_name="48993_DA-COM-GEM-PH-TERRITORY-ALIGNMENT",
    trigger_dag_id="DA-COM-GEM-PH-CHILD-TERRITORY-ALIGNMENT",
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    execution_timeout=timedelta(minutes=180),
    pool=POOLS["PH_POOL_2"],
    dag=dag,
)

_task_id = "PH-ADMIN-MAPPRODCODE-FILEWATCH"

A722046 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-ADMIN-MAPPRODCODE-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

A739684 = PythonOperator (task_id = "739684_MT_APAC_PH_GLOBO_FILE_SCHEMA", python_callable = test_function, task_display_name = "48973_MT_APAC_PH_GLOBO_FILE_SCHEMA", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "PH-ADMIN-TARGETSALES-FILEWATCH"

A748287 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-ADMIN-TARGETSALES-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

# A727171 = DummyOperator (task_id = "727171_007-RUN-LAKE-LAMBDA-PH-ADMIN-TARGETSALES", task_display_name = "48938_007-RUN-LAKE-LAMBDA-PH-ADMIN-TARGETSALES", execution_timeout = timedelta(minutes=60), dag = dag)

# A733682_SUCCESS = DummyOperator (task_id = "733682.SUCCESS", task_display_name = "48916.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A742096_SUCCESS = DummyOperator (task_id = "742096.SUCCESS", task_display_name = "48930.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A720621_SUCCESS = DummyOperator (task_id = "720621.SUCCESS", task_display_name = "48920.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "PH-CUSTOMER-EXCLUDE-LAKELOAD"
A746316 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-CUSTOMER-EXCLUDE-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

A729602_SUCCESS = DummyOperator (task_id = "729602.SUCCESS", task_display_name = "48970.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A729736 = TriggerDagRunOperator(
    task_id="729736_DA-COM-GEM-PH-CUSTOMER-MASTER",
    task_display_name="48976_DA-COM-GEM-PH-CUSTOMER-MASTER",
    trigger_dag_id="DA-COM-GEM-PH-CHILD-CUSTOMER-MASTER",
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    execution_timeout=timedelta(minutes=180),
    pool=POOLS["PH_POOL_1"],
    dag=dag,
)

# _task_id = "736563_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES"
# A736563 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48989_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     on_success_callback=on_success_callback_dbx_lf_job_wrapper,
#     dag=dag,
# )

A724944_SUCCESS = DummyOperator (task_id = "724944.SUCCESS", task_display_name = "48974.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A743573 = DummyOperator (task_id = "743573_013-RUN-LAKE-LAMBDA-PH-ZPC-CSMO-SALES", task_display_name = "48962_013-RUN-LAKE-LAMBDA-PH-ZPC-CSMO-SALES", execution_timeout = timedelta(minutes=60), dag = dag)

A726983 = TriggerDagRunOperator(
    task_id="726983_DA-COM-GEM-PH-SRC-ZPC",
    task_display_name="48975_DA-COM-GEM-PH-SRC-ZPC",
    trigger_dag_id="DA-COM-GEM-PH-CHILD-SRC-ZPC",
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    execution_timeout=timedelta(minutes=180),
    pool=POOLS["PH_POOL_1"],
    dag=dag,
)

# A747836 = DummyOperator (task_id = "747836_007-PH-ADMIN-TARGETSALES-FILEWATCH", task_display_name = "48936_007-PH-ADMIN-TARGETSALES-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "730157_001-PH-LAKETOLAKE-PH-GLOBO-STG"
# A730157 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48978_001-PH-LAKETOLAKE-PH-GLOBO-STG",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     dag=dag,
# )

# A728791 = DummyOperator (task_id = "728791_004-RUN-LAKE-LAMBDA-PH-NONCHARM-USER", task_display_name = "48926_004-RUN-LAKE-LAMBDA-PH-NONCHARM-USER", execution_timeout = timedelta(minutes=60), dag = dag)

A732781_SUCCESS = DummyOperator (task_id = "732781.SUCCESS", task_display_name = "48922.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)


_task_id = "PH-GLOBO-PCSO-LAKELOAD"

_job_parameters = dict(params[af_env][_task_id]["job_parameters"])

if "*" in params[af_env]["PH-GLOBO-PCSO-FILEWATCH"]["bucket_key"]:
    _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='PH-GLOBO-PCSO-FILEWATCH', key='resolved_key') }}"

A720146 = DatabricksRunNowOperator(
    task_id=_task_id,
    task_display_name="PH-GLOBO-PCSO-LAKELOAD",
    databricks_conn_id=DATABRICKS_CONN_ID,
    job_id=params[af_env][_task_id]["job_id"],
    json={"job_parameters": _job_parameters},
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

A743573_SUCCESS = DummyOperator (task_id = "743573.SUCCESS", task_display_name = "48962.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A732511 = DummyOperator (task_id = "732511_012-RUN-LAKE-LAMBDA-PH-ZPC", task_display_name = "48958_012-RUN-LAKE-LAMBDA-PH-ZPC", execution_timeout = timedelta(minutes=60), dag = dag)

# A747836_SUCCESS = DummyOperator (task_id = "747836.SUCCESS", task_display_name = "48936.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

_task_id = "PH-PRODUCT-FILEWATCH"

A736180 = S3KeySensor(
    task_id=_task_id,
    task_display_name="PH-PRODUCT-FILEWATCH",
    bucket_name=params[af_env][_task_id]["bucket_name"],
    bucket_key=params[af_env][_task_id]["bucket_key"],
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=SENSOR_POKE_INTERVAL_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
    on_failure_callback=on_failure_callback_S3_wrapper,
    on_success_callback=on_success_callback_S3_wrapper,
    dag=dag
)

# A744286 = DummyOperator (task_id = "744286_008-PH-CHANNEL-MAPPING-FILEWATCH, task_display_name = "48940_008-PH-CHANNEL-MAPPING-FILEWATCH, execution_timeout = timedelta(minutes=60), dag = dag)

A729612_SUCCESS = DummyOperator (task_id = "729612.SUCCESS", task_display_name = "48954.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A744819 = DummyOperator (task_id = "744819_013-PH-ZPC-CSMO-SALES-FILEWATCH", task_display_name = "48960_013-PH-ZPC-CSMO-SALES-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

# A721943_SUCCESS = DummyOperator (task_id = "721943.SUCCESS", task_display_name = "48952.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A709366_SUCCESS = DummyOperator (task_id = "709366.SUCCESS", task_display_name = "61124.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A716410 = TriggerDagRunOperator(
    task_id="716410_DA-COM-GEM-PH-TRADE-DISCOUNT",
    task_display_name="48995_DA-COM-GEM-PH-TRADE-DISCOUNT",
    trigger_dag_id="DA-COM-GEM-PH-CHILD-TRADE-DISCOUNT",
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    execution_timeout=timedelta(minutes=180),
    pool=POOLS["PH_POOL_2"],
    dag=dag,
)

# A720951 = DummyOperator (task_id = "720951_006-PH-ADMIN-MAPPRODCODE-FILEWATCH", task_display_name = "48932_006-PH-ADMIN-MAPPRODCODE-FILEWATCH", execution_timeout = timedelta(minutes=60), dag = dag)

# A732410 = DummyOperator (task_id = "732410_010-SHOW-DATABRICKS-JOB-STATUS-DMP-EXCEPTION-SYNC", task_display_name = "61128_010-SHOW-DATABRICKS-JOB-STATUS-DMP-EXCEPTION-SYNC", execution_timeout = timedelta(minutes=60), dag = dag)

A746315_SUCCESS = DummyOperator (task_id = "746315.SUCCESS", task_display_name = "48918.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A724944 = TriggerDagRunOperator(
    task_id="724944_DA-COM-GEM-PH-SRC-GLOBO-PCSO",
    task_display_name="48974_DA-COM-GEM-PH-SRC-GLOBO-PCSO",
    trigger_dag_id="DA-COM-GEM-PH-CHILD-SRC-GLOBO-PCSO",
    wait_for_completion=True,
    poke_interval=60,
    reset_dag_run=True,
    execution_timeout=timedelta(minutes=180),
    pool=POOLS["PH_POOL_1"],
    dag=dag,
)

# A725493 = DummyOperator (task_id = "725493_014-RUN-LAKE-LAMBDA-PH-MAP-CALENDAR", task_display_name = "48966_014-RUN-LAKE-LAMBDA-PH-MAP-CALENDAR", execution_timeout = timedelta(minutes=60), dag = dag)

# A717813 = DummyOperator (task_id = "717813_010-RUN-LAKE-LAMBDA-PH-GLOBO-PCSO", task_display_name = "48950_010-RUN-LAKE-LAMBDA-PH-GLOBO-PCSO", execution_timeout = timedelta(minutes=60), dag = dag)

# A715344_SUCCESS = DummyOperator (task_id = "715344.SUCCESS", task_display_name = "48948.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# _task_id = "725593_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES"
# A725593 = DatabricksRunNowOperator(
#     task_id=_task_id,
#     task_display_name="48985_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES",
#     databricks_conn_id=DATABRICKS_CONN_ID,
#     job_id=params[af_env][_task_id]["job_id"],
#     json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
#     execution_timeout=timedelta(minutes=60),
#     on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
#     on_success_callback=on_success_callback_dbx_lf_job_wrapper,
#     dag=dag,
# )

# A738232_SUCCESS = DummyOperator (task_id = "738232.SUCCESS", task_display_name = "48956.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A725592_SUCCESS = DummyOperator (task_id = "725592.SUCCESS", task_display_name = "48977.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A734698_SUCCESS = DummyOperator (task_id = "734698.SUCCESS", task_display_name = "48946.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

A717813_SUCCESS = DummyOperator (task_id = "717813.SUCCESS", task_display_name = "48950.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A709365_SUCCESS = DummyOperator (task_id = "709365.SUCCESS", task_display_name = "61123.SUCCESS", execution_timeout = timedelta(minutes=60), dag = dag)

# A734698 = DummyOperator (task_id = "734698_009-RUN-LAKE-LAMBDA-PH-GLOBO", task_display_name = "48946_009-RUN-LAKE-LAMBDA-PH-GLOBO", execution_timeout = timedelta(minutes=60), dag = dag)


####================== Dependency Management: Task Dependencies =================


# ============================================================
# START
# ============================================================

ASTART >> A709364


# ============================================================
# MAIN FLOW
# ============================================================

A709364 >> [
    A715343,
    A724944,
    A725592,
    A739684
]


# ============================================================
# CUSTOMER
# ============================================================

A715343 >> A723398
A723398 >> A742788
A742788 >> A722673_SUCCESS
A722673_SUCCESS >> A729736


# ============================================================
# ADMIN MAP PRODUCT CODE
# ============================================================

A715343 >> A722046
A722046 >> A740740
A740740 >> A740209_SUCCESS


# ============================================================
# MAP TERRITORY
# ============================================================

A715343 >> A721469
A721469 >> A733309
A733309 >> A732781_SUCCESS
A732781_SUCCESS >> A719255


# ============================================================
# TRADE DISCOUNT
# ============================================================

A715343 >> A722662
A722662 >> A729613
A729613 >> A729612_SUCCESS
A729612_SUCCESS >> A716410


# ============================================================
# ZPC
# ============================================================

A715343 >> A738233
A738233 >> A733156
A733156 >> A732511_SUCCESS
A732511_SUCCESS >> A726983


# ============================================================
# ZPC CSMO SALES
# ============================================================

A715343 >> A745279
A745279 >> A743574
A743574 >> A743573_SUCCESS
A743573_SUCCESS >> A729602


# ============================================================
# GLOBO
# ============================================================

A715343 >> A747137
A747137 >> A735224
A735224 >> A734698_SUCCESS


# ============================================================
# GLOBO PCSO
# ============================================================

A715343 >> A715345
A715345 >> A720146
A720146 >> A717813_SUCCESS


# ============================================================
# PRODUCT
# ============================================================

A715343 >> A736180
A736180 >> A742097
A742097 >> A742096_SUCCESS
A742096_SUCCESS >> A720191


# ============================================================
# NONCHARM USER
# ============================================================

A715343 >> A741827
A741827 >> A728792
A728792 >> A728791_SUCCESS


# ============================================================
# CUSTOMER EXCLUDE
# ============================================================

A715343 >> A741509
A741509 >> A746316
A746316 >> A746315_SUCCESS


# ============================================================
# CHANNEL MAPPING
# ============================================================

A715343 >> A744749
A744749 >> A724219
A724219 >> A724218_SUCCESS


# ============================================================
# MAP CALENDAR
# ============================================================

A715343 >> A741716
A741716 >> A725494
A725494 >> A725493_SUCCESS


# ============================================================
# ADMIN TARGET SALES
# ============================================================

A715343 >> A748287
A748287 >> A727771
A727771 >> A727171_SUCCESS
A727171_SUCCESS >> A714316


# ============================================================
# DMP EXCEPTION / CALLBACK FLOW
# ============================================================

# A732409 >> A732410
# A732410 >> A732409_SUCCESS
# A732409_SUCCESS >> A709367

# ============================================================
# ZPC CSMO DOWNSTREAM FLOW
# ============================================================

A729602 >> A739231
A739231 >> A729603
A729603 >> A729602_SUCCESS


# ============================================================
# CHILD DAG SUCCESS MARKERS
# ============================================================

A725592 >> A725592_SUCCESS
A724944 >> A724944_SUCCESS
A726983 >> A726983_SUCCESS
A716410 >> A716410_SUCCESS
A729736 >> A729736_SUCCESS
A714316 >> A714316_SUCCESS
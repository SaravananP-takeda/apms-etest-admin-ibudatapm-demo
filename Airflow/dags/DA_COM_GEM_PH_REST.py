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
from airflow.utils.task_group import TaskGroup
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

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "parameters"))
from DA_COM_GEM_PH_PARAMETERS import (
PARAMS_DICT, SNOW_ATTRIBUTES_DEFAULTS, EMAIL_RECIPIENTS,
DATABRICKS_CONN_ID, AWS_CONN_ID, GLOBALSCAPE_CONN_ID, SSH_CONN_ID,POOLS,S3_SENSOR_POKE_INTERVAL_SECONDS, S3_SENSOR_TIMEOUT_SECONDS
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
    def RUN_PH_LAKETOHUB_TXN_PH_WHOLESALER_SALES_GLOBO(context):
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


    
    def PH_ZPC_WHOLESALER_SALES_LOAD_FAILURE(context):
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


   
    def PH_ZPC_CSMO_SALES_LOAD_FAILURE(context):
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
    def RUN_PH_LAKETOHUB_TXN_PH_WHOLESALER_SALES_GLOBO_PCSO(context):
        pass


    
    def PH_TRADE_DISCOUNT_LOAD_FAILURE(context):
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
    def RUN_PH_LAKETOHUB_TXN_PH_WHOLESALER_SALES_ZPC(context):
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
    dag_id = "DA-COM-GEM-PH-REST",
    default_args = default_args,
    description="Tidal to Airflow Migration - PH (jobs with no dependency on the lambda flow)",
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

# MT_APAC_PH_GLOBO_FILE_SCHEMA has no dependency in Tidal (BATCH dependency metadata),
# so it runs in its own DAG: triggering the lambda flow (DA-COM-GEM-PH-GRP) does not trigger it.

ASTART = DummyOperator (task_id = "START", execution_timeout = timedelta(minutes=60), dag = dag)

A709364 = DummyOperator (task_id = "709364_DA-COM-GEM-PH", task_display_name = "48910_DA-COM-GEM-PH", execution_timeout = timedelta(minutes=60), dag = dag)

A739684 = PythonOperator (task_id = "739684_MT_APAC_PH_GLOBO_FILE_SCHEMA", python_callable = test_function, task_display_name = "48973_MT_APAC_PH_GLOBO_FILE_SCHEMA", execution_timeout = timedelta(minutes=60), dag = dag)

####================== Dependency Management =================

ASTART >> A709364
A709364 >> A739684

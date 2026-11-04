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
from DA_COM_GEM_ID_PARAMETERS import (
PARAMS_DICT, SNOW_ATTRIBUTES_DEFAULTS, EMAIL_RECIPIENTS,
DATABRICKS_CONN_ID, AWS_CONN_ID,
S3_SENSOR_POKE_INTERVAL_SECONDS, S3_SENSOR_TIMEOUT_SECONDS,
PYTHON_SENSOR_POKE_INTERVAL_SECONDS, PYTHON_SENSOR_TIMEOUT_SECONDS

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
    'apms_id':            'APMS-93057',
    'bsn_id':             'BSN0020621',
    'type':               'AWS-Airflow-EU',
    'env':                af_env,
}

####======= Class to handle Callback Action Functions ==========================================

class DAGCallbackAction():

    def __init__(self):

        self.snow_attributes = snow_attributes
        self.EMAIL_TO = EMAIL_RECIPIENTS.get(af_env)

        # On Success/ On Failure Callback Handlers List for each Task
        self.on_success_completion_handlers = {
            '730873_004-ID-LAKETOLAKE-VACCINE-DAILY-STOCK-STG': ['RUN-ID-LAKETOHUB-TXN-VACCINE-INVENTORY-IDN'],
            '710100_009-ID-LAKETOHUB-REF-PROD-PRICE-IDN': ['ID-LAKETOHUB-REF-PROD-PRICE-IDN-SET-TRUE', 'RUN-ID-HUBTOMART-REF-PROD-PRICE-IDN'],
            'ID-VACCINE-DAILY-STOCK-LAKELOAD': ['RUN-ID-LAKETOLAKE-VACCINE-DAILY-STOCK-STG'],
            'ID-TARGET-SALES-OTC-LAKELOAD': ['ID-TARGET-SALES-OTC-SET-TRUE', 'RUN-ID-LAKETOLAKE-TARGET-SALES-STG'],
            '711942_001-ID-LAKETOHUB-CUST-XREF-IDN': ['ID-LAKETOHUB-CUST-XREF-IDN-SET-TRUE', 'RUN-ID-HUBTOMART-CUST-XREF-IDN'],
            'ID-TB-MASTER-PRODUCT-LAKELOAD': ['ID-TB-MASTER-PRODUCT-SET-TRUE'],
            '724359_002-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG': ['RUN-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN', 'ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG-SET-TRUE', 'RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-APL-SUCCESS'],
            '743417_010-ID-LAKETOHUB-TXN-VACCINE-INVENTORY-IDN': ['RUN-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN'],
            '735869_009-ID-HUBTOMART-REF-PROD-PRICE-IDN': ['ID-HUBTOMART-REF-PROD-PRICE-IDN-SET-FALSE'],
            '732269_007-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN': ['ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-SET-FALSE', 'RUN-JOB-ID-SALESFORCE-CUST-ALIGN-EXCEPTION-DATABRICKS-NOTEBOOK'],
            '748793_007-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN': ['RUN-ID-LAKE-TO-HUB-TXN-ACTUAL-SALES-IDN', 'RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN', 'ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN-SET-TRUE'],
            'ID-MASTER-EMPLOYEE-LAKELOAD': ['ID-MASTER-EMPLOYEE-SET-TRUE', 'RUN-ID-LAKETOHUB-REF-EMP-IDN'],
            '729935_004-ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': ['ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN-SET-FALSE'],
            '735446_008-ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN': ['ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN-SET-FALSE'],
            'ID-TB-HOSPITAL-OUTLET-MAP-LAKELOAD': ['ID-TB-HOSPITAL-OUTLET-MAP-SET-TRUE'],
            'ID-SALESFORCE-CUSTOMER-ALIGNMENT-LAKELOAD': ['ID-SALESFORCE-CUSTOMER-ALIGNMENT-SET-TRUE', 'RUN-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN'],
            '735229_006-ID-HUBTOMART-TXN-TARGET-SALES-IDN': ['ID-HUBTOMART-TXN-TARGET-SALES-IDN-SET-FALSE', 'RUN-ID-HUBTOMART-TXN-TARGET-SALES-IDN-SUCCESS'],
            '747788_011-ID-TXN-ACTUAL-SALES-IDN-APL-SUCCESS-MAIL-SLEEP-JOB': ['RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-APL-SUCCESS-SEND-MAIL'],
            '717929_005-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN': ['RUN-ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN', 'ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN-SET-TRUE'],
            '738608_012-ID-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS-MAIL-SLEEP-JOB': ['RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS-SEND-MAIL'],
            'ID-CUSTOMER-UDC-LAKELOAD': ['ID-CUSTOMER-UDC-SET-TRUE'],
            '738127_003-ID-LAKETOHUB-REF-EMP-IDN': ['ID-LAKETOHUB-REF-EMP-IDN-SET-TRUE', 'RUN-ID-HUBTOMART-REF-EMP-IDN'],
            '736543_001-ID-HUBTOMART-REF-CUST-IDN': ['ID-HUBTOMART-REF-CUST-IDN-SET-FALSE'],
            '738717_010-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN': ['ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN-SET-FALSE', 'RUN-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN-SUCCESS'],
            'ID-PRODUCT-UDC-LAKELOAD': ['ID-PRODUCT-UDC-SET-TRUE'],
            '747491_008-ID-LAKETOHUB-TXN-TARGET-SALES-IDN': ['RUN-ID-HUBTOMART-TXN-TARGET-SALES-IDN', 'ID-LAKETOHUB-TXN-TARGET-SALES-IDN-SET-TRUE', 'RUN-ID-LAKE-TO-HUB-TXN-TARGET-SALES-IDN'],
            '722138_001-GEM-DATABRICKS-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL': ['RUN-ID-LAKETOLAKE-ID-TB-SALES-UDC'],
            '730674_002-ID-HUBTOMART-REF-PROD-IDN': ['ID-HUBTOMART-REF-PROD-IDN-SET-FALSE'],
            'ID-EMPLOYEE-TEAM-LAKELOAD': ['RUN-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN', 'ID-EMPLOYEE-TEAM-SET-TRUE'],
            'ID-SALES-UDC-LAKELOAD': ['ID-SALES-UDC-SET-TRUE', 'RUN-JOB-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL'],
            '727062_003-ID-HUBTOMART-CUST-XREF-IDN': ['ID-HUBTOMART-CUST-XREF-IDN-SET-FALSE'],
            'ID-TB-PRODUCT-PRICE-LAKELOAD': ['ID-TB-PRODUCT-PRICE-SET-TRUE', 'RUN-ID-LAKETOHUB-REF-PROD-PRICE-IDN'],
            'ID-VACCINE-SALES-LAKELOAD': ['RUN-ID-LAKETOHUB-REF-CUST-IDN', 'RUN-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG', 'ID-VACCINE-SALES-SET-TRUE'],
            '742267_002-ID-LAKETOHUB-REF-PROD-IDN': ['RUN-ID-HUBTOMART-REF-PROD-IDN', 'ID-LAKETOHUB-REF-PROD-IDN-SET-TRUE'],
            '728541_003-ID-LAKETOLAKE-TARGET-SALES-STG': ['RUN-ID-LAKETOHUB-TXN-TARGET-SALES-IDN', 'ID-LAKETOLAKE-TARGET-SALES-STG-SET-TRUE'],
            'ID-TARGET-SALES-TBE-LAKELOAD': ['ID-TARGET-SALES-TBE-SET-TRUE', 'RUN-ID-LAKETOLAKE-TARGET-SALES-STG'],
            '741885_001-ID-LAKETOLAKE-ID-TB-SALES-UDC': ['ID-LAKETOLAKE-ID-TB-SALES-UDC-SET-TRUE', 'RUN-ID-LAKETOHUB-CUST-XREF-IDN', 'RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS', 'RUN-ID-LAKETOHUB-REF-PROD-IDN', 'RUN-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN'],
            '738868_006-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': ['RUN-ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN', 'ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN-SET-TRUE'],
            '744144_004-ID-LAKETOHUB-REF-CUST-IDN': ['RUN-ID-HUBTOMART-REF-CUST-IDN', 'ID-LAKETOHUB-REF-CUST-IDN-SET-TRUE'],
            'ID-TARGET-SALES-VAC-LAKELOAD': ['RUN-ID-LAKETOLAKE-TARGET-SALES-STG', 'ID-TARGET-SALES-VAC-SET-TRUE'],
            '748694_005-ID-HUBTOMART-REF-EMP-IDN': ['ID-HUBTOMART-REF-EMP-IDN-SET-FALSE']
        }

        self.on_failure_abnormal_completion_handlers = {
            '732269_007-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN': ['RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-FAILURE'],
            '735229_006-ID-HUBTOMART-TXN-TARGET-SALES-IDN': ['RUN-ID-HUBTOMART-TXN-TARGET-SALES-IDN-FAILURE'],
            '738717_010-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN': ['RUN-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN-FAILURE']
        }

        self.on_failure_error_handlers = {}

        # On Success/ On Failure Callback Handler name to Callback function mapping
        self.handler_callback_fun_map = {
            'RUN-ID-LAKETOHUB-TXN-VACCINE-INVENTORY-IDN': self.RUN_ID_LAKETOHUB_TXN_VACCINE_INVENTORY_IDN,
            'ID-LAKETOHUB-REF-PROD-PRICE-IDN-SET-TRUE': self.ID_LAKETOHUB_REF_PROD_PRICE_IDN_SET_TRUE,
            'RUN-ID-HUBTOMART-REF-PROD-PRICE-IDN': self.RUN_ID_HUBTOMART_REF_PROD_PRICE_IDN,
            'RUN-ID-LAKETOLAKE-VACCINE-DAILY-STOCK-STG': self.RUN_ID_LAKETOLAKE_VACCINE_DAILY_STOCK_STG,
            'ID-TARGET-SALES-OTC-SET-TRUE': self.ID_TARGET_SALES_OTC_SET_TRUE,
            'RUN-ID-LAKETOLAKE-TARGET-SALES-STG': self.RUN_ID_LAKETOLAKE_TARGET_SALES_STG,
            'ID-LAKETOHUB-CUST-XREF-IDN-SET-TRUE': self.ID_LAKETOHUB_CUST_XREF_IDN_SET_TRUE,
            'RUN-ID-HUBTOMART-CUST-XREF-IDN': self.RUN_ID_HUBTOMART_CUST_XREF_IDN,
            'ID-TB-MASTER-PRODUCT-SET-TRUE': self.ID_TB_MASTER_PRODUCT_SET_TRUE,
            'RUN-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN': self.RUN_ID_LAKETOHUB_TXN_ACTUAL_SALES_IDN,
            'ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG-SET-TRUE': self.ID_LAKETOLAKE_ID_TB_VACCINE_SALES_STG_SET_TRUE,
            'RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-APL-SUCCESS': self.RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_APL_SUCCESS,
            'RUN-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN': self.RUN_ID_HUBTOMART_TXN_VACCINE_INVENTORY_IDN,
            'ID-HUBTOMART-REF-PROD-PRICE-IDN-SET-FALSE': self.ID_HUBTOMART_REF_PROD_PRICE_IDN_SET_FALSE,
            'ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-SET-FALSE': self.ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_SET_FALSE,
            'RUN-JOB-ID-SALESFORCE-CUST-ALIGN-EXCEPTION-DATABRICKS-NOTEBOOK': self.RUN_JOB_ID_SALESFORCE_CUST_ALIGN_EXCEPTION_DATABRICKS_NOTEBOOK,
            'RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-FAILURE': self.RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_FAILURE,
            'RUN-ID-LAKE-TO-HUB-TXN-ACTUAL-SALES-IDN': self.RUN_ID_LAKE_TO_HUB_TXN_ACTUAL_SALES_IDN,
            'RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN': self.RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN,
            'ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN-SET-TRUE': self.ID_LAKETOHUB_TXN_ACTUAL_SALES_IDN_SET_TRUE,
            'ID-MASTER-EMPLOYEE-SET-TRUE': self.ID_MASTER_EMPLOYEE_SET_TRUE,
            'RUN-ID-LAKETOHUB-REF-EMP-IDN': self.RUN_ID_LAKETOHUB_REF_EMP_IDN,
            'ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN-SET-FALSE': self.ID_HUBTOMART_REF_EMP_TERR_ALIGNMENT_HIERARCHY_IDN_SET_FALSE,
            'ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN-SET-FALSE': self.ID_HUBTOMART_REF_CUST_TERR_ALIGNMENT_IDN_SET_FALSE,
            'ID-TB-HOSPITAL-OUTLET-MAP-SET-TRUE': self.ID_TB_HOSPITAL_OUTLET_MAP_SET_TRUE,
            'ID-SALESFORCE-CUSTOMER-ALIGNMENT-SET-TRUE': self.ID_SALESFORCE_CUSTOMER_ALIGNMENT_SET_TRUE,
            'RUN-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN': self.RUN_ID_LAKETOHUB_REF_CUST_TERR_ALIGNMENT_IDN,
            'RUN-ID-HUBTOMART-TXN-TARGET-SALES-IDN-FAILURE': self.RUN_ID_HUBTOMART_TXN_TARGET_SALES_IDN_FAILURE,
            'ID-HUBTOMART-TXN-TARGET-SALES-IDN-SET-FALSE': self.ID_HUBTOMART_TXN_TARGET_SALES_IDN_SET_FALSE,
            'RUN-ID-HUBTOMART-TXN-TARGET-SALES-IDN-SUCCESS': self.RUN_ID_HUBTOMART_TXN_TARGET_SALES_IDN_SUCCESS,
            'RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-APL-SUCCESS-SEND-MAIL': self.RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_APL_SUCCESS_SEND_MAIL,
            'RUN-ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN': self.RUN_ID_HUBTOMART_REF_CUST_TERR_ALIGNMENT_IDN,
            'ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN-SET-TRUE': self.ID_LAKETOHUB_REF_CUST_TERR_ALIGNMENT_IDN_SET_TRUE,
            'RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS-SEND-MAIL': self.RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_DICO_SUCCESS_SEND_MAIL,
            'ID-CUSTOMER-UDC-SET-TRUE': self.ID_CUSTOMER_UDC_SET_TRUE,
            'ID-LAKETOHUB-REF-EMP-IDN-SET-TRUE': self.ID_LAKETOHUB_REF_EMP_IDN_SET_TRUE,
            'RUN-ID-HUBTOMART-REF-EMP-IDN': self.RUN_ID_HUBTOMART_REF_EMP_IDN,
            'ID-HUBTOMART-REF-CUST-IDN-SET-FALSE': self.ID_HUBTOMART_REF_CUST_IDN_SET_FALSE,
            'RUN-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN-FAILURE': self.RUN_ID_HUBTOMART_TXN_VACCINE_INVENTORY_IDN_FAILURE,
            'ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN-SET-FALSE': self.ID_HUBTOMART_TXN_VACCINE_INVENTORY_IDN_SET_FALSE,
            'RUN-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN-SUCCESS': self.RUN_ID_HUBTOMART_TXN_VACCINE_INVENTORY_IDN_SUCCESS,
            'ID-PRODUCT-UDC-SET-TRUE': self.ID_PRODUCT_UDC_SET_TRUE,
            'RUN-ID-HUBTOMART-TXN-TARGET-SALES-IDN': self.RUN_ID_HUBTOMART_TXN_TARGET_SALES_IDN,
            'ID-LAKETOHUB-TXN-TARGET-SALES-IDN-SET-TRUE': self.ID_LAKETOHUB_TXN_TARGET_SALES_IDN_SET_TRUE,
            'RUN-ID-LAKE-TO-HUB-TXN-TARGET-SALES-IDN': self.RUN_ID_LAKE_TO_HUB_TXN_TARGET_SALES_IDN,
            'RUN-ID-LAKETOLAKE-ID-TB-SALES-UDC': self.RUN_ID_LAKETOLAKE_ID_TB_SALES_UDC,
            'ID-HUBTOMART-REF-PROD-IDN-SET-FALSE': self.ID_HUBTOMART_REF_PROD_IDN_SET_FALSE,
            'RUN-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': self.RUN_ID_LAKETOHUB_REF_EMP_TERR_ALIGNMENT_HIERARCHY_IDN,
            'ID-EMPLOYEE-TEAM-SET-TRUE': self.ID_EMPLOYEE_TEAM_SET_TRUE,
            'ID-SALES-UDC-SET-TRUE': self.ID_SALES_UDC_SET_TRUE,
            'RUN-JOB-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL': self.RUN_JOB_NOTEBOOK_ID_SALES_UDC_FILE_VALIDATION_EMAIL,
            'ID-HUBTOMART-CUST-XREF-IDN-SET-FALSE': self.ID_HUBTOMART_CUST_XREF_IDN_SET_FALSE,
            'ID-TB-PRODUCT-PRICE-SET-TRUE': self.ID_TB_PRODUCT_PRICE_SET_TRUE,
            'RUN-ID-LAKETOHUB-REF-PROD-PRICE-IDN': self.RUN_ID_LAKETOHUB_REF_PROD_PRICE_IDN,
            'RUN-ID-LAKETOHUB-REF-CUST-IDN': self.RUN_ID_LAKETOHUB_REF_CUST_IDN,
            'RUN-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG': self.RUN_ID_LAKETOLAKE_ID_TB_VACCINE_SALES_STG,
            'ID-VACCINE-SALES-SET-TRUE': self.ID_VACCINE_SALES_SET_TRUE,
            'RUN-ID-HUBTOMART-REF-PROD-IDN': self.RUN_ID_HUBTOMART_REF_PROD_IDN,
            'ID-LAKETOHUB-REF-PROD-IDN-SET-TRUE': self.ID_LAKETOHUB_REF_PROD_IDN_SET_TRUE,
            'RUN-ID-LAKETOHUB-TXN-TARGET-SALES-IDN': self.RUN_ID_LAKETOHUB_TXN_TARGET_SALES_IDN,
            'ID-LAKETOLAKE-TARGET-SALES-STG-SET-TRUE': self.ID_LAKETOLAKE_TARGET_SALES_STG_SET_TRUE,
            'ID-TARGET-SALES-TBE-SET-TRUE': self.ID_TARGET_SALES_TBE_SET_TRUE,
            'ID-LAKETOLAKE-ID-TB-SALES-UDC-SET-TRUE': self.ID_LAKETOLAKE_ID_TB_SALES_UDC_SET_TRUE,
            'RUN-ID-LAKETOHUB-CUST-XREF-IDN': self.RUN_ID_LAKETOHUB_CUST_XREF_IDN,
            'RUN-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS': self.RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_DICO_SUCCESS,
            'RUN-ID-LAKETOHUB-REF-PROD-IDN': self.RUN_ID_LAKETOHUB_REF_PROD_IDN,
            'RUN-ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': self.RUN_ID_HUBTOMART_REF_EMP_TERR_ALIGNMENT_HIERARCHY_IDN,
            'ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN-SET-TRUE': self.ID_LAKETOHUB_REF_EMP_TERR_ALIGNMENT_HIERARCHY_IDN_SET_TRUE,
            'RUN-ID-HUBTOMART-REF-CUST-IDN': self.RUN_ID_HUBTOMART_REF_CUST_IDN,
            'ID-LAKETOHUB-REF-CUST-IDN-SET-TRUE': self.ID_LAKETOHUB_REF_CUST_IDN_SET_TRUE,
            'ID-TARGET-SALES-VAC-SET-TRUE': self.ID_TARGET_SALES_VAC_SET_TRUE,
            'ID-HUBTOMART-REF-EMP-IDN-SET-FALSE': self.ID_HUBTOMART_REF_EMP_IDN_SET_FALSE
        }

    def update_af_variable(self, context, key, new_value):
        try:
            curr_value = Variable.get(key)
            logging.info(f"{LOG_PFX} Current value of AF Variable: {key} is {curr_value}")
            logging.info(f"{LOG_PFX} Updating value of AF Variable: {key} to {new_value}")
            Variable.set(key = key, value = new_value)
            logging.info(f"{LOG_PFX} Value of AF Variable: {key} is updated")
        except Exception as e:
            logging.error(f"{LOG_PFX} Error received while updating value of AF Variable: {key} : {e}")

    ####========On Success/ On Failure Callback functions for Tasks ============

    def ID_HUBTOMART_REF_CUST_TERR_ALIGNMENT_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-SALESFORCE-CUSTOMER-ALIGNMENT", False)
        self.update_af_variable(context, "ID-HUB-REF-CUST-TERR-ALIGNMENT-IDN", False)

    def ID_HUBTOMART_REF_CUST_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-CUSTOMER-UDC", False)
    
    def ID_TB_MASTER_PRODUCT_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-TB-MASTER-PRODUCT", True)

    def ID_SALES_UDC_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-SALES-UDC", True)

    def ID_EMPLOYEE_TEAM_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-EMPLOYEE-TEAM", True)

    def ID_VACCINE_SALES_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-VACCINE-SALES", True)

    def ID_LAKETOHUB_REF_EMP_IDN_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-HUB-REF-EMP-IDN", True)

    def RUN_ID_LAKETOHUB_TXN_TARGET_SALES_IDN(self, context):
        self.update_af_variable(context, "ID-LAKETOLAKE-TARGET-SALES-STG", True)

    def ID_HUBTOMART_REF_EMP_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-HUB-REF-EMP-IDN", False)
        self.update_af_variable(context, "ID-MASTER-EMPLOYEE", False)

    def ID_PRODUCT_UDC_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-PRODUCT-UDC", True)
    
    def ID_TARGET_SALES_VAC_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-TARGET-SALES-VAC", True)

    def ID_HUBTOMART_TXN_VACCINE_INVENTORY_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-VACCINE-DAILY-STOCK", False)

    def ID_HUBTOMART_REF_EMP_TERR_ALIGNMENT_HIERARCHY_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-HUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN", False)
        self.update_af_variable(context, "ID-EMPLOYEE-TEAM", False)

    def ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-SALES-UDC", False)
        self.update_af_variable(context, "ID-VACCINE-SALES", False)
        self.update_af_variable(context, "ID-HUB-TXN-ACTUAL-SALES-IDN", False)
        self.update_af_variable(context, "ID-HUB-REF-CUST-IDN", False)
        self.update_af_variable(context, "ID-LAKETOLAKE-ID-TB-SALES-UDC", False)
        self.update_af_variable(context, "ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG", False)

    def ID_HUBTOMART_CUST_XREF_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-CUSTOMER-UDC", False)
        self.update_af_variable(context, "ID-TB-HOSPITAL-OUTLET-MAP", False)
        self.update_af_variable(context, "ID-HUB-CUST-XREF-IDN", False)
        self.update_af_variable(context, "ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG", False)

    def ID_LAKETOHUB_REF_CUST_TERR_ALIGNMENT_IDN_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-HUB-REF-CUST-TERR-ALIGNMENT-IDN", True)

    def ID_LAKETOLAKE_ID_TB_SALES_UDC_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-LAKETOLAKE-ID-TB-SALES-UDC", True)

    def ID_MASTER_EMPLOYEE_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-MASTER-EMPLOYEE", True)

    def ID_LAKETOHUB_REF_PROD_IDN_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-HUB-REF-PROD-IDN", True)

    def ID_LAKETOHUB_REF_EMP_TERR_ALIGNMENT_HIERARCHY_IDN_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-HUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN", True)

    def ID_CUSTOMER_UDC_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-CUSTOMER-UDC", True)

    def ID_HUBTOMART_REF_PROD_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-TB-MASTER-PRODUCT", False)
        self.update_af_variable(context, "ID-HUB-REF-PROD-IDN", False)
        self.update_af_variable(context, "ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG", False)
        self.update_af_variable(context, "ID-PRODUCT-UDC", False)
        self.update_af_variable(context, "ID-VACCINE-SALES", False)

    def ID_TARGET_SALES_TBE_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-TARGET-SALES-TBE", True)

    def ID_LAKETOHUB_TXN_ACTUAL_SALES_IDN_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-HUB-TXN-ACTUAL-SALES-IDN", True)

    def ID_LAKETOHUB_CUST_XREF_IDN_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-HUB-CUST-XREF-IDN", True)

    def ID_HUBTOMART_TXN_TARGET_SALES_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-TARGET-SALES-OTC", False)
        self.update_af_variable(context, "ID-TARGET-SALES-TBE", False)
        self.update_af_variable(context, "ID-TARGET-SALES-VAC", False)
        self.update_af_variable(context, "ID-HUB-TXN-TARGET-SALES-IDN", False)
        self.update_af_variable(context, "ID-LAKETOLAKE-TARGET-SALES-STG", False)

    def ID_LAKETOHUB_REF_CUST_IDN_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-HUB-REF-CUST-IDN", True)

    def ID_SALESFORCE_CUSTOMER_ALIGNMENT_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-SALESFORCE-CUSTOMER-ALIGNMENT", True)

    def ID_LAKETOHUB_REF_PROD_PRICE_IDN_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-HUB-REF-PROD-PRICE-IDN", True)

    def ID_TARGET_SALES_OTC_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-TARGET-SALES-OTC", True)

    def RUN_ID_LAKETOLAKE_VACCINE_DAILY_STOCK_STG(self, context):
        self.update_af_variable(context, "ID-VACCINE-DAILY-STOCK", True)

    def ID_LAKETOLAKE_ID_TB_VACCINE_SALES_STG_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG", True)

    def ID_TB_HOSPITAL_OUTLET_MAP_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-TB-HOSPITAL-OUTLET-MAP", True)

    def ID_TB_PRODUCT_PRICE_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-TB-PRODUCT-PRICE", True)

    def ID_LAKETOLAKE_TARGET_SALES_STG_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-LAKETOLAKE-TARGET-SALES-STG", True)

    def ID_LAKETOHUB_TXN_TARGET_SALES_IDN_SET_TRUE(self, context):
        self.update_af_variable(context, "ID-HUB-TXN-TARGET-SALES-IDN", True)

    def ID_HUBTOMART_REF_PROD_PRICE_IDN_SET_FALSE(self, context):
        self.update_af_variable(context, "ID-TB-PRODUCT-PRICE", False)
        self.update_af_variable(context, "ID-HUB-REF-PROD-PRICE-IDN", False)

    def RUN_ID_HUBTOMART_REF_CUST_TERR_ALIGNMENT_IDN(self, context):
        pass

    def RUN_ID_LAKETOHUB_REF_CUST_TERR_ALIGNMENT_IDN(self, context):
        pass

    def RUN_ID_LAKE_TO_HUB_TXN_TARGET_SALES_IDN(self, context):
        pass

    def RUN_ID_LAKETOHUB_REF_EMP_IDN(self, context):
        pass

    def RUN_ID_HUBTOMART_REF_EMP_TERR_ALIGNMENT_HIERARCHY_IDN(self, context):
        pass

    def RUN_ID_LAKETOHUB_REF_CUST_IDN(self, context):
        pass

    def RUN_ID_LAKETOHUB_REF_PROD_PRICE_IDN(self, context):
        pass

    def RUN_JOB_ID_SALESFORCE_CUST_ALIGN_EXCEPTION_DATABRICKS_NOTEBOOK(self, context):
        pass

    def RUN_ID_HUBTOMART_REF_PROD_IDN(self, context):
        pass

    def RUN_ID_LAKETOLAKE_ID_TB_SALES_UDC(self, context):
        pass

    def RUN_ID_LAKETOHUB_REF_PROD_IDN(self, context):
        pass

    def RUN_ID_LAKETOHUB_CUST_XREF_IDN(self, context):
        pass

    def RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_DICO_SUCCESS(self, context):
        pass

    def RUN_ID_LAKETOHUB_REF_EMP_TERR_ALIGNMENT_HIERARCHY_IDN(self, context):
        pass

    def RUN_ID_HUBTOMART_REF_PROD_PRICE_IDN(self, context):
        pass

    def RUN_ID_HUBTOMART_TXN_VACCINE_INVENTORY_IDN(self, context):
        pass

    def RUN_JOB_NOTEBOOK_ID_SALES_UDC_FILE_VALIDATION_EMAIL(self, context):
        pass

    def RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_APL_SUCCESS(self, context):
        pass

    def RUN_ID_HUBTOMART_REF_EMP_IDN(self, context):
        pass

    def RUN_ID_LAKETOHUB_TXN_VACCINE_INVENTORY_IDN(self, context):
        pass

    def RUN_ID_LAKETOLAKE_ID_TB_VACCINE_SALES_STG(self, context):
        pass

    def RUN_ID_HUBTOMART_REF_CUST_IDN(self, context):
        pass

    def RUN_ID_HUBTOMART_CUST_XREF_IDN(self, context):
        pass

    def RUN_ID_LAKETOLAKE_TARGET_SALES_STG(self, context):
        pass

    def RUN_ID_LAKETOHUB_TXN_ACTUAL_SALES_IDN(self, context):
        pass

    def RUN_ID_LAKE_TO_HUB_TXN_ACTUAL_SALES_IDN(self, context):
        pass

    def RUN_ID_HUBTOMART_TXN_TARGET_SALES_IDN(self, context):
        pass

    def RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN(self, context):
        pass

    ####========Email Events for Tasks ============
    
    def RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_DICO_SUCCESS_SEND_MAIL(self, context):
        email_to = self.EMAIL_TO
        email_subject = "TXN Actual Sales refresh success for Indonesia DICO Data"
        email_message = """
        <p>Dear All,</p>
        <p>TXN Actual sales data for DICO has been refreshed successfully for Indonesia.</p>
        <p>Thanks,<br>
        Airflow</p>
        """
        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )
    
    def RUN_ID_HUBTOMART_TXN_TARGET_SALES_IDN_FAILURE(self, context):
        email_to = self.EMAIL_TO
        email_subject = "TXN Target Sales refresh failure for Indonesia"
        email_message = """
        <p>Dear All,</p>
        <p>Attempts to refresh TXN Target sales data for Indonesia failed and the team is working to resolve.</p>
        <p>Thanks,<br>
        Airflow</p>
        """
        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )

    def RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_APL_SUCCESS_SEND_MAIL(self, context):
        email_to = self.EMAIL_TO
        email_subject = "TXN Actual Sales refresh success for Indonesia APL Data"
        email_message = """
        <p>Dear All,</p>
        <p>TXN Actual sales data for APL has been refreshed successfully for Indonesia.</p>
        <p>Thanks,<br>
        Airflow</p>
        """
        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )

    def RUN_ID_HUBTOMART_TXN_TARGET_SALES_IDN_SUCCESS(self, context):
        email_to = self.EMAIL_TO
        email_subject = "TXN Target Sales refresh success for Indonesia"
        email_message = """
        <p>Dear All,</p>
        <p>TXN Target sales data has been refreshed successfully for Indonesia.</p>
        <p>Thanks,<br>
        Airflow</p>
        """
        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )

    def RUN_ID_HUBTOMART_TXN_VACCINE_INVENTORY_IDN_FAILURE(self, context):
        email_to = self.EMAIL_TO
        email_subject = "TXN Target Sales refresh failure for Indonesia"
        email_message = """
        <p>Dear All,</p>
        <p>Attempts to refresh TXN Vaccine Inventory data for Indonesia failed and the team is working to resolve.</p>
        <p>Thanks,<br>
        Airflow</p>
        """
        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )
    
    def RUN_ID_HUBTOMART_TXN_VACCINE_INVENTORY_IDN_SUCCESS(self, context):
        email_to = self.EMAIL_TO
        email_subject = "TXN Target Sales refresh success for Indonesia"
        email_message = """
        <p>Dear All,</p>
        <p>TXN Vaccine Inventory data has been refreshed successfully for Indonesia.</p>
        <p>Thanks,<br>
        Airflow</p>"""
        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )

    def RUN_ID_HUBTOMART_TXN_ACTUAL_SALES_IDN_FAILURE(self, context):
        email_to = self.EMAIL_TO
        email_subject = "TXN Actual Sales refresh failure for Indonesia"
        email_message = """
        <p>Dear All,</p>
        <p>Attempts to refresh TXN Actual sales data for Indonesia failed and the team is working to resolve.</p>
        <p>Thanks,<br>
        Airflow</p>
        """
        send_email(
            to=email_to,
            subject=email_subject,
            html_content=email_message
        )


####========Variable Dependencies / Pre Checks ============

VARIABLE_DEPENDENCIES = {
    "ID-PRODUCT-UDC-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-PRODUCT-UDC",
        "expected_value": False,
    },
    "ID-EMPLOYEE-TEAM-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-EMPLOYEE-TEAM",
        "expected_value": False,
    },
    "ID-MASTER-EMPLOYEE-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-MASTER-EMPLOYEE",
        "expected_value": False,
    },
    "ID-TARGET-SALES-OTC-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-TARGET-SALES-OTC",
        "expected_value": False,
    },
    "ID-SALES-UDC-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-SALES-UDC",
        "expected_value": False, 
    },
    "ID-TB-PRODUCT-PRICE-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-TB-PRODUCT-PRICE",
        "expected_value": False, 
    },
    "ID-CUSTOMER-UDC-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-CUSTOMER-UDC",
        "expected_value": False, 
    },
    "ID-TB-MASTER-PRODUCT-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-TB-MASTER-PRODUCT",
        "expected_value": False,
    },
    "ID-SALESFORCE-CUSTOMER-ALIGNMENT-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-SALESFORCE-CUSTOMER-ALIGNMENT",
        "expected_value": False,
    },
    "ID-TB-HOSPITAL-OUTLET-MAP-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-TB-HOSPITAL-OUTLET-MAP",
        "expected_value": False,
    },
    "ID-TARGET-SALES-TBE-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-TARGET-SALES-TBE",
        "expected_value": False,
    },
    "ID-VACCINE-SALES-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-VACCINE-SALES",
        "expected_value": False,
    },
    "ID-TARGET-SALES-VAC-FILEWATCH": {
        "type": "simple",
        "variable_name": "ID-TARGET-SALES-VAC",
        "expected_value": False,
    },
    "003-ID-LAKETOHUB-REF-EMP-IDN": {
        "type": "simple",
        "variable_name": "ID-HUB-REF-EMP-IDN",
        "expected_value": False,
    },
    "005-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN": {
        "type": "simple",
        "variable_name": "ID-HUB-REF-CUST-TERR-ALIGNMENT-IDN",
        "expected_value": False,
    },
    "006-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN": {
        "type": "simple",
        "variable_name": "ID-HUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN",
        "expected_value": False,
    },
    "009-ID-LAKETOHUB-REF-PROD-PRICE-IDN": {
        "type": "simple",
        "variable_name": "ID-HUB-REF-PROD-PRICE-IDN",
        "expected_value": False,
    },
    "001-ID-LAKETOLAKE-ID-TB-SALES-UDC": {
        "type": "simple",
        "variable_name": "ID-LAKETOLAKE-ID-TB-SALES-UDC",
        "expected_value": False,
    },
    "002-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG": {
        "type": "simple",
        "variable_name": "ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG",
        "expected_value": False,
    },
    "003-ID-LAKETOLAKE-TARGET-SALES-STG": {
        "type": "simple",
        "variable_name": "ID-LAKETOLAKE-TARGET-SALES-STG",
        "expected_value": False,
    },
    "011-ID-TXN-ACTUAL-SALES-IDN-APL-SUCCESS-MAIL-SLEEP-JOB": {
        "type": "simple",
        "variable_name": "ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG",
        "expected_value": False,
    },
    "012-ID-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS-MAIL-SLEEP-JOB": {
        "type": "simple",
        "variable_name": "ID-LAKETOLAKE-ID-TB-SALES-UDC",
        "expected_value": False,
    },
    "001-ID-LAKETOHUB-CUST-XREF-IDN": {
        "type": "compound",
        "formula": "([58657] AND [58658] AND [58660]) OR ([58657] AND [58658] AND [58661]) OR ([58657] AND [58659] AND [58660])",
        "atomic": {
            58660: ("ID-TB-HOSPITAL-OUTLET-MAP", True),
            58657: ("ID-HUB-CUST-XREF-IDN", False),
            58661: ("ID-TB-HOSPITAL-OUTLET-MAP", False),
            58659: ("ID-CUSTOMER-UDC", False),
            58658: ("ID-CUSTOMER-UDC", True)
        },
    },
    "002-ID-LAKETOHUB-REF-PROD-IDN": {
        "type": "compound",
        "formula": "([58663] AND [58664] AND [58666]) OR ([58663] AND [58664] AND [58667]) OR ([58663] AND [58665] AND [58666])",
        "atomic": {
            58664: ("ID-PRODUCT-UDC", True),
            58665: ("ID-PRODUCT-UDC", False),
            58663: ("ID-HUB-REF-PROD-IDN", False),
            58667: ("ID-TB-MASTER-PRODUCT", False),
            58666: ("ID-TB-MASTER-PRODUCT", True)
        }
    },
    "004-ID-LAKETOHUB-REF-CUST-IDN": {
        "type": "compound",
        "formula": "([58669] AND [58670]) OR ([58669] AND [58671]) OR ([69623] AND [69626])",
        "atomic": {
            58670: ("ID-CUSTOMER-UDC", True),
            69623: ("ID-HUB-REF-CUST-IDN", True),
            58671: ("ID-CUSTOMER-UDC", False),
            58669: ("ID-HUB-REF-CUST-IDN", False),
            69626: ("ID-VACCINE-SALES", True)
        }
    },
    "007-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN": {
        "type": "compound",
        "formula": "([58674] OR [58675] OR [58676] OR [58677] OR [58678] OR [58679] OR [58680] OR [69636]) AND (([58683] AND [58688]) OR [58674]) AND ([58683] OR [58675]) AND ([58687] OR [58676]) AND ([58685] OR [58677]) AND ([58684] OR [58678]) AND (([58686] AND [58689]) OR [58679])",
        "atomic": {
            58685: ("ID-MASTER-EMPLOYEE", False),
            58680: ("ID-LAKETOLAKE-ID-TB-SALES-UDC", True),
            58687: ("ID-SALESFORCE-CUSTOMER-ALIGNMENT", False),
            58678: ("ID-HUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN", True),
            58676: ("ID-HUB-REF-CUST-TERR-ALIGNMENT-IDN", True),
            58686: ("ID-PRODUCT-UDC", False),
            58683: ("ID-CUSTOMER-UDC", False),
            58689: ("ID-TB-MASTER-PRODUCT", False),
            58674: ("ID-HUB-CUST-XREF-IDN", True),
            58688: ("ID-TB-HOSPITAL-OUTLET-MAP", False),
            58675: ("ID-HUB-REF-CUST-IDN", True),
            58679: ("ID-HUB-REF-PROD-IDN", True),
            58684: ("ID-EMPLOYEE-TEAM", False),
            69636: ("ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG", True),
            58677: ("ID-HUB-REF-EMP-IDN", True)
        }
    },
    "008-ID-LAKETOHUB-TXN-TARGET-SALES-IDN": {
        "type": "compound",
        "formula": "([58695] OR [58694] OR [58693] OR [58690] OR [58691] OR [58692] OR [63545]) AND (([58701] AND [58704] AND [58697]) OR [58695]) AND ([58699] OR [58694]) AND ([58700] OR [58693]) AND (([58697] AND [58698] AND [58703]) OR [58690]) AND (([58697] AND [58698]) OR [58691]) AND ([58702] OR [58692])",
        "atomic": {
            58704: ("ID-TB-MASTER-PRODUCT", False),
            58690: ("ID-HUB-CUST-XREF-IDN", True),
            58697: ("ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG", False),
            58695: ("ID-HUB-REF-PROD-IDN", True),
            63545: ("ID-HUB-TXN-TARGET-SALES-IDN", False),
            58691: ("ID-HUB-REF-CUST-IDN", True),
            58694: ("ID-HUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN", True),
            58693: ("ID-HUB-REF-EMP-IDN", True),
            58699: ("ID-EMPLOYEE-TEAM", False),
            58692: ("ID-HUB-REF-CUST-TERR-ALIGNMENT-IDN", True),
            58700: ("ID-MASTER-EMPLOYEE", False),
            58701: ("ID-PRODUCT-UDC", False),
            58703: ("ID-TB-HOSPITAL-OUTLET-MAP", False),
            58698: ("ID-CUSTOMER-UDC", False),
            58702: ("ID-SALESFORCE-CUSTOMER-ALIGNMENT", False)
        }
    }
}


def _variable_matches(var_name, expected_value):
    actual = Variable.get(var_name, default_var="False")
    result = (str(actual) == str(expected_value))
    logging.info(f"{LOG_PFX}[dependency check] {var_name} == {expected_value}? actual={actual!r} -> {result}")
    return result

def check_variable_dependency(job_task_id):
    """Returns a PythonSensor-compatible callable that checks job_task_id's dependency
    (simple or compound) against VARIABLE_DEPENDENCIES, evaluating live Variable values."""
    def _check(**context):
        dep = VARIABLE_DEPENDENCIES[job_task_id]

        if dep["type"] == "simple":
            return _variable_matches(dep["variable_name"], dep["expected_value"])

        expr = dep["formula"]
        for dep_tes_id, (var_name, expected) in dep["atomic"].items():
            expr = expr.replace(f"[{dep_tes_id}]", str(_variable_matches(var_name, expected)))
        expr_py = expr.replace(" OR ", " or ").replace(" AND ", " and ")
        result = eval(expr_py, {"__builtins__": {}}, {})
        logging.info(f"{LOG_PFX}[dependency check] {job_task_id}: formula resolved to {expr} -> {result}")
        return result

    return _check

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
    'owner': 'airflow',
    'email' : '',
    "priority_weight": 1, # TO DO - to check this later
    "weight_rule": "absolute"
}

####========= Timezone ===================================================================


dag = DAG(
    dag_id = "DA-COM-GEM-ID",
    description = "-- Deployed from DEFRATAGAD001 by onetakeda\nxi2827 on 09/28/2022 12:18:59 PM -- Deployed from DEFRATAGAD001 by onetakeda\nxi2827 on 09/28/2022 12:18:59 PM -- Deployed from DEFRATAGAD001 by onetakeda\nxi2827 on 09/28/2022 12:18:59 PM -- Deployed from DEFRATAGAPT001 by onetakeda\sagpatil on 06/27/2023 11:07:34 AM -- Updated from DEFRATAGAPT001 by qpf2671 on 07/29/2024 10:38:52 AM -- Updated from DEFRATAGAPT001 by cef9990 on 08/12/2024 11:29:22 AM",
    default_args = default_args,
    schedule_interval = "*/30 * * * *",
    concurrency = 16,
    start_date = datetime(2026,9,6),
    catchup = False,
    max_active_runs = 1
)

def test_function():
    logging.info(f"{LOG_PFX}Tidal To Airflow Migration...")
    return True

####================== Jobs: Task Definitions ===================================

with TaskGroup(
    group_id="720539_DA-COM-GEM-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
    tooltip="57501_DA-COM-GEM-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
    prefix_group_id=False,
    dag=dag
) as A720539:
    A720540 = DummyOperator(
        task_id="720540_002-GEM-DATABRICKS-CALLBACK-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
        task_display_name="57504_002-GEM-DATABRICKS-CALLBACK-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
        dag=dag
    )
    A722138 = DummyOperator(
        task_id="722138_001-GEM-DATABRICKS-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
        task_display_name="57502_001-GEM-DATABRICKS-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
        dag=dag
    )
    A720541 = DummyOperator(
        task_id="720541_010-SHOW-DATABRICKS-JOB-STATUS-ID-FILE-VALIDATION-EMAIL",
        task_display_name="57506_010-SHOW-DATABRICKS-JOB-STATUS-ID-FILE-VALIDATION-EMAIL",
        dag=dag
    )
    A722139 = DummyOperator(
        task_id="722139_020-ASSOCIATED-CALLBACK-JOB-MONITOR-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
        task_display_name="57505_020-ASSOCIATED-CALLBACK-JOB-MONITOR-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
        dag=dag
    )
    _task_id="738046_010-DATABRICKS-RUN-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL"
    A738046 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="57503_010-DATABRICKS-RUN-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )

with TaskGroup(
    group_id="724741_DA-COM-GEM-ID-HUB-TO-MART-BDM-JOBS",
    tooltip="54175_DA-COM-GEM-ID-HUB-TO-MART-BDM-JOBS",
    prefix_group_id=False,
    dag=dag
) as A724741:
    _task_id="727062_003-ID-HUBTOMART-CUST-XREF-IDN"
    A727062 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54180_003-ID-HUBTOMART-CUST-XREF-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="729935_004-ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN"
    A729935 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54181_004-ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="730674_002-ID-HUBTOMART-REF-PROD-IDN"
    A730674 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54177_002-ID-HUBTOMART-REF-PROD-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="732269_007-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN"
    A732269 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54184_007-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="735229_006-ID-HUBTOMART-TXN-TARGET-SALES-IDN"
    A735229 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54183_006-ID-HUBTOMART-TXN-TARGET-SALES-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="735446_008-ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN"
    A735446 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54185_008-ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="735869_009-ID-HUBTOMART-REF-PROD-PRICE-IDN"
    A735869 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54186_009-ID-HUBTOMART-REF-PROD-PRICE-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="736543_001-ID-HUBTOMART-REF-CUST-IDN"
    A736543 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54176_001-ID-HUBTOMART-REF-CUST-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="012-ID-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS-MAIL-SLEEP-JOB-DEPCHECK"
    A738608_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("012-ID-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS-MAIL-SLEEP-JOB"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="738717_010-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN"
    A738717 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54187_010-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="011-ID-TXN-ACTUAL-SALES-IDN-APL-SUCCESS-MAIL-SLEEP-JOB-DEPCHECK"
    A747788_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("011-ID-TXN-ACTUAL-SALES-IDN-APL-SUCCESS-MAIL-SLEEP-JOB"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="748694_005-ID-HUBTOMART-REF-EMP-IDN"
    A748694 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54182_005-ID-HUBTOMART-REF-EMP-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    A738608 = DummyOperator(
        task_id="738608_012-ID-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS-MAIL-SLEEP-JOB",
        task_display_name="58293_012-ID-TXN-ACTUAL-SALES-IDN-DICO-SUCCESS-MAIL-SLEEP-JOB",
        on_success_callback = on_success_callback_python_wrapper,
        on_failure_callback = on_failure_callback_python_wrapper,
        dag=dag
    )

    A747788 = DummyOperator(
        task_id="747788_011-ID-TXN-ACTUAL-SALES-IDN-APL-SUCCESS-MAIL-SLEEP-JOB",
        task_display_name="58292_011-ID-TXN-ACTUAL-SALES-IDN-APL-SUCCESS-MAIL-SLEEP-JOB",
        on_success_callback = on_success_callback_python_wrapper,
        on_failure_callback = on_failure_callback_python_wrapper,
        dag=dag
    )

with TaskGroup(
    group_id="710099_DA-COM-GEM-ID-LAKE-TO-HUB-BDM-JOBS",
    tooltip="54178_DA-COM-GEM-ID-LAKE-TO-HUB-BDM-JOBS",
    prefix_group_id=False,
    dag=dag
) as A710099:
    _task_id="009-ID-LAKETOHUB-REF-PROD-PRICE-IDN-DEPCHECK"
    A710100_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("009-ID-LAKETOHUB-REF-PROD-PRICE-IDN"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="001-ID-LAKETOHUB-CUST-XREF-IDN-DEPCHECK"
    A711942_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("001-ID-LAKETOHUB-CUST-XREF-IDN"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="005-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN-DEPCHECK"
    A717929_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("005-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="003-ID-LAKETOHUB-REF-EMP-IDN-DEPCHECK"
    A738127_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("003-ID-LAKETOHUB-REF-EMP-IDN"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="006-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN-DEPCHECK"
    A738868_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("006-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="002-ID-LAKETOHUB-REF-PROD-IDN-DEPCHECK"
    A742267_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("002-ID-LAKETOHUB-REF-PROD-IDN"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="743417_010-ID-LAKETOHUB-TXN-VACCINE-INVENTORY-IDN"
    A743417 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54196_010-ID-LAKETOHUB-TXN-VACCINE-INVENTORY-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="004-ID-LAKETOHUB-REF-CUST-IDN-DEPCHECK"
    A744144_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("004-ID-LAKETOHUB-REF-CUST-IDN"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="008-ID-LAKETOHUB-TXN-TARGET-SALES-IDN-DEPCHECK"
    A747491_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("008-ID-LAKETOHUB-TXN-TARGET-SALES-IDN"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="007-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN-DEPCHECK"
    A748793_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("007-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="710100_009-ID-LAKETOHUB-REF-PROD-PRICE-IDN"
    A710100 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54195_009-ID-LAKETOHUB-REF-PROD-PRICE-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="711942_001-ID-LAKETOHUB-CUST-XREF-IDN"
    A711942 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54179_001-ID-LAKETOHUB-CUST-XREF-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="717929_005-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN"
    A717929 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54191_005-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="738127_003-ID-LAKETOHUB-REF-EMP-IDN"
    A738127 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54189_003-ID-LAKETOHUB-REF-EMP-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="747491_008-ID-LAKETOHUB-TXN-TARGET-SALES-IDN"
    A747491 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54194_008-ID-LAKETOHUB-TXN-TARGET-SALES-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="742267_002-ID-LAKETOHUB-REF-PROD-IDN"
    A742267 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54188_002-ID-LAKETOHUB-REF-PROD-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="738868_006-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN"
    A738868 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54192_006-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="744144_004-ID-LAKETOHUB-REF-CUST-IDN"
    A744144 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54190_004-ID-LAKETOHUB-REF-CUST-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="748793_007-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN"
    A748793 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54193_007-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )

with TaskGroup(
    group_id="728327_DA-COM-GEM-ID-EXCEPTION",
    tooltip="58286_DA-COM-GEM-ID-EXCEPTION",
    prefix_group_id=False,
    dag=dag
) as A728327:
    A733452 = DummyOperator(
        task_id="733452_001-GEM-DATABRICKS-NOTEBOOK-ID-SALESFORCE-CUSTOMER-ALIGNMENT-FOR-SEND-EMAIL",
        task_display_name="58289_001-GEM-DATABRICKS-NOTEBOOK-ID-SALESFORCE-CUSTOMER-ALIGNMENT-FOR-SEND-EMAIL",
        dag=dag
    )
    A728328 = DummyOperator(
        task_id="728328_001-GEM-DATABRICKS-NOTEBOOK-CALLBACK-ID-SALESFORCE-CUSTOMER-ALIGNMENT-FOR-SEND-EMAIL",
        task_display_name="58287_001-GEM-DATABRICKS-NOTEBOOK-CALLBACK-ID-SALESFORCE-CUSTOMER-ALIGNMENT-FOR-SEND-EMAIL",
        dag=dag
    )
    A738762 = DummyOperator(
        task_id="738762_020-ASSOCIATED-CALLBACK-JOB-MONITOR-SALESFORCE-CUSTOMER-ALIGNMENT-EXCEPTION-EMAIL",
        task_display_name="58291_020-ASSOCIATED-CALLBACK-JOB-MONITOR-SALESFORCE-CUSTOMER-ALIGNMENT-EXCEPTION-EMAIL",
        dag=dag
    )
    _task_id="747667_010-DATABRICKS-RUN-NOTEBOOK-SALESFORCE-CUSTOMER-ALIGNMENT-EXCEPTION-EMAIL"
    A747667 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="58290_010-DATABRICKS-RUN-NOTEBOOK-SALESFORCE-CUSTOMER-ALIGNMENT-EXCEPTION-EMAIL",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    A734713 = DummyOperator(
        task_id="734713_010-SHOW-DATABRICKS-JOB-STATUS-SALESFORCE-CUSTOMER-ALIGNMENT-EXCEPTION-EMAIL",
        task_display_name="58288_010-SHOW-DATABRICKS-JOB-STATUS-SALESFORCE-CUSTOMER-ALIGNMENT-EXCEPTION-EMAIL",
        dag=dag
    )

with TaskGroup(
    group_id="717008_DA-COM-GEM-ID-DMP-EXCEPTION-SYNC",
    tooltip="61105_DA-COM-GEM-ID-DMP-EXCEPTION-SYNC",
    prefix_group_id=False,
    dag=dag
) as A717008:
    A717009 = DummyOperator(
        task_id="717009_002-GEM-DATABRICKS-CALLBACK-DMP-EXCEPTION-SYNC-ID",
        task_display_name="61108_002-GEM-DATABRICKS-CALLBACK-DMP-EXCEPTION-SYNC-ID",
        dag=dag
    )
    A721256 = DummyOperator(
        task_id="721256_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-ID",
        task_display_name="61106_001-GEM-DATABRICKS-NOTEBOOK-DMP-EXCEPTION-SYNC-ID",
        dag=dag
    )
    A742574 = DummyOperator(
        task_id="742574_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DMP-EXCEPTION-SYNC",
        task_display_name="61109_020-ASSOCIATED-CALLBACK-JOB-MONITOR-DMP-EXCEPTION-SYNC",
        dag=dag
    )
    A740418 = DummyOperator(
        task_id="740418_010-SHOW-DATABRICKS-JOB-STATUS-DMP-EXCEPTION-SYNC",
        task_display_name="61110_010-SHOW-DATABRICKS-JOB-STATUS-DMP-EXCEPTION-SYNC",
        dag=dag
    )
    _task_id="721257_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC"
    A721257 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="61107_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )

ASTART = DummyOperator(task_id="START", dag=dag)

A709247 = DummyOperator(
    task_id="709247_DA-COM-GEM-ID",
    task_display_name="54117_DA-COM-GEM-ID",
    dag=dag
)

with TaskGroup(
    group_id="724358_DA-COM-GEM-ID-LAKE-TO-LAKE-BDM-JOBS",
    tooltip="54197_DA-COM-GEM-ID-LAKE-TO-LAKE-BDM-JOBS",
    prefix_group_id=False,
    dag=dag
) as A724358:
    _task_id="002-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG-DEPCHECK"
    A724359_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("002-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="003-ID-LAKETOLAKE-TARGET-SALES-STG-DEPCHECK"
    A728541_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("003-ID-LAKETOLAKE-TARGET-SALES-STG"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="001-ID-LAKETOLAKE-ID-TB-SALES-UDC-DEPCHECK"
    A741885_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("001-ID-LAKETOLAKE-ID-TB-SALES-UDC"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="730873_004-ID-LAKETOLAKE-VACCINE-DAILY-STOCK-STG"
    A730873 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54201_004-ID-LAKETOLAKE-VACCINE-DAILY-STOCK-STG",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="741885_001-ID-LAKETOLAKE-ID-TB-SALES-UDC"
    A741885 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54198_001-ID-LAKETOLAKE-ID-TB-SALES-UDC",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="728541_003-ID-LAKETOLAKE-TARGET-SALES-STG"
    A728541 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54200_003-ID-LAKETOLAKE-TARGET-SALES-STG",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="724359_002-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG"
    A724359 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name="54199_002-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={"job_parameters": params[af_env][_task_id]["job_parameters"]},
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )

with TaskGroup(
    group_id="709248_DA-COM-GEM-ID-COMMON-LAMBDA",
    tooltip="54118_DA-COM-GEM-ID-COMMON-LAMBDA",
    prefix_group_id=False,
    dag=dag
) as A709248:
    _task_id="ID-TB-MASTER-PRODUCT-DEPCHECK"
    A709250_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-TB-MASTER-PRODUCT-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-TARGET-SALES-TBE-DEPCHECK"
    A737431_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-TARGET-SALES-TBE-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-TB-HOSPITAL-OUTLET-MAP-DEPCHECK"
    A710447_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-TB-HOSPITAL-OUTLET-MAP-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-VACCINE-SALES-DEPCHECK"
    A748147_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-VACCINE-SALES-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-TARGET-SALES-VAC-DEPCHECK"
    A737571_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-TARGET-SALES-VAC-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-PRODUCT-UDC-DEPCHECK"
    A742458_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-PRODUCT-UDC-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-EMPLOYEE-TEAM-DEPCHECK"
    A747953_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-EMPLOYEE-TEAM-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-MASTER-EMPLOYEE-DEPCHECK"
    A747831_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-MASTER-EMPLOYEE-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-TARGET-SALES-OTC-DEPCHECK"
    A713117_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-TARGET-SALES-OTC-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-SALES-UDC-DEPCHECK"
    A719539_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-SALES-UDC-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-TB-PRODUCT-PRICE-DEPCHECK"
    A720348_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-TB-PRODUCT-PRICE-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-CUSTOMER-UDC-DEPCHECK"
    A737634_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-CUSTOMER-UDC-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-SALESFORCE-CUSTOMER-ALIGNMENT-DEPCHECK"
    A715145_DEP_CHECK = PythonSensor (
        task_id=_task_id,
        task_display_name=_task_id,
        python_callable=check_variable_dependency("ID-SALESFORCE-CUSTOMER-ALIGNMENT-FILEWATCH"),
        poke_interval=PYTHON_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=PYTHON_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        dag=dag
    )
    _task_id="ID-VACCINE-DAILY-STOCK-FILEWATCH"
    A739616 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-TARGET-SALES-TBE-FILEWATCH"
    A737431 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,  
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-PRODUCT-UDC-FILEWATCH"
    A742458 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-TB-PRODUCT-PRICE-FILEWATCH"
    A720348 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-SALESFORCE-CUSTOMER-ALIGNMENT-FILEWATCH"
    A715145 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-TARGET-SALES-OTC-FILEWATCH"
    A713117 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-EMPLOYEE-TEAM-FILEWATCH"
    A747953 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID ,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-CUSTOMER-UDC-FILEWATCH"
    A737634 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-VACCINE-SALES-FILEWATCH"
    A748147 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-TARGET-SALES-VAC-FILEWATCH"
    A737571 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-SALES-UDC-FILEWATCH"
    A719539 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-TB-MASTER-PRODUCT-FILEWATCH"
    A709250 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-CUSTOMER-UDC-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-CUSTOMER-UDC-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-CUSTOMER-UDC-FILEWATCH', key='resolved_key') }}"
    A738058 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-MASTER-EMPLOYEE-FILEWATCH"
    A747831 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS,
        aws_conn_id = AWS_CONN_ID ,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-TB-HOSPITAL-OUTLET-MAP-FILEWATCH"
    A710447 = S3KeySensor (
        task_id=_task_id,
        poke_interval = S3_SENSOR_POKE_INTERVAL_SECONDS, 
        aws_conn_id = AWS_CONN_ID,
        bucket_name = params[af_env][_task_id]["bucket_name"],
        bucket_key = params[af_env][_task_id]["bucket_key"],
        wildcard_match = True,
        task_display_name=_task_id,
        timeout = S3_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
        on_failure_callback=on_failure_callback_S3_wrapper,
        on_success_callback=on_success_callback_S3_wrapper,
        dag=dag
    )
    _task_id="ID-TARGET-SALES-TBE-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-TARGET-SALES-TBE-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-TARGET-SALES-TBE-FILEWATCH', key='resolved_key') }}"
    A743058 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-TB-MASTER-PRODUCT-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-TB-MASTER-PRODUCT-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-TB-MASTER-PRODUCT-FILEWATCH', key='resolved_key') }}"
    A718848 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-SALESFORCE-CUSTOMER-ALIGNMENT-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-SALESFORCE-CUSTOMER-ALIGNMENT-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-SALESFORCE-CUSTOMER-ALIGNMENT-FILEWATCH', key='resolved_key') }}"
    A731370 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-SALES-UDC-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-SALES-UDC-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-SALES-UDC-FILEWATCH', key='resolved_key') }}"
    A746586 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-TB-HOSPITAL-OUTLET-MAP-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-TB-HOSPITAL-OUTLET-MAP-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-TB-HOSPITAL-OUTLET-MAP-FILEWATCH', key='resolved_key') }}"
    A739578 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-TARGET-SALES-OTC-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-TARGET-SALES-OTC-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-TARGET-SALES-OTC-FILEWATCH', key='resolved_key') }}"
    A729800 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-VACCINE-DAILY-STOCK-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-VACCINE-DAILY-STOCK-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-VACCINE-DAILY-STOCK-FILEWATCH', key='resolved_key') }}"
    A714664 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-TB-PRODUCT-PRICE-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-TB-PRODUCT-PRICE-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-TB-PRODUCT-PRICE-FILEWATCH', key='resolved_key') }}"
    A726545 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-VACCINE-SALES-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-VACCINE-SALES-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-VACCINE-SALES-FILEWATCH', key='resolved_key') }}"
    A732893 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-TARGET-SALES-VAC-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-TARGET-SALES-VAC-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-TARGET-SALES-VAC-FILEWATCH', key='resolved_key') }}"
    A742973 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-MASTER-EMPLOYEE-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-MASTER-EMPLOYEE-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-MASTER-EMPLOYEE-FILEWATCH', key='resolved_key') }}"
    A740367 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-PRODUCT-UDC-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-PRODUCT-UDC-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-PRODUCT-UDC-FILEWATCH', key='resolved_key') }}"
    A722687 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )
    _task_id="ID-EMPLOYEE-TEAM-LAKELOAD"
    _job_parameters = dict(params[af_env][_task_id]["job_parameters"])
    if "*" in params[af_env]["ID-EMPLOYEE-TEAM-FILEWATCH"]["bucket_key"]:
        _job_parameters["bucket_key"] = "{{ ti.xcom_pull(task_ids='ID-EMPLOYEE-TEAM-FILEWATCH', key='resolved_key') }}"
    A734374 = DatabricksRunNowOperator(
        task_id=_task_id,
        task_display_name=_task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=params[af_env][_task_id]["job_id"],
        json={ "job_parameters": _job_parameters },
        on_failure_callback=on_failure_callback_dbx_lf_job_wrapper,
        on_success_callback=on_success_callback_dbx_lf_job_wrapper,
        dag=dag,
    )

####================== Dependency Management: Task Dependencies =================

A742458_DEP_CHECK >> [A742458]
A742458 >> [A722687]
A710100_DEP_CHECK >> [A710100]
A720348_DEP_CHECK >> [A720348]
A720348 >> [A726545]
A711942_DEP_CHECK >> [A711942]
A711942 >> [A727062]
A733452 >> [A747667]
A724359_DEP_CHECK >> [A724359]
A739616 >> [A714664]
A740418 >> [A742574]
A747953_DEP_CHECK >> [A747953]
A747953 >> [A734374]
A748793_DEP_CHECK >> [A748793]
A737571_DEP_CHECK >> [A737571]
A737571 >> [A742973]
A720541 >> [A722139]
A747831_DEP_CHECK >> [A747831]
A747831 >> [A740367]
A737634_DEP_CHECK >> [A737634]
A737634 >> [A738058]
A747667 >> [A738762]
A734713 >> [A738762]
A747788_DEP_CHECK >> [A747788]
A717929_DEP_CHECK >> [A717929]
A738608_DEP_CHECK >> [A738608]
A738127_DEP_CHECK >> [A738127]
A717009 >> [A740418]
A748147_DEP_CHECK >> [A748147]
A748147 >> [A732893]
A710447_DEP_CHECK >> [A710447]
A710447 >> [A739578]
A747491_DEP_CHECK >> [A747491]
A713117_DEP_CHECK >> [A713117]
A713117 >> [A729800]
A722138 >> [A738046]
A738046 >> [A722139]
A721256 >> [A721257]
A719539_DEP_CHECK >> [A719539]
A719539 >> [A746586]
A728328 >> [A734713]
ASTART >> [A709247]
A715145_DEP_CHECK >> [A715145]
A715145 >> [A731370]
A742267_DEP_CHECK >> [A742267]
A728541_DEP_CHECK >> [A728541]
A720540 >> [A720541]
A709250_DEP_CHECK >> [A709250]
A709250 >> [A718848]
A721257 >> [A742574]
A741885_DEP_CHECK >> [A741885]
A738868_DEP_CHECK >> [A738868]
A744144_DEP_CHECK >> [A744144]
A737431_DEP_CHECK >> [A737431]
A737431 >> [A743058]

####================== JOB-EVENT edges (action_type=4) =================

A722138 >> [A741885]
A741885 >> [A711942, A742267, A744144, A748793, A738608]
A742267 >> [A730674]
A724359 >> [A748793, A747788]
A734374 >> [A738868]
A738127 >> [A748694]
A728541 >> [A747491]
A740367 >> [A738127]
A744144 >> [A736543]
A730873 >> [A743417]
A729800 >> [A728541]
A717929 >> [A735446]
A746586 >> [A722138]
A738868 >> [A729935]
A726545 >> [A710100]
A732269 >> [A733452]
A748793 >> [A721256, A732269]
A747491 >> [A721256, A735229]
A710100 >> [A735869]
A731370 >> [A717929]
A743417 >> [A738717]
A743058 >> [A728541]
A732893 >> [A724359, A744144]
A742973 >> [A728541]
A714664 >> [A730873]

####================== Group fan-out (kept LAST so TaskGroup roots are computed on the complete graph) =================

A709247 >> [A709248, A710099, A717008, A720539, A724358, A724741, A728327]

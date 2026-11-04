DATABRICKS_CONN_ID = "Databricks_BDMToDBX_Default"
AWS_CONN_ID = "AWS_Default"
GLOBALSCAPE_CONN_ID = "GLOBALSCAPE_CONN_BDMToDBX_Default"
SSH_CONN_ID = "SSH_Default"

EMAIL_RECIPIENTS = {
    "DEV": "DL.IBU.Data.Architecture.Products.POD@takeda.com, DL.GLBL.BDM.DBX.TEAM@takeda.com",
    "TEST": "DL.IBU.Data.Architecture.Products.POD@takeda.com, DL.GLBL.BDM.DBX.TEAM@takeda.com",
    "PROD": "DL.EDB.GEM.ID.COMM@takeda.com, DL.EDB.COMM.UAMS@takeda.com",
}

S3_SENSOR_POKE_INTERVAL_SECONDS = 1 * 60
S3_SENSOR_TIMEOUT_SECONDS = 25 * 60

PYTHON_SENSOR_POKE_INTERVAL_SECONDS = 5 * 60
PYTHON_SENSOR_TIMEOUT_SECONDS = 25 * 60

SNOW_ATTRIBUTES_DEFAULTS = {
    "configuration_item": "Commercial Common Data Pipeline [EDB] - Production-AWS-Airflow-EU",
    "severity": "2",
    "ci_apms_id": "APMS-93057",
    "ci_bsn_id": "BSN0020621",
}

PARAMS_DICT = {
    "DEV": {
        # Lambdas
        'ID-TARGET-SALES-TBE-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_TBE_pv_*.xlsx',
        },
        'ID-TARGET-SALES-TBE-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_TBE_pv_*.xlsx',
            },
        },
        'ID-TARGET-SALES-VAC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_Vaccine_*.xlsx',
        },
        'ID-TARGET-SALES-VAC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_Vaccine_*.xlsx',
            },
        },
        'ID-VACCINE-DAILY-STOCK-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/DailyStock_Takeda_*.CSV',
        },
        'ID-VACCINE-DAILY-STOCK-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/DailyStock_Takeda_*.CSV',
            },
        },
        'ID-VACCINE-SALES-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/DailySales_Takeda_*.CSV',
        },
        'ID-VACCINE-SALES-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/DailySales_Takeda_*.CSV',
            },
        },
        'ID-CUSTOMER-UDC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/Customer_Udc.csv',
        },
        'ID-CUSTOMER-UDC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/Customer_Udc.csv',
            },
        },
        'ID-EMPLOYEE-TEAM-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/employee_team.xlsx',
        },
        'ID-EMPLOYEE-TEAM-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/employee_team.xlsx',
            },
        },
        'ID-TB-HOSPITAL-OUTLET-MAP-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/hospital_outlet_map.xlsx',
        },
        'ID-TB-HOSPITAL-OUTLET-MAP-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/hospital_outlet_map.xlsx',
            },
        },
        'ID-MASTER-EMPLOYEE-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/master_employee.xlsx',
        },
        'ID-MASTER-EMPLOYEE-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/master_employee.xlsx',
            },
        },
        'ID-TB-MASTER-PRODUCT-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/master_product.xlsx',
        },
        'ID-TB-MASTER-PRODUCT-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/master_product.xlsx',
            },
        },
        'ID-TB-PRODUCT-PRICE-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/product_price.xlsx',
        },
        'ID-TB-PRODUCT-PRICE-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/product_price.xlsx',
            },
        },
        'ID-PRODUCT-UDC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/Product_Udc.csv',
        },
        'ID-PRODUCT-UDC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/Product_Udc.csv',
            },
        },
        'ID-SALES-UDC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/*Sales_Udc.csv',
        },
        'ID-SALES-UDC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/*Sales_Udc.csv',
            },
        },
        'ID-SALESFORCE-CUSTOMER-ALIGNMENT-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/salesforce_customer_alignment.xlsx',
        },
        'ID-SALESFORCE-CUSTOMER-ALIGNMENT-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/salesforce_customer_alignment.xlsx',
            },
        },
        'ID-TARGET-SALES-OTC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_OTC_pv_*.xlsx',
        },
        'ID-TARGET-SALES-OTC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_OTC_pv_*.xlsx',
            },
        },
        # Lake to Lake
        '730873_004-ID-LAKETOLAKE-VACCINE-DAILY-STOCK-STG': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '777704363212029',
            'job_parameters': {},
        },
        '724359_002-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '504736764460847',
            'job_parameters': {},
        },
        '728541_003-ID-LAKETOLAKE-TARGET-SALES-STG': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '954580359345422',
            'job_parameters': {},
        },
        '741885_001-ID-LAKETOLAKE-ID-TB-SALES-UDC': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1048393005242676',
            'job_parameters': {},
        },
        # Lake to Hub
        '710100_009-ID-LAKETOHUB-REF-PROD-PRICE-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '475130474171725',
            'job_parameters': {},
        },
        '711942_001-ID-LAKETOHUB-CUST-XREF-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '582151469524393',
            'job_parameters': {},
        },
        '743417_010-ID-LAKETOHUB-TXN-VACCINE-INVENTORY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1035129836947731',
            'job_parameters': {},
        },
        '748793_007-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '165346856931811',
            'job_parameters': {},
        },
        '717929_005-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '346434941222080',
            'job_parameters': {},
        },
        '738127_003-ID-LAKETOHUB-REF-EMP-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '20282108909552',
            'job_parameters': {},
        },
        '747491_008-ID-LAKETOHUB-TXN-TARGET-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '200787997652300',
            'job_parameters': {},
        },
        '738868_006-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '800746436485380',
            'job_parameters': {},
        },
        '744144_004-ID-LAKETOHUB-REF-CUST-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '899332983486149',
            'job_parameters': {},
        },
        '742267_002-ID-LAKETOHUB-REF-PROD-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1042242351971023',
            'job_parameters': {},
        },
        # Hub to Mart
        '735869_009-ID-HUBTOMART-REF-PROD-PRICE-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1037810259751756',
            'job_parameters': {},
        },
        '732269_007-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '761573738438855',
            'job_parameters': {},
        },
        '729935_004-ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '294194859883365',
            'job_parameters': {},
        },
        '735446_008-ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '318564719411935',
            'job_parameters': {},
        },
        '735229_006-ID-HUBTOMART-TXN-TARGET-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '877200976249694',
            'job_parameters': {},
        },
        '736543_001-ID-HUBTOMART-REF-CUST-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1067451479379866',
            'job_parameters': {},
        },
        '738717_010-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1119949704114022',
            'job_parameters': {},
        },
        '748694_005-ID-HUBTOMART-REF-EMP-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '480179658184959',
            'job_parameters': {},
        },
        '730674_002-ID-HUBTOMART-REF-PROD-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1012112905829625',
            'job_parameters': {},
        },
        '727062_003-ID-HUBTOMART-CUST-XREF-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '960215564961080',
            'job_parameters': {},
        },
        # DBX run
        '747667_010-DATABRICKS-RUN-NOTEBOOK-SALESFORCE-CUSTOMER-ALIGNMENT-EXCEPTION-EMAIL': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '719779021179197',
            'job_parameters': {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "001-GEM-DATABRICKS-NOTEBOOK-CALLBACK-SALESFORCE-CUSTOMER-ALIGNMENT-FOR-SEND-EMAIL",
                "tidal_callback_jobid": "94318",
                "outbound_name": "ID_EXCEPTIONS",
                "notebook_name": "/COMM/COMM_IT_GEM/INDONESIA/id_salesforce_customer_alignment_send_email"
            },
        },
        '738046_010-DATABRICKS-RUN-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '719779021179197',
            'job_parameters': {
                "loadtype": "full",
                "timeperiod": "Adhoc",
                "tidal_callbackname": "002-GEM-DATABRICKS-CALLBACK-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
                "tidal_callback_jobid": "92299",
                "outbound_name": "SALES_UDC",
                "notebook_name": "/COMM/COMM_IT_GEM/INDONESIA/id_sales_udc_send_email"
            },
        },
        '721257_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '174413206305646',
            'job_parameters': {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "002-GEM-DATABRICKS-CALLBACK-DMP-EXCEPTION-SYNC-ID",
                "tidal_callback_jobid": "102457",
                "country_name": "INDONESIA",
                "notebook_name": "/COMM/COMM_IT_GEM/DM Portal/Notebook_DM_Portal_Exception_New"
            },
        },
    },
    "TEST": {
        # Lambdas
        'ID-TARGET-SALES-TBE-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_TBE_pv_*.xlsx',
        },
        'ID-TARGET-SALES-TBE-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_TBE_pv_*.xlsx',
            },
        },
        'ID-TARGET-SALES-VAC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_Vaccine_*.xlsx',
        },
        'ID-TARGET-SALES-VAC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_Vaccine_*.xlsx',
            },
        },
        'ID-VACCINE-DAILY-STOCK-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/DailyStock_Takeda_*.CSV',
        },
        'ID-VACCINE-DAILY-STOCK-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/DailyStock_Takeda_*.CSV',
            },
        },
        'ID-VACCINE-SALES-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/DailySales_Takeda_*.CSV',
        },
        'ID-VACCINE-SALES-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/DailySales_Takeda_*.CSV',
            },
        },
        'ID-CUSTOMER-UDC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/Customer_Udc.csv',
        },
        'ID-CUSTOMER-UDC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/Customer_Udc.csv',
            },
        },
        'ID-EMPLOYEE-TEAM-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/employee_team.xlsx',
        },
        'ID-EMPLOYEE-TEAM-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/employee_team.xlsx',
            },
        },
        'ID-TB-HOSPITAL-OUTLET-MAP-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/hospital_outlet_map.xlsx',
        },
        'ID-TB-HOSPITAL-OUTLET-MAP-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/hospital_outlet_map.xlsx',
            },
        },
        'ID-MASTER-EMPLOYEE-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/master_employee.xlsx',
        },
        'ID-MASTER-EMPLOYEE-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/master_employee.xlsx',
            },
        },
        'ID-TB-MASTER-PRODUCT-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/master_product.xlsx',
        },
        'ID-TB-MASTER-PRODUCT-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/master_product.xlsx',
            },
        },
        'ID-TB-PRODUCT-PRICE-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/product_price.xlsx',
        },
        'ID-TB-PRODUCT-PRICE-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/product_price.xlsx',
            },
        },
        'ID-PRODUCT-UDC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/Product_Udc.csv',
        },
        'ID-PRODUCT-UDC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/Product_Udc.csv',
            },
        },
        'ID-SALES-UDC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/*Sales_Udc.csv',
        },
        'ID-SALES-UDC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/*Sales_Udc.csv',
            },
        },
        'ID-SALESFORCE-CUSTOMER-ALIGNMENT-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/salesforce_customer_alignment.xlsx',
        },
        'ID-SALESFORCE-CUSTOMER-ALIGNMENT-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/salesforce_customer_alignment.xlsx',
            },
        },
        'ID-TARGET-SALES-OTC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_OTC_pv_*.xlsx',
        },
        'ID-TARGET-SALES-OTC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_OTC_pv_*.xlsx',
            },
        },
        # Lake to Lake
        '730873_004-ID-LAKETOLAKE-VACCINE-DAILY-STOCK-STG': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '31700550725003',
            'job_parameters': {},
        },
        '724359_002-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '722535912946634',
            'job_parameters': {},
        },
        '728541_003-ID-LAKETOLAKE-TARGET-SALES-STG': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '385953021670859',
            'job_parameters': {},
        },
        '741885_001-ID-LAKETOLAKE-ID-TB-SALES-UDC': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '750868912983451',
            'job_parameters': {},
        },
        # Lake to Hub
        '710100_009-ID-LAKETOHUB-REF-PROD-PRICE-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '113914526730313',
            'job_parameters': {},
        },
        '711942_001-ID-LAKETOHUB-CUST-XREF-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '64831652918642',
            'job_parameters': {},
        },
        '743417_010-ID-LAKETOHUB-TXN-VACCINE-INVENTORY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '160313207924432',
            'job_parameters': {},
        },
        '748793_007-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '115538512793397',
            'job_parameters': {},
        },
        '717929_005-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1068008005297116',
            'job_parameters': {},
        },
        '738127_003-ID-LAKETOHUB-REF-EMP-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '854933754044315',
            'job_parameters': {},
        },
        '747491_008-ID-LAKETOHUB-TXN-TARGET-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1078404489222866',
            'job_parameters': {},
        },
        '738868_006-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '613226193554621',
            'job_parameters': {},
        },
        '744144_004-ID-LAKETOHUB-REF-CUST-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '35976630851413',
            'job_parameters': {},
        },
        '742267_002-ID-LAKETOHUB-REF-PROD-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '986792497416116',
            'job_parameters': {},
        },
        # Hub to Mart
        '735869_009-ID-HUBTOMART-REF-PROD-PRICE-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '361260414327438',
            'job_parameters': {},
        },
        '732269_007-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '146541015595347',
            'job_parameters': {},
        },
        '729935_004-ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1116952730987066',
            'job_parameters': {},
        },
        '735446_008-ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '953269689848909',
            'job_parameters': {},
        },
        '735229_006-ID-HUBTOMART-TXN-TARGET-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1062437126979887',
            'job_parameters': {},
        },
        '736543_001-ID-HUBTOMART-REF-CUST-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '177849627190409',
            'job_parameters': {},
        },
        '738717_010-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '639509717592440',
            'job_parameters': {},
        },
        '748694_005-ID-HUBTOMART-REF-EMP-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1053424203611191',
            'job_parameters': {},
        },
        '730674_002-ID-HUBTOMART-REF-PROD-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '431178293606160',
            'job_parameters': {},
        },
        '727062_003-ID-HUBTOMART-CUST-XREF-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1110783010697344',
            'job_parameters': {},
        },
        # DBX run
        '747667_010-DATABRICKS-RUN-NOTEBOOK-SALESFORCE-CUSTOMER-ALIGNMENT-EXCEPTION-EMAIL': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '557081953715500',
            'job_parameters': {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "001-GEM-DATABRICKS-NOTEBOOK-CALLBACK-SALESFORCE-CUSTOMER-ALIGNMENT-FOR-SEND-EMAIL",
                "tidal_callback_jobid": "79786",
                "outbound_name": "ID_EXCEPTIONS",
                "notebook_name": "/COMM/COMM_IT_GEM/INDONESIA/id_salesforce_customer_alignment_send_email"
            },
        },
        '738046_010-DATABRICKS-RUN-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '557081953715500',
            'job_parameters': {
                "loadtype": "full",
                "timeperiod": "Adhoc",
                "tidal_callbackname": "002-GEM-DATABRICKS-CALLBACK-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
                "tidal_callback_jobid": "78256",
                "outbound_name": "SALES_UDC",
                "notebook_name": "/COMM/COMM_IT_GEM/INDONESIA/id_sales_udc_send_email"
            },
        },
        '721257_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '269749650678524',
            'job_parameters': {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "002-GEM-DATABRICKS-CALLBACK-DMP-EXCEPTION-SYNC-ID",
                "tidal_callback_jobid": "61108",
                "country_name": "INDONESIA",
                "notebook_name": "/COMM/COMM_IT_GEM/DM Portal/Notebook_DM_Portal_Exception_New"
            },
        },
    },
    "PROD": {
        # Lambdas
        'ID-TARGET-SALES-TBE-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_TBE_pv_*.xlsx',
        },
        'ID-TARGET-SALES-TBE-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_TBE_pv_*.xlsx',
            },
        },
        'ID-TARGET-SALES-VAC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_Vaccine_*.xlsx',
        },
        'ID-TARGET-SALES-VAC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_Vaccine_*.xlsx',
            },
        },
        'ID-VACCINE-DAILY-STOCK-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/DailyStock_Takeda_*.CSV',
        },
        'ID-VACCINE-DAILY-STOCK-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/DailyStock_Takeda_*.CSV',
            },
        },
        'ID-VACCINE-SALES-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/DailySales_Takeda_*.CSV',
        },
        'ID-VACCINE-SALES-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/DailySales_Takeda_*.CSV',
            },
        },
        'ID-CUSTOMER-UDC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/Customer_Udc.csv',
        },
        'ID-CUSTOMER-UDC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/Customer_Udc.csv',
            },
        },
        'ID-EMPLOYEE-TEAM-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/employee_team.xlsx',
        },
        'ID-EMPLOYEE-TEAM-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/employee_team.xlsx',
            },
        },
        'ID-TB-HOSPITAL-OUTLET-MAP-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/hospital_outlet_map.xlsx',
        },
        'ID-TB-HOSPITAL-OUTLET-MAP-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/hospital_outlet_map.xlsx',
            },
        },
        'ID-MASTER-EMPLOYEE-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/master_employee.xlsx',
        },
        'ID-MASTER-EMPLOYEE-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/master_employee.xlsx',
            },
        },
        'ID-TB-MASTER-PRODUCT-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/master_product.xlsx',
        },
        'ID-TB-MASTER-PRODUCT-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/master_product.xlsx',
            },
        },
        'ID-TB-PRODUCT-PRICE-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/product_price.xlsx',
        },
        'ID-TB-PRODUCT-PRICE-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/product_price.xlsx',
            },
        },
        'ID-PRODUCT-UDC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/Product_Udc.csv',
        },
        'ID-PRODUCT-UDC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/Product_Udc.csv',
            },
        },
        'ID-SALES-UDC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/*Sales_Udc.csv',
        },
        'ID-SALES-UDC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/*Sales_Udc.csv',
            },
        },
        'ID-SALESFORCE-CUSTOMER-ALIGNMENT-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/salesforce_customer_alignment.xlsx',
        },
        'ID-SALESFORCE-CUSTOMER-ALIGNMENT-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/salesforce_customer_alignment.xlsx',
            },
        },
        'ID-TARGET-SALES-OTC-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_OTC_pv_*.xlsx',
        },
        'ID-TARGET-SALES-OTC-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                'bucket_name': 'tpc-aws-ted-prd-edpp-raw-com-eu-central-1',
                'bucket_key': 'de/gem/external/pre_contract/UploadTargetAMMR_OTC_pv_*.xlsx',
            },
        },
        
        # Lake to Lake
        '730873_004-ID-LAKETOLAKE-VACCINE-DAILY-STOCK-STG': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '724359_002-ID-LAKETOLAKE-ID-TB-VACCINE-SALES-STG': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '728541_003-ID-LAKETOLAKE-TARGET-SALES-STG': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '741885_001-ID-LAKETOLAKE-ID-TB-SALES-UDC': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        # Lake to Hub
        '710100_009-ID-LAKETOHUB-REF-PROD-PRICE-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '711942_001-ID-LAKETOHUB-CUST-XREF-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '743417_010-ID-LAKETOHUB-TXN-VACCINE-INVENTORY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '748793_007-ID-LAKETOHUB-TXN-ACTUAL-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '717929_005-ID-LAKETOHUB-REF-CUST-TERR-ALIGNMENT-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '738127_003-ID-LAKETOHUB-REF-EMP-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '747491_008-ID-LAKETOHUB-TXN-TARGET-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '738868_006-ID-LAKETOHUB-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '744144_004-ID-LAKETOHUB-REF-CUST-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '742267_002-ID-LAKETOHUB-REF-PROD-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        # Hub to Mart
        '735869_009-ID-HUBTOMART-REF-PROD-PRICE-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '732269_007-ID-HUBTOMART-TXN-ACTUAL-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '729935_004-ID-HUBTOMART-REF-EMP-TERR-ALIGNMENT-HIERARCHY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '735446_008-ID-HUBTOMART-REF-CUST-TERR-ALIGNMENT-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '735229_006-ID-HUBTOMART-TXN-TARGET-SALES-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '736543_001-ID-HUBTOMART-REF-CUST-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '738717_010-ID-HUBTOMART-TXN-VACCINE-INVENTORY-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '748694_005-ID-HUBTOMART-REF-EMP-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '730674_002-ID-HUBTOMART-REF-PROD-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        '727062_003-ID-HUBTOMART-CUST-XREF-IDN': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {},
        },
        # DBX run
        '747667_010-DATABRICKS-RUN-NOTEBOOK-SALESFORCE-CUSTOMER-ALIGNMENT-EXCEPTION-EMAIL': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "001-GEM-DATABRICKS-NOTEBOOK-CALLBACK-SALESFORCE-CUSTOMER-ALIGNMENT-FOR-SEND-EMAIL",
                "tidal_callback_jobid": "",
                "outbound_name": "ID_EXCEPTIONS",
                "notebook_name": ""
            },
        },
        '738046_010-DATABRICKS-RUN-NOTEBOOK-ID-SALES-UDC-FILE-VALIDATION-EMAIL': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                "loadtype": "full",
                "timeperiod": "Adhoc",
                "tidal_callbackname": "002-GEM-DATABRICKS-CALLBACK-ID-SALES-UDC-FILE-VALIDATION-EMAIL",
                "tidal_callback_jobid": "",
                "outbound_name": "SALES_UDC",
                "notebook_name": ""
            },
        },
        '721257_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '',
            'job_parameters': {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "002-GEM-DATABRICKS-CALLBACK-DMP-EXCEPTION-SYNC-ID",
                "tidal_callback_jobid": "",
                "country_name": "INDONESIA",
                "notebook_name": ""
            },
        },
    },
}

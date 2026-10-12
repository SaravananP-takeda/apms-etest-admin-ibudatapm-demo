DATABRICKS_CONN_ID = "Databricks_BDMToDBX_Default"

EMAIL_RECIPIENTS = {
    'DEV': 'anush.gowda@takeda.com,premkumar.duraisamy@takeda.com,udayan.awasthi@takeda.com,arjun.dalal@takeda.com,dheeraj.jaiman@takeda.com,DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com',
    'TEST': 'anush.gowda@takeda.com,premkumar.duraisamy@takeda.com,udayan.awasthi@takeda.com,arjun.dalal@takeda.com,dheeraj.jaiman@takeda.com,DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com',
    'PROD': 'DL.EDB.GEM.UMEA.COMM@takeda.com, DL.EDB.COMM.UAMS@takeda.com'
}

S3_SENSOR_POKE_INTERVAL_SECONDS = 15 * 60
S3_SENSOR_TIMEOUT_SECONDS = 23 * 60 * 60 + 50 * 60

PYTHON_SENSOR_POKE_INTERVAL_SECONDS = 5 * 60
PYTHON_SENSOR_TIMEOUT_SECONDS = 3 * 60 * 60

SNOW_ATTRIBUTES_DEFAULTS = {
    "configuration_item": "Commercial Common Data Pipeline [EDB] - Production-AWS-Airflow-EU",
    "severity":           "2",
    "ci_apms_id":         "APMS-92363",
    "ci_bsn_id":          "BSN0022119",
}

PARAMS_DICT = {

    "DEV": {
        #Existing databricks jobs
        "614349_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC": {
            "job_type": "DBX_LF_JOB",
            "job_id": "877928726445456",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "country_name": "UMEA",
                "notebook_name": "/COMM/COMM_IT_GEM/DM Portal/Notebook_DM_Portal_Exception_New",
            },
        },
        "614752_010-DATABRICKS-RUN-NOTEBOOK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "719779021179197",
            "job_parameters": {
                "loadtype":"full",
                "timeperiod":"Adhoc",
                "tidal_callbackname":"",
                "tidal_callback_jobid":"",
                "outbound_name":"DISTRIBUTOR_SALES",
                "notebook_name":"/COMM/COMM_IT_GEM/UMEA/umea_target_sales_send_email",
            },
        },
        "629958_010-DATABRICKS-RUN-NOTEBOOK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "719779021179197",
            "job_parameters": {
                "loadtype":"full",
                "timeperiod":"Adhoc",
                "tidal_callbackname":"",
                "tidal_callback_jobid":"",
                "outbound_name":"TARGET_SALES",
	            "notebook_name":"/COMM/COMM_IT_GEM/UMEA/umea_target_sales_send_email",
            },
        },

        # BDM converted jobs
        "604835_001-UMEA-HUBTOMART-REF-COUNTRY": {
            "job_type": "DBX_LF_JOB",
            "job_id": "660783284566277",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "601308_002-UMEA-HUBTOMART-REF-PRODUCT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "850713065770146",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "629600_003-UMEA-HUBTOMART-REF-CUSTOMER-MAP": {
            "job_type": "DBX_LF_JOB",
            "job_id": "239273109518362",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "618585_004-UMEA-HUBTOMART-REF-PRICE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "864368731810195",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "614727_005-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "346961036617276",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "617593_006-UMEA-HUBTOMART-TXN-TARGET-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "884933892759850",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "617175_001-UMEA-LAKETOHUB-REF-COUNTRY-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "177892059993716",
            "job_parameters": {
                'src_code': 'SRC_UMEA_COUNTRY_MAPPING',
                'env_catalog': 'dedev_gpd',
            },
        },
        "603722_002-UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "700203812128935",
            "job_parameters": {
                'src_code': 'SRC_UMEA_ACCOUNT_MAPPING',
                'env_catalog': 'dedev_gpd',
            },
        },
        "602672_003-UMEA-LAKETOHUB-REF-PROD-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "831872418632072",
            "job_parameters": {
                'src_code': 'SRC_UMEA_DISTRIBUTOR_PRODUCT',
                'env_catalog': 'dedev_gpd',
            },
        },
        "616931_004-UMEA-LAKETOHUB-REF-PRICE-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "912478168726373",
            "job_parameters": {
                'day_of_mth': '1',
                'scenario_myc': 'MYC',
                'scenario_pln': 'PLN',
                'src_code': 'SRC_UMEA_PRICE',
                'scenario_lbe': 'LBE',
                'env_catalog': 'dedev_gpd',
            },
        },
        "629725_001-UMEA-LAKETOLAKE-PRICE-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "24462214393842",
            "job_parameters": {
                'header_column_name': 'Country',
                'env_catalog': 'dedev_gpd',
            },
        },
        "607462_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1002171290526330",
            "job_parameters": {
                'src_code': 'SRC_UMEA_DISTRIBUTOR_SALES',
                'date': '01',
                'env_catalog': 'dedev_gpd',
            },
        },
        "614621_003-UMEA-LAKETOLAKE-DISTRIBUTOR-SALES-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1115417772312712",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "628884_002-UMEA-LAKETOLAKE-TARGET-SALES-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "333053681441420",
            "job_parameters": {
                'header_column_name_pri_acc': 'Standardized Primary Account',
                'header_column_name_cntry': 'Country',
                'pre_sql_del_query': 
                    '''DELETE FROM com_de_lake.umea_tb_target_sales_stg AS stg
                    WHERE EXISTS (
                    SELECT src.country,src.standardized_primary_account,src.standardized_secondary_account,src.takeda_sku 
                    FROM com_de_lake.umea_tb_target_sales_src AS src 
                    WHERE stg.country=ltrim(rtrim(src.country))
                    and stg.standardized_primary_account=ltrim(rtrim(src.standardized_primary_account))
                    and stg.standardized_secondary_account=ltrim(rtrim(src.standardized_secondary_account))
                    and stg.takeda_sku=ltrim(rtrim(src.takeda_sku)) )
                    and ((stg.target_sales_type='PLN' and stg.date in (
                    select distinct stack(12,pln_month1,pln_month2,pln_month3,pln_month4,pln_month5,pln_month6,pln_month7,pln_month8,pln_month9,pln_month10,pln_month11,pln_month12) as date from com_de_lake.umea_tb_target_sales_src where country='Country'
                    )) 
                    or
                    (stg.target_sales_type='MYC' and stg.date in (
                    select distinct stack(12,myc_month1,myc_month2,myc_month3,myc_month4,myc_month5,myc_month6,myc_month7,myc_month8,myc_month9,myc_month10,myc_month11,myc_month12) as date from com_de_lake.umea_tb_target_sales_src where country='Country'
                    ))
                    or
                    (stg.target_sales_type='LBE' and stg.date in (
                    select distinct stack(12,lbe_month1,lbe_month2,lbe_month3,lbe_month4,lbe_month5,lbe_month6,lbe_month7,lbe_month8,lbe_month9,lbe_month10,lbe_month11,lbe_month12) as date from com_de_lake.umea_tb_target_sales_src where country='Country'
                    )))''',
                'env_catalog': 'dedev_gpd',
            },
        },

        "609983_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1104073963748854",
            "job_parameters": {
                'scenario_pln': 'PLN',
                'scenario_myc': 'MYC',
                'src_code': 'SRC_UMEA_TARGET_SALES',
                'scenario_lbe': 'LBE',
                'env_catalog': 'dedev_gpd',
            },
        },

        # Lambda jobs
        "UMEA-TARGET-SALES-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_target_sales.xlsx",
        },
        "UMEA-TARGET-SALES-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_target_sales.xlsx",
            },
        },
        "UMEA-DISTRIBUTOR-SALES-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_distributor_sales.xlsx",
        },
        "UMEA-DISTRIBUTOR-SALES-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_distributor_sales.xlsx",
            },
        },
        "UMEA-ACCOUNT-LEAD-CONTRIBUTION-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_account_lead_contribution.xlsx",
        },
        "UMEA-ACCOUNT-LEAD-CONTRIBUTION-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_account_lead_contribution.xlsx",
            },
        },
        "UMEA-PRICE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_price.xlsx",
        },
        "UMEA-PRICE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_price.xlsx",
            },
        },
        "UMEA-ACCOUNT-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_account_mapping.xlsx",
        },
        "UMEA-ACCOUNT-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_account_mapping.xlsx",
            },
        },
        "UMEA-COUNTRY-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_country_mapping.xlsx",
        },
        "UMEA-COUNTRY-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_country_mapping.xlsx",
            },
        },
        "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_distributor_product_mapping.xlsx",
        },
        "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_distributor_product_mapping.xlsx",
            },
        },
    },

    "TEST": {

        #Existing databricks jobs
        "614349_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC": {
            "job_type": "DBX_LF_JOB",
            "job_id": "269749650678524",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "country_name": "UMEA",
                "notebook_name": "/COMM/COMM_IT_GEM/DM Portal/Notebook_DM_Portal_Exception_New",
            },
        },
        "614752_010-DATABRICKS-RUN-NOTEBOOK-UMEA-DISTRIBUTOR-SALES-FILE-VALIDATION-EMAIL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "917547190836131",
            "job_parameters": {
                "loadtype":"full",
                "timeperiod":"Adhoc",
                "tidal_callbackname":"",
                "tidal_callback_jobid":"",
                "outbound_name":"DISTRIBUTOR_SALES",
                "notebook_name":"/COMM/COMM_IT_GEM/UMEA/umea_target_sales_send_email",
            },
        },
        "629958_010-DATABRICKS-RUN-NOTEBOOK-UMEA-TARGET-SALES-FILE-VALIDATION-EMAIL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "917547190836131",
            "job_parameters": {
                "loadtype":"full",
                "timeperiod":"Adhoc",
                "tidal_callbackname":"",
                "tidal_callback_jobid":"",
                "outbound_name":"TARGET_SALES",
	            "notebook_name":"/COMM/COMM_IT_GEM/UMEA/umea_target_sales_send_email",
            },
        },

        # BDM converted jobs
        "628884_002-UMEA-LAKETOLAKE-TARGET-SALES-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "720516086611728",
            "job_parameters": {
                'header_column_name_pri_acc': 'Standardized Primary Account',
                'header_column_name_cntry': 'Country',
                'pre_sql_del_query': '''DELETE FROM com_de_lake.umea_tb_target_sales_stg AS stg  WHERE EXISTS (  SELECT src.country,src.standardized_primary_account,src.standardized_secondary_account,src.takeda_sku   FROM com_de_lake.umea_tb_target_sales_src AS src   WHERE stg.country=ltrim(rtrim(src.country))  and stg.standardized_primary_account=ltrim(rtrim(src.standardized_primary_account))  and stg.standardized_secondary_account=ltrim(rtrim(src.standardized_secondary_account))  and stg.takeda_sku=ltrim(rtrim(src.takeda_sku)) )  and ((stg.target_sales_type='PLN' and stg.date in (  select distinct stack(12,pln_month1,pln_month2,pln_month3,pln_month4,pln_month5,pln_month6,pln_month7,pln_month8,pln_month9,pln_month10,pln_month11,pln_month12) as date from com_de_lake.umea_tb_target_sales_src where country='Country'  ))   or  (stg.target_sales_type='MYC' and stg.date in (  select distinct stack(12,myc_month1,myc_month2,myc_month3,myc_month4,myc_month5,myc_month6,myc_month7,myc_month8,myc_month9,myc_month10,myc_month11,myc_month12) as date from com_de_lake.umea_tb_target_sales_src where country='Country'  ))  or  (stg.target_sales_type='LBE' and stg.date in (  select distinct stack(12,lbe_month1,lbe_month2,lbe_month3,lbe_month4,lbe_month5,lbe_month6,lbe_month7,lbe_month8,lbe_month9,lbe_month10,lbe_month11,lbe_month12) as date from com_de_lake.umea_tb_target_sales_src where country='Country')))''',
                'env_catalog': 'detst_gpd',
            },
        },
        "609983_006-UMEA-LAKETOHUB-TXN-TARGET-SALES-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "396157077480721",
            "job_parameters": {
                'scenario_pln': 'PLN',
                'scenario_myc': 'MYC',
                'src_code': 'SRC_UMEA_TARGET_SALES',
                'scenario_lbe': 'LBE',
                'env_catalog': 'detst_gpd',
            },
        },
        "607462_005-UMEA-LAKETOHUB-TXN-DISTRIBUTOR-SALES-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "160824516227510",
            "job_parameters": {
                'src_code': 'SRC_UMEA_DISTRIBUTOR_SALES',
                'date': '01',
                'env_catalog': 'detst_gpd',
            },
        },
        "603722_002-UMEA-LAKETOHUB-REF-CUSTOMER-MAP-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "716050937359858",
            "job_parameters": {
                'src_code': 'SRC_UMEA_ACCOUNT_MAPPING',
                'env_catalog': 'detst_gpd',
            },
        },
        "602672_003-UMEA-LAKETOHUB-REF-PROD-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "439185305105174",
            "job_parameters": {
                'src_code': 'SRC_UMEA_DISTRIBUTOR_PRODUCT',
                'env_catalog': 'detst_gpd',
            },
        },
        "616931_004-UMEA-LAKETOHUB-REF-PRICE-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "905032751978697",
            "job_parameters": {
                'day_of_mth': '1',
                'scenario_myc': 'MYC',
                'scenario_pln': 'PLN',
                'src_code': 'SRC_UMEA_PRICE',
                'scenario_lbe': 'LBE',
                'env_catalog': 'detst_gpd',
            },
        },
        "617175_001-UMEA-LAKETOHUB-REF-COUNTRY-UMEA": {
            "job_type": "DBX_LF_JOB",
            "job_id": "902544938398108",
            "job_parameters": {
                'src_code': 'SRC_UMEA_COUNTRY_MAPPING',
                'env_catalog': 'detst_gpd',
            },
        },
        "629725_001-UMEA-LAKETOLAKE-PRICE-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1091761959179060",
            "job_parameters": {
                'env_catalog': 'detst_gpd',
                'header_column_name': 'Country',
            },
        },
        "614621_003-UMEA-LAKETOLAKE-DISTRIBUTOR-SALES-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "564081563441994",
            "job_parameters": {
                'env_catalog': 'detst_gpd',
            },
        },
        "614727_005-UMEA-HUBTOMART-TXN-DISTRIBUTOR-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "797770600394886",
            "job_parameters": {
                'env_catalog': 'detst_gpd',
            },
        },
        "617593_006-UMEA-HUBTOMART-TXN-TARGET-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "949933766140083",
            "job_parameters": {
                'env_catalog': 'detst_gpd',
            },
        },
        "629600_003-UMEA-HUBTOMART-REF-CUSTOMER-MAP": {
            "job_type": "DBX_LF_JOB",
            "job_id": "215813761750267",
            "job_parameters": {
                'env_catalog': 'detst_gpd',
            },
        },
        "601308_002-UMEA-HUBTOMART-REF-PRODUCT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "694708068855264",
            "job_parameters": {
                'env_catalog': 'detst_gpd',
            },
        },
        "604835_001-UMEA-HUBTOMART-REF-COUNTRY": {
            "job_type": "DBX_LF_JOB",
            "job_id": "640459849662816",
            "job_parameters": {
                'env_catalog': 'detst_gpd',
            },
        },
        "618585_004-UMEA-HUBTOMART-REF-PRICE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "407285766714065",
            "job_parameters": {
                'env_catalog': 'detst_gpd',
            },
        },

        # Lambda jobs
        "UMEA-TARGET-SALES-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_target_sales.xlsx",
        },
        "UMEA-TARGET-SALES-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_target_sales.xlsx",
            },
        },
        "UMEA-DISTRIBUTOR-SALES-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_distributor_sales.xlsx",
        },
        "UMEA-DISTRIBUTOR-SALES-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_distributor_sales.xlsx",
            },
        },
        "UMEA-ACCOUNT-LEAD-CONTRIBUTION-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_account_lead_contribution.xlsx",
        },
        "UMEA-ACCOUNT-LEAD-CONTRIBUTION-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_account_lead_contribution.xlsx",
            },
        },
        "UMEA-PRICE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_price.xlsx",
        },
        "UMEA-PRICE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_price.xlsx",
            },
        },
        "UMEA-ACCOUNT-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_account_mapping.xlsx",
        },
        "UMEA-ACCOUNT-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_account_mapping.xlsx",
            },
        },
        "UMEA-COUNTRY-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_country_mapping.xlsx",
        },
        "UMEA-COUNTRY-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_country_mapping.xlsx",
            },
        },
        "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/umea_distributor_product_mapping.xlsx",
        },
        "UMEA-DISTRIBUTOR-PRODUCT-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/umea_distributor_product_mapping.xlsx",
            },
        },
    },
    "PROD": {},
}

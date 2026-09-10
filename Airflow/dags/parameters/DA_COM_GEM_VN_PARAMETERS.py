EMAIL_RECIPIENTS = {
    'DEV': 'anush.gowda@takeda.com,premkumar.duraisamy@takeda.com,udayan.awasthi@takeda.com,arjun.dalal@takeda.com,dheeraj.jaiman@takeda.com,DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com',
    'TEST': 'anush.gowda@takeda.com,premkumar.duraisamy@takeda.com,udayan.awasthi@takeda.com,arjun.dalal@takeda.com,dheeraj.jaiman@takeda.com,DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com,wajahat.khan@takeda.com,shraddha.singh@takeda.com',
    'PROD': 'DL.EDB.GEM.VN.COMM@takeda.com, DL.EDB.COMM.UAMS@takeda.com'
}

SENSOR_POKE_INTERVAL_SECONDS = 15 * 60
SENSOR_TIMEOUT_SECONDS = 23 * 60 * 60 + 50 * 60

SNOW_ATTRIBUTES_DEFAULTS = {
    "configuration_item": "Commercial Common Data Pipeline [EDB] - Production-AWS-Airflow-EU",
    "severity":           "2",
    "ci_apms_id":         "APMS-92363",
    "ci_bsn_id":          "BSN0022119",
}

PARAMS_DICT = {

    "DEV": {

        # ---- Databricks notebook jobs (4) ----
        "593423_010-DATABRICKS-RUN-NOTEBOOK-VN-EPLUS-SRC-TO-LAKE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1045765402162388",
            "job_parameters": {
                    "loadtype": "full",
                    "timeperiod": "Daily",
                    "tidal_callbackname": "",
                    "tidal_callback_jobid": "",
                    "notebook_name": "/COMM/COMM_IT_GEM/VIETNAM/Vietnam Eplus Lake Ingestion",
            },
        },
        "624973_010-DATABRICKS-RUN-NOTEBOOK-HUBTOMART-TXN-EPLUS-WHS-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1045765402162388",
            "job_parameters": {
                    "loadtype": "full",
                    "timeperiod": "Daily",
                    "tidal_callbackname": "",
                    "tidal_callback_jobid": "",
                    "notebook_name": "/COMM/COMM_IT_GEM/VIETNAM/Vietnam_Eplus_Hub_to_mart",
                },
            },
        "626879_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC": {
            "job_type": "DBX_LF_JOB",
            "job_id": "877928726445456",
            "job_parameters": {
                    "loadtype": "full",
                    "timeperiod": "Daily",
                    "tidal_callbackname": "",
                    "tidal_callback_jobid": "",
                    "country_name": "VIETNAM",
                    "notebook_name": "/COMM/COMM_IT_GEM/DM Portal/Notebook_DM_Portal_Exception_New",
            },
        },
        "720968_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-SRC-TO-LAKE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1045765402162388",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "notebook_name": "/COMM/COMM_IT_GEM/VIETNAM/Vietnam_Long_Chau_Sharepoint_to_Lake_Ingestion",
                "s3_bucket": "",
                "secret_arn": "{{ var.value.get('VN-LONG-CHAU-SECRET-ARN') }}",
                "drive_id": "013WYKEX7FGR6RXDPJRFH34UB5JCVZKH3C",
                "site_id": "d637e5f4-b494-49e0-9ba4-43bf4420e470",
                "client_secret": "client_secret",
                "client_id": "client_id",
                "tenant_id": "tenant_id",
            },
        },
        "728219_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-LAKE-TO-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1045765402162388",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "notebook_name": "/COMM/COMM_IT_GEM/VIETNAM/Vietnam_Long_Chau_Lake_To_Stg",
                "file_folder_path": "",
                "search_value": "",
                "table_folder_path": "",
                "encoding": "",
            },
        },
        "739887_010-DATABRICKS-RUN-NOTEBOOK-VN-LONG-CHAU-STG-TO-MART": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1045765402162388",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "notebook_name": "/COMM/COMM_IT_GEM/VIETNAM/Vietnam_Long_Chau_Stg_To_Mart",
                "file_folder_path": "",
                "search_value": "",
                "table_folder_path": "",
                "encoding": "",
            },
        },

        # ---- BDM -> Databricks jobs ----
        "599619_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "29365055394789",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "599961_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "29365055394789",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "601185_001-VN-LAKETOLAKE-VN_DKSH_SHIRE_STG-LOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "655707757784612",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "604687_004-VN-HUBTOMART-TXN_WHS_SALES_DETAIL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "29365055394789",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "608175_003-VN-HUBTOMART-TXN_TGT_SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1047892298708026",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "610683_002-VN-LAKETOHUB-TXN_VN_TGT_SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "491127834069497",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "610932_001-VN-LAKETOLAKE-TXN_VN_TGT_SALES_STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "943180964525963",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "611378_001-VN-LAKETOLAKE-GEM_PRODUCT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "99129668762642",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "618429_003-VN-LAKETOHUB-TXN_WHOLESALER_SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "808799261804115",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "619028_003-VN-LAKETOHUB-TXN_WHOLESALER_SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "808799261804115",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "619520_001-VN-LAKETOHUB-REF_CUST_INTL_MASTER": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1043537133930824",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "619558_003-VN-LAKETOHUB-TXN_WHOLESALER_SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "808799261804115",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "619828_002-VN-LAKETOHUB-REF_CUST_INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "320275910467266",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "620394_002-VN-LAKETOHUB-REF_CUST_INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "320275910467266",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "620597_002-VN-LAKETOHUB-REF_PROD_INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "793022545496307",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "620826_001-VN-LAKETOLAKE-VN_ZP_STG-LOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1011921624876263",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "621835_001-VN-LAKETOHUB-REF_TERR_VN": {
            "job_type": "DBX_LF_JOB",
            "job_id": "360817896419432",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "624482_002-VN-HUBTOMART-REF_TERR_ALIGNMENT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "468641575356071",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "628013_002-VN-LAKETOHUB-REF_CUST_INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "320275910467266",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "630472_001-VN-LAKETOLAKE-VN_DKSH_STG-LOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "456713023217978",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },

        #LAMBDAS
        "VN-CHANNEL-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/VN_channel_mapping.xlsx",
        },
        "VN-CHANNEL-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/VN_channel_mapping.xlsx",
            },
        },
        "VN-ADMIN-CUSTOMER-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/vn_admin_customer.xlsx",
        },
        "VN-ADMIN-CUSTOMER-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/vn_admin_customer.xlsx",
            },
        },
        "VN-CUSTOMER-EXCLUDE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/VN_Customer_Exclude.xlsx",
        },
        "VN-CUSTOMER-EXCLUDE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/VN_Customer_Exclude.xlsx",
            },
        },
        "VN-DKSH-TAKEDA-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/VN_DKSH_TAKEDA_*.xlsx",
        },
        "VN-DKSH-TAKEDA-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/VN_DKSH_TAKEDA_*.xlsx",
            },
        },
        "VN-DKSH-SHIRE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/VN_DKSH_SHIRE_*.xlsx",
        },
        "VN-DKSH-SHIRE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/VN_DKSH_SHIRE_*.xlsx",
            },
        },
        "VN-ADMIN-MAPCALENDAR-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/vn_admin_mapcalendar*.xlsx",
        },
        "VN-ADMIN-MAPCALENDAR-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/vn_admin_mapcalendar*.xlsx",
            },
        },
        "VN-ADMIN-MAPTERRITORY-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/vn_admin_mapterritory.xlsx",
        },
        "VN-ADMIN-MAPTERRITORY-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/vn_admin_mapterritory.xlsx",
            },
        },
        "VN-NONCHARM-USER-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/VN_NonCHARM_User*.xlsx",
        },
        "VN-NONCHARM-USER-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/VN_NonCHARM_User*.xlsx",
            },
        },
        "VN-GEM-PRODUCT-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/Gem_product_VN.xlsx",
        },
        "VN-GEM-PRODUCT-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/Gem_product_VN.xlsx",
            },
        },
        "VN-TARGETSALES-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/vn*targetsales*.xlsx",
        },
        "VN-TARGETSALES-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/vn*targetsales*.xlsx",
            },
        },
        "VN-DAILYNETSALESDATA-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/DailyNetSalesData-*.xlsx",
        },
        "VN-DAILYNETSALESDATA-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/DailyNetSalesData-*.xlsx",
            },
        },

        # ---- IICS jobs ----
        "606336_001-VN-COM-DE-LAKETOLAKE-EPLUS-SALES-STG": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_VN_EPLUS_SALES MTT",
        },
        "619846_007-VN-COM-DE-LAKETOLAKE-EPLUS-PENDING-PO-STG": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_VN_EPLUS_PENDING_PO MTT",
        },
        "619951_002-VN-COM-DE-LAKETOHUB-EPLUS-REF-CUST-INTL": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOHUB_VN_EPLUS_REF_CUST_INTL MTT",
        },
        "619971_005-VN-COM-DE-LAKETOLAKE-EPLUS-ITEMS-BY-LOCATION-STG": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_VN_EPLUS_ITEMS_BY_LOCATION MTT",
        },
        "622159_006-VN-COM-DE-LAKETOLAKE-EPLUS-MSTOCK-STG": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_VN_EPLUS_MSTOCK_STG MTT",
        },
        "628671_003-VN-COM-DE-LAKETOHUB-TXN-EPLUS-WHS-SALES": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOHUB_TXN_VN_EPLUS_WHS_SALES MTT",
        },
    },

    "TEST": {},
    "PROD": {},
}

POOLS = {
    "SET1": "vn_pool_1",   # DKSH, DKSH-SHIRE, ZP, CUSTOMER-MASTER
    "SET2": "vn_pool_2",   # PRODUCT, TARGETSALES, TERRITORY-ALIGNMENT
}

# dag_id of the child DAG each group's entry trigger points at
CHILD_DAG_IDS = {
    "DKSH": "DA-COM-GEM-VN-CHILD-DKSH",
    "DKSH-SHIRE": "DA-COM-GEM-VN-CHILD-DKSH-SHIRE",
    "ZP": "DA-COM-GEM-VN-CHILD-ZP",
    "PRODUCT": "DA-COM-GEM-VN-CHILD-PRODUCT",
    "TARGETSALES": "DA-COM-GEM-VN-CHILD-TARGETSALES",
    "TERRITORY-ALIGNMENT": "DA-COM-GEM-VN-CHILD-TERRITORY-ALIGNMENT",
}

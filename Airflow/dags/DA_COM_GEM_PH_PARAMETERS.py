DATABRICKS_CONN_ID = "Databricks_BDMToDBX_Default"
AWS_CONN_ID = "AWS_Default"
GLOBALSCAPE_CONN_ID = "GLOBALSCAPE_CONN"
SSH_CONN_ID = "SSH_Default"

EMAIL_RECIPIENTS = {
    "DEV": "astha.chaturvedi@takeda.com,premkumar.duraisamy@takeda.com,udayan.awasthi@takeda.com,dheeraj.jaiman@takeda.com",
    "TEST": "astha.chaturvedi@takeda.com,premkumar.duraisamy@takeda.com,udayan.awasthi@takeda.com,arjun.dalal@takeda.com,dheeraj.jaiman@takeda.com,DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com,wajahat.khan@takeda.com,shraddha.singh@takeda.com",
    "PROD": "DL.EDB.GEM.VN.COMM@takeda.com, DL.EDB.COMM.UAMS@takeda.com"
}

POOLS = {
    "PH_POOL_1": "PH_POOL_1",
    "PH_POOL_2": "PH_POOL_2",
}

SENSOR_POKE_INTERVAL_SECONDS = 15 * 60
SENSOR_TIMEOUT_SECONDS = 23 * 60 * 60 + 50 * 60

SNOW_ATTRIBUTES_DEFAULTS = {
    "configuration_item": "",
    "severity": "2",
    "ci_apms_id": "APMS-92144",
    "ci_bsn_id": "BSN0020621",
}

PARAMS_DICT = {

    "DEV": {
        #Existing databricks jobs
        "748097_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC": {
            "job_type": "DBX_LF_JOB",
            "job_id": "174413206305646",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "country_name": "Philippines",
                "notebook_name": "/COMM/COMM_IT_GEM/DM Portal/Notebook_DM_Portal_Exception_New",
            },
        },

        # BDM converted jobs
        # ---- LAKE TO LAKE ----
        "742431_001-PH-LAKETOLAKE-GEM-PRODUCT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "372590290127157",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "740906_001-PH-LAKETOLAKE-GLOBO-PCSO-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "14675742150845",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "730157_001-PH-LAKETOLAKE-PH-GLOBO-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "924834889958005",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "747809_001-PH-LAKETOLAKE-PH-ZPC-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1026310051311024",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "714317_001-PH-LAKETOLAKE-TXN-PH-TGT-SALES-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "226355627604419",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        # ---- LAKE TO HUB ----
        "734220_002-PH-LAKETOHUB-REF-CUST-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "210142002891060",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "735246_002-PH-LAKETOHUB-REF-CUST-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "210142002891060",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "736318_002-PH-LAKETOHUB-REF-CUST-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "210142002891060",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "731820_001-PH-LAKETOHUB-REF-CUST-INTL-MASTER": {
            "job_type": "DBX_LF_JOB",
            "job_id": "949060459217549",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "730724_002-PH-LAKETOHUB-REF-PROD-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "595206269445188",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "721787_001-PH-LAKETOHUB-REF-TERR-PH": {
            "job_type": "DBX_LF_JOB",
            "job_id": "28701393762019",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "723138_002-PH-LAKETOHUB-TXN-PH-TGT-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "545021811647080",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "731310_001-PH-LAKETOHUB-TXN-PH-TRADE-DISCOUNT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "333143088548804",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "735954_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "612679496613267",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "736563_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "612679496613267",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "737130_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "612679496613267",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "739231_001-PH-LAKETOHUB-TXN-PH-ZPC-CSMO-SALES": {  # main DAG
            "job_type": "DBX_LF_JOB",
            "job_id": "240198084413293",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        # ---- HUB TO MART ----
        "740911_002-PH-HUBTOMART-REF-PH-TERR-ALIGNMENT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "97859223060445",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "728889_003-PH-HUBTOMART-TXN-PH-TGT-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "25939780986378",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "743912_002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "994563695224974",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "724945_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "144201404749822",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "725593_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "144201404749822",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "726984_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "144201404749822",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },
        "729603_002-PH-HUBTOMART-TXN-PH-ZPC-CSMO-SALES": {  # main DAG
            "job_type": "DBX_LF_JOB",
            "job_id": "1092941872830117",
            "job_parameters": {'env_catalog': 'dedev_gpd'},
        },

        # Lambda jobs (FILEWATCH + LAKELOAD per source, in the order the DAG runs them)
        "PH-CUSTOMER-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_customer.xlsx",
        },
        "PH-CUSTOMER-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_customer.xlsx",
            },
        },
        "PH-ADMIN-MAPPRODCODE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_mapprodcode.xlsx",
        },
        "PH-ADMIN-MAPPRODCODE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_mapprodcode.xlsx",
            },
        },
        "PH-MAP-TERRITORY-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_mapterritory.xlsx",
        },
        "PH-MAP-TERRITORY-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_mapterritory.xlsx",
            },
        },
        "PH-TRADE-DISCOUNT-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_Trade_Discount*.xlsx",
        },
        "PH-TRADE-DISCOUNT-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_Trade_Discount*.xlsx",
            },
        },
        "PH-ZPC-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/Daily_Sales_Data_Detail*.TXT",
        },
        "PH-ZPC-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/Daily_Sales_Data_Detail*.TXT",
            },
        },
        "PH-ZPC-CSMO-SALES-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_ZPC_CSMO_Sales_Template*.xlsx",
        },
        "PH-ZPC-CSMO-SALES-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_ZPC_CSMO_Sales_Template*.xlsx",
            },
        },
        "PH-GLOBO-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDAHEALTHCAREPHILIPPINESINC_Commercial.txt",
        },
        "PH-GLOBO-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDAHEALTHCAREPHILIPPINESINC_Commercial.txt",
            },
        },
        "PH-GLOBO-PCSO-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_HEALTHCARE_PHILIPPINES_INC_PCSO_Final_Sales_*.xlsx",
        },
        "PH-GLOBO-PCSO-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_HEALTHCARE_PHILIPPINES_INC_PCSO_Final_Sales_*.xlsx",
            },
        },
        "PH-PRODUCT-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/Gem_product_PH.xlsx",
        },
        "PH-PRODUCT-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/Gem_product_PH.xlsx",
            },
        },
        "PH-NONCHARM-USER-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_NonCHARM_User.xlsx",
        },
        "PH-NONCHARM-USER-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_NonCHARM_User.xlsx",
            },
        },
        "PH-CUSTOMER-EXCLUDE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_Customer_Exclude.xlsx",
        },
        "PH-CUSTOMER-EXCLUDE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_Customer_Exclude.xlsx",
            },
        },
        "PH-CHANNEL-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_channel_mapping.xlsx",
        },
        "PH-CHANNEL-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_channel_mapping.xlsx",
            },
        },
        "PH-MAP-CALENDAR-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_mapcalendar*.xlsx",
        },
        "PH-MAP-CALENDAR-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_mapcalendar*.xlsx",
            },
        },
        "PH-ADMIN-TARGETSALES-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_targetsales.xlsx",
        },
        "PH-ADMIN-TARGETSALES-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_targetsales.xlsx",
            },
        },
    },

    "TEST": {
        #Existing databricks jobs
        "748097_010-DATABRICKS-RUN-NOTEBOOK-DMP-EXCEPTION-SYNC": {
            "job_type": "DBX_LF_JOB",
            "job_id": "269749650678524",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "country_name": "Philippines",
                "notebook_name": "/COMM/COMM_IT_GEM/DM Portal/Notebook_DM_Portal_Exception_New",
            },
        },

        # BDM converted jobs
        # ---- LAKE TO LAKE ----
        "742431_001-PH-LAKETOLAKE-GEM-PRODUCT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "32454988085668",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "740906_001-PH-LAKETOLAKE-GLOBO-PCSO-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "348042728771820",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "730157_001-PH-LAKETOLAKE-PH-GLOBO-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1006489660802745",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "747809_001-PH-LAKETOLAKE-PH-ZPC-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1029705508099381",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "714317_001-PH-LAKETOLAKE-TXN-PH-TGT-SALES-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "49641514231430",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        # ---- LAKE TO HUB ----
        "734220_002-PH-LAKETOHUB-REF-CUST-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "292844294964843",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "735246_002-PH-LAKETOHUB-REF-CUST-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "292844294964843",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "736318_002-PH-LAKETOHUB-REF-CUST-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "292844294964843",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "731820_001-PH-LAKETOHUB-REF-CUST-INTL-MASTER": {
            "job_type": "DBX_LF_JOB",
            "job_id": "521033542232768",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "730724_002-PH-LAKETOHUB-REF-PROD-INTL": {
            "job_type": "DBX_LF_JOB",
            "job_id": "575068170439708",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "721787_001-PH-LAKETOHUB-REF-TERR-PH": {
            "job_type": "DBX_LF_JOB",
            "job_id": "407817027866805",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "723138_002-PH-LAKETOHUB-TXN-PH-TGT-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "383990033321207",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "731310_001-PH-LAKETOHUB-TXN-PH-TRADE-DISCOUNT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "88378420232496",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "735954_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "760844824722851",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "736563_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "760844824722851",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "737130_003-PH-LAKETOHUB-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "760844824722851",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "739231_001-PH-LAKETOHUB-TXN-PH-ZPC-CSMO-SALES": {  # main DAG
            "job_type": "DBX_LF_JOB",
            "job_id": "644187048096487",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        # ---- HUB TO MART ----
        "740911_002-PH-HUBTOMART-REF-PH-TERR-ALIGNMENT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "994686245567329",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "728889_003-PH-HUBTOMART-TXN-PH-TGT-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "901939552235756",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "743912_002-PH-HUBTOMART-TXN-PH-TRADE-DISCOUNT": {
            "job_type": "DBX_LF_JOB",
            "job_id": "514469642788203",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "724945_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1013739270730649",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "725593_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1013739270730649",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "726984_004-PH-HUBTOMART-TXN-PH-WHOLESALER-SALES": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1013739270730649",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },
        "729603_002-PH-HUBTOMART-TXN-PH-ZPC-CSMO-SALES": {  # main DAG
            "job_type": "DBX_LF_JOB",
            "job_id": "952637703554681",
            "job_parameters": {'env_catalog': 'detst_gpd'},
        },

        # Lambda jobs (FILEWATCH + LAKELOAD per source, in the order the DAG runs them)
        "PH-CUSTOMER-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_customer.xlsx",
        },
        "PH-CUSTOMER-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_customer.xlsx",
            },
        },
        "PH-ADMIN-MAPPRODCODE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_mapprodcode.xlsx",
        },
        "PH-ADMIN-MAPPRODCODE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_mapprodcode.xlsx",
            },
        },
        "PH-MAP-TERRITORY-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_mapterritory.xlsx",
        },
        "PH-MAP-TERRITORY-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_mapterritory.xlsx",
            },
        },
        "PH-TRADE-DISCOUNT-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_Trade_Discount*.xlsx",
        },
        "PH-TRADE-DISCOUNT-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_Trade_Discount*.xlsx",
            },
        },
        "PH-ZPC-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/Daily_Sales_Data_Detail*.TXT",
        },
        "PH-ZPC-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/Daily_Sales_Data_Detail*.TXT",
            },
        },
        "PH-ZPC-CSMO-SALES-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_ZPC_CSMO_Sales_Template*.xlsx",
        },
        "PH-ZPC-CSMO-SALES-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_ZPC_CSMO_Sales_Template*.xlsx",
            },
        },
        "PH-GLOBO-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDAHEALTHCAREPHILIPPINESINC_Commercial.txt",
        },
        "PH-GLOBO-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDAHEALTHCAREPHILIPPINESINC_Commercial.txt",
            },
        },
        "PH-GLOBO-PCSO-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_HEALTHCARE_PHILIPPINES_INC_PCSO_Final_Sales_*.xlsx",
        },
        "PH-GLOBO-PCSO-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_HEALTHCARE_PHILIPPINES_INC_PCSO_Final_Sales_*.xlsx",
            },
        },
        "PH-PRODUCT-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/Gem_product_PH.xlsx",
        },
        "PH-PRODUCT-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/Gem_product_PH.xlsx",
            },
        },
        "PH-NONCHARM-USER-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_NonCHARM_User.xlsx",
        },
        "PH-NONCHARM-USER-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_NonCHARM_User.xlsx",
            },
        },
        "PH-CUSTOMER-EXCLUDE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_Customer_Exclude.xlsx",
        },
        "PH-CUSTOMER-EXCLUDE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_Customer_Exclude.xlsx",
            },
        },
        "PH-CHANNEL-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/PH_channel_mapping.xlsx",
        },
        "PH-CHANNEL-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/PH_channel_mapping.xlsx",
            },
        },
        "PH-MAP-CALENDAR-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_mapcalendar*.xlsx",
        },
        "PH-MAP-CALENDAR-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_mapcalendar*.xlsx",
            },
        },
        "PH-ADMIN-TARGETSALES-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ph_admin_targetsales.xlsx",
        },
        "PH-ADMIN-TARGETSALES-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ph_admin_targetsales.xlsx",
            },
        },
    },

    "PROD": {},
}

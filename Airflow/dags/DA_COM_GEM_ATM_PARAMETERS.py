DATABRICKS_CONN_ID = "Databricks_BDMToDBX_Default"

# Files this DAG waits on arrive at ad hoc times during the day, so every S3KeySensor
# pokes every 15 minutes and keeps waiting for its file until the 23:59 CET/CEST cutoff.
# SENSOR_TIMEOUT_SECONDS is a fixed duration (23h59m) counted from the DAG's own 00:00
# CET/CEST daily start - a sensor's timeout is measured from when it starts, not a
# wall-clock cutoff, so it must line up with the DAG's schedule_interval start time.
SENSOR_POKE_INTERVAL_SECONDS = 5 * 60
SENSOR_TIMEOUT_SECONDS = 23 * 60 * 60 + 50 * 60

SNOW_ATTRIBUTES = {
    "configuration_item": "Commercial Common Data Pipeline [EDB] - Production-AWS-Airflow-EU",
    "severity":           "2",
    "ci_apms_id":         "APMS-92363",
    "ci_bsn_id":          "BSN0022119",
}

PARAMS_DICT = {

    "DEV": {
        "598618_010-DATABRICKS-RUN-NOTEBOOK-ATM-ARCHIVE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "719779021179197",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "outbound_name": "dummy",
                "notebook_name": "/COMM/COMM_IT_GEM/ATM/Notebook_ATM_Archive",
            },
        },
        "626540_003-BDM-LAKETOLAKE-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1082974637422266",
            "job_parameters": {},
        },
        "624966_004-BDM-LAKETOLAKE-SUSTAINIBILITY-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "39988749125061",
            "job_parameters": {},
        },
        "616712_002-BDM-LAKETOLAKE-ICLUSIG-MONTHLY-PROGRAM-UPDATE-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1077069542601417",
            "job_parameters": {},
        },
        "627861_010-DATABRICKS-RUN-NOTEBOOK-DATA-COLLECTION-TEMPLATE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "719779021179197",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "outbound_name": "dummy",
                "notebook_name": "/COMM/COMM_IT_GEM/ATM/Notebook_ATM_Data_Collection_Template",
            },
        },
        "597446_005-BDM-LAKETOLAKE_ATM_DATA_ENTRY_STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "903634563779735",
            "job_parameters": {},
        },

        "617561_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_COMBINED_EXPORTS_TAKEDA_GLOBAL_STG MTT",
        },
        "621923_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_ATM_HSS_DATA MTT",
        },

        "ATM-HSS-DATA-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-com-data-analytics-gem-eu-central-1",
            "bucket_key": "external/pre_contract/atm/ATM_HSS_all_data.xlsx",
        },

        "ATM-HSS-DATA-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-com-data-analytics-gem-eu-central-1",
                "bucket_key": "external/pre_contract/atm/ATM_HSS_all_data.xlsx",
            },
        },

        "ATM-COMBINED-EXPORTS-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/COMBINED_EXPORTS_TAKEDA_GLOBAL_*.xlsx",
        },
        "ATM-COMBINED-EXPORTS-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/COMBINED_EXPORTS_TAKEDA_GLOBAL_*.xlsx",
            },
        },


        "ATM-DATA-ENTRY-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ATM_DATA_ENTRY.xlsx",
        },
        "ATM-DATA-ENTRY-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ATM_DATA_ENTRY.xlsx",
            },
        },

        "ATM-BRAND-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/BRAND_MAPPING.xlsx",
        },
        "ATM-BRAND-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/BRAND_MAPPING.xlsx",
            },
        },

        "ATM-COUNTRY-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-com-data-analytics-gem-eu-central-1",
            "bucket_key": "external/pre_contract/atm/COUNTRY_MAPPING.xlsx",
        },
        "ATM-COUNTRY-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-com-data-analytics-gem-eu-central-1",
                "bucket_key": "external/pre_contract/atm/COUNTRY_MAPPING.xlsx",
            },
        },

        "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_SUSTAINABILITY_ALL_TIME_*.csv",
        },
        "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_SUSTAINABILITY_ALL_TIME_*.csv",
            },
        },

        "ATM-MONTHLY-PROGRAM-UPDATE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_ICLUSIG_MONTHLY_PROGRAM_UPDATE_*.xlsx",
        },
        "ATM-MONTHLY-PROGRAM-UPDATE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_ICLUSIG_MONTHLY_PROGRAM_UPDATE_*.xlsx",
            },
        },

        "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_LSD_CAP_ACTIVE_PATIENT_ENROLLMENT_UPDATE_*.xlsx",
        },
        "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "689611953171815",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_LSD_CAP_ACTIVE_PATIENT_ENROLLMENT_UPDATE_*.xlsx",
            },
        },
    },
    
    "TEST": {
        "598618_010-DATABRICKS-RUN-NOTEBOOK-ATM-ARCHIVE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "557081953715500",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "outbound_name": "dummy",
                "notebook_name": "/COMM/COMM_IT_GEM/ATM/Notebook_ATM_Archive",
            },
        },
        "626540_003-BDM-LAKETOLAKE-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "780219969981832",
            "job_parameters": {},
        },
        "624966_004-BDM-LAKETOLAKE-SUSTAINIBILITY-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "963161590060387",
            "job_parameters": {},
        },
        "616712_002-BDM-LAKETOLAKE-ICLUSIG-MONTHLY-PROGRAM-UPDATE-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "964165111127509",
            "job_parameters": {},
        },
        "627861_010-DATABRICKS-RUN-NOTEBOOK-DATA-COLLECTION-TEMPLATE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "557081953715500",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "outbound_name": "dummy",
                "notebook_name": "/COMM/COMM_IT_GEM/ATM/Notebook_ATM_Data_Collection_Template",
            },
        },
        "597446_005-BDM-LAKETOLAKE_ATM_DATA_ENTRY_STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "459222753173071",
            "job_parameters": {},
        },

        "617561_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_COMBINED_EXPORTS_TAKEDA_GLOBAL_STG MTT",
        },
        "621923_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_ATM_HSS_DATA MTT",
        },

        "ATM-HSS-DATA-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-com-data-analytics-gem-eu-central-1",
            "bucket_key": "external/pre_contract/atm/ATM_HSS_all_data.xlsx",
        },

        "ATM-HSS-DATA-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-com-data-analytics-gem-eu-central-1",
                "bucket_key": "external/pre_contract/atm/ATM_HSS_all_data.xlsx",
            },
        },

        "ATM-COMBINED-EXPORTS-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/COMBINED_EXPORTS_TAKEDA_GLOBAL_*.xlsx",
        },
        "ATM-COMBINED-EXPORTS-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/COMBINED_EXPORTS_TAKEDA_GLOBAL_*.xlsx",
            },
        },

        "ATM-DATA-ENTRY-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ATM_DATA_ENTRY.xlsx",
        },
        "ATM-DATA-ENTRY-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ATM_DATA_ENTRY.xlsx",
            },
        },

        "ATM-BRAND-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/BRAND_MAPPING.xlsx",
        },
        "ATM-BRAND-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/BRAND_MAPPING.xlsx",
            },
        },

        "ATM-COUNTRY-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-com-data-analytics-gem-eu-central-1",
            "bucket_key": "external/pre_contract/atm/COUNTRY_MAPPING.xlsx",
        },
        "ATM-COUNTRY-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-com-data-analytics-gem-eu-central-1",
                "bucket_key": "external/pre_contract/atm/COUNTRY_MAPPING.xlsx",
            },
        },

        "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_SUSTAINABILITY_ALL_TIME_*.csv",
        },
        "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_SUSTAINABILITY_ALL_TIME_*.csv",
            },
        },

        "ATM-MONTHLY-PROGRAM-UPDATE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_ICLUSIG_MONTHLY_PROGRAM_UPDATE_*.xlsx",
        },
        "ATM-MONTHLY-PROGRAM-UPDATE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_ICLUSIG_MONTHLY_PROGRAM_UPDATE_*.xlsx",
            },
        },

        "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_LSD_CAP_ACTIVE_PATIENT_ENROLLMENT_UPDATE_*.xlsx",
        },
        "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "216931983214676",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_LSD_CAP_ACTIVE_PATIENT_ENROLLMENT_UPDATE_*.xlsx",
            },
        },   
    },
    "PROD": {
        "598618_010-DATABRICKS-RUN-NOTEBOOK-ATM-ARCHIVE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "718387300224551",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "outbound_name": "dummy",
                "notebook_name": "/COMM/COMM_IT_GEM/ATM/Notebook_ATM_Archive",
            },
        },
        "626540_003-BDM-LAKETOLAKE-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "888685166849012",
            "job_parameters": {},
        },
        "624966_004-BDM-LAKETOLAKE-SUSTAINIBILITY-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "343946601826961",
            "job_parameters": {},
        },
        "616712_002-BDM-LAKETOLAKE-ICLUSIG-MONTHLY-PROGRAM-UPDATE-STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "1028038154595280",
            "job_parameters": {},
        },
        "627861_010-DATABRICKS-RUN-NOTEBOOK-DATA-COLLECTION-TEMPLATE": {
            "job_type": "DBX_LF_JOB",
            "job_id": "718387300224551",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "outbound_name": "dummy",
                "notebook_name": "/COMM/COMM_IT_GEM/ATM/Notebook_ATM_Data_Collection_Template",
            },
        },
        "597446_005-BDM-LAKETOLAKE_ATM_DATA_ENTRY_STG": {
            "job_type": "DBX_LF_JOB",
            "job_id": "915129157815014",
            "job_parameters": {},
        },

        "617561_002-IICS-COM-DE-LAKE-TO-LAKE-COMBINED-EXPORTS-TAKEDA-GLOBAL-STG": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_COMBINED_EXPORTS_TAKEDA_GLOBAL_STG MTT",
        },
        "621923_001-MT-COM-DE-LAKETOLAKE-ATM-HSS-DATA": {
            "job_type": "IICS",
            "job_args": "0 mt_COM_DE_LAKETOLAKE_ATM_HSS_DATA MTT",
        },

        "ATM-HSS-DATA-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-com-data-analytics-gem-eu-central-1",
            "bucket_key": "external/pre_contract/atm/ATM_HSS_all_data.xlsx",
        },

        "ATM-HSS-DATA-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "547361243102309",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-com-data-analytics-gem-eu-central-1",
                "bucket_key": "external/pre_contract/atm/ATM_HSS_all_data.xlsx",
            },
        },

        "ATM-COMBINED-EXPORTS-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/COMBINED_EXPORTS_TAKEDA_GLOBAL_*.xlsx",
        },
        "ATM-COMBINED-EXPORTS-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "547361243102309",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/COMBINED_EXPORTS_TAKEDA_GLOBAL_*.xlsx",
            },
        },

        "ATM-DATA-ENTRY-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/ATM_DATA_ENTRY.xlsx",
        },
        "ATM-DATA-ENTRY-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "547361243102309",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/ATM_DATA_ENTRY.xlsx",
            },
        },

        "ATM-BRAND-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/BRAND_MAPPING.xlsx",
        },
        "ATM-BRAND-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "547361243102309",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/BRAND_MAPPING.xlsx",
            },
        },

        "ATM-COUNTRY-MAPPING-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-com-data-analytics-gem-eu-central-1",
            "bucket_key": "external/pre_contract/atm/COUNTRY_MAPPING.xlsx",
        },
        "ATM-COUNTRY-MAPPING-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "547361243102309",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-com-data-analytics-gem-eu-central-1",
                "bucket_key": "external/pre_contract/atm/COUNTRY_MAPPING.xlsx",
            },
        },

        "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_SUSTAINABILITY_ALL_TIME_*.csv",
        },
        "ATM-TAKEDA-SUSTAINABILITY-ALL-TIME-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "547361243102309",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_SUSTAINABILITY_ALL_TIME_*.csv",
            },
        },

        "ATM-MONTHLY-PROGRAM-UPDATE-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_ICLUSIG_MONTHLY_PROGRAM_UPDATE_*.xlsx",
        },
        "ATM-MONTHLY-PROGRAM-UPDATE-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "547361243102309",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_ICLUSIG_MONTHLY_PROGRAM_UPDATE_*.xlsx",
            },
        },

        "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-FILEWATCH": {
            "job_type": "S3_SENSOR",
            "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
            "bucket_key": "de/gem/external/pre_contract/TAKEDA_LSD_CAP_ACTIVE_PATIENT_ENROLLMENT_UPDATE_*.xlsx",
        },
        "ATM-TAKEDA-LSD-CAP-ACTIVE-PATIENT-ENROLLMENT-LAKELOAD": {
            "job_type": "DBX_LF_JOB",
            "job_id": "547361243102309",
            "job_parameters": {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/TAKEDA_LSD_CAP_ACTIVE_PATIENT_ENROLLMENT_UPDATE_*.xlsx",
            },
        },   
    },
}

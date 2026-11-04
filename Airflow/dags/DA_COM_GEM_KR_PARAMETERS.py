DATABRICKS_CONN_ID = "Databricks_BDMToDBX_Default"
AWS_CONN_ID = "AWS_Default"
GLOBALSCAPE_CONN_ID = "GLOBALSCAPE_CONN"
SSH_CONN_ID = "SSH_Default"

EMAIL_RECIPIENTS = {
    "DEV": "astha.chaturvedi@takeda.com,premkumar.duraisamy@takeda.com,udayan.awasthi@takeda.com,dheeraj.jaiman@takeda.com",
    "TEST": "astha.chaturvedi@takeda.com,premkumar.duraisamy@takeda.com,udayan.awasthi@takeda.com,arjun.dalal@takeda.com,dheeraj.jaiman@takeda.com,DL.GLBL.BDM.DBX.AXT.TEAM@takeda.com,wajahat.khan@takeda.com,shraddha.singh@takeda.com",
    "PROD": "",
}

# SENSOR_POKE_INTERVAL_SECONDS = 15 * 60
# SENSOR_TIMEOUT_SECONDS = 23 * 60 * 60 + 50 * 60

S3_SENSOR_POKE_INTERVAL_SECONDS = 1 * 60
S3_SENSOR_TIMEOUT_SECONDS = 25 * 60


SNOW_ATTRIBUTES_DEFAULTS = {
    "configuration_item": "",
    "severity": "2",
    "ci_apms_id": "APMS-92116",
    "ci_bsn_id": "BSN0020621",
}

PARAMS_DICT = {
    "DEV": {
        '742679_001-KR-HUB-REF-MSG-KAKAO-TALK': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '1042777345510747',
            'job_parameters': {'env_catalog': 'dedev_gpd'},
        },
        '728171_001-KR-MTASK-REF-MSG-KAKAO-TALK': {
            'job_type': 'IICS',
            'job_args': '0 mt_REF_MSG_KAKAO_TALK_KR MTT',
        },
        '738952_003-IICS-KR-TXN-CRM-EM-EVENT-OUTBOUND': {
            'job_type': 'IICS',
            'job_args': '0 mt_com_kr_txn_crm_em_event_outbound MTT',
        },
        '743321_001-KR-MART-REF-MSG-KAKAO-TALK': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '891560278740838',
            'job_parameters': {'env_catalog': 'dedev_gpd'},
        },
        'MSG-KAKAO-TALK-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-dev-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/KR_KAKAO_TALK.csv',
        },
        # '732769_004-IICS-KR-REF-CUST-CRM-OUTBOUND': {
        #     'job_type': 'IICS',
        #     'job_args': '0 mt_com_kr_ref_cust_crm_outbound MTT',
        # },
        
        
        '132887_010-DATABRICKS-RUN-NOTEBOOK-REF-CUST-CRM-OUTBOUND': {
           "job_type": "DBX_LF_JOB",
            "job_id": "1045765402162388",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "country_name": "Korea",
                "notebook_name": "/COMM/COMM_IT_GEM/KOREA/KR_REF_CUST_CRM_OUTBOUND",	 
    
            },
        },
        '132869_005-IICS-KR-CRM-MCCP-VW-OUTBOUND': {
            'job_type': 'IICS',
            'job_args': '0 mt_com_kr_crm_mccp_vw_outbound MTT',
        },
        '743086_002-IICS-KR-TXN-CRM-EM-EVENT-SPEAKER-OUTBOUND': {
            'job_type': 'IICS',
            'job_args': '0 mt_com_kr_txn_crm_em_event_speaker_outbound MTT',
        },
        'MSG-KAKAO-TALK-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '689611953171815',
            'job_parameters': {
                "bucket_name": "tpc-aws-ted-dev-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/KR_KAKAO_TALK.csv",
            },
        },
    },
    "TEST": {
        '742679_001-KR-HUB-REF-MSG-KAKAO-TALK': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '837352622982441',
            'job_parameters': {'env_catalog': 'detst_gpd'},
        },
        '728171_001-KR-MTASK-REF-MSG-KAKAO-TALK': {
            'job_type': 'IICS',
            'job_args': '0 mt_REF_MSG_KAKAO_TALK_KR MTT',
        },
        '738952_003-IICS-KR-TXN-CRM-EM-EVENT-OUTBOUND': {
            'job_type': 'IICS',
            'job_args': '0 mt_com_kr_txn_crm_em_event_outbound MTT',
        },
        '743321_001-KR-MART-REF-MSG-KAKAO-TALK': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '100985711621316',
            'job_parameters': {'env_catalog': 'detst_gpd'},
        },
        'MSG-KAKAO-TALK-FILEWATCH': {
            'job_type': 'S3_SENSOR',
            'bucket_name': 'tpc-aws-ted-tst-edpp-raw-com-eu-central-1',
            'bucket_key': 'de/gem/external/pre_contract/KR_KAKAO_TALK.csv',
        },
        # '732769_004-IICS-KR-REF-CUST-CRM-OUTBOUND': {
        #     'job_type': 'IICS',
        #     'job_args': '0 mt_com_kr_ref_cust_crm_outbound MTT',
        # },
        
        
        '132887_010-DATABRICKS-RUN-NOTEBOOK-REF-CUST-CRM-OUTBOUND': {
           "job_type": "DBX_LF_JOB",
            "job_id": "1016294340216001",
            "job_parameters": {
                "loadtype": "full",
                "timeperiod": "Daily",
                "tidal_callbackname": "",
                "tidal_callback_jobid": "",
                "country_name": "Korea",
                "notebook_name": "/COMM/COMM_IT_GEM/KOREA/KR_REF_CUST_CRM_OUTBOUND",	 
    
            },
        },
        '132869_005-IICS-KR-CRM-MCCP-VW-OUTBOUND': {
            'job_type': 'IICS',
            'job_args': '0 mt_com_kr_crm_mccp_vw_outbound MTT',
        },
        '743086_002-IICS-KR-TXN-CRM-EM-EVENT-SPEAKER-OUTBOUND': {
            'job_type': 'IICS',
            'job_args': '0 mt_com_kr_txn_crm_em_event_speaker_outbound MTT',
        },
        'MSG-KAKAO-TALK-LAKELOAD': {
            'job_type': 'DBX_LF_JOB',
            'job_id': '216931983214676',
            'job_parameters': {
                "bucket_name": "tpc-aws-ted-tst-edpp-raw-com-eu-central-1",
                "bucket_key": "de/gem/external/pre_contract/KR_KAKAO_TALK.csv",
            },
        },
    },
    "PROD": {},
}

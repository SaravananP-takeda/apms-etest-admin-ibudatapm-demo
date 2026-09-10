CREATE CATALOG IF NOT EXISTS dbx_uc_detst;
CREATE SCHEMA IF NOT EXISTS dbx_uc_detst.bdm_to_dbx_migration_meta;

CREATE TABLE IF NOT EXISTS dbx_uc_detst.bdm_to_dbx_migration_meta.dc_entity_mstr (
	app_id STRING COMMENT 'iloc 0 — app id (also filename prefix)' NOT NULL,
	entity_id STRING COMMENT 'iloc 1' NOT NULL,
	entity_nm STRING COMMENT 'iloc 2 — target table / parquet base name',
	raw_pre_processing_path STRING COMMENT 'iloc 3 — pre_contract landing path',
	raw_post_processing_path STRING COMMENT 'iloc 4 — post_contract path',
	file_nm_validation_flg STRING COMMENT 'iloc 5 — Y/N filename+ext check',
	file_nm STRING COMMENT 'iloc 6 — e.g. PAT270*.csv',
	key_column_check_flg STRING COMMENT 'iloc 7',
	key_column_list STRING COMMENT 'iloc 8 — pipe-delimited',
	column_header_check_flg STRING COMMENT 'iloc 9',
	no_of_columns STRING COMMENT 'iloc 10',
	mandatory_column_value_check_flg STRING COMMENT 'iloc 11',
	mandatory_column_list STRING COMMENT 'iloc 12',
	duplicate_value_check_flg STRING COMMENT 'iloc 13',
	duplicate_check_column_list STRING COMMENT 'iloc 14',
	file_delimeter STRING COMMENT 'iloc 15 — \\t supported',
	date_format_check_flg STRING COMMENT 'iloc 16',
	date_format_column_list STRING COMMENT 'iloc 17',
	decimal_value_check_flg STRING COMMENT 'iloc 18',
	decimal_value_check_col_list STRING COMMENT 'iloc 19',
	record_count_check_flg STRING COMMENT 'iloc 20',
	data_element_enclosure_character STRING COMMENT 'iloc 21',
	email STRING COMMENT 'iloc 22',
	header STRING COMMENT 'iloc 23',
	date_formats STRING COMMENT 'iloc 24 — strptime',
	parquet_header STRING COMMENT 'iloc 25 — lake column names',
	encoding STRING COMMENT 'iloc 26 — default utf-8',
	no_validation_flag STRING COMMENT 'iloc 27 — Y bypasses all checks',
	parquet_key_col STRING COMMENT 'iloc 28 — lake dedup keys',
	tidal_job_id_contract STRING,
	tidal_job_id_lake STRING,
	skiprows STRING,
    service STRING COMMENT 'iloc 32 — INF=>BDM else native',
	param_file_path STRING COMMENT 'iloc 33',
	BU STRING COMMENT 'iloc 34'
) USING DELTA;

CREATE TABLE IF NOT EXISTS dbx_uc_detst.bdm_to_dbx_migration_meta.dc_load_strategy (
    app_id STRING NOT NULL,
	entity_id STRING NOT NULL,
    raw_source_file_path STRING COMMENT 'iloc 2',
	lake_target_file_path STRING COMMENT 'iloc 3',
    load_typ STRING COMMENT 'iloc 4 — truncate|append|upsert|rolling|ByPassed',
	BU STRING
) USING DELTA;

-- audit (audit_load.py)
CREATE TABLE IF NOT EXISTS dbx_uc_detst.bdm_to_dbx_migration_meta.dc_excn_stat (
    excn_id STRING,
	app_id STRING,
	entity_id STRING,
    file_name STRING,
	status STRING,
	file_arrival_time TIMESTAMP,
    processed_time TIMESTAMP,
	duration_secs BIGINT,
	destination_dir STRING
)USING DELTA ;

CREATE TABLE IF NOT EXISTS dbx_uc_detst.bdm_to_dbx_migration_meta.dc_error_stat (
    excn_id STRING,
	app_id STRING NOT NULL,
	entity_id STRING NOT NULL,
    file_name STRING NOT NULL,
	validation_type STRING NOT NULL,
	error_msg STRING NOT NULL
) USING DELTA;
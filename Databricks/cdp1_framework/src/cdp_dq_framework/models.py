from dataclasses import dataclass

def _as_flag(v):  # legacy stored Y/N/NULL strings
    return str(v).strip().upper() == "Y"

def _split_pipe(v):  # legacy stored lists as 'A|B|C', with '\n' artifacts
    return [] if v is None or str(v).strip() == "" else str(v).replace("\\n", "").replace("\n", "").split("|")


@dataclass
class EntityContract:
    app_id: str
    entity_id: str
    entity_nm: str
    raw_post_processing_path: str
    filename_flag: bool
    filename_pattern: str
    key_col_flag: bool
    key_col_list: list
    num_cols_flag: bool
    num_cols_expected: int
    mandatory_col_flag: bool
    mandatory_col_list: list
    duplicate_flag: bool
    duplicate_key_list: list
    delimiter: str
    date_flag: bool
    date_col_list: list
    date_formats: list
    decimal_flag: bool
    decimal_col_list: list
    encoding: str
    upsert_key_list: list
    quote_char: str
    header_flag: str
    no_validation_flag: bool
    parquet_header: str
    skiprows: int
    service_type: str
    param_file_path: str
    jobid_contract: str
    jobid_lake: str
    BU: str

    @classmethod
    def from_row(cls, row):  # row = Spark Row.asDict()
        g = row.get
        return cls(
            app_id=str(g("app_id", "")),
            entity_id=str(g("entity_id", "")),
            entity_nm=str(g("entity_nm", "")),
            raw_post_processing_path=str(g("raw_post_processing_path", "")),
            filename_flag=_as_flag(g("file_nm_validation_flg")),
            filename_pattern=str(g("file_nm", "")),
            key_col_flag=_as_flag(g("key_column_check_flg")),
            key_col_list=_split_pipe(g("key_column_list")),
            num_cols_flag=_as_flag(g("column_header_check_flg")),
            num_cols_expected=int(g("no_of_columns", 0)),
            mandatory_col_flag=_as_flag(g("mandatory_column_value_check_flg")),
            mandatory_col_list=_split_pipe(g("mandatory_column_list")),
            duplicate_flag=_as_flag(g("duplicate_value_check_flg")),
            duplicate_key_list=_split_pipe(g("duplicate_check_column_list")),
            date_flag=_as_flag(g("date_format_check_flg")),
            date_col_list=_split_pipe(g("date_format_column_list")),
            date_formats=_split_pipe(g("date_formats")),
            decimal_flag=_as_flag(g("decimal_value_check_flg")),
            decimal_col_list=_split_pipe(g("decimal_value_check_col_list")),
            quote_char=str(g("data_element_enclosure_character", "")),
            header_flag=_as_flag(g("header")),
            delimiter=str(g("file_delimeter", ",") or ",").replace("\\t", "\t"),
            parquet_header=_split_pipe(g("parquet_header")),
            encoding=str(g("encoding", "utf-8")),
            no_validation_flag=_as_flag(g("no_validation_flag")),
            upsert_key_list=_split_pipe(g("parquet_key_col")),
            skiprows=str(g("skiprows", "")),
            service_type=str(g("service", "")),
            param_file_path=str(g("param_file_path", "")),
            jobid_contract=str(g("tidal_job_id_contract", "")),
            jobid_lake=str(g("tidal_job_id_lake", "")),
            BU=str(g("BU", ""))
        )

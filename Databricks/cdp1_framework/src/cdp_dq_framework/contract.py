import re
import pyspark.sql.functions as F


class ContractResult:
    """
    Represents the result of running contract checks on a file or DataFrame.

    Attributes:
        app_id: str
        entity_id: str
        file_name: str
        checks: list
        failed_checks: list

    Methods:
        add(check_name, status, details): Add a check result
        passed: Property indicating overall contract check status ("SUCCESS", "FAILED" or "BYPASSED").
    """
    def __init__(self, app_id, entity_id, file_name):
        self.app_id = app_id
        self.entity_id = entity_id
        self.file_name = file_name
        self.checks = []
        self.failed_checks = []

    def add(self, check_name, status, details):
        if status == "FAILED":
            self.failed_checks.append((check_name, status, details))
        if check_name != "Metadata Consistency":
            self.checks.append((check_name, status, details))

    @property
    def passed(self):
        if len(self.checks) == 1 and self.checks[0][1] == "BYPASSED":
            res = "BYPASSED"
        elif all([c[1] == "SUCCESS" for c in self.checks]):
            res = "SUCCESS"
        else:
            res = "FAILED"
        return res

def strip_glob(p):
	return re.sub(r"\*.[a-zA-Z]*" , "" , p or "")

def check_mandatory_columns(df, c, result, df_cols):
    # legacy: every mandatory col must exist (case-insensitive) and contain no null/empty
    if not c.mandatory_col_flag or not c.mandatory_col_list: return
    print("Mandatory Columns check in progress..")
    lower = {col.casefold(): col for col in df_cols}
    missing = [k for k in c.mandatory_col_list if k.casefold() not in lower]
    if missing:
        print("Mandatory Columns check failed!")
        result.add("Mandatory Columns check", "FAILED" , f"Missing mandatory columns: {missing}")
    else:
        print("Mandatory Columns check passed!")
        result.add("Mandatory Columns check", "SUCCESS", "")

def check_date_formats(df, c, result, df_cols):
    # legacy: every date col must exist (case-insensitive) and contain no null/empty
    if not c.date_flag or not c.date_col_list: return
    print("Date Contract check in progress..")
    lower = {col.casefold(): col for col in df_cols}
    missing = [k for k in c.date_col_list if k.casefold() not in lower]
    if missing:
        print("Date Contract check failed!")
        result.add("Date Contract check", "FAILED", f"Missing date columns: {missing}")
        return
    nulls = [lower[k.casefold()] for k in c.date_col_list
           if df.filter(F.col(lower[k.casefold()]).isNull() | 
                        (F.trim(F.col(lower[k.casefold()]))==F.lit(""))).limit(1).count()]
    if nulls:
        print("Date Contract check failed!")
        result.add("Date Contract check" , "FAILED", f"Null/empty values in {nulls}")
        return
    invalid = []
    for date_col in c.date_col_list:
        actual_col = lower[date_col.casefold()]
        invalid_count = df.filter(
            (F.col(actual_col).cast("string") != "00000000") &
            (F.to_date(F.col(actual_col).cast("string"), c.date_formats).isNull())
        ).limit(1).count()
        if invalid_count:
            invalid.append(actual_col)
    if invalid:
        print("Date Contract check failed!")
        result.add("Date Contract check", "FAILED", f"Invalid date format in columns {invalid}")
    else:
        print("Date Contract check passed!")
        result.add("Date Contract check", "SUCCESS", "")

def check_filename(file_name, c, result):
    # legacy: every file must have a valid filename
    if not c.filename_flag or not c.filename_pattern: return
    print("File Name check in progress..")
    if strip_glob(c.filename_pattern).casefold() in file_name.casefold():
        print("File Name check passed!")
        result.add("File Name check", "SUCCESS", "")
    else:
        print("File Name check failed!")
        result.add("File Name check", "FAILED", f"Expected file name in pattern {c.filename_pattern} but found {file_name}")

def check_file_extension(file_name, c, result):
    # legacy: every file must have a valid extension
    if not c.filename_flag or not c.filename_pattern: return
    print("File Extension check in progress..")
    file_ext = c.filename_pattern.split(".")[1]
    if file_name.casefold().endswith(file_ext.casefold()):
        print("File Extension check passed!")
        result.add("File Extension check", "SUCCESS", "")
    else:
        print("File Extension check failed!")
        result.add("File Extension check", "FAILED", f"Expected {file_name} with a valid extension")

def check_decimal_values(df, c, result, df_cols):
    # incl. mainframe trailing-minus '123-'
    if not c.decimal_flag or not c.decimal_col_list: return
    print("Decimal Value check in progress..")
    lower = {col.casefold(): col for col in df_cols}
    failed = []
    for dcol in c.decimal_col_list:
        real = lower.get(dcol.casefold())
        if real is None:
            failed.append(dcol)
            continue
        val = F.trim(F.col(real))
        norm = F.when(val.rlike("-$"), F.concat(F.lit("-"), F.regexp_replace(val, "-$", ""))).otherwise(val)
        if df.filter(~((val==F.lit(""))|val.isNull()|norm.cast("double").isNotNull())).limit(1).count():
            failed.append(real) 
    if failed:
        print("Decimal Value check failed!")
        result.add("Decimal Value check", "FAILED", f"Invalid decimal columns: {failed}")
    else:
        print("Decimal Value check passed!")
        result.add("Decimal Value check", "SUCCESS", "")

def check_key_columns(df, c, result, df_cols):
    # legacy: every key col must exist (case-insensitive) and contain no null/empty
    if not c.key_col_flag or not c.key_col_list: return
    print("Key Columns check in progress..")
    lower = {col.casefold(): col for col in df_cols}
    missing = [k for k in c.key_col_list if k.casefold() not in lower]
    if missing:
        print("Key Columns check failed!")
        result.add("Key Columns check", "FAILED", f"Missing Key columns: {missing}")
        return
    # Check if key column value is NULL or empty
    invalid = [lower[k.casefold()] for k in c.key_col_list
           if df.filter(F.col(lower[k.casefold()]).isNull() | (F.trim(F.col(lower[k.casefold()]))==F.lit(""))).limit(1).count()]
    if invalid:
        print("Key Columns check failed!")
        result.add("Key Columns check", "FAILED", f"Null/empty values in {invalid}")
    else:
        print("Key Columns check passed!")
        result.add("Key Columns check", "SUCCESS", "")

def check_num_columns(df, c, result, df_cols):
    # legacy: every num col must exist (case-insensitive) and contain no null/empty
    if not c.num_cols_flag or not c.num_cols_expected: return
    print("Number of Columns check in progress..")
    equality = int(c.num_cols_expected) == len(df_cols)
    print(f"No. of columns expected: {int(c.num_cols_expected)}")
    print(f"No. of columns found: {len(df_cols)}")
    if equality:
        print("Number of Columns check passed!")
        result.add("Number of Columns check", "SUCCESS", "")
    else:
        print("Number of Columns check failed!")
        result.add("Number of Columns check", "FAILED", f"Expected {c.num_cols_expected} columns, but found {len(df_cols)} columns")

def check_duplicates(df, c, result, df_cols):
    # legacy: pandas duplicated(subset)
    if not c.duplicate_flag or not c.duplicate_key_list: return
    print("Duplicate check in progress..")
    lower = {col.casefold(): col for col in df_cols}
    cols = [lower.get(k.casefold(), k) for k in c.duplicate_key_list]
    total, distinct = df.count(), df.select(*cols).distinct().count()
    if total == distinct:
        print("Duplicate check passed!")
        result.add("Duplicate check", "SUCCESS", "")
    else:
        print("Duplicate check failed!")
        result.add("Duplicate check", "FAILED", f"Expected {distinct} unique rows, but found total {total} rows")

def run_contract_checks(df, contract, file_name=""):
    """
    Run all contract checks against a Spark DataFrame.

    Args:
        df: Spark DataFrame to validate against contract
        contract: Contract object
        file_name: optional file name
    Returns:
        ContractResult object
    """
    print(f"Running Contract Checks for {file_name}")
    result = ContractResult(contract.app_id, contract.entity_id, file_name)
    if contract.no_validation_flag:  # legacy no_validation_flag bypass
        print("DQ Checks Bypased")
        result.add("File Validation Check", "BYPASSED", "")
        return result
    df_cols = df.columns  # Single Analyze RPC — reused by all checks
    try:
        check_mandatory_columns(df, contract, result, df_cols)
        check_date_formats(df, contract, result, df_cols)
        check_filename(file_name, contract, result)
        check_file_extension(file_name, contract, result)
        check_decimal_values(df, contract, result, df_cols)
        check_num_columns(df, contract, result, df_cols)
        check_key_columns(df, contract, result, df_cols)
        check_duplicates(df, contract, result, df_cols)
    except Exception as e:
        result.failed_checks = []
        result.add("Metadata Consistency", "FAILED", f"Exception: {str(e)[:100]}")
    return result  # .passed == no single check FAILED (legacy overall-status rule)

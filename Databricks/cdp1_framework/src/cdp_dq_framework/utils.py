import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from io import BytesIO
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from pyspark.sql import Window
import pandas as pd

# longest-filename-prefix match (port of read_db.get_data_frm_db)
def strip_glob(p):
	return re.sub(r"\*.[a-zA-Z]*", "", p or "")  # 'PAT270*.csv' -> 'PAT270'

def read_csv(spark, file_path, contract, read_purpose, service_type):
    if service_type == "INF":
        if read_purpose == "contract_check":
            reader = spark.read.option("sep", contract.delimiter).option("encoding", contract.encoding).option("quote", contract.quote_char or '"').option("multiLine", "true").option("mode", "PERMISSIVE").option("header", "true" if contract.header_flag else "false")
            df = reader.csv(file_path)
            if not contract.header_flag and contract.mandatory_col_list:
                df = df.toDF(*contract.mandatory_col_list)
        elif read_purpose == "lake_load":    
            df = spark.read.option("sep", contract.delimiter).csv(file_path)
            df = df.toDF(*contract.parquet_header)
        else:
            raise Exception(f"Invalid read purpose: {read_purpose}")
    else:
        # Set column names based on read purpose
        if read_purpose == "contract_check":
            column_names = contract.mandatory_col_list if not contract.header_flag else None
        elif read_purpose == "lake_load":
            column_names = contract.parquet_header
        else:
            raise Exception(f"Invalid read purpose: {read_purpose}")
        # Read raw file bytes via Spark/UC governed access — no CSV parsing happens here
        binary_df = spark.read.format("binaryFile").load(file_path)
        file_bytes = binary_df.select("content").first()["content"]

        # Hand off to pandas for the actual parsing logic — identical to your original code
        df = pd.read_csv(
            BytesIO(file_bytes),
            sep=contract.delimiter,
            parse_dates=contract.date_col_list if contract.date_flag else False,
            date_format=contract.date_formats,
            names=column_names,
            header=0 if contract.header_flag else None,
            doublequote=contract.quote_char,
            keep_default_na=False,
            encoding=contract.encoding,
            dtype=str,
            na_values=['null', '']
        )
        df = spark.createDataFrame(df.astype(str))
    return df

def read_excel(spark, file_path, contract, read_purpose, service_type):
    if service_type == "INF":
        if read_purpose == "contract_check":
            df = spark.read.format("excel").option("header", "true" if contract.header_flag else "false").laod(file_path)
            if not contract.header_flag and contract.mandatory_col_list:
                df = df.toDF(*contract.mandatory_col_list)
        elif read_purpose == "lake_load":    
            df = spark.read.format("excel").load(file_path)
            df = df.toDF(*contract.parquet_header)
        else:
            raise Exception(f"Invalid read purpose: {read_purpose}")
    else:
        # Set column names based on read purpose
        if read_purpose == "contract_check":
            if contract.header_flag :
                column_names = None
            else:
                column_names = contract.mandatory_col_list
        elif read_purpose == "lake_load":
            column_names = contract.parquet_header
        else:
            raise Exception(f"Invalid read purpose: {read_purpose}")
        # Read raw file bytes via Spark/UC governed access — no CSV parsing happens here
        binary_df = spark.read.format("binaryFile").load(file_path)
        file_bytes = binary_df.select("content").first()["content"]
        # Hand off to pandas for the actual parsing logic — identical to your original code
        df = pd.read_excel(
            BytesIO(file_bytes),
            names=column_names,
            header=0 if contract.header_flag else None,
            keep_default_na=False,
            skiprows=int(contract.skiprows) if contract.skiprows else 0,
            engine='openpyxl'
        )
        df = spark.createDataFrame(df.fillna("").astype(str))
    return df

def read_parquet(spark, file_path):
    sdf = spark.read.parquet(file_path)
    pdf = sdf.toPandas()
    return spark.createDataFrame(pdf.fillna("").astype(str))

def notify_email(email_context):
    smtp_host = "SMTPRELAY.Onetakeda.com"
    smtp_port = 25  # Common port for internal relays (no TLS needed)
    msg = MIMEMultipart("alternative")
    msg["Subject"] = email_context["subject"]
    msg["From"] = "databricks-jobs@takeda.com"  # Use a valid sender address
    msg["To"] = ", ".join(email_context["recipient_list"])
    html_body = f"""
    <p>{email_context["message"].replace("\n", "<br>").replace("\t", "&emsp;")}</p>
    """
    msg.attach(MIMEText(html_body, "html"))
    # No login needed — just connect and send
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.sendmail(msg["From"], email_context["recipient_list"], msg.as_string())
    print("Email sent successfully!")

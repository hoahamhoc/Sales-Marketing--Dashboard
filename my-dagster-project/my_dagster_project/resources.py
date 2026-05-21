import urllib
import json
from dagster import resource
from sqlalchemy import create_engine


# ============================
# Resource 1: SQL Server
# ============================
@resource
def sql_engine(context):
    DB_CONFIG_FILE = r"D:\THIENTRANG\Python file (support for task schedule)\API Key\db_info.json"
    with open(DB_CONFIG_FILE, 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    params = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={cfg['server']};DATABASE={cfg['database']};"
        f"UID={cfg['username']};PWD={cfg['password']};"
        "TrustServerCertificate=YES;"
    )
    connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(params)
    return create_engine(connection_string, fast_executemany=True)


# ============================
# Resource 2: Sales + Lead API
# (dùng chung 1 token)
# ============================
@resource
def sales_lead_api(context):
    TOKEN_FILE = r"D:\THIENTRANG\Python file (support for task schedule)\API Key\Sales_api_key.txt"
    with open(TOKEN_FILE, 'r', encoding='utf-8') as f:
        token = f.read().strip()
    return {"token": token}


# ============================
# Resource 3: Pancake API
# ============================
@resource
def pancake_api(context):
    TOKEN_FILE = r"D:\THIENTRANG\Python file (support for task schedule)\API Key\Pancake_api_key.txt"
    with open(TOKEN_FILE, 'r', encoding='utf-8') as f:
        token = f.read().strip()
    return {"token": token}
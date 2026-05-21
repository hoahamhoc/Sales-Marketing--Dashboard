import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import text, types as satypes
from dagster import asset

SCHEMA_NAME = "lead"
TABLE_NAME  = "dim_stage"

@asset(
    group_name="lead",
    required_resource_keys={"sql_engine", "sales_lead_api"},
    deps=["dim_sales_team"],
)
def dim_stage(context) -> None:
    token  = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # CONFIG
    # =========================
    API_URL = "https://xxxx/v1/crm/leadStages"  
    headers = {
        "Authorization" : f"Bearer {token}",
        "Accept"        : "application/json",
        "Content-Type"  : "application/json",
    }

    # =========================
    # STEP 1: CALL API
    # =========================
    context.log.info("Step 1: Fetching data from leadStages API...")

    response = requests.get(API_URL, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    context.log.info(f"Fetched {len(data)} lead stages")

    # =========================
    # STEP 2: PROCESS DATA
    # =========================
    context.log.info("Step 2: Processing data...")

    df = pd.DataFrame(data)
    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for col in ["color", "background", "index"]:
        if col not in df.columns:
            df[col] = None

    bool_columns = ["is_closed_lost", "is_closed_won", "is_conversion_stage", "is_milestone"]
    for col in bool_columns:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(int)

    DTYPE_MAP = {
        "id"                  : satypes.VARCHAR(50),
        "name"                : satypes.NVARCHAR(200),
        "index"               : satypes.INTEGER,
        "conversion_weight"   : satypes.FLOAT,
        "forecast_category"   : satypes.VARCHAR(50),
        "is_closed_lost"      : satypes.INTEGER,
        "is_closed_won"       : satypes.INTEGER,
        "is_conversion_stage" : satypes.INTEGER,
        "is_milestone"        : satypes.INTEGER,
        "stage_type"          : satypes.VARCHAR(50),
        "order"               : satypes.INTEGER,
        "color"               : satypes.VARCHAR(20),
        "background"          : satypes.VARCHAR(20),
        "updated_at"          : satypes.NVARCHAR(50),
    }

    df_to_save = df[[col for col in DTYPE_MAP.keys() if col in df.columns]]
    context.log.info(f"Total: {len(df_to_save)} stages")

    # =========================
    # STEP 3: LOAD TO SQL SERVER
    # =========================
    context.log.info("Step 3: Loading to SQL Server (TRUNCATE + INSERT)...")

    with engine.begin() as conn:
        try:
            conn.execute(text(f"TRUNCATE TABLE [{SCHEMA_NAME}].[{TABLE_NAME}]"))
        except Exception:
            conn.execute(text(f"DELETE FROM [{SCHEMA_NAME}].[{TABLE_NAME}]"))

        df_to_save.to_sql(
            name      = TABLE_NAME,
            schema    = SCHEMA_NAME,
            con       = conn,
            if_exists = "append",
            index     = False,
            dtype     = DTYPE_MAP,
        )

    context.log.info(f"Inserted {len(df_to_save)} stages into [{SCHEMA_NAME}].[{TABLE_NAME}]")
    if "is_closed_won" in df_to_save.columns:
        context.log.info(f"Closed Won : {df_to_save['is_closed_won'].sum()}")
    if "is_closed_lost" in df_to_save.columns:
        context.log.info(f"Closed Lost: {df_to_save['is_closed_lost'].sum()}")
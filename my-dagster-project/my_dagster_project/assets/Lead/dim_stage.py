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
    # ⚙️ CẤU HÌNH
    # =========================
    API_URL = "https://sapi.btpc.vn/v1/crm/leadStages"
    headers = {
        "Authorization" : f"Bearer {token}",
        "Accept"        : "application/json",
        "Content-Type"  : "application/json",
    }

    # =========================
    # BƯỚC 1: GỌI API
    # =========================
    context.log.info("BƯỚC 1: Lấy dữ liệu từ API leadStages...")

    response = requests.get(API_URL, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    context.log.info(f"✅ Đã lấy {len(data)} lead stages")

    # =========================
    # BƯỚC 2: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 2: Xử lý dữ liệu...")

    df = pd.DataFrame(data)
    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Đảm bảo các cột nullable
    for col in ["color", "background", "index"]:
        if col not in df.columns:
            df[col] = None

    # Bool → 0/1 cho BIT SQL Server
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
    context.log.info(f"✅ Tổng: {len(df_to_save)} stages")

    # =========================
    # BƯỚC 3: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 3: Cập nhật SQL Server (TRUNCATE + INSERT)...")

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

    context.log.info(f"🎯 Đã insert {len(df_to_save)} stages vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
    if "is_closed_won" in df_to_save.columns:
        context.log.info(f"📊 Closed Won : {df_to_save['is_closed_won'].sum()}")
    if "is_closed_lost" in df_to_save.columns:
        context.log.info(f"📊 Closed Lost: {df_to_save['is_closed_lost'].sum()}")
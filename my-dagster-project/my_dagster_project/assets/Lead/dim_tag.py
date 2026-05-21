import requests
import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import VARCHAR, Integer, NVARCHAR
from dagster import asset

SCHEMA_NAME = "crm"
TABLE_NAME  = "dim_tag"

@asset(
    group_name="lead",
    required_resource_keys={"sql_engine", "sales_lead_api"},
    deps=["fact_lead_daily_full_metric_include_page_id"],
)
def dim_tag(context) -> None:
    token  = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    LABELS_URL = "https://sapi.btpc.vn/v1/crm/labels"
    headers    = {
        "Authorization": token,
        "Accept"       : "application/json",
    }

    # =========================
    # BƯỚC 1: GỌI API
    # =========================
    context.log.info("BƯỚC 1: Lấy dữ liệu từ API labels...")

    response = requests.get(LABELS_URL, headers=headers, timeout=30)
    response.encoding = "utf-8"
    response.raise_for_status()
    tags = response.json()

    context.log.info(f"✅ Lấy được {len(tags)} tags")

    # =========================
    # BƯỚC 2: XỬ LÝ DỮ LIỆU
    # =========================
    df = pd.DataFrame([{
        "tag_id"    : t["id"],
        "tag_name"  : t["text"],
        "background": t.get("background", ""),
        "color"     : t.get("color", ""),
        "is_public" : 1 if t.get("public", False) else 0,
        "created_at": t.get("created_at", None),
    } for t in tags])

    context.log.info(f"✅ Tổng: {len(df)} tags")

    # =========================
    # BƯỚC 3: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 3: Cập nhật SQL Server (TRUNCATE + INSERT)...")

    DTYPE_MAP = {
        "tag_id"    : VARCHAR(50),
        "tag_name"  : NVARCHAR(255),
        "background": VARCHAR(50),
        "color"     : VARCHAR(50),
        "is_public" : Integer,
        "created_at": Integer,
    }

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE [{SCHEMA_NAME}].[{TABLE_NAME}]"))

    df.to_sql(
        name      = TABLE_NAME,
        schema    = SCHEMA_NAME,
        con       = engine,
        if_exists = "append",
        index     = False,
        dtype     = DTYPE_MAP,
    )

    context.log.info(f"🎯 Đã insert {len(df)} tags vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
    context.log.info(f"📊 Tags: {df[['tag_id', 'tag_name']].to_string(index=False)}")
import requests
import pandas as pd
from sqlalchemy import text, types as satypes
from dagster import asset

SCHEMA_NAME = "crm"
TABLE_NAME  = "dim_sales_team"

@asset(
    group_name="lead",
    required_resource_keys={"sql_engine", "sales_lead_api"},
)
def dim_sales_team(context) -> None:
    token  = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    API_URL = "https://sapi.btpc.vn/v1/salesTeams"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    # =========================
    # BƯỚC 1: GỌI API
    # =========================
    context.log.info("BƯỚC 1: Lấy dữ liệu từ API salesTeams...")

    response = requests.get(API_URL, headers=headers, timeout=30)
    response.raise_for_status()
    json_data = response.json()

    # =========================
    # BƯỚC 2: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 2: Xử lý dữ liệu...")

    teams_data    = json_data.get("data", []) if isinstance(json_data, dict) else json_data
    unique_persons = {}

    for team in teams_data:
        all_people = team.get("managers", []) + team.get("members", [])
        for person in all_people:
            person_id = person.get("id")
            if person_id and person_id not in unique_persons:
                first_name = person.get("first_name", "")
                last_name  = person.get("last_name", "") or ""
                full_name  = f"{last_name} {first_name}".strip() if last_name else first_name
                unique_persons[person_id] = {
                    "person_id": person_id,
                    "full_name": full_name,
                }

    if not unique_persons:
        raise Exception("❌ Không có dữ liệu persons!")

    df = pd.DataFrame(list(unique_persons.values()))
    df = df.sort_values("full_name").reset_index(drop=True)

    context.log.info(f"✅ Lấy được {len(df)} persons")

    # =========================
    # BƯỚC 3: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 3: Cập nhật SQL Server (DELETE + INSERT)...")

    DTYPE_MAP = {
        "person_id": satypes.NVARCHAR(100),
        "full_name" : satypes.NVARCHAR(255),
    }

    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM [{SCHEMA_NAME}].[{TABLE_NAME}]"))
        df.to_sql(
            name      = TABLE_NAME,
            schema    = SCHEMA_NAME,
            con       = conn,
            if_exists = "append",
            index     = False,
            dtype     = DTYPE_MAP,
        )

    context.log.info(f"🎯 Đã insert {len(df)} persons vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
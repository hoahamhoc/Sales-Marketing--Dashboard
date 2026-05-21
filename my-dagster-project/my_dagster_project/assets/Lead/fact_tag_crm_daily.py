import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from sqlalchemy.types import DATE, VARCHAR, Integer, Float, NVARCHAR
from dagster import asset

SCHEMA_NAME = "crm"
TABLE_NAME  = "fact_tag_crm_daily"

SALES_TEAM_ID = "67b82d1e53f6cc5ba6038e02"

@asset(
    group_name="lead",
    required_resource_keys={"sql_engine", "sales_lead_api"},
    deps=["dim_tag"],
)
def fact_tag_crm_daily(context) -> None:
    token  = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    API_URL         = "https://sapi.btpc.vn/v1/crm/leads/report"
    BEARER_TOKEN    = token
    N_DAYS          = 3
    MAX_WORKERS     = 2
    REQUEST_DELAY   = 1.0
    TIMEOUT_SECONDS = 60
    MAX_RETRIES     = 5
    RETRY_DELAY     = 15

    TARGET_DATES = [
        (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(N_DAYS)
    ]

    context.log.info(f"📅 Fetch {N_DAYS} ngày: {TARGET_DATES[-1]} → {TARGET_DATES[0]}")

    # =========================
    # BƯỚC 1: LẤY TAG_IDS TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Lấy tag_ids từ SQL...")

    with engine.connect() as conn:
        df_tags = pd.read_sql(
            f"SELECT tag_id, tag_name FROM [{SCHEMA_NAME}].[dim_tag]",
            conn
        )

    TAG_IDS = df_tags.to_dict("records")
    context.log.info(f"✅ {len(TAG_IDS)} tags từ [{SCHEMA_NAME}].[dim_tag]")

    if not TAG_IDS:
        raise Exception("❌ Không có tags!")

    # =========================
    # BƯỚC 2: SETUP SESSION & FETCH
    # =========================
    def create_session():
        session = requests.Session()
        session.mount("http://",  requests.adapters.HTTPAdapter())
        session.mount("https://", requests.adapters.HTTPAdapter())
        return session

    headers = {"Authorization": BEARER_TOKEN, "Accept": "application/json"}

    def fetch_data(date_str, tag, session):
        params = {
            "from_date"    : date_str,
            "to_date"      : date_str,
            "sales_team_id": SALES_TEAM_ID,
            "tag_ids"      : tag["tag_id"],
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = session.get(API_URL, headers=headers, params=params, timeout=TIMEOUT_SECONDS)

                if response.status_code == 429:
                    wait = RETRY_DELAY * attempt
                    context.log.warning(f"⚠️ 429 [{date_str}|{tag['tag_name']}] attempt {attempt}/{MAX_RETRIES} → chờ {wait}s...")
                    time.sleep(wait)
                    continue

                if response.status_code in [500, 502, 503, 504]:
                    wait = 5 * attempt
                    context.log.warning(f"⚠️ {response.status_code} [{date_str}|{tag['tag_name']}] → chờ {wait}s...")
                    time.sleep(wait)
                    continue

                if response.status_code != 200:
                    return {"success": False, "date": date_str, "tag": tag["tag_name"], "error": f"HTTP {response.status_code}", "rows": []}

                data         = response.json()
                by_sales_rep = data.get("bySalesRep", {})
                total_new    = data.get("totalNew", 0)
                last_update  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                rows = []
                if by_sales_rep:
                    for sales_rep_id, sales_data in by_sales_rep.items():
                        rows.append({
                            "date"           : date_str,
                            "tag_id"         : tag["tag_id"],
                            "tag_name"       : tag["tag_name"],
                            "sales_rep_id"   : sales_rep_id or None,
                            "totalLead"      : sales_data.get("count", 0),
                            "totalQuantity"  : sales_data.get("totalValue", 0),
                            "lastdate_update": last_update,
                        })

                if not rows and total_new > 0:
                    rows.append({
                        "date"           : date_str,
                        "tag_id"         : tag["tag_id"],
                        "tag_name"       : tag["tag_name"],
                        "sales_rep_id"   : None,
                        "totalLead"      : total_new,
                        "totalQuantity"  : data.get("totalQuantity", 0),
                        "lastdate_update": last_update,
                    })

                return {"success": True, "date": date_str, "tag": tag["tag_name"], "rows": rows}

            except requests.Timeout:
                wait = 10 * attempt
                context.log.warning(f"⏱️ Timeout [{date_str}|{tag['tag_name']}] attempt {attempt}/{MAX_RETRIES} → chờ {wait}s...")
                time.sleep(wait)

            except Exception as e:
                return {"success": False, "date": date_str, "tag": tag["tag_name"], "error": str(e), "rows": []}

        return {"success": False, "date": date_str, "tag": tag["tag_name"], "error": f"Hết {MAX_RETRIES} lần retry", "rows": []}

    # =========================
    # BƯỚC 3: FETCH SONG SONG
    # =========================
    context.log.info("BƯỚC 3: Fetch dữ liệu song song...")

    tasks    = [(d, tag) for d in TARGET_DATES for tag in TAG_IDS]
    sessions = [create_session() for _ in range(MAX_WORKERS)]

    context.log.info(f"📡 Tổng requests: {len(tasks)} ({len(TARGET_DATES)} ngày × {len(TAG_IDS)} tags) | Workers: {MAX_WORKERS}")

    all_rows  = []
    failed    = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {}
        for idx, (d, tag) in enumerate(tasks):
            future = executor.submit(fetch_data, d, tag, sessions[idx % MAX_WORKERS])
            future_to_task[future] = (d, tag)
            time.sleep(REQUEST_DELAY)

        for future in as_completed(future_to_task):
            result    = future.result()
            completed += 1
            all_rows.extend(result.get("rows", []))

            if not result["success"]:
                failed.append(result)
                context.log.warning(f"❌ {result['date']} | {result['tag']} | {result.get('error')}")

            if completed % 20 == 0 or completed == len(tasks):
                context.log.info(f"⏳ {completed}/{len(tasks)} | Rows: {len(all_rows):,}")

    context.log.info(f"✅ Fetch xong {completed} tasks | Thất bại: {len(failed)}")

    if not all_rows:
        raise Exception("❌ Không có dữ liệu!")

    # =========================
    # BƯỚC 4: XỬ LÝ DỮ LIỆU
    # =========================
    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["date", "tag_id", "sales_rep_id"]).reset_index(drop=True)

    context.log.info(f"✅ Tổng: {len(df):,} rows | Tags: {df['tag_id'].nunique()} | Reps: {df['sales_rep_id'].nunique()}")

    # =========================
    # BƯỚC 5: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 5: Cập nhật SQL Server...")

    DTYPE_MAP = {
        "date"           : DATE,
        "tag_id"         : VARCHAR(50),
        "tag_name"       : NVARCHAR(255),
        "sales_rep_id"   : VARCHAR(50),
        "totalLead"      : Integer,
        "totalQuantity"  : Float,
        "lastdate_update": VARCHAR(50),
    }

    with engine.begin() as conn:
        for d in TARGET_DATES:
            result = conn.execute(
                text(f"DELETE FROM [{SCHEMA_NAME}].[{TABLE_NAME}] WHERE CAST([date] AS DATE) = :d"),
                {"d": d}
            )
            context.log.info(f"🗑️ Xóa {result.rowcount} dòng ngày {d}")

    df.to_sql(
        name      = TABLE_NAME,
        schema    = SCHEMA_NAME,
        con       = engine,
        if_exists = "append",
        index     = False,
        dtype     = DTYPE_MAP,
    )

    context.log.info(f"🎯 Đã insert {len(df):,} rows vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
    context.log.info(f"📊 Tổng leads   : {df['totalLead'].sum():,}")
    context.log.info(f"📊 Tổng quantity: {df['totalQuantity'].sum():,.0f}")

    if failed:
        context.log.warning(f"⚠️ {len(failed)} requests thất bại sau {MAX_RETRIES} lần retry:")
        for f in failed:
            context.log.warning(f"   - {f['date']} | {f['tag']} | {f.get('error')}")
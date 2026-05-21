import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text
from sqlalchemy.types import DATE, VARCHAR, Integer, Float
from dagster import asset

SCHEMA_NAME = "crm"
TABLE_NAME  = "fact_lead_daily"

SALES_TEAM_ID = "67b82d1e53f6cc5ba6038e02"

@asset(
    group_name="lead",
    required_resource_keys={"sql_engine", "sales_lead_api"},
    deps=["dim_stage"],
    output_required=False,   # ← thêm dòng này
)
def fact_lead_daily(context) -> None:
    token  = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    API_URL         = "https://sapi.btpc.vn/v1/crm/leads/report"
    BEARER_TOKEN    = token
    N_DAYS          = 5
    MAX_WORKERS     = 10
    TIMEOUT_SECONDS = 60

    TARGET_DATES = [
        (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(N_DAYS)
    ]

    context.log.info(f"📅 Fetch {N_DAYS} ngày: {TARGET_DATES[-1]} → {TARGET_DATES[0]}")

    # =========================
    # BƯỚC 1: LẤY SOURCE_IDS TỪ API
    # =========================
    context.log.info("BƯỚC 1: Lấy source_ids từ API...")

    def create_session():
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=15, pool_maxsize=15)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    session_init = create_session()
    headers = {
        "Authorization": BEARER_TOKEN,
        "Accept"       : "application/json",
        "Content-Type" : "application/json",
    }

    resp = session_init.get(API_URL, headers=headers, params={
        "from_date"    : TARGET_DATES[-1],
        "to_date"      : TARGET_DATES[0],
        "sales_team_id": SALES_TEAM_ID,
    }, timeout=TIMEOUT_SECONDS)

    if resp.status_code != 200:
        raise Exception(f"❌ API trả về HTTP {resp.status_code}")

    by_source  = resp.json().get("bySource", {})
    SOURCE_IDS = list(by_source.keys())

    if not SOURCE_IDS:
        raise Exception("❌ Không có source_id nào trong khoảng ngày này!")

    context.log.info(f"✅ {len(SOURCE_IDS)} source_ids active")

    # =========================
    # BƯỚC 2: FETCH SONG SONG
    # =========================
    context.log.info("BƯỚC 2: Fetch dữ liệu song song...")

    def fetch_data(date_str, source_id, session):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = session.get(API_URL, headers=headers, params={
                    "from_date"    : date_str,
                    "to_date"      : date_str,
                    "source_id"    : source_id,
                    "sales_team_id": SALES_TEAM_ID,
                }, timeout=TIMEOUT_SECONDS)

                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        time.sleep((2 ** attempt) * 2)
                        continue
                    return {"success": False, "date": date_str, "source_id": source_id, "error": "Rate limit", "rows": []}

                if response.status_code in [500, 502, 503, 504]:
                    if attempt < max_retries - 1:
                        time.sleep((2 ** attempt) * 2)
                        continue
                    return {"success": False, "date": date_str, "source_id": source_id, "error": f"HTTP {response.status_code}", "rows": []}

                if response.status_code != 200:
                    return {"success": False, "date": date_str, "source_id": source_id, "error": f"HTTP {response.status_code}", "rows": []}

                data           = response.json()
                by_sales_rep   = data.get("bySalesRep", {})
                total_new      = data.get("totalNew", 0)
                total_quantity = data.get("totalQuantity", 0)
                last_update    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                rows = []
                if by_sales_rep:
                    for sales_rep_id, sales_data in by_sales_rep.items():
                        rows.append({
                            "date"           : date_str,
                            "source_id"      : source_id,
                            "sales_rep_id"   : sales_rep_id if sales_rep_id else None,
                            "totalLead"      : sales_data.get("count", 0),
                            "totalQuantity"  : sales_data.get("totalValue", 0),
                            "lastdate_update": last_update,
                        })

                if not rows and (total_new > 0 or total_quantity > 0):
                    rows.append({
                        "date"           : date_str,
                        "source_id"      : source_id,
                        "sales_rep_id"   : None,
                        "totalLead"      : total_new,
                        "totalQuantity"  : total_quantity,
                        "lastdate_update": last_update,
                    })

                return {"success": True, "date": date_str, "source_id": source_id, "rows": rows}

            except requests.Timeout:
                if attempt < max_retries - 1:
                    time.sleep(10 * (attempt + 1))
                    continue
                return {"success": False, "date": date_str, "source_id": source_id, "error": "Timeout", "rows": []}

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return {"success": False, "date": date_str, "source_id": source_id, "error": str(e), "rows": []}

    tasks    = [(d, source_id) for d in TARGET_DATES for source_id in SOURCE_IDS]
    sessions = [create_session() for _ in range(MAX_WORKERS)]

    context.log.info(f"📡 Tổng requests: {len(tasks)} ({len(TARGET_DATES)} ngày × {len(SOURCE_IDS)} sources)")

    all_rows        = []
    failed_requests = []
    completed       = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {}
        for idx, (date_str, source_id) in enumerate(tasks):
            future = executor.submit(fetch_data, date_str, source_id, sessions[idx % MAX_WORKERS])
            future_to_task[future] = (date_str, source_id)
            time.sleep(0.1)

        for future in as_completed(future_to_task):
            result    = future.result()
            completed += 1
            if result.get("success"):
                all_rows.extend(result.get("rows", []))
            else:
                failed_requests.append(result)
                context.log.warning(f"❌ {result['date']} | {result['source_id'][:12]} | {result.get('error')}")

    context.log.info(f"✅ Fetch xong {completed} tasks | Thất bại: {len(failed_requests)}")

    if not all_rows:
        raise Exception("❌ Không có dữ liệu để insert!")

    # =========================
    # BƯỚC 3: XỬ LÝ DỮ LIỆU
    # =========================
    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(by=["date", "source_id", "sales_rep_id"]).reset_index(drop=True)
    df = df.fillna("")

    context.log.info(f"✅ Tổng: {len(df):,} rows | Sources: {df['source_id'].nunique()} | Reps: {df['sales_rep_id'].nunique()}")

    # =========================
    # BƯỚC 4: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 4: Cập nhật SQL Server...")

    DTYPE_MAP = {
        "date"           : DATE,
        "source_id"      : VARCHAR(50),
        "sales_rep_id"   : VARCHAR(50),
        "totalLead"      : Integer,
        "totalQuantity"  : Float,
        "lastdate_update": VARCHAR(50),
    }

    with engine.begin() as conn:
        for target_date in TARGET_DATES:
            result = conn.execute(text(f"""
                DELETE FROM [{SCHEMA_NAME}].[{TABLE_NAME}]
                WHERE CAST([date] AS DATE) = :d
            """), {"d": target_date})
            context.log.info(f"🗑️ Xóa {result.rowcount} dòng ngày {target_date}")

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

    if failed_requests:
        context.log.warning(f"⚠️ {len(failed_requests)} requests thất bại:")
        for req in failed_requests:
            context.log.warning(f"   - {req['date']} | {req['source_id'][:12]} | {req.get('error')}")


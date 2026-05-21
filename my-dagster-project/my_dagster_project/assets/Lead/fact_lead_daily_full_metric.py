import requests
import pandas as pd
import time
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text
from sqlalchemy.types import DATE, VARCHAR, Integer, Float
from dagster import asset

SCHEMA_NAME      = "crm"
TABLE_NAME_SRC   = "fact_lead_daily"
TABLE_NAME_TGT   = "fact_lead_daily_full_metric"

SALES_TEAM_ID = "67b82d1e53f6cc5ba6038e02"

@asset(
    group_name="lead",
    required_resource_keys={"sql_engine", "sales_lead_api"},
    deps=["fact_lead_daily"],
)
def fact_lead_daily_full_metric(context) -> None:
    token  = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    API_URL         = "https://sapi.btpc.vn/v1/crm/leads/report"
    BEARER_TOKEN    = token
    MAX_WORKERS     = 3
    REQUEST_DELAY   = 0.5
    RETRY_DELAY     = 3.0
    TIMEOUT_SECONDS = 45
    MAX_RETRIES     = 5

    END_DATE   = date.today()
    START_DATE = END_DATE - timedelta(days=1)
    TARGET_DATES = [
        (START_DATE + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((END_DATE - START_DATE).days + 1)
    ]

    context.log.info(f"📅 Từ: {TARGET_DATES[0]} → Đến: {TARGET_DATES[-1]} ({len(TARGET_DATES)} ngày)")

    # =========================
    # BƯỚC 1: LẤY SOURCE & SALES_REP IDs TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Lấy source_ids và sales_rep_ids từ SQL...")

    with engine.connect() as conn:
        date_conditions = " OR ".join([f"CAST([date] AS DATE) = '{d}'" for d in TARGET_DATES])

        source_ids = [row[0] for row in conn.execute(text(f"""
            SELECT DISTINCT source_id FROM [{SCHEMA_NAME}].[{TABLE_NAME_SRC}]
            WHERE source_id IS NOT NULL AND source_id != ''
            AND ({date_conditions})
        """))]

        sales_rep_ids = [row[0] for row in conn.execute(text(f"""
            SELECT DISTINCT sales_rep_id FROM [{SCHEMA_NAME}].[{TABLE_NAME_SRC}]
            WHERE sales_rep_id IS NOT NULL AND sales_rep_id != ''
            AND ({date_conditions})
        """))]

    context.log.info(f"✅ {len(source_ids)} sources | {len(sales_rep_ids)} sales reps")

    if not source_ids or not sales_rep_ids:
        raise Exception("❌ Không có source_id hoặc sales_rep_id!")

    # =========================
    # BƯỚC 2: SETUP SESSION & FETCH
    # =========================
    def create_session():
        session = requests.Session()
        retry_strategy = Retry(
            total=10,
            backoff_factor=3,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=5, pool_maxsize=5)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    headers = {
        "Authorization": BEARER_TOKEN,
        "Accept"       : "application/json",
        "Content-Type" : "application/json",
    }

    def fetch_daily_data(date_str, source_id, sales_rep_id, session):
        params = {
            "from_date"    : date_str,
            "to_date"      : date_str,
            "source_id"    : source_id,
            "sales_team_id": SALES_TEAM_ID,
            "sales_rep_id" : sales_rep_id,
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = session.get(API_URL, headers=headers, params=params, timeout=TIMEOUT_SECONDS)

                if response.status_code == 429:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (attempt + 2))
                        continue
                    return {"success": False, "date": date_str, "source_id": source_id, "sales_rep_id": sales_rep_id, "error": "Rate limit", "rows": []}

                if response.status_code in [500, 502, 503, 504]:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (attempt + 2))
                        continue
                    return {"success": False, "date": date_str, "source_id": source_id, "sales_rep_id": sales_rep_id, "error": f"HTTP {response.status_code}", "rows": []}

                if response.status_code != 200:
                    return {"success": False, "date": date_str, "source_id": source_id, "sales_rep_id": sales_rep_id, "error": f"HTTP {response.status_code}", "rows": []}

                data        = response.json()
                last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                by_stage             = data.get("byStage", {})
                by_customer_type     = data.get("byCustomerType", {})
                total_won            = data.get("totalWon", 0)
                total_lost           = data.get("totalLost", 0)

                new_customer_count      = by_customer_type.get("new",      {}).get("count",      0) if by_customer_type else 0
                existing_customer_count = by_customer_type.get("existing", {}).get("count",      0) if by_customer_type else 0
                new_customer_value      = by_customer_type.get("new",      {}).get("totalValue", 0) if by_customer_type else 0
                existing_customer_value = by_customer_type.get("existing", {}).get("totalValue", 0) if by_customer_type else 0

                num_stages = len(by_stage) if by_stage else 1
                rows = []

                if by_stage and isinstance(by_stage, dict):
                    for stage_id, stage_data in by_stage.items():
                        rows.append({
                            "date"                    : date_str,
                            "source_id"               : source_id,
                            "sales_rep_id"            : sales_rep_id,
                            "stage_id"                : stage_id,
                            "totalLead"               : stage_data.get("count", 0),
                            "totalQuantity"           : stage_data.get("totalValue", 0),
                            "won_count"               : round(total_won              / num_stages, 8),
                            "lost_count"              : round(total_lost             / num_stages, 8),
                            "new_customer_count"      : round(new_customer_count     / num_stages, 8),
                            "existing_customer_count" : round(existing_customer_count / num_stages, 8),
                            "new_customer_value"      : round(new_customer_value     / num_stages, 8),
                            "existing_customer_value" : round(existing_customer_value / num_stages, 8),
                            "lastdate_update"         : last_update,
                        })
                else:
                    rows.append({
                        "date"                    : date_str,
                        "source_id"               : source_id,
                        "sales_rep_id"            : sales_rep_id,
                        "stage_id"                : None,
                        "totalLead"               : data.get("totalNew", 0),
                        "totalQuantity"           : data.get("totalQuantity", 0),
                        "won_count"               : total_won,
                        "lost_count"              : total_lost,
                        "new_customer_count"      : new_customer_count,
                        "existing_customer_count" : existing_customer_count,
                        "new_customer_value"      : new_customer_value,
                        "existing_customer_value" : existing_customer_value,
                        "lastdate_update"         : last_update,
                    })

                return {"success": True, "date": date_str, "source_id": source_id, "sales_rep_id": sales_rep_id, "rows": rows}

            except requests.Timeout:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 3))
                    continue
                return {"success": False, "date": date_str, "source_id": source_id, "sales_rep_id": sales_rep_id, "error": "Timeout", "rows": []}

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 2))
                    continue
                return {"success": False, "date": date_str, "source_id": source_id, "sales_rep_id": sales_rep_id, "error": str(e), "rows": []}

    # =========================
    # BƯỚC 3: FETCH SONG SONG
    # =========================
    context.log.info("BƯỚC 3: Fetch dữ liệu song song...")

    tasks = [
        (date_str, source_id, sales_rep_id)
        for date_str    in TARGET_DATES
        for source_id   in source_ids
        for sales_rep_id in sales_rep_ids
    ]

    context.log.info(f"📡 Tổng requests: {len(tasks):,} | Workers: {MAX_WORKERS}")

    sessions         = [create_session() for _ in range(MAX_WORKERS)]
    all_rows         = []
    failed_requests  = []
    completed        = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {}
        for idx, (date_str, source_id, sales_rep_id) in enumerate(tasks):
            future = executor.submit(fetch_daily_data, date_str, source_id, sales_rep_id, sessions[idx % MAX_WORKERS])
            future_to_task[future] = (date_str, source_id, sales_rep_id)
            time.sleep(REQUEST_DELAY)

        for future in as_completed(future_to_task):
            result    = future.result()
            completed += 1
            if result.get("success"):
                all_rows.extend(result.get("rows", []))
            else:
                failed_requests.append(result)
                context.log.warning(f"⚠️ Failed: {result['date']} | {result.get('error')}")

    context.log.info(f"✅ Fetch xong {completed} tasks | Thất bại: {len(failed_requests)}")

    if not all_rows:
        raise Exception("❌ Không có dữ liệu để insert!")

    # =========================
    # BƯỚC 4: XỬ LÝ DỮ LIỆU
    # =========================
    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.fillna("")
    df = df.sort_values(by=["date", "source_id", "sales_rep_id", "stage_id"]).reset_index(drop=True)

    context.log.info(f"✅ Tổng: {len(df):,} rows | Sources: {df['source_id'].nunique()} | Reps: {df['sales_rep_id'].nunique()} | Stages: {df['stage_id'].nunique()}")

    # =========================
    # BƯỚC 5: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 5: Cập nhật SQL Server...")

    DTYPE_MAP = {
        "date"                    : DATE,
        "source_id"               : VARCHAR(50),
        "sales_rep_id"            : VARCHAR(50),
        "stage_id"                : VARCHAR(50),
        "totalLead"               : Integer,
        "totalQuantity"           : Float,
        "won_count"               : Float,
        "lost_count"              : Float,
        "new_customer_count"      : Float,
        "existing_customer_count" : Float,
        "new_customer_value"      : Float,
        "existing_customer_value" : Float,
        "lastdate_update"         : VARCHAR(50),
    }

    with engine.begin() as conn:
        for target_date in TARGET_DATES:
            result = conn.execute(text(f"""
                DELETE FROM [{SCHEMA_NAME}].[{TABLE_NAME_TGT}]
                WHERE CAST([date] AS DATE) = :d
            """), {"d": target_date})
            context.log.info(f"🗑️ Xóa {result.rowcount} dòng ngày {target_date}")

    df.to_sql(
        name      = TABLE_NAME_TGT,
        schema    = SCHEMA_NAME,
        con       = engine,
        if_exists = "append",
        index     = False,
        dtype     = DTYPE_MAP,
    )

    context.log.info(f"🎯 Đã insert {len(df):,} rows vào [{SCHEMA_NAME}].[{TABLE_NAME_TGT}]!")
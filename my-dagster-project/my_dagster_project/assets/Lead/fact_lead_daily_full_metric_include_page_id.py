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

SCHEMA_NAME    = "crm"
TABLE_NAME_SRC = "fact_lead_daily"
TABLE_NAME_TGT = "fact_lead_daily_full_metric_include_page_id"

SALES_TEAM_ID = "67b82d1e53f6cc5ba6038e02"

@asset(
    group_name="lead",
    required_resource_keys={"sql_engine", "sales_lead_api"},
    deps=["fact_lead_daily_full_metric"],
)
def fact_lead_daily_full_metric_include_page_id(context) -> None:
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
    BATCH_SIZE      = 20

    TARGET_DATES = [
        (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(2)
    ]

    context.log.info(f"📅 Từ: {TARGET_DATES[-1]} → Đến: {TARGET_DATES[0]} ({len(TARGET_DATES)} ngày)")

    # =========================
    # BƯỚC 1: LẤY SOURCE-PAGE PAIRS TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Lấy source-page pairs từ SQL...")

    with engine.connect() as conn:
        result = conn.execute(text("""
            WITH tb1 AS (
                SELECT page_id, page_name, source_id
                FROM [btpc].[fact_sales]
                GROUP BY page_id, source_id, page_name
            ),
            tb2 AS (
                SELECT tb1.*,
                    CASE
                        WHEN page_id='6745580a1ae07022fd0447e2' THEN '535186876644413'
                        WHEN page_id='67455781f30a8c72aa0a2407' THEN '110268431112952'
                        WHEN page_id='674557b7f30a8c72aa0a2408' THEN '1493246357462715'
                        WHEN page_id='674557bfb6ed9e86c0024139' THEN '285665794907218'
                        WHEN page_id='67455756274976cd07095b5c' THEN '101275504989593'
                        WHEN page_id='67455776dd047c63f10ba4b1' THEN '1570656619679594'
                        WHEN page_name LIKE N'Đồng Phục Truyền Thông%Sự Kiện' THEN '106091408006416'
                        WHEN page_id='674557c91ae07022fd0447e1' THEN '100218605206324'
                        WHEN page_id='67455764dd047c63f10ba4b0' THEN '1964077233607607'
                        WHEN page_id='67457f4a11737bcaba09a7db' THEN 'pzl_265186354542135972'
                        WHEN page_id='67511b3261c10c417f0af51f' THEN 'pzl_187217287986577317'
                        WHEN page_id='674557a0b528c3648b02b259' THEN '101275504989593'
                        WHEN page_id='67455888dd047c63f10ba4b4' AND page_name=N'Xưởng Đồng Phục' THEN 'tt_6592548275924025346'
                        ELSE 'None'
                    END AS pancake_page_id
                FROM tb1
            )
            SELECT DISTINCT page_id, source_id
            FROM tb2
            WHERE pancake_page_id <> 'None'
        """))
        source_page_pairs = [(row[1], row[0]) for row in result]  # (source_id, page_id)

    context.log.info(f"✅ {len(source_page_pairs)} cặp (source_id, page_id)")
    context.log.info(f"   Sources: {len(set(p[0] for p in source_page_pairs))} | Pages: {len(set(p[1] for p in source_page_pairs))}")

    if not source_page_pairs:
        raise Exception("❌ Không có source-page pairs!")

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

    def fetch_daily_data(date_str, source_id, page_id, session):
        params = {
            "from_date"    : date_str,
            "to_date"      : date_str,
            "source_id"    : source_id,
            "page_id"      : page_id,
            "sales_team_id": SALES_TEAM_ID,
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = session.get(API_URL, headers=headers, params=params, timeout=TIMEOUT_SECONDS)

                if response.status_code == 429:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (attempt + 2))
                        continue
                    return {"success": False, "date": date_str, "source_id": source_id, "page_id": page_id, "error": "Rate limit", "rows": []}

                if response.status_code in [500, 502, 503, 504]:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (attempt + 2))
                        continue
                    return {"success": False, "date": date_str, "source_id": source_id, "page_id": page_id, "error": f"HTTP {response.status_code}", "rows": []}

                if response.status_code != 200:
                    return {"success": False, "date": date_str, "source_id": source_id, "page_id": page_id, "error": f"HTTP {response.status_code}", "rows": []}

                data        = response.json()
                last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                by_customer_type        = data.get("byCustomerType", {})
                new_customer_count      = by_customer_type.get("new",      {}).get("count",      0) if by_customer_type else 0
                existing_customer_count = by_customer_type.get("existing", {}).get("count",      0) if by_customer_type else 0
                new_customer_value      = by_customer_type.get("new",      {}).get("totalValue", 0) if by_customer_type else 0
                existing_customer_value = by_customer_type.get("existing", {}).get("totalValue", 0) if by_customer_type else 0

                rows = [{
                    "date"                    : date_str,
                    "source_id"               : source_id,
                    "page_id"                 : page_id,
                    "totalLead"               : data.get("totalNew", 0),
                    "totalQuantity"           : data.get("totalQuantity", 0),
                    "new_customer_count"      : new_customer_count,
                    "existing_customer_count" : existing_customer_count,
                    "new_customer_value"      : new_customer_value,
                    "existing_customer_value" : existing_customer_value,
                    "lastdate_update"         : last_update,
                }]

                return {"success": True, "date": date_str, "source_id": source_id, "page_id": page_id, "rows": rows}

            except requests.Timeout:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 3))
                    continue
                return {"success": False, "date": date_str, "source_id": source_id, "page_id": page_id, "error": "Timeout", "rows": []}

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 2))
                    continue
                return {"success": False, "date": date_str, "source_id": source_id, "page_id": page_id, "error": str(e), "rows": []}

    # =========================
    # BƯỚC 3: FETCH SONG SONG
    # =========================
    context.log.info("BƯỚC 3: Fetch dữ liệu song song...")

    tasks = [
        (date_str, source_id, page_id)
        for date_str in TARGET_DATES
        for source_id, page_id in source_page_pairs
    ]

    context.log.info(f"📡 Tổng requests: {len(tasks):,} | Workers: {MAX_WORKERS}")

    sessions        = [create_session() for _ in range(MAX_WORKERS)]
    all_rows        = []
    failed_requests = []
    completed       = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {}
        for idx, (date_str, source_id, page_id) in enumerate(tasks):
            future = executor.submit(fetch_daily_data, date_str, source_id, page_id, sessions[idx % MAX_WORKERS])
            future_to_task[future] = (date_str, source_id, page_id)
            time.sleep(REQUEST_DELAY)

        batch_count = 0
        for future in as_completed(future_to_task):
            result    = future.result()
            completed += 1
            batch_count += 1

            if result.get("success"):
                all_rows.extend(result.get("rows", []))
            else:
                failed_requests.append(result)
                context.log.warning(f"⚠️ Failed: {result['date']} | {result.get('error')}")

            if completed % 20 == 0 or completed == len(tasks):
                context.log.info(f"⏳ {completed}/{len(tasks)} | Rows: {len(all_rows):,}")

    context.log.info(f"✅ Fetch xong {completed} tasks | Thất bại: {len(failed_requests)}")

    if not all_rows:
        raise Exception("❌ Không có dữ liệu để insert!")

    # =========================
    # BƯỚC 4: XỬ LÝ DỮ LIỆU
    # =========================
    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.fillna("")
    df = df.sort_values(by=["date", "source_id", "page_id"]).reset_index(drop=True)

    context.log.info(f"✅ Tổng: {len(df):,} rows | Sources: {df['source_id'].nunique()} | Pages: {df['page_id'].nunique()}")

    # =========================
    # BƯỚC 5: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 5: Cập nhật SQL Server...")

    DTYPE_MAP = {
        "date"                    : DATE,
        "source_id"               : VARCHAR(50),
        "page_id"                 : VARCHAR(50),
        "totalLead"               : Integer,
        "totalQuantity"           : Float,
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
    context.log.info(f"📊 Sources: {df['source_id'].nunique()} | Pages: {df['page_id'].nunique()}")
import requests
import pandas as pd
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text, types as satypes
from dagster import asset

SCHEMA_NAME = "pancake"
TABLE_NAME  = "fact_pages_campaigns"

@asset(
    group_name="pancake",
    required_resource_keys={"sql_engine", "pancake_api"},
    deps=["fact_engagement_staticstic"]
)
def fact_pages_campaigns(context):
    token  = context.resources.pancake_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    ACCESS_TOKEN           = token
    MAX_RETRIES            = 5
    RETRY_BACKOFF          = 3
    TIMEOUT                = 30
    SLEEP_BETWEEN_REQUESTS = 0.5
    MAX_WORKERS            = 5

    END_DATE   = datetime.now()
    START_DATE = END_DATE - timedelta(days=2)

    context.log.info(f"📅 Từ: {START_DATE.strftime('%Y-%m-%d')} → Đến: {END_DATE.strftime('%Y-%m-%d')}")

    # =========================
    # BƯỚC 1: LẤY PAGE TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Lấy danh sách pages từ SQL Server...")

    with engine.connect() as conn:
        df_pages = pd.read_sql(
            "SELECT DISTINCT page_id FROM pancake.dim_pages ORDER BY page_id",
            conn
        )

    PAGE_IDS = df_pages["page_id"].astype(str).tolist()
    context.log.info(f"✅ Lấy được {len(PAGE_IDS)} pages")

    if not PAGE_IDS:
        raise Exception("❌ Không có pages nào!")

    # =========================
    # BƯỚC 2: SETUP SESSION
    # =========================
    thread_local = threading.local()

    def get_session():
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        return thread_local.session

    # =========================
    # BƯỚC 3: HÀM GỌI API
    # =========================
    def fetch_with_retry(page_id, date_range, retry_count=0):
        """
        Returns: (data_list, status)
          status: "success" | "no_data" | "permission_denied" | "failed" | "http_xxx"
        """
        session = get_session()
        url     = f"https://pancake.vn/api/v1/pages/{page_id}/statistics/pages_campaigns"
        params  = {
            "access_token"  : ACCESS_TOKEN,
            "date_range"    : date_range,
            "type_statistic": "day",
            "platform"      : "facebook",
        }

        try:
            response = session.get(url, params=params, timeout=TIMEOUT)

            if response.status_code == 200:
                result = response.json()
                if result.get("success") and result.get("data"):
                    return result["data"], "success"
                return [], "no_data"

            elif response.status_code == 403:
                return [], "permission_denied"

            elif response.status_code == 429:
                wait = int(response.headers.get("Retry-After", RETRY_BACKOFF ** (retry_count + 1)))
                context.log.warning(f"⏳ Rate limit [{page_id}]! Chờ {wait}s...")
                time.sleep(wait)
                if retry_count < MAX_RETRIES:
                    return fetch_with_retry(page_id, date_range, retry_count + 1)
                return [], "failed"

            elif response.status_code in [500, 502, 503, 504]:
                if retry_count < MAX_RETRIES:
                    wait = RETRY_BACKOFF ** retry_count
                    time.sleep(wait)
                    return fetch_with_retry(page_id, date_range, retry_count + 1)
                return [], "failed"

            else:
                return [], f"http_{response.status_code}"

        except requests.exceptions.Timeout:
            if retry_count < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** retry_count)
                return fetch_with_retry(page_id, date_range, retry_count + 1)
            return [], "failed"

        except Exception as e:
            context.log.error(f"💥 [{page_id}] {e}")
            if retry_count < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** retry_count)
                return fetch_with_retry(page_id, date_range, retry_count + 1)
            return [], "failed"

    DTYPE_MAP = {
        "fetch_status"                      : satypes.NVARCHAR(20),
        "date"                              : satypes.DATE,
        "account_id"                        : satypes.NVARCHAR(50),
        "page_id"                           : satypes.NVARCHAR(50),
        "camp_id"                           : satypes.NVARCHAR(50),
        "camp_name"                         : satypes.NVARCHAR(255),
        "ad_id"                             : satypes.NVARCHAR(50),
        "ad_status"                         : satypes.NVARCHAR(50),
        "status"                            : satypes.NVARCHAR(50),
        "type"                              : satypes.NVARCHAR(50),
        "currency"                          : satypes.NVARCHAR(10),
        "daily_budget"                      : satypes.BIGINT,
        "lifetime_budget"                   : satypes.BIGINT,
        "budget_remaining"                  : satypes.BIGINT,
        "spend"                             : satypes.BIGINT,
        "impressions"                       : satypes.BIGINT,
        "reach"                             : satypes.BIGINT,
        "link_click"                        : satypes.BIGINT,
        "ctr"                               : satypes.FLOAT,
        "cpc"                               : satypes.FLOAT,
        "cpm"                               : satypes.FLOAT,
        "post_comments"                     : satypes.BIGINT,
        "messaging_conversation_started_7d" : satypes.BIGINT,
        "messaging_first_reply"             : satypes.BIGINT,
        "lead_events"                       : satypes.BIGINT,
        "purchases"                         : satypes.BIGINT,
        "purchases_conversion_value"        : satypes.BIGINT,
        "purchase_roas"                     : satypes.FLOAT,
        "updated_at"                        : satypes.NVARCHAR(50),
    }

    COLUMN_ORDER = [
        "fetch_status", "date", "account_id", "page_id",
        "camp_id", "camp_name", "ad_id", "ad_status", "status", "type",
        "currency",
        "daily_budget", "lifetime_budget", "budget_remaining",
        "spend", "impressions", "reach", "link_click",
        "ctr", "cpc", "cpm",
        "post_comments", "messaging_conversation_started_7d",
        "messaging_first_reply", "lead_events",
        "purchases", "purchases_conversion_value", "purchase_roas",
        "updated_at",
    ]

    NUMERIC_COLS = [
        "budget_remaining", "daily_budget", "lifetime_budget",
        "spend", "impressions", "reach", "link_click",
        "post_comments", "messaging_conversation_started_7d",
        "messaging_first_reply", "lead_events", "purchases",
        "purchases_conversion_value",
    ]

    FLOAT_COLS = ["cpc", "cpm", "ctr", "purchase_roas"]

    def make_placeholder_row(page_id, date, fetch_status):
        return {
            "fetch_status"                      : fetch_status,
            "date"                              : date.strftime("%Y-%m-%d"),
            "page_id"                           : page_id,
            "account_id"                        : None,
            "camp_id"                           : None,
            "camp_name"                         : None,
            "ad_id"                             : None,
            "ad_status"                         : None,
            "status"                            : None,
            "type"                              : None,
            "currency"                          : None,
            "daily_budget"                      : 0,
            "lifetime_budget"                   : 0,
            "budget_remaining"                  : 0,
            "spend"                             : 0,
            "impressions"                       : 0,
            "reach"                             : 0,
            "link_click"                        : 0,
            "ctr"                               : 0.0,
            "cpc"                               : 0.0,
            "cpm"                               : 0.0,
            "post_comments"                     : 0,
            "messaging_conversation_started_7d" : 0,
            "messaging_first_reply"             : 0,
            "lead_events"                       : 0,
            "purchases"                         : 0,
            "purchases_conversion_value"        : 0,
            "purchase_roas"                     : 0.0,
            "updated_at"                        : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def fetch_task(page_id, date):
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        date_str   = date.strftime("%d/%m/%Y")
        date_range = f"{date_str} 00:00:00 - {date_str} 23:59:59"

        data, status = fetch_with_retry(page_id, date_range)
        rows = []

        if status == "success":
            for record in data:
                if record.get("account_id") and record.get("name"):
                    record["page_id"]      = page_id
                    record["date"]         = date.strftime("%Y-%m-%d")
                    record["fetch_status"] = "success"
                    rows.append(record)

        elif status == "no_data":
            rows.append(make_placeholder_row(page_id, date, "success"))

        elif status == "permission_denied":
            rows.append(make_placeholder_row(page_id, date, "error"))

        else:
            # failed / http_xxx → trả None để retry
            return page_id, date, None

        return page_id, date, rows

    # =========================
    # BƯỚC 4: FETCH SONG SONG
    # =========================
    context.log.info("BƯỚC 4: Fetch dữ liệu song song...")

    dates_list = []
    cur = START_DATE
    while cur <= END_DATE:
        dates_list.append(cur)
        cur += timedelta(days=1)

    tasks = [
        (page_id, date)
        for page_id in PAGE_IDS
        for date in dates_list
    ]

    all_data     = []
    failed_tasks = []
    completed    = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(fetch_task, page_id, date): (page_id, date)
            for page_id, date in tasks
        }
        for future in as_completed(future_to_task):
            completed += 1
            try:
                page_id, date, rows = future.result()
                if rows is None:
                    failed_tasks.append((page_id, date))
                else:
                    all_data.extend(rows)
            except Exception as e:
                page_id, date = future_to_task[future]
                context.log.error(f"💥 [{page_id}|{date.strftime('%d/%m/%Y')}]: {e}")
                failed_tasks.append((page_id, date))

    context.log.info(f"✅ Fetch xong {completed} tasks | Thất bại: {len(failed_tasks)}")

    # =========================
    # BƯỚC 4b: RETRY TUẦN TỰ
    # =========================
    if failed_tasks:
        context.log.info(f"🔁 Retry {len(failed_tasks)} tasks thất bại (chờ 30s)...")
        time.sleep(30)

        for page_id, date in failed_tasks:
            date_str   = date.strftime("%d/%m/%Y")
            date_range = f"{date_str} 00:00:00 - {date_str} 23:59:59"
            data, status = fetch_with_retry(page_id, date_range)

            if status == "success":
                for record in data:
                    if record.get("account_id") and record.get("name"):
                        record["page_id"]      = page_id
                        record["date"]         = date.strftime("%Y-%m-%d")
                        record["fetch_status"] = "success"
                        all_data.append(record)
            elif status in ["no_data", "permission_denied"]:
                fetch_st = "success" if status == "no_data" else "error"
                all_data.append(make_placeholder_row(page_id, date, fetch_st))
            else:
                context.log.warning(f"⚠️ Retry vẫn thất bại [{page_id}|{date_str}] → placeholder (error)")
                all_data.append(make_placeholder_row(page_id, date, "error"))

            time.sleep(SLEEP_BETWEEN_REQUESTS)

    if not all_data:
        raise Exception("❌ Không có dữ liệu để lưu!")

    # =========================
    # BƯỚC 5: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 5: Xử lý dữ liệu...")

    df = pd.DataFrame(all_data)

    # Rename notebook → SQL
    df.rename(columns={"id": "camp_id", "name": "camp_name"}, inplace=True)

    # Drop cột thừa
    for col in ["adset_id", "proposals"]:
        if col in df.columns:
            df.drop(columns=col, inplace=True)

    # Ép kiểu
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("float64")

    # Chuẩn hoá date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Sắp xếp cột & xử lý duplicate
    df = df[[col for col in COLUMN_ORDER if col in df.columns]]
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    # Lọc chỉ các cột có trong DTYPE_MAP
    df        = df[[col for col in df.columns if col in DTYPE_MAP]]
    dtype_map = {k: v for k, v in DTYPE_MAP.items() if k in df.columns}

    context.log.info(f"✅ Tổng: {len(df)} dòng | {df.columns.tolist()}")

    # =========================
    # BƯỚC 6: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 6: Cập nhật SQL Server...")

    delete_date = START_DATE.strftime("%Y-%m-%d")

    with engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}
            WHERE CAST(date AS DATE) >= :d
        """), {"d": delete_date})
        context.log.info(f"✅ Đã xóa dữ liệu từ {delete_date}")

        df.to_sql(
            name      = TABLE_NAME,
            schema    = SCHEMA_NAME,
            con       = conn,
            if_exists = "append",
            index     = False,
            dtype     = dtype_map,
        )

    context.log.info(f"🎯 Đã insert {len(df)} dòng vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
    context.log.info(f"📊 Pages    : {df['page_id'].nunique() if 'page_id' in df.columns else 'N/A'}")
    context.log.info(f"📊 Campaigns: {df['camp_id'].nunique() if 'camp_id' in df.columns else 'N/A'}")
    if "spend" in df.columns:
        context.log.info(f"📊 Tổng spend: {df['spend'].sum():,.0f} VND")
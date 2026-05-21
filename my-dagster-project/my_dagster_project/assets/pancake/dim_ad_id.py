import requests
import pandas as pd
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text, types as satypes
from dagster import asset

SCHEMA_NAME = "pancake"
TABLE_NAME  = "dim_ad_id"

@asset(
    group_name="pancake",
    required_resource_keys={"sql_engine", "pancake_api"},
    deps=["fact_tag_staticstic"]
)
def dim_ad_id(context):
    token  = context.resources.pancake_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    ACCESS_TOKEN           = token
    DAYS_BACK              = 3
    MAX_WORKERS            = 10
    SLEEP_BETWEEN_REQUESTS = 0.3
    MAX_RETRIES            = 3
    RETRY_BACKOFF          = 2

    today      = datetime.combine(datetime.now().date(), datetime.min.time())
    start_date = today - timedelta(days=DAYS_BACK - 1)

    DATE_LIST = []
    current = start_date
    while current <= today:
        DATE_LIST.append(current.strftime("%d/%m/%Y"))
        current += timedelta(days=1)

    context.log.info(f"📅 Từ: {DATE_LIST[0]} → Đến: {DATE_LIST[-1]} ({len(DATE_LIST)} ngày)")

    # =========================
    # BƯỚC 1: LẤY PAGE TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Lấy danh sách pages từ SQL Server...")

    with engine.connect() as conn:
        df_pages = pd.read_sql(
            "SELECT page_id, platform FROM pancake.dim_pages ORDER BY page_id",
            conn
        )

    PAGES = df_pages[["page_id", "platform"]].to_dict(orient="records")
    context.log.info(f"✅ Lấy được {len(PAGES)} pages")

    if not PAGES:
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
    def fetch_with_retry(url, params, retry_count=0):
        session = get_session()
        try:
            resp = session.get(url, params=params, timeout=30)

            if resp.status_code == 200:
                result = resp.json()
                if isinstance(result, dict) and result.get("data"):
                    return result, "success"
                return None, "no_data"

            elif resp.status_code in (429, 500, 502, 503, 504):
                if retry_count < MAX_RETRIES:
                    wait = RETRY_BACKOFF ** retry_count
                    context.log.warning(f"⚠️ HTTP {resp.status_code} | Retry {retry_count+1}/{MAX_RETRIES} sau {wait}s...")
                    time.sleep(wait)
                    return fetch_with_retry(url, params, retry_count + 1)
                return None, "failed"

            else:
                return None, f"http_{resp.status_code}"

        except requests.exceptions.Timeout:
            if retry_count < MAX_RETRIES:
                wait = RETRY_BACKOFF ** retry_count
                time.sleep(wait)
                return fetch_with_retry(url, params, retry_count + 1)
            return None, "failed"

        except Exception as e:
            context.log.error(f"💥 {str(e)[:60]}")
            if retry_count < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** retry_count)
                return fetch_with_retry(url, params, retry_count + 1)
            return None, "failed"

    def fetch_ad_ids_by_date(page_id, platform, date_str):
        url    = f"https://pancake.vn/api/v1/pages/{page_id}/statistics/ads"
        params = {
            "access_token"  : ACCESS_TOKEN,
            "date_range"    : f"{date_str} 00:00:00 - {date_str} 23:59:59",
            "type_statistic": "day",
            "type"          : "by_id",
        }

        result, status = fetch_with_retry(url, params)
        if status != "success":
            return [], status

        records = []
        for item in result["data"]:
            if item.get("ad_id"):
                records.append({
                    "date"    : datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d"),
                    "page_id" : page_id,
                    "platform": platform,
                    "ad_id"   : str(item["ad_id"]),
                })
        return records, "success"

    def fetch_task(page_id, platform, date_str):
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        records, status = fetch_ad_ids_by_date(page_id, platform, date_str)
        if status == "success" and records:
            context.log.info(f"✅ [{page_id}|{date_str}] {len(records)} ads")
        elif status == "no_data":
            context.log.warning(f"⚠️ [{page_id}|{date_str}] Không có data")
        else:
            context.log.warning(f"❌ [{page_id}|{date_str}] {status}")
        return page_id, platform, date_str, records, status

    # =========================
    # BƯỚC 4: FETCH SONG SONG
    # =========================
    context.log.info("BƯỚC 4: Fetch dữ liệu song song...")

    tasks = [
        (str(page["page_id"]), page["platform"], date_str)
        for page in PAGES
        for date_str in DATE_LIST
    ]

    context.log.info(f"📡 Tổng API calls: {len(tasks)}")

    all_records  = []
    failed_tasks = []
    completed    = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(fetch_task, page_id, platform, date_str): (page_id, platform, date_str)
            for page_id, platform, date_str in tasks
        }
        for future in as_completed(future_to_task):
            completed += 1
            page_id, platform, date_str = future_to_task[future]
            try:
                _, _, _, records, status = future.result()
                if records:
                    all_records.extend(records)
                elif status == "failed":
                    failed_tasks.append((page_id, platform, date_str))
            except Exception as e:
                context.log.error(f"💥 [{page_id}|{date_str}] Unhandled: {e}")
                failed_tasks.append((page_id, platform, date_str))

    context.log.info(f"✅ Fetch xong {completed} tasks | Thất bại: {len(failed_tasks)}")

    # =========================
    # BƯỚC 4b: RETRY TUẦN TỰ
    # =========================
    if failed_tasks:
        context.log.info(f"🔁 Retry {len(failed_tasks)} tasks (chờ 30s)...")
        time.sleep(30)

        retry_success = 0
        for page_id, platform, date_str in failed_tasks:
            records, status = fetch_ad_ids_by_date(page_id, platform, date_str)
            if records:
                all_records.extend(records)
                context.log.info(f"✅ Retry thành công [{page_id}|{date_str}]: {len(records)} ads")
                retry_success += 1
            else:
                context.log.warning(f"✗ Retry vẫn thất bại [{page_id}|{date_str}]: {status}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        context.log.info(f"📊 Kết quả retry: {retry_success}/{len(failed_tasks)} thành công")
    else:
        context.log.info("✅ Không có task nào thất bại!")

    if not all_records:
        raise Exception("❌ Không có data. Kiểm tra lại access_token!")

    # =========================
    # BƯỚC 5: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 5: Xử lý dữ liệu...")

    df = pd.DataFrame(all_records)
    df["last_date_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df.drop_duplicates(subset=["date", "page_id", "ad_id"])

    context.log.info(f"✅ Tổng: {len(df)} records | Pages: {df['page_id'].nunique()} | Ads: {df['ad_id'].nunique()}")

    # =========================
    # BƯỚC 6: UPSERT SQL SERVER
    # =========================
    context.log.info("BƯỚC 6: Upsert vào SQL Server...")

    upsert_sql = """
    MERGE pancake.dim_ad_id AS target
    USING (VALUES (:date, :page_id, :platform, :ad_id, :last_date_update))
          AS source (date, page_id, platform, ad_id, last_date_update)
    ON  target.date    = source.date
    AND target.page_id = source.page_id
    AND target.ad_id   = source.ad_id
    WHEN MATCHED THEN
        UPDATE SET
            platform         = source.platform,
            last_date_update = source.last_date_update
    WHEN NOT MATCHED THEN
        INSERT (date, page_id, platform, ad_id, last_date_update)
        VALUES (source.date, source.page_id, source.platform, source.ad_id, source.last_date_update);
    """

    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text(upsert_sql), records)

    context.log.info(f"🎯 Đã upsert {len(records)} records vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
    context.log.info(f"📊 Pages: {df['page_id'].nunique()} | Ads: {df['ad_id'].nunique()}")
    context.log.info(f"📅 Từ {df['date'].min()} → {df['date'].max()}")
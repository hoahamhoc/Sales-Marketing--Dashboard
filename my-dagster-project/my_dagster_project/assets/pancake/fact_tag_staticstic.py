import requests
import pandas as pd
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text, types as satypes
from dagster import asset

SCHEMA_NAME = "pancake"
TABLE_NAME  = "fact_tag_staticstic"

@asset(
    group_name="pancake",
    required_resource_keys={"sql_engine", "pancake_api"},
    deps=["fact_pages_campaigns"]
)
def fact_tag_staticstic(context):
    token  = context.resources.pancake_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    BASE_URL               = "https://pancake.vn/api/v1/statistics/tags"
    ACCESS_TOKEN           = token
    DAYS_BACK              = 2
    MAX_WORKERS            = 10
    SLEEP_BETWEEN_REQUESTS = 0.3
    MAX_RETRIES            = 3
    RETRY_BACKOFF          = 2

    today    = datetime.now().date()
    end_dt   = datetime.combine(today, datetime.min.time())
    start_dt = datetime.combine(today - timedelta(days=DAYS_BACK - 1), datetime.min.time())

    context.log.info(f"📅 Từ: {start_dt.strftime('%Y-%m-%d')} → Đến: {end_dt.strftime('%Y-%m-%d')}")

    # =========================
    # BƯỚC 1: LẤY PAGE TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Lấy danh sách pages từ SQL Server...")

    with engine.connect() as conn:
        df_pages = pd.read_sql(
            "SELECT page_id, platform, page_name FROM pancake.dim_pages ORDER BY platform, page_id",
            conn
        )

    context.log.info(f"✅ Lấy được {len(df_pages)} pages")
    context.log.info(f"   Facebook     : {len(df_pages[df_pages['platform'] == 'facebook'])}")
    context.log.info(f"   TikTok       : {len(df_pages[df_pages['platform'] == 'tiktok'])}")
    context.log.info(f"   Personal Zalo: {len(df_pages[df_pages['platform'] == 'personal_zalo'])}")

    if df_pages.empty:
        raise Exception("❌ Không có pages nào!")

    # Lọc bỏ TikTok (API không hỗ trợ)
    filtered_df = df_pages[~df_pages["page_id"].astype(str).str.startswith("tt_")].copy().reset_index(drop=True)
    skipped     = df_pages[df_pages["page_id"].astype(str).str.startswith("tt_")]
    if not skipped.empty:
        context.log.warning(f"⚠️ Bỏ qua {len(skipped)} TikTok pages: {skipped['page_id'].tolist()}")

    context.log.info(f"📄 Pages xử lý: {len(filtered_df)}")

    # =========================
    # BƯỚC 2: SETUP SESSION
    # =========================
    thread_local = threading.local()

    def get_session():
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        return thread_local.session

    # =========================
    # BƯỚC 3: HÀM GỌI API & PARSE
    # =========================
    def fetch_with_retry(page_id, date_range, max_retries=MAX_RETRIES, backoff_factor=RETRY_BACKOFF):
        session    = get_session()
        api_params = {
            "page_ids"      : page_id,
            "date_range"    : date_range,
            "access_token"  : ACCESS_TOKEN,
            "statistic_type": "day",
        }

        for attempt in range(1, max_retries + 1):
            try:
                response = session.get(BASE_URL, params=api_params, timeout=30)

                if response.status_code == 200:
                    result = response.json()
                    if result.get("success", False):
                        return result, "success"
                    return None, f"api_error: {result.get('message', 'Unknown')}"

                elif response.status_code in (429, 500, 502, 503, 504):
                    wait = backoff_factor ** attempt
                    context.log.warning(f"⚠️ [{page_id}] HTTP {response.status_code} | Retry {attempt}/{max_retries} sau {wait}s...")
                    time.sleep(wait)

                else:
                    return None, f"http_{response.status_code}"

            except requests.exceptions.Timeout:
                wait = backoff_factor ** attempt
                context.log.warning(f"⏱️ [{page_id}] Timeout | Retry {attempt}/{max_retries} sau {wait}s...")
                time.sleep(wait)

            except Exception as e:
                wait = backoff_factor ** attempt
                context.log.error(f"💥 [{page_id}] {str(e)[:60]} | Retry {attempt}/{max_retries} sau {wait}s...")
                time.sleep(wait)

        return None, f"failed_after_{max_retries}_retries"

    def parse_response(response_data, page_id, page_name):
        rows         = []
        data         = response_data.get("data", {})
        tags_list    = response_data.get("tags_list", {})
        categories   = data.get("categories", [])
        series       = data.get("series", {})
        current_year = datetime.now().year

        # Map index → ngày yyyy-mm-dd
        date_map = {}
        for i, d in enumerate(categories):
            try:
                date_obj = datetime.strptime(f"{d}/{current_year}", "%d/%m/%Y")
                if date_obj > datetime.now():
                    date_obj = datetime.strptime(f"{d}/{current_year - 1}", "%d/%m/%Y")
                date_map[i] = date_obj.strftime("%Y-%m-%d")
            except Exception:
                date_map[i] = d

        for tag_key, values_list in series.items():
            if not str(tag_key).startswith("tag_"):
                continue

            numeric_id_str = tag_key.replace("tag_", "")
            tag_meta       = tags_list.get(numeric_id_str, {})
            tag_numeric_id = tag_meta.get("id", None)
            tag_text       = tag_meta.get("text", "").strip()

            for idx, values in enumerate(values_list):
                rows.append({
                    "page_id"       : page_id,
                    "page_name"     : page_name,
                    "ngay"          : date_map.get(idx, ""),
                    "tag_id"        : tag_key,
                    "tag_numeric_id": tag_numeric_id,
                    "tag_name"      : tag_text,
                    "pin"           : values.get("pin", 0),
                    "total"         : values.get("total", 0),
                })

        return rows

    def fetch_task(page_id, page_name, target_date):
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        date_str   = target_date.strftime("%d/%m/%Y")
        date_range = f"{date_str} 00:00:00 - {date_str} 23:59:59"

        result, status = fetch_with_retry(page_id, date_range)

        if status == "success":
            rows = parse_response(result, page_id, page_name)
            return page_id, page_name, target_date, rows, status
        else:
            return page_id, page_name, target_date, [], status

    # =========================
    # BƯỚC 4: FETCH SONG SONG
    # =========================
    context.log.info("BƯỚC 4: Fetch dữ liệu song song...")

    date_list = [start_dt + timedelta(days=i) for i in range(DAYS_BACK)]
    tasks = [
        (str(row["page_id"]), row.get("page_name", ""), date)
        for _, row in filtered_df.iterrows()
        for date in date_list
    ]

    context.log.info(f"📡 Tổng requests: {len(tasks)} ({len(filtered_df)} pages × {len(date_list)} ngày)")

    all_rows     = []
    failed_tasks = []
    completed    = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(fetch_task, page_id, page_name, date): (page_id, page_name, date)
            for page_id, page_name, date in tasks
        }
        for future in as_completed(future_to_task):
            completed += 1
            page_id, page_name, date = future_to_task[future]
            try:
                _, _, _, rows, status = future.result()
                if status == "success":
                    all_rows.extend(rows)
                else:
                    context.log.warning(f"❌ [{page_id}|{date.strftime('%Y-%m-%d')}] {status}")
                    failed_tasks.append((page_id, page_name, date))
            except Exception as e:
                context.log.error(f"💥 [{page_id}|{date.strftime('%Y-%m-%d')}] Unhandled: {e}")
                failed_tasks.append((page_id, page_name, date))

    context.log.info(f"✅ Fetch xong {completed} tasks | Thất bại: {len(failed_tasks)}")

    # =========================
    # BƯỚC 4b: RETRY TUẦN TỰ
    # =========================
    if failed_tasks:
        context.log.info(f"🔁 Retry {len(failed_tasks)} tasks (chờ 30s)...")
        time.sleep(30)

        retry_success = 0
        for page_id, page_name, date in failed_tasks:
            date_str   = date.strftime("%d/%m/%Y")
            date_range = f"{date_str} 00:00:00 - {date_str} 23:59:59"

            result, status = fetch_with_retry(page_id, date_range, max_retries=5, backoff_factor=3)
            if status == "success":
                rows = parse_response(result, page_id, page_name)
                all_rows.extend(rows)
                context.log.info(f"✅ Retry thành công [{page_id}|{date_str}]: {len(rows)} dòng")
                retry_success += 1
            else:
                context.log.warning(f"✗ Retry vẫn thất bại [{page_id}|{date_str}]: {status}")

            time.sleep(SLEEP_BETWEEN_REQUESTS)

        context.log.info(f"📊 Kết quả retry: {retry_success}/{len(failed_tasks)} thành công")
    else:
        context.log.info("✅ Không có task nào thất bại!")

    if not all_rows:
        raise Exception("❌ Không có dữ liệu để lưu!")

    # =========================
    # BƯỚC 5: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 5: Xử lý dữ liệu...")

    df = pd.DataFrame(all_rows)

    df["ngay"]        = pd.to_datetime(df["ngay"]).dt.strftime("%Y-%m-%d")
    df["last_update"] = pd.to_datetime(datetime.now().strftime("%Y-%m-%d %H:%M:00"))

    col_order = ["page_id", "page_name", "ngay", "tag_id", "tag_numeric_id", "tag_name", "pin", "total", "last_update"]
    df = df[[c for c in col_order if c in df.columns]]
    df = df.sort_values(["page_id", "ngay", "tag_numeric_id"]).reset_index(drop=True)

    context.log.info(f"✅ Tổng: {len(df)} dòng | Pages: {df['page_id'].nunique()} | Tags: {df['tag_id'].nunique()}")

    # =========================
    # BƯỚC 6: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 6: Cập nhật SQL Server...")

    min_date = df["ngay"].min()

    DTYPE_MAP = {
        "page_id"       : satypes.NVARCHAR(100),
        "page_name"     : satypes.NVARCHAR(500),
        "ngay"          : satypes.DATE,
        "tag_id"        : satypes.NVARCHAR(100),
        "tag_numeric_id": satypes.INTEGER,
        "tag_name"      : satypes.NVARCHAR(500),
        "pin"           : satypes.INTEGER,
        "total"         : satypes.INTEGER,
        "last_update"   : satypes.DATETIME,
    }

    df["ngay"] = pd.to_datetime(df["ngay"])

    with engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}
            WHERE ngay >= :min_date
        """), {"min_date": min_date})
        context.log.info(f"✅ Đã xóa dữ liệu từ {min_date}")

        df.to_sql(
            name      = TABLE_NAME,
            schema    = SCHEMA_NAME,
            con       = conn,
            if_exists = "append",
            index     = False,
            dtype     = DTYPE_MAP,
            chunksize = 1000,
        )

    context.log.info(f"🎯 Đã insert {len(df)} dòng vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
    context.log.info(f"📊 Pages  : {df['page_id'].nunique()}")
    context.log.info(f"📊 Tags   : {df['tag_id'].nunique()}")
    context.log.info(f"📅 Từ {df['ngay'].min()} → {df['ngay'].max()}")
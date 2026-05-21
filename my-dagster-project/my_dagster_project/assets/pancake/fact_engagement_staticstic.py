import requests
import pandas as pd
import urllib
import time
import threading
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text, types as satypes
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from dagster import asset

SCHEMA_NAME = "pancake"
TABLE_NAME  = "fact_engagement_staticstic"

@asset(
    group_name="pancake",
    required_resource_keys={"sql_engine", "pancake_api"},
    deps=["fact_salesrep_staticstic"]
)
def fact_engagement_staticstic(context):
    token  = context.resources.pancake_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    BASE_URL     = "https://pancake.vn/api/v1/statistics/customer_engagements"
    ACCESS_TOKEN = token

    MAX_RETRIES            = 3
    RETRY_BACKOFF          = 2
    TIMEOUT                = 60
    SLEEP_BETWEEN_REQUESTS = 0.5
    MAX_WORKERS            = 5
    RATE_LIMIT_LOCK        = threading.Lock()

    END_DATE   = datetime.now().strftime("%Y-%m-%d")
    START_DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

    context.log.info(f"📅 Từ: {START_DATE} → Đến: {END_DATE}")

    # =========================
    # BƯỚC 1: LẤY PAGE TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Lấy danh sách pages từ SQL Server...")

    with engine.connect() as conn:
        df_pages = pd.read_sql(
            "SELECT page_id, platform, page_name FROM pancake.dim_pages ORDER BY page_id",
            conn
        )

    PAGE_IDS = df_pages["page_id"].tolist()
    context.log.info(f"✅ Lấy được {len(PAGE_IDS)} pages")

    if not PAGE_IDS:
        raise Exception("❌ Không có pages nào!")

    # =========================
    # BƯỚC 2: SETUP SESSION
    # =========================
    thread_local = threading.local()

    def get_session():
        if not hasattr(thread_local, "session"):
            session = requests.Session()
            try:
                retry_strategy = Retry(
                    total=MAX_RETRIES,
                    backoff_factor=RETRY_BACKOFF,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET"]
                )
            except TypeError:
                retry_strategy = Retry(
                    total=MAX_RETRIES,
                    backoff_factor=RETRY_BACKOFF,
                    status_forcelist=[429, 500, 502, 503, 504],
                    method_whitelist=["GET"]
                )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            thread_local.session = session
        return thread_local.session

    # =========================
    # BƯỚC 3: HÀM GỌI API
    # =========================
    def generate_date_list(start_date, end_date):
        start  = datetime.strptime(start_date, "%Y-%m-%d")
        end    = datetime.strptime(end_date, "%Y-%m-%d")
        result = []
        cur    = start
        while cur <= end:
            result.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return result

    def fetch_engagement_by_date(page_id, date, retry_count=0):
        session  = get_session()
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        date_range = (
            f"{date_obj.strftime('%d/%m/%Y')} 00:00:00 - "
            f"{date_obj.strftime('%d/%m/%Y')} 23:59:59"
        )
        params = {
            "page_id"     : page_id,
            "date_range"  : date_range,
            "access_token": ACCESS_TOKEN,
        }

        try:
            response = session.get(BASE_URL, params=params, timeout=TIMEOUT)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                context.log.warning(f"⏳ Rate limit [{page_id}|{date}]! Chờ {retry_after}s...")
                with RATE_LIMIT_LOCK:
                    time.sleep(retry_after)
                if retry_count < MAX_RETRIES:
                    return fetch_engagement_by_date(page_id, date, retry_count + 1)
                return None

            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    return result.get("users_engagements", [])
                errors = result.get("errors", [])
                for err in errors:
                    if err.get("error_code") == 105:
                        return []
                if retry_count < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF ** retry_count)
                    return fetch_engagement_by_date(page_id, date, retry_count + 1)
                return None

            if response.status_code in [500, 502, 503, 504] and retry_count < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** retry_count)
                return fetch_engagement_by_date(page_id, date, retry_count + 1)
            return None

        except requests.exceptions.Timeout:
            if retry_count < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** retry_count)
                return fetch_engagement_by_date(page_id, date, retry_count + 1)
            return None
        except Exception as e:
            context.log.error(f"💥 [{page_id}|{date}] {e}")
            return None

    USER_COLUMN_RENAME = {
        "user_id"                      : "ma_nhan_vien",
        "name"                         : "ten_nhan_vien",
        "inbox_count"                  : "so_tin_nhan",
        "comment_count"                : "so_comment",
        "total_engagement"             : "tong_tuong_tac",
        "new_customer_replied_count"   : "khach_moi_da_tra_loi",
        "customer_engagement_new_inbox": "tuong_tac_hoi_thoai_moi",
        "order_count"                  : "so_don_hang",
        "old_order_count"              : "so_don_hang_cu",
    }

    COLUMN_ORDER = [
        "page_id", "ngay", "fetch_status", "last_Date_update",
        "ma_nhan_vien", "ten_nhan_vien",
        "tong_tuong_tac", "so_tin_nhan", "so_comment",
        "khach_moi_da_tra_loi", "tuong_tac_hoi_thoai_moi",
        "so_don_hang", "so_don_hang_cu",
    ]

    def build_rows(users_list, page_id, date):
        base = {
            "page_id"                : str(page_id),
            "ngay"                   : pd.to_datetime(date),
            "last_Date_update"       : datetime.now(),
            "ma_nhan_vien"           : None,
            "ten_nhan_vien"          : None,
            "tong_tuong_tac"         : 0,
            "so_tin_nhan"            : 0,
            "so_comment"             : 0,
            "khach_moi_da_tra_loi"   : 0,
            "tuong_tac_hoi_thoai_moi": 0,
            "so_don_hang"            : 0,
            "so_don_hang_cu"         : 0,
        }

        if users_list is None:
            base["fetch_status"] = "error"
            return pd.DataFrame([base])

        if len(users_list) == 0:
            base["fetch_status"] = "success"
            return pd.DataFrame([base])

        df = pd.DataFrame(users_list)
        df["page_id"]          = str(page_id)
        df["ngay"]             = pd.to_datetime(date)
        df["fetch_status"]     = "success"
        df["last_Date_update"] = datetime.now()
        df = df.rename(columns=USER_COLUMN_RENAME)
        df = df[[col for col in COLUMN_ORDER if col in df.columns]]
        return df

    def fetch_task(page_id, date):
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        users_list = fetch_engagement_by_date(page_id, date)
        return build_rows(users_list, page_id, date)

    # =========================
    # BƯỚC 4: FETCH SONG SONG
    # =========================
    context.log.info("BƯỚC 4: Fetch dữ liệu song song...")

    date_list = generate_date_list(START_DATE, END_DATE)
    tasks = [
        (page_id, date)
        for date in date_list
        for page_id in PAGE_IDS
    ]

    all_data  = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(fetch_task, page_id, date): (page_id, date)
            for page_id, date in tasks
        }
        for future in as_completed(future_to_task):
            completed += 1
            try:
                df_rows = future.result()
                all_data.append(df_rows)
            except Exception as e:
                page_id, date = future_to_task[future]
                context.log.error(f"❌ [{page_id}|{date}]: {e}")
                all_data.append(build_rows(None, page_id, date))

    context.log.info(f"✅ Fetch xong {completed} tasks")

    # =========================
    # BƯỚC 5: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 5: Xử lý dữ liệu...")

    df_final = pd.concat(all_data, ignore_index=True)
    df_final = df_final.sort_values(["ngay", "page_id"]).reset_index(drop=True)
    min_date = df_final["ngay"].min()

    context.log.info(f"✅ Tổng: {len(df_final)} dòng")

    # =========================
    # BƯỚC 6: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 6: Cập nhật SQL Server...")

    DTYPE_MAP = {
        "page_id"                : satypes.NVARCHAR(200),
        "ngay"                   : satypes.DATE,
        "fetch_status"           : satypes.NVARCHAR(20),
        "last_Date_update"       : satypes.DATETIME,
        "ma_nhan_vien"           : UNIQUEIDENTIFIER,
        "ten_nhan_vien"          : satypes.NVARCHAR(100),
        "tong_tuong_tac"         : satypes.INTEGER,
        "so_tin_nhan"            : satypes.INTEGER,
        "so_comment"             : satypes.INTEGER,
        "khach_moi_da_tra_loi"   : satypes.INTEGER,
        "tuong_tac_hoi_thoai_moi": satypes.INTEGER,
        "so_don_hang"            : satypes.INTEGER,
        "so_don_hang_cu"         : satypes.INTEGER,
    }

    with engine.begin() as conn:
        for page_id in df_final["page_id"].unique():
            conn.execute(text(f"""
                DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}
                WHERE ngay >= :min_date AND page_id = :page_id
            """), {"min_date": min_date, "page_id": page_id})

        context.log.info("✅ Đã xóa dữ liệu cũ!")

        df_final.to_sql(
            name=TABLE_NAME,
            schema=SCHEMA_NAME,
            con=conn,
            if_exists="append",
            index=False,
            dtype=DTYPE_MAP
        )

    context.log.info(f"🎯 Đã insert {len(df_final)} dòng vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
    context.log.info(f"📊 Số pages: {df_final['page_id'].nunique()}")
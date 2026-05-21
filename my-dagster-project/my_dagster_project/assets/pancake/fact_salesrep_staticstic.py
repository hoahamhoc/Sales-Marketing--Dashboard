import requests
import pandas as pd
import urllib
import time
import json
from datetime import datetime, timedelta
from sqlalchemy import text, types as satypes
from dagster import asset

SCHEMA_NAME = "pancake"
TABLE_NAME = "fact_salesrep_staticstic"

@asset(
    group_name="pancake",
    required_resource_keys={"sql_engine", "pancake_api"},
    deps=["dim_pages"]
)
def fact_salesrep_staticstic(context):
    token = context.resources.pancake_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # 📅 THỜI GIAN
    # =========================
    today = datetime.now()
    start_date = today - timedelta(days=3)

    START_DATE_STR    = start_date.strftime("%Y-%m-%d")
    END_DATE_STR      = today.strftime("%Y-%m-%d")
    START_TIME        = start_date.strftime("%Y-%m-%d 00:00:00")
    END_TIME          = today.strftime("%Y-%m-%d 23:59:59")
    CURRENT_TIMESTAMP = today.strftime("%Y-%m-%d %H:%M:%S")

    since      = int(datetime.strptime(START_TIME, "%Y-%m-%d %H:%M:%S").timestamp())
    until      = int(datetime.strptime(END_TIME,   "%Y-%m-%d %H:%M:%S").timestamp())
    date_range = (
        f"{datetime.fromtimestamp(since).strftime('%d/%m/%Y %H:%M:%S')} - "
        f"{datetime.fromtimestamp(until).strftime('%d/%m/%Y %H:%M:%S')}"
    )

    context.log.info(f"📅 Từ: {START_TIME} → Đến: {END_TIME}")

    # =========================
    # BƯỚC 1: LẤY DANH SÁCH PAGE TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Lấy danh sách pages từ SQL Server...")

    with engine.connect() as conn:
        df_pages = pd.read_sql(
            "SELECT page_id, platform, page_name FROM pancake.dim_pages ORDER BY platform, page_id",
            conn
        )

    FACEBOOK_PAGES      = df_pages[df_pages["platform"] == "facebook"]["page_id"].tolist()
    TIKTOK_PAGES        = df_pages[df_pages["platform"] == "tiktok"]["page_id"].tolist()
    PERSONAL_ZALO_PAGES = df_pages[df_pages["platform"] == "personal_zalo"]["page_id"].tolist()

    context.log.info(f"✅ Facebook: {len(FACEBOOK_PAGES)} | TikTok: {len(TIKTOK_PAGES)} | Zalo: {len(PERSONAL_ZALO_PAGES)}")

    # =========================
    # 🟦 CẤU HÌNH API
    # =========================
    BASE_URL = "https://pancake.vn/api/v1/statistics/user"
    ACCESS_TOKEN = token

    SELECT_FIELDS = [
        "private_reply_count", "comment_count", "unique_comment_count",
        "inbox_count", "unique_inbox_count", "average_response_time",
        "phone_number_count", "order_count"
    ]

    COLUMN_RENAME = {
        "average_response_time": "thoi_gian_phan_hoi_trung_binh",
        "comment_count":         "tong_so_binh_luan_thuc",
        "inbox_count":           "tong_so_tin_nhan_thuc",
        "phone_number_count":    "tong_so_dien_thoai",
        "private_reply_count":   "hanh_dong_gui_tin_nhan_tu_binh_luan",
        "unique_comment_count":  "phan_hoi_cho_moi_binh_luan_khach_hang",
        "unique_inbox_count":    "phan_hoi_cho_moi_tin_nhan_khach_hang",
        "order_count":           "don_hang"
    }

    # =========================
    # BƯỚC 2: HÀM GỌI API CÓ RETRY
    # =========================
    def fetch_user_statistics_for_pages(page_ids_list, platform_name, max_retries=3, backoff_factor=2):
        if not page_ids_list:
            return None, []

        api_params = {
            "date_range":    date_range,
            "access_token":  ACCESS_TOKEN,
            "select_fields": json.dumps(SELECT_FIELDS)
        }
        post_data = {"page_ids": ",".join(page_ids_list)}

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(BASE_URL, params=api_params, data=post_data, timeout=120)
                if response.status_code == 200:
                    result = response.json()
                    if not result.get("success", False):
                        error_pages = [
                            err.get("page_id")
                            for err in result.get("errors", [])
                            if err.get("error_code") == 105
                        ]
                        return None, error_pages
                    statistics = result.get("data", {}).get("statistics", {})
                    users_info = result.get("data", {}).get("users", {})
                    return {"statistics": statistics, "users": users_info}, []
                elif response.status_code in (429, 500, 502, 503, 504):
                    wait = backoff_factor ** attempt
                    context.log.warning(f"⚠️ [{platform_name}] HTTP {response.status_code} | Retry {attempt} sau {wait}s...")
                    time.sleep(wait)
                else:
                    return None, []
            except Exception as e:
                wait = backoff_factor ** attempt
                context.log.error(f"💥 [{platform_name}] {str(e)[:60]} | Retry {attempt} sau {wait}s...")
                time.sleep(wait)
        return None, []

    def fetch_platform_data(page_list, platform_name):
        all_data        = []
        remaining_pages = page_list.copy()
        failed_pages    = []

        if not remaining_pages:
            return all_data, failed_pages

        context.log.info(f"🔹 [{platform_name}] Đang xử lý {len(remaining_pages)} pages")

        if platform_name == "Personal Zalo" and len(remaining_pages) > 1:
            for page_id in remaining_pages:
                result, error_pages = fetch_user_statistics_for_pages([page_id], platform_name)
                if result:
                    for user_id, records in result["statistics"].items():
                        for record in records:
                            record["user_id"]  = user_id
                            record["platform"] = platform_name
                            all_data.append(record)
                elif error_pages:
                    failed_pages.extend(error_pages)
                else:
                    failed_pages.append(page_id)
            return all_data, failed_pages

        while remaining_pages:
            result, error_pages = fetch_user_statistics_for_pages(remaining_pages, platform_name)
            if result:
                for user_id, records in result["statistics"].items():
                    for record in records:
                        record["user_id"]  = user_id
                        record["platform"] = platform_name
                        all_data.append(record)
                break
            elif error_pages:
                failed_pages.extend(error_pages)
                for ep in error_pages:
                    if ep in remaining_pages:
                        remaining_pages.remove(ep)
                if not remaining_pages:
                    break
            else:
                break

        return all_data, failed_pages

    # =========================
    # BƯỚC 3: LẤY DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 3: Lấy dữ liệu từ API...")

    all_data = []
    for page_list, platform_name in [
        (FACEBOOK_PAGES,      "Facebook"),
        (TIKTOK_PAGES,        "TikTok"),
        (PERSONAL_ZALO_PAGES, "Personal Zalo"),
    ]:
        if not page_list:
            continue
        data, failed = fetch_platform_data(page_list, platform_name)
        all_data.extend(data)

    if not all_data:
        context.log.warning("⚠️ Không có dữ liệu!")
        return

    # =========================
    # BƯỚC 4: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 4: Xử lý dữ liệu...")

    df = pd.DataFrame(all_data)

    if "hour" in df.columns:
        df["hour"] = pd.to_datetime(df["hour"])
        df["ngay"] = df["hour"].dt.strftime("%Y-%m-%d")
        df["gio"]  = df["hour"].dt.strftime("%H:%M:%S")
        df.drop(columns=["hour"], inplace=True)

    for col in ["hour_in_integer", "user_name", "user_fb_id", "platform"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    df.rename(columns=COLUMN_RENAME, inplace=True)
    df["last_updated"] = CURRENT_TIMESTAMP

    column_order = [
        "user_id", "page_id", "ngay", "gio",
        "thoi_gian_phan_hoi_trung_binh", "tong_so_binh_luan_thuc",
        "phan_hoi_cho_moi_binh_luan_khach_hang", "tong_so_tin_nhan_thuc",
        "phan_hoi_cho_moi_tin_nhan_khach_hang", "hanh_dong_gui_tin_nhan_tu_binh_luan",
        "tong_so_dien_thoai", "don_hang", "last_updated"
    ]
    df = df[[col for col in column_order if col in df.columns]]

    group_cols  = ["user_id", "page_id", "ngay", "gio", "last_updated"]
    metric_cols = [col for col in df.columns if col not in group_cols]
    agg_dict    = {col: "sum" for col in metric_cols}
    if "thoi_gian_phan_hoi_trung_binh" in agg_dict:
        agg_dict["thoi_gian_phan_hoi_trung_binh"] = "mean"

    df = df.groupby(group_cols, as_index=False).agg(agg_dict)

    if "thoi_gian_phan_hoi_trung_binh" in df.columns:
        df["thoi_gian_phan_hoi_trung_binh"] = df["thoi_gian_phan_hoi_trung_binh"].round(0).astype(int)

    context.log.info(f"✅ Tổng: {len(df)} dòng")

    # =========================
    # BƯỚC 5: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 5: Cập nhật SQL Server...")

    dtype_map = {
        "user_id":                               satypes.NVARCHAR(100),
        "page_id":                               satypes.NVARCHAR(100),
        "ngay":                                  satypes.NVARCHAR(20),
        "gio":                                   satypes.NVARCHAR(20),
        "thoi_gian_phan_hoi_trung_binh":         satypes.INTEGER,
        "tong_so_binh_luan_thuc":                satypes.INTEGER,
        "phan_hoi_cho_moi_binh_luan_khach_hang": satypes.INTEGER,
        "tong_so_tin_nhan_thuc":                 satypes.INTEGER,
        "phan_hoi_cho_moi_tin_nhan_khach_hang":  satypes.INTEGER,
        "hanh_dong_gui_tin_nhan_tu_binh_luan":   satypes.INTEGER,
        "tong_so_dien_thoai":                    satypes.INTEGER,
        "don_hang":                              satypes.INTEGER,
        "last_updated":                          satypes.NVARCHAR(20),
    }

    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {SCHEMA_NAME}.{TABLE_NAME} WHERE ngay >= :start_date AND ngay <= :end_date"),
            {"start_date": START_DATE_STR, "end_date": END_DATE_STR}
        )
        context.log.info("✅ Đã xóa dữ liệu cũ!")

        df.to_sql(
            name=TABLE_NAME,
            schema=SCHEMA_NAME,
            con=conn,
            if_exists="append",
            index=False,
            dtype=dtype_map
        )

    context.log.info(f"🎯 Đã insert {len(df)} dòng vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
    context.log.info(f"📊 Số pages: {df['page_id'].nunique()}")
    context.log.info(f"👥 Số users: {df['user_id'].nunique()}")
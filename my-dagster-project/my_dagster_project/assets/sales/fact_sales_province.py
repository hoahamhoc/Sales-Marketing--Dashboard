import requests
import pandas as pd
import urllib
from datetime import datetime, timedelta
from dagster import asset
from sqlalchemy import text, types as satypes

TABLE_NAME = "btpc.fact_sales_province"

@asset(
    group_name="sales",
    required_resource_keys={"sql_engine", "sales_lead_api"},
    deps=["fact_product"]
)
def fact_sales_province(context):
    token = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # 📅 THỜI GIAN LẤY DỮ LIỆU
    # =========================
    today = datetime.now()
    start_date = today - timedelta(days=10)

    START_DATE_STR = start_date.strftime("%Y-%m-%d")
    END_DATE_STR = today.strftime("%Y-%m-%d")
    CURRENT_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    context.log.info(f"📅 Lấy dữ liệu từ: {START_DATE_STR} → {END_DATE_STR}")

    # =========================
    # 🟦 API CONFIGURATION
    # =========================
    url = "https://sapi.btpc.vn/v1/order/analyticsSale"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    def generate_date_ranges(start, end, chunk_days=90):
        ranges = []
        current_start = start
        while current_start < end:
            current_end = min(current_start + timedelta(days=chunk_days), end)
            ranges.append({
                'since': current_start.strftime("%Y-%m-%d"),
                'until': current_end.strftime("%Y-%m-%d")
            })
            current_start = current_end + timedelta(days=1)
        return ranges

    # =========================
    # BƯỚC 1: LẤY DANH SÁCH USER
    # =========================
    context.log.info("BƯỚC 1: Lấy danh sách user...")

    date_ranges = generate_date_ranges(start_date, today)
    all_users = []

    for i, dr in enumerate(date_ranges, 1):
        payload = {
            "since": dr['since'],
            "until": dr['until'],
            "split_by": ["user_id"]
        }
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            json_data = response.json()
            for item in json_data.get('data', []):
                user_info = item.get('user', {})
                all_users.append({
                    "user_id": item.get('user_id'),
                    "first_name": user_info.get('first_name')
                })
        else:
            raise Exception(f"❌ Lỗi lấy users chunk {i}: {response.status_code} - {response.text}")

    df_users = pd.DataFrame(all_users).drop_duplicates(subset=['user_id'])
    context.log.info(f"✅ Tổng số user unique: {len(df_users)}")

    # =========================
    # BƯỚC 2: LẤY DỮ LIỆU SALES BY PROVINCE
    # =========================
    context.log.info("BƯỚC 2: Lấy dữ liệu sales by province...")

    all_dataframes = []

    for i, dr in enumerate(date_ranges, 1):
        payload = {
            "split_by": ["day", "province", "user_id"],
            "since": dr['since'],
            "until": dr['until']
        }
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            data = response.json()
            if data.get("data"):
                df_chunk = pd.json_normalize(data["data"])
                all_dataframes.append(df_chunk)
                context.log.info(f"✅ Chunk {i}: {len(df_chunk)} dòng")
        else:
            raise Exception(f"❌ Lỗi lấy sales chunk {i}: {response.status_code} - {response.text}")

    if not all_dataframes:
        context.log.warning("⚠️ Không có dữ liệu")
        return

    df_sales = pd.concat(all_dataframes, ignore_index=True)
    context.log.info(f"✅ Tổng số dòng: {len(df_sales)}")

    # =========================
    # BƯỚC 3: XỬ LÝ VÀ MAP DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 3: Xử lý và map dữ liệu...")

    column_mapping = {
        'day': 'order_date',
        'province_slug': 'province_slug',
        'user_id': 'user_id',
        'data.totalSale': 'total_sales',
        'data.totalOrder': 'total_orders',
        'data.totalArchive': 'total_archived_orders',
        'data.totalDiscount': 'total_discount',
        'data.totalDiscountAfterVat': 'total_discount_after_vat',
        'data.totalProduct': 'total_products',
        'data.newCustomerCount': 'new_customer_count',
        'data.totalSaleNewCustomer': 'new_customer_sales',
        'data.oldCustomerCount': 'returning_customer_count',
        'data.totalSaleOldCustomer': 'returning_customer_sales',
        'data.depositedCount': 'deposited_order_count',
        'data.totalRevenue': 'total_revenue',  # ✅ THÊM MỚI
    }
    df_sales.rename(columns=column_mapping, inplace=True)

    df_final = df_sales.merge(df_users, on="user_id", how="left")
    df_final['last_updated'] = CURRENT_TIMESTAMP

    priority_cols = ['order_date', 'user_id', 'first_name', 'province_slug',
                     'total_sales', 'total_revenue', 'total_orders', 'last_updated']
    other_cols = [col for col in df_final.columns if col not in priority_cols]
    df_final = df_final[[col for col in priority_cols if col in df_final.columns] + other_cols]

    if "order_date" in df_final.columns:
        df_final["order_date"] = df_final["order_date"].astype(str)

    # =========================
    # BƯỚC 4: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 4: Cập nhật SQL Server...")

    dtype_map = {
        "order_date": satypes.NVARCHAR(20),
        "user_id": satypes.NVARCHAR(100),
        "first_name": satypes.NVARCHAR(255),
        "province_slug": satypes.NVARCHAR(100),
        "total_sales": satypes.FLOAT,
        "total_orders": satypes.INTEGER,
        "total_archived_orders": satypes.INTEGER,
        "total_discount": satypes.FLOAT,
        "total_discount_after_vat": satypes.FLOAT,
        "total_products": satypes.INTEGER,
        "new_customer_count": satypes.INTEGER,
        "new_customer_sales": satypes.FLOAT,
        "returning_customer_count": satypes.INTEGER,
        "returning_customer_sales": satypes.FLOAT,
        "deposited_order_count": satypes.INTEGER,
        "total_revenue": satypes.FLOAT,  # ✅ THÊM MỚI
        "last_updated": satypes.NVARCHAR(20),
    }

    dtype_map_filtered = {k: v for k, v in dtype_map.items() if k in df_final.columns}

    # ✅ Chỉ giữ cột đã định nghĩa → tránh lỗi khi API thêm field mới
    valid_cols = [col for col in dtype_map_filtered.keys() if col in df_final.columns]
    unexpected_cols = set(df_final.columns) - set(dtype_map_filtered.keys())
    if unexpected_cols:
        context.log.warning(f"⚠️ API có cột mới chưa được map (bỏ qua): {unexpected_cols}")
    df_final = df_final[valid_cols]

    min_date = df_final['order_date'].min()
    context.log.info(f"🧹 Xóa dữ liệu từ ngày {min_date} trở đi trong {TABLE_NAME}...")

    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE_NAME} WHERE order_date >= '{min_date}'"))
        context.log.info("✅ Đã xóa dữ liệu cũ!")

        df_final.to_sql(
            name=TABLE_NAME.split(".")[1],
            schema=TABLE_NAME.split(".")[0],
            con=conn,
            if_exists='append',
            index=False,
            dtype=dtype_map_filtered
        )

    context.log.info(f"🎯 Đã insert {len(df_final)} dòng vào [{TABLE_NAME}] thành công!")
    context.log.info(f"👤 Số user unique: {df_final['user_id'].nunique()}")
    context.log.info(f"🌍 Số province unique: {df_final['province_slug'].nunique()}")
    context.log.info(f"💰 Tổng doanh thu (total_sales): {df_final['total_sales'].sum():,.0f} VNĐ")
    context.log.info(f"💰 Tổng doanh thu (total_revenue): {df_final['total_revenue'].sum():,.0f} VNĐ")
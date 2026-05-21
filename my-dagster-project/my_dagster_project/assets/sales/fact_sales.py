import requests
import pandas as pd
import urllib
from datetime import datetime, timedelta
from dagster import asset
from sqlalchemy import text, types as satypes

TABLE_NAME = "btpc.fact_sales"

@asset(
    group_name="sales",
    required_resource_keys={"sql_engine", "sales_lead_api"}
)
def fact_sales(context):
    token = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # 📅 THỜI GIAN LẤY DỮ LIỆU
    # =========================
    today = datetime.now()
    start_date = today - timedelta(days=90)

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

    # =========================
    # BƯỚC 1: LẤY DANH SÁCH SOURCE & PAGE
    # =========================
    context.log.info("BƯỚC 1: Lấy danh sách source & page...")

    payload_source = {
        "since": START_DATE_STR,
        "until": END_DATE_STR,
        "split_by": ["source_id"]
    }

    response = requests.post(url, headers=headers, json=payload_source)
    all_sources = []

    if response.status_code == 200:
        json_data = response.json()
        for item in json_data.get('data', []):
            if not isinstance(item, dict):
                continue
            source = item.get("source") or {}
            page = item.get("page") or {}
            all_sources.append({
                "source_id": item.get("source_id"),
                "page_id": item.get("page_id"),
                "source_name": source.get("name") if isinstance(source, dict) else None,
                "page_name": page.get("name") if isinstance(page, dict) else None
            })
        context.log.info(f"✅ Lấy được {len(json_data.get('data', []))} sources")
    else:
        raise Exception(f"❌ Lỗi lấy sources: {response.status_code} - {response.text}")

    df_sources = pd.DataFrame(all_sources).drop_duplicates()

    # =========================
    # BƯỚC 2: LẤY DANH SÁCH USER
    # =========================
    context.log.info("BƯỚC 2: Lấy danh sách user...")

    payload_user = {
        "since": START_DATE_STR,
        "until": END_DATE_STR,
        "split_by": ["user_id"]
    }

    response = requests.post(url, headers=headers, json=payload_user)
    all_users = []

    if response.status_code == 200:
        json_data = response.json()
        for item in json_data.get('data', []):
            user_info = item.get('user', {})
            all_users.append({
                "user_id": item.get('user_id'),
                "first_name": user_info.get('first_name')
            })
        context.log.info(f"✅ Lấy được {len(json_data.get('data', []))} users")
    else:
        raise Exception(f"❌ Lỗi lấy users: {response.status_code} - {response.text}")

    df_users = pd.DataFrame(all_users).drop_duplicates(subset=['user_id'])

    # =========================
    # BƯỚC 3: LẤY DỮ LIỆU ANALYTICS SALE
    # =========================
    context.log.info("BƯỚC 3: Lấy dữ liệu analytics sale...")

    payload_sales = {
        "split_by": ["day", "source_id", "user_id"],
        "since": START_DATE_STR,
        "until": END_DATE_STR
    }

    response = requests.post(url, headers=headers, json=payload_sales)

    if response.status_code != 200:
        raise Exception(f"❌ Lỗi lấy sales data: {response.status_code} - {response.text}")

    data = response.json()

    if not data.get("data"):
        context.log.warning("⚠️ Không có dữ liệu sales")
        return

    df_sales = pd.json_normalize(data["data"])
    context.log.info(f"✅ Lấy được {len(df_sales)} dòng")

    # =========================
    # BƯỚC 4: XỬ LÝ VÀ MAP DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 4: Xử lý và map dữ liệu...")

    column_mapping = {
        'day': 'order_date',
        'source_id': 'source_id',
        'page_id': 'page_id',
        'province_slug': 'province_slug',
        'user_id': 'user_id',
        'data.totalSale': 'total_sales',
        'data.totalArchive': 'total_archived_orders',
        'data.totalOrder': 'total_orders',
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

    df_final = df_sales.merge(df_users[['user_id', 'first_name']], on='user_id', how='left')

    df_final = df_final.merge(
        df_sources[['source_id', 'page_id', 'source_name', 'page_name']],
        on=['source_id', 'page_id'],
        how='left',
        suffixes=('', '_mapped')
    )

    if 'source_name_mapped' in df_final.columns:
        df_final['source_name'] = df_final['source_name_mapped'].fillna(df_final.get('source_name', ''))
        df_final.drop('source_name_mapped', axis=1, inplace=True)

    if 'page_name_mapped' in df_final.columns:
        df_final['page_name'] = df_final['page_name_mapped'].fillna(df_final.get('page_name', ''))
        df_final.drop('page_name_mapped', axis=1, inplace=True)

    df_final['last_updated'] = CURRENT_TIMESTAMP

    priority_cols = ['order_date', 'user_id', 'first_name', 'source_id', 'source_name',
                     'page_id', 'page_name', 'province_slug', 'total_sales', 'total_revenue',
                     'total_orders', 'last_updated']
    other_cols = [col for col in df_final.columns if col not in priority_cols]
    df_final = df_final[[col for col in priority_cols if col in df_final.columns] + other_cols]

    if "order_date" in df_final.columns:
        df_final["order_date"] = df_final["order_date"].astype(str)

    # =========================
    # BƯỚC 5: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 5: Cập nhật SQL Server...")

    dtype_map = {
        "order_date": satypes.NVARCHAR(20),
        "user_id": satypes.NVARCHAR(100),
        "first_name": satypes.NVARCHAR(255),
        "source_id": satypes.NVARCHAR(100),
        "source_name": satypes.NVARCHAR(255),
        "page_id": satypes.NVARCHAR(100),
        "page_name": satypes.NVARCHAR(255),
        "province_slug": satypes.NVARCHAR(100),
        "total_sales": satypes.FLOAT,
        "total_archived_orders": satypes.INTEGER,
        "total_orders": satypes.INTEGER,
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
    context.log.info(f"💰 Tổng doanh thu (total_sales): {df_final['total_sales'].sum():,.0f} VNĐ")
    context.log.info(f"💰 Tổng doanh thu (total_revenue): {df_final['total_revenue'].sum():,.0f} VNĐ")
import requests
import pandas as pd
import urllib
import pyodbc
from datetime import datetime, timedelta
from dagster import asset
from sqlalchemy import text, types as satypes

SCHEMA_NAME = "btpc"
TABLE_NAME = "fact_product"

@asset(
    group_name="sales",
    required_resource_keys={"sql_engine", "sales_lead_api"},
    deps=["fact_sales"]
)
def fact_product(context):
    token = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # 📅 THỜI GIAN
    # =========================
    today = datetime.now()
    current_year = today.year
    current_week = today.isocalendar()[1]
    current_month = today.month

    def get_first_day_of_week(year, week_number):
        jan_4 = datetime(year, 1, 4)
        week_1_monday = jan_4 - timedelta(days=jan_4.weekday())
        return week_1_monday + timedelta(weeks=week_number - 1)

    def get_last_day_of_week(year, week_number):
        return get_first_day_of_week(year, week_number) + timedelta(days=6)

    week_first_day = get_first_day_of_week(current_year, current_week)
    week_last_day = min(get_last_day_of_week(current_year, current_week), today)

    since_str_week = week_first_day.strftime("%Y-%m-%d")
    until_str_week = week_last_day.strftime("%Y-%m-%d")

    month_first_day = datetime(current_year, current_month, 1)
    if current_month == 12:
        month_last_day = datetime(current_year, 12, 31)
    else:
        month_last_day = datetime(current_year, current_month + 1, 1) - timedelta(days=1)
    month_last_day = min(month_last_day, today)

    since_str_month = month_first_day.strftime("%Y-%m-%d")
    until_str_month = month_last_day.strftime("%Y-%m-%d")

    context.log.info(f"📅 Tuần {current_week}/{current_year}: {since_str_week} → {until_str_week}")
    context.log.info(f"📅 Tháng {current_month}/{current_year}: {since_str_month} → {until_str_month}")

    # =========================
    # BƯỚC 1: LẤY DANH SÁCH USER TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Lấy danh sách user từ SQL Server...")

    query = f"""
    SELECT DISTINCT user_id
    FROM [btpc].[fact_sales]
    WHERE (order_date >= '{since_str_week}' AND order_date <= '{until_str_week}')
       OR (order_date >= '{since_str_month}' AND order_date <= '{until_str_month}')
    ORDER BY user_id
    """

    with engine.connect() as conn:
        df_users = pd.read_sql(query, conn)

    USER_IDS = df_users['user_id'].tolist()
    context.log.info(f"✅ Lấy được {len(USER_IDS)} User IDs")

    if not USER_IDS:
        context.log.warning("⚠️ Không có User ID!")
        return

    # =========================
    # 🟦 API CONFIGURATION
    # =========================
    url = "https://sapi.btpc.vn/v1/order/getStatisticProduct"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    # =========================
    # BƯỚC 2: LẤY DỮ LIỆU WEEK_VIEW
    # =========================
    context.log.info("BƯỚC 2: Lấy dữ liệu tuần hiện tại (Week_view)...")

    week_data = []
    for idx, user_id in enumerate(USER_IDS, 1):
        payload = {
            "since": since_str_week,
            "until": until_str_week,
            "withColor": False,
            "withPorE": False,
            "withMaterial": False,
            "filter": {
                "trademark_ids": [],
                "source_ids": [],
                "user_ids": [user_id],
                "page_ids": []
            }
        }
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                json_data = response.json()
                if json_data.get("success") and json_data.get("data"):
                    products = json_data.get("data", [])
                    for product in products:
                        product['View'] = 'Week_view'
                        product['order_date'] = week_first_day.strftime("%Y-%m-%d")
                        product['user_id'] = user_id
                    week_data.extend(products)
        except Exception as e:
            context.log.error(f"❌ User {user_id}: {str(e)}")

    context.log.info(f"✅ Week_view: {len(week_data)} records")

    # =========================
    # BƯỚC 3: LẤY DỮ LIỆU MONTH_VIEW
    # =========================
    context.log.info("BƯỚC 3: Lấy dữ liệu tháng hiện tại (Month_view)...")

    month_data = []
    for idx, user_id in enumerate(USER_IDS, 1):
        payload = {
            "since": since_str_month,
            "until": until_str_month,
            "withColor": False,
            "withPorE": False,
            "withMaterial": False,
            "filter": {
                "trademark_ids": [],
                "source_ids": [],
                "user_ids": [user_id],
                "page_ids": []
            }
        }
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                json_data = response.json()
                if json_data.get("success") and json_data.get("data"):
                    products = json_data.get("data", [])
                    for product in products:
                        product['View'] = 'Month_view'
                        product['order_date'] = month_first_day.strftime("%Y-%m-%d")
                        product['user_id'] = user_id
                    month_data.extend(products)
        except Exception as e:
            context.log.error(f"❌ User {user_id}: {str(e)}")

    context.log.info(f"✅ Month_view: {len(month_data)} records")

    if not week_data and not month_data:
        context.log.warning("⚠️ Không có dữ liệu!")
        return

    # =========================
    # BƯỚC 4: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 4: Xử lý dữ liệu...")

    df = pd.DataFrame(week_data + month_data)
    df = df[['View', 'order_date', 'user_id', 'name', 'number', 'money', 'minMoney', 'maxMoney']].copy()

    df.rename(columns={
        'order_date': 'Ngày',
        'user_id': 'User ID',
        'name': 'Sản phẩm',
        'number': 'Số lượng',
        'money': 'Tổng tiền',
        'minMoney': 'Giá min',
        'maxMoney': 'Giá max'
    }, inplace=True)

    df['Số lượng'] = df['Số lượng'].astype(int)
    df['Tổng tiền'] = df['Tổng tiền'].astype(float).round(2)
    df['Giá min'] = df['Giá min'].astype(float).round(2)
    df['Giá max'] = df['Giá max'].astype(float).round(2)

    context.log.info(f"✅ Tổng cộng: {len(df):,} dòng")

    # =========================
    # BƯỚC 5: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 5: Cập nhật SQL Server...")

    dtype_map = {
        "View": satypes.NVARCHAR(20),
        "Ngày": satypes.DATE,
        "User ID": satypes.NVARCHAR(100),
        "Sản phẩm": satypes.NVARCHAR(500),
        "Số lượng": satypes.INTEGER,
        "Tổng tiền": satypes.DECIMAL(18, 2),
        "Giá min": satypes.DECIMAL(18, 2),
        "Giá max": satypes.DECIMAL(18, 2)
    }

    with engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM [{SCHEMA_NAME}].[{TABLE_NAME}]
            WHERE ([View] = 'Week_view' AND [Ngày] = '{since_str_week}')
               OR ([View] = 'Month_view' AND [Ngày] = '{since_str_month}')
        """))
        context.log.info("✅ Đã xóa dữ liệu cũ!")

        df.to_sql(
            name=TABLE_NAME,
            schema=SCHEMA_NAME,
            con=conn,
            if_exists='append',
            index=False,
            dtype=dtype_map
        )

    context.log.info(f"🎯 Đã insert {len(df):,} dòng vào [{SCHEMA_NAME}].[{TABLE_NAME}] thành công!")
    context.log.info(f"📦 Số sản phẩm unique: {df['Sản phẩm'].nunique():,}")
    context.log.info(f"👥 Số User ID: {df['User ID'].nunique():,}")
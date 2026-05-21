import requests
import pandas as pd
import urllib
from datetime import datetime, timedelta
from dagster import asset
from sqlalchemy import text, types as satypes

SCHEMA_NAME = "btpc"
TABLE_NAME = "fact_product_daily"

@asset(
    group_name="sales",
    required_resource_keys={"sql_engine", "sales_lead_api"},
    deps=["fact_sales_province"]
)
def fact_product_daily(context):
    token = context.resources.sales_lead_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # 📅 THỜI GIAN LẤY DỮ LIỆU
    # =========================
    today = datetime.now()
    start_date = today - timedelta(days=20)

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = today.strftime("%Y-%m-%d")

    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d")
             for i in range((end - start).days + 1)]

    context.log.info(f"📅 Lấy dữ liệu từ: {start_date_str} → {end_date_str} ({len(dates)} ngày)")

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
    # BƯỚC 1: LẤY DỮ LIỆU TỪNG NGÀY
    # =========================
    context.log.info("BƯỚC 1: Lấy dữ liệu từng ngày...")

    all_data = []

    for idx, date_str in enumerate(dates, 1):
        payload = {
            "since": date_str,
            "until": date_str,
            "withColor": False,
            "withPorE": False,
            "withMaterial": False,
            "filter": {
                "trademark_ids": [],
                "source_ids": [],
                "user_ids": [],
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
                        product['order_date'] = date_str
                    all_data.extend(products)
                    context.log.info(f"✅ [{idx}/{len(dates)}] {date_str}: {len(products)} sản phẩm")
                else:
                    context.log.warning(f"⚠️ [{idx}/{len(dates)}] {date_str}: Không có data")
            else:
                raise Exception(f"❌ [{idx}/{len(dates)}] {date_str}: Lỗi {response.status_code}")
        except Exception as e:
            context.log.error(str(e))

    if not all_data:
        context.log.warning("⚠️ Không có dữ liệu")
        return

    context.log.info(f"✅ Tổng cộng: {len(all_data)} records")

    # =========================
    # BƯỚC 2: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 2: Xử lý dữ liệu...")

    df = pd.DataFrame(all_data)
    df = df[['order_date', 'name', 'number', 'money', 'minMoney', 'maxMoney']].copy()

    df.rename(columns={
        'order_date': 'Ngày',
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
    # BƯỚC 3: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 3: Cập nhật SQL Server...")

    dtype_map = {
        "Ngày": satypes.DATE,
        "Sản phẩm": satypes.NVARCHAR(500),
        "Số lượng": satypes.INTEGER,
        "Tổng tiền": satypes.DECIMAL(18, 2),
        "Giá min": satypes.DECIMAL(18, 2),
        "Giá max": satypes.DECIMAL(18, 2)
    }

    with engine.begin() as conn:
        date_list = "', '".join(dates)
        conn.execute(text(f"""
            DELETE FROM [{SCHEMA_NAME}].[{TABLE_NAME}]
            WHERE [Ngày] IN ('{date_list}')
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
    context.log.info(f"💰 Tổng doanh thu: {df['Tổng tiền'].sum():,.2f} VNĐ")
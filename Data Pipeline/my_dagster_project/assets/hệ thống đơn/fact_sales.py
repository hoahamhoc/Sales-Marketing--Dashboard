import requests
import pandas as pd
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
    # DATE RANGE
    # =========================
    today = datetime.now()
    start_date = today - timedelta(days=5)

    START_DATE_STR    = start_date.strftime("%Y-%m-%d")
    END_DATE_STR      = today.strftime("%Y-%m-%d")
    CURRENT_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    context.log.info(f"Fetching data from: {START_DATE_STR} to {END_DATE_STR}")

    # =========================
    # API CONFIGURATION
    # =========================
    url = "https://xxxx/v1/order/analyticsSale"  # Order analytics API endpoint
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept"       : "application/json",
        "Content-Type" : "application/json"
    }

    # =========================
    # STEP 1: GET SOURCE & PAGE LIST
    # =========================
    context.log.info("Step 1: Fetching source & page list...")

    payload_source = {
        "since"   : START_DATE_STR,
        "until"   : END_DATE_STR,
        "split_by": ["source_id"]
    }

    response    = requests.post(url, headers=headers, json=payload_source)
    all_sources = []

    if response.status_code == 200:
        json_data = response.json()
        for item in json_data.get('data', []):
            if not isinstance(item, dict):
                continue
            source = item.get("source") or {}
            page   = item.get("page") or {}
            all_sources.append({
                "source_id"  : item.get("source_id"),
                "page_id"    : item.get("page_id"),
                "source_name": source.get("name") if isinstance(source, dict) else None,
                "page_name"  : page.get("name") if isinstance(page, dict) else None
            })
        context.log.info(f"Fetched {len(json_data.get('data', []))} sources")
    else:
        raise Exception(f"Failed to fetch sources: {response.status_code} - {response.text}")

    df_sources = pd.DataFrame(all_sources).drop_duplicates()

    # =========================
    # STEP 2: GET USER LIST
    # =========================
    context.log.info("Step 2: Fetching user list...")

    payload_user = {
        "since"   : START_DATE_STR,
        "until"   : END_DATE_STR,
        "split_by": ["user_id"]
    }

    response  = requests.post(url, headers=headers, json=payload_user)
    all_users = []

    if response.status_code == 200:
        json_data = response.json()
        for item in json_data.get('data', []):
            user_info = item.get('user', {})
            all_users.append({
                "user_id"   : item.get('user_id'),
                "first_name": user_info.get('first_name')
            })
        context.log.info(f"Fetched {len(json_data.get('data', []))} users")
    else:
        raise Exception(f"Failed to fetch users: {response.status_code} - {response.text}")

    df_users = pd.DataFrame(all_users).drop_duplicates(subset=['user_id'])

    # =========================
    # STEP 3: GET ANALYTICS SALE DATA
    # =========================
    context.log.info("Step 3: Fetching analytics sale data...")

    payload_sales = {
        "split_by": ["day", "source_id", "user_id"],
        "since"   : START_DATE_STR,
        "until"   : END_DATE_STR
    }

    response = requests.post(url, headers=headers, json=payload_sales)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch sales data: {response.status_code} - {response.text}")

    data = response.json()

    if not data.get("data"):
        context.log.warning("No sales data returned.")
        return

    df_sales = pd.json_normalize(data["data"])
    context.log.info(f"Fetched {len(df_sales)} rows")

    # =========================
    # STEP 4: PROCESS & MAP DATA
    # =========================
    context.log.info("Step 4: Processing and mapping data...")
column_mapping = {
    'day'                       : 'order_date',
    'source_id'                 : 'source_id',
    'page_id'                   : 'page_id',
    'user_id'                   : 'user_id',
    'data.totalSale'            : 'total_sales',
    'data.totalOrder'           : 'total_orders',
    'data.newCustomerCount'     : 'new_customer_count',
    'data.totalSaleNewCustomer' : 'new_customer_sales',
    'data.oldCustomerCount'     : 'returning_customer_count',
    'data.totalSaleOldCustomer' : 'returning_customer_sales',
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

    priority_cols = [
        'order_date', 'user_id', 'first_name', 'source_id', 'source_name',
        'page_id', 'page_name', 'province_slug', 'total_sales', 'total_revenue',
        'total_orders', 'last_updated'
    ]
    other_cols = [col for col in df_final.columns if col not in priority_cols]
    df_final   = df_final[[col for col in priority_cols if col in df_final.columns] + other_cols]

    if "order_date" in df_final.columns:
        df_final["order_date"] = df_final["order_date"].astype(str)

    # =========================
    # STEP 5: LOAD TO SQL SERVER
    # =========================
    context.log.info("Step 5: Loading to SQL Server...")

    dtype_map = {
        "order_date"              : satypes.NVARCHAR(20),
        "user_id"                 : satypes.NVARCHAR(100),
        "first_name"              : satypes.NVARCHAR(255),
        "source_id"               : satypes.NVARCHAR(100),
        "source_name"             : satypes.NVARCHAR(255),
        "page_id"                 : satypes.NVARCHAR(100),
        "page_name"               : satypes.NVARCHAR(255),
        "province_slug"           : satypes.NVARCHAR(100),
        "total_sales"             : satypes.FLOAT,
        "total_orders"            : satypes.INTEGER,     
        "new_customer_sales"      : satypes.FLOAT,
        "returning_customer_sales": satypes.FLOAT,  
        "last_updated"            : satypes.NVARCHAR(20),
    }

    dtype_map_filtered = {k: v for k, v in dtype_map.items() if k in df_final.columns}

    unexpected_cols = set(df_final.columns) - set(dtype_map_filtered.keys())
    if unexpected_cols:
        context.log.warning(f"Unexpected columns from API (skipped): {unexpected_cols}")
    df_final = df_final[[col for col in dtype_map_filtered.keys() if col in df_final.columns]]

    min_date = df_final['order_date'].min()
    context.log.info(f"Deleting data from {min_date} onwards in {TABLE_NAME}...")

    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE_NAME} WHERE order_date >= '{min_date}'"))
        context.log.info("Deleted existing data.")

        df_final.to_sql(
            name      = TABLE_NAME.split(".")[1],
            schema    = TABLE_NAME.split(".")[0],
            con       = conn,
            if_exists = 'append',
            index     = False,
            dtype     = dtype_map_filtered
        )

    context.log.info(f"Inserted {len(df_final)} rows into [{TABLE_NAME}]")
    context.log.info(f"Unique users    : {df_final['user_id'].nunique()}")
    context.log.info(f"Total sales     : {df_final['total_sales'].sum():,.0f}")
    context.log.info(f"Total revenue   : {df_final['total_revenue'].sum():,.0f}")
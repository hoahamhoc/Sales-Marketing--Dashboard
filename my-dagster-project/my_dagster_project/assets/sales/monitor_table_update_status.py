import pandas as pd
from datetime import datetime
from dagster import asset
from sqlalchemy import text

@asset(
    group_name="sales",
    required_resource_keys={"sql_engine"},
    deps=["fact_sales", "fact_sales_province", "fact_product_daily", "fact_product"]
)
def monitor_table_update_status(context):
    engine = context.resources.sql_engine

    # =========================
    # 📊 QUERY LẤY STATUS
    # =========================
    query = """
    WITH base AS (
        SELECT 'fact_sales'                                   AS table_name, N'Doanh số' AS table_info, MAX(last_updated)     AS last_date_update FROM [btpc].[fact_sales]
        UNION ALL
        SELECT 'fact_salesrep_staticstic',                    'Pancake',                              MAX(last_updated)     FROM [pancake].[fact_salesrep_staticstic]
        UNION ALL
        SELECT 'fact_engagement_staticstic',                  'Pancake',                              MAX(last_Date_update) FROM [pancake].[fact_engagement_staticstic]
        UNION ALL
        SELECT 'fact_pages_campaigns',                        'Pancake',                              MAX(updated_at)       FROM [pancake].[fact_pages_campaigns]
        UNION ALL
        SELECT 'fact_tag_staticstic',                         'Pancake',                              MAX(last_update)      FROM [pancake].[fact_tag_staticstic]
        UNION ALL
        SELECT 'fact_lead_daily',                             'CRM',                                  MAX(lastdate_update)  FROM [crm].[fact_lead_daily]
        UNION ALL
        SELECT 'fact_lead_daily_full_metric',                 'CRM',                                  MAX(lastdate_update)  FROM [crm].[fact_lead_daily_full_metric]
        UNION ALL
        SELECT 'fact_lead_daily_full_metric_include_page_id', 'CRM',                                  MAX(lastdate_update)  FROM [crm].[fact_lead_daily_full_metric_include_page_id]
    )
    SELECT 
        table_name,
        table_info,
        last_date_update,
        DATEDIFF(MINUTE, last_date_update, MAX(last_date_update) OVER())    AS diff_with_latest_minutes,
        DATEDIFF(MINUTE, last_date_update, DATEADD(HOUR, 7, GETDATE()))     AS diff_with_now_minutes,
        CASE 
            WHEN DATEDIFF(MINUTE, last_date_update, DATEADD(HOUR, 7, GETDATE())) > 40
                THEN N'⚠️ CHECK - Quá 40 phút so với hiện tại'
            WHEN DATEDIFF(MINUTE, last_date_update, MAX(last_date_update) OVER()) > 7
                THEN N'⚠️ CHECK - Lệch so với bảng mới nhất'
            ELSE N'✅ OK' 
        END AS status
    FROM base
    ORDER BY last_date_update DESC
    """

    # =========================
    # 🔄 REFRESH BẢNG
    # =========================
    context.log.info("🔄 Đang refresh bảng monitor_table_update_status...")

    with engine.begin() as conn:

        # Tạo bảng nếu chưa có
        conn.execute(text("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'dbo' 
                AND TABLE_NAME = 'monitor_table_update_status'
            )
            BEGIN
                CREATE TABLE [dbo].[monitor_table_update_status] (
                    table_name               NVARCHAR(100),
                    table_info               NVARCHAR(100),
                    last_date_update         DATETIME,
                    diff_with_latest_minutes INT,
                    diff_with_now_minutes    INT,
                    status                   NVARCHAR(200),
                    refreshed_at             DATETIME
                )
            END
        """))

        # Lấy data
        df = pd.read_sql(query, conn)
        df["refreshed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Xóa data cũ và insert mới
        conn.execute(text("TRUNCATE TABLE [dbo].[monitor_table_update_status]"))
        df.to_sql(
            name="monitor_table_update_status",
            schema="dbo",
            con=conn,
            if_exists="append",
            index=False
        )

    context.log.info(f"✅ Đã refresh {len(df)} bảng thành công!")
    context.log.info(f"🕐 Refreshed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    for _, row in df.iterrows():
        icon = "✅" if "OK" in str(row["status"]) else "⚠️"
        context.log.info(f"{icon} {row['table_name']} | {str(row['last_date_update'])} | {row['status']}")
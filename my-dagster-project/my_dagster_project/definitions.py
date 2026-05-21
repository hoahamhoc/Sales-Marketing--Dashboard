from dagster import (
    Definitions,
    load_assets_from_package_module,
    AssetSelection,
    define_asset_job,
    ScheduleDefinition,
)

from . import assets
from .resources import sql_engine, sales_lead_api, pancake_api

# ============================
# Load assets (❗ thiếu cái này là lỗi bạn gặp)
# ============================
all_assets = load_assets_from_package_module(assets)

# ============================
# Jobs
# ============================
sales_job = define_asset_job(
    name="sales_job",
    selection=AssetSelection.groups("sales")
)

lead_pancake_job = define_asset_job(
    name="lead_pancake_job",
    selection=AssetSelection.groups("lead", "pancake")
)

# ============================
# Schedules
# ============================
sales_schedule = ScheduleDefinition(
    name="sales_schedule",
    job=sales_job,
    cron_schedule="25,55 6-23 * * *",
    execution_timezone="Asia/Ho_Chi_Minh"
)

lead_pancake_schedule = ScheduleDefinition(
    name="lead_pancake_schedule",
    job=lead_pancake_job,
    cron_schedule="20,50 6-23 * * *",
    execution_timezone="Asia/Ho_Chi_Minh"
)

# ============================
# Definitions
# ============================
defs = Definitions(
    assets=all_assets,
    jobs=[sales_job, lead_pancake_job],
    schedules=[sales_schedule, lead_pancake_schedule],
    resources={
        "sql_engine": sql_engine,
        "sales_lead_api": sales_lead_api,
        "pancake_api": pancake_api,
    }
)
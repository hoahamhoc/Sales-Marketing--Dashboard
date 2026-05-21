import requests
import pandas as pd
from sqlalchemy import types as satypes
from dagster import asset

SCHEMA_NAME = "pancake"
TABLE_NAME  = "dim_pages"

@asset(
    group_name="pancake",
    required_resource_keys={"sql_engine", "pancake_api"}
)
def dim_pages(context):
    token  = context.resources.pancake_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # STEP 1: FETCH PAGE LIST
    # =========================
    context.log.info("Step 1: Fetching page list from Pancake API...")

    url     = "https://xxxx/api/v1/pages"  # Pancake API endpoint
    params  = {"access_token": token}
    headers = {"Accept": "application/json"}

    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        raise Exception(f"API error: {response.status_code} - {response.text}")

    data  = response.json()
    pages = data.get("categorized", {}).get("activated", [])

    page_list = [
        {
            "page_id"  : p.get("id"),
            "platform" : p.get("platform"),
            "page_name": p.get("name")
        }
        for p in pages
    ]

    context.log.info(f"Fetched {len(page_list)} pages")

    # =========================
    # STEP 2: LOAD TO SQL SERVER
    # =========================
    context.log.info("Step 2: Loading to SQL Server...")

    df = pd.DataFrame(page_list)

    dtype_map = {
        "page_id"  : satypes.NVARCHAR(50),
        "platform" : satypes.NVARCHAR(50),
        "page_name": satypes.NVARCHAR(255)
    }

    with engine.begin() as conn:
        df.to_sql(
            name      = TABLE_NAME,
            schema    = SCHEMA_NAME,
            con       = conn,
            if_exists = "replace",
            index     = False,
            dtype     = dtype_map
        )

    context.log.info(f"Inserted {len(df)} pages into [{SCHEMA_NAME}].[{TABLE_NAME}]")
import json
import requests
import pandas as pd
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text, types as satypes
from dagster import asset

SCHEMA_NAME = "pancake"
TABLE_NAME  = "fact_ad_id"

@asset(
    group_name="pancake",
    required_resource_keys={"sql_engine", "pancake_api"},
    deps=["dim_ad_id"]
)
def fact_ad_id(context):
    token  = context.resources.pancake_api["token"]
    engine = context.resources.sql_engine

    # =========================
    # ⚙️ CẤU HÌNH
    # =========================
    ACCESS_TOKEN        = token
    NUM_DAYS            = 2
    BATCH_SIZE          = 20
    MAX_RETRY           = 5
    RETRY_DELAY         = 5    # giây ban đầu (exponential backoff)
    OUTER_WORKERS       = 6    # (page × ngày) song song
    INNER_WORKERS       = 3    # batch song song bên trong mỗi combo
    DELAY_BETWEEN_CALLS = 0.2

    today        = datetime.combine(datetime.now().date(), datetime.min.time())
    date_from_dt = today - timedelta(days=NUM_DAYS - 1)

    DATE_FROM_SQL = date_from_dt.strftime("%Y-%m-%d")
    DATE_TO_SQL   = today.strftime("%Y-%m-%d")

    DATE_LIST = []
    current = date_from_dt
    while current <= today:
        DATE_LIST.append(current.strftime("%d/%m/%Y"))
        current += timedelta(days=1)

    context.log.info(f"📅 Từ: {DATE_FROM_SQL} → Đến: {DATE_TO_SQL} ({len(DATE_LIST)} ngày)")

    # =========================
    # BƯỚC 1: LẤY AD_ID TỪ SQL
    # =========================
    context.log.info("BƯỚC 1: Query ad_id từ SQL Server (pancake.dim_ad_id)...")

    with engine.connect() as conn:
        df_ads = pd.read_sql(
            text("""
                SELECT page_id, ad_id, [platform]
                FROM pancake.dim_ad_id
                WHERE [date] BETWEEN :date_from AND :date_to
                GROUP BY page_id, ad_id, [platform]
            """),
            conn,
            params={"date_from": DATE_FROM_SQL, "date_to": DATE_TO_SQL}
        )

    context.log.info(f"✅ {len(df_ads)} ads | {df_ads['page_id'].nunique()} pages")

    if df_ads.empty:
        raise Exception("❌ Không có ad_id. Chạy dim_ad_id trước!")

    # =========================
    # BƯỚC 2: SETUP SESSION
    # =========================
    thread_local = threading.local()

    def get_session():
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        return thread_local.session

    # =========================
    # BƯỚC 3: HÀM GỌI API
    # =========================
    def fetch_batch(page_id, platform, batch, date_from, date_to, batch_idx, total_batches):
        """Gọi API 1 batch ad_ids. Trả về (list_records, list_failed_ad_ids)."""
        session    = get_session()
        url        = f"https://pancake.vn/api/v1/pages/{page_id}/statistics/ads/get_insight"
        date_range = f"{date_from} - {date_to}"

        params = {
            "access_token": ACCESS_TOKEN,
            "date_range"  : date_range,
            "data"        : json.dumps(batch),
            "platform"    : platform,
        }

        for attempt in range(1, MAX_RETRY + 1):
            try:
                resp   = session.get(url, params=params, timeout=30)
                result = resp.json()

                if isinstance(result, dict) and result.get("success"):
                    batch_data   = result.get("data", [])
                    returned_ids = {r.get("ad_id") for r in batch_data}
                    missing_ids  = [aid for aid in batch if aid not in returned_ids]

                    context.log.info(
                        f"    Batch {batch_idx}/{total_batches} | page={page_id} | "
                        f"✅ {len(batch_data)} records | missing={len(missing_ids)}"
                    )
                    return batch_data, missing_ids

                else:
                    context.log.warning(f"    Batch {batch_idx} attempt {attempt}: API error → {result}")

            except Exception as e:
                context.log.warning(f"    Batch {batch_idx} attempt {attempt}: Exception → {e}")

            if attempt < MAX_RETRY:
                delay = RETRY_DELAY * (2 ** (attempt - 1))
                context.log.info(f"    ⏳ Retry sau {delay}s...")
                time.sleep(delay)

        context.log.error(f"    ❌ Batch {batch_idx} thất bại sau {MAX_RETRY} lần.")
        return [], batch  # toàn bộ batch là missing

    def fetch_insight_for_combo(page_id, platform, ad_ids, date_str):
        """
        Xử lý 1 combo (page_id, date_str):
          - Round 1: các batch song song (INNER_WORKERS)
          - Round 2: retry tuần tự các ad_id bị thiếu
        """
        time.sleep(DELAY_BETWEEN_CALLS)

        date_sql      = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        date_from_day = f"{date_str} 00:00:00"
        date_to_day   = f"{date_str} 23:59:59"

        batches     = [ad_ids[i:i + BATCH_SIZE] for i in range(0, len(ad_ids), BATCH_SIZE)]
        all_data    = []
        all_missing = []

        # Round 1: batch song song (inner)
        context.log.info(f"  📡 [{page_id}|{date_str}] Round 1: {len(batches)} batches | {len(ad_ids)} ads")
        with ThreadPoolExecutor(max_workers=INNER_WORKERS) as executor:
            futures = {
                executor.submit(
                    fetch_batch, page_id, platform, batch,
                    date_from_day, date_to_day, idx + 1, len(batches)
                ): batch
                for idx, batch in enumerate(batches)
            }
            for future in as_completed(futures):
                data, missing = future.result()
                all_data.extend(data)
                all_missing.extend(missing)

        # Round 2: retry ad_id bị thiếu (tuần tự)
        if all_missing:
            context.log.warning(f"  ⚠️ [{page_id}|{date_str}] Round 2: retry {len(all_missing)} ad_ids thiếu...")
            retry_batches = [all_missing[i:i + BATCH_SIZE] for i in range(0, len(all_missing), BATCH_SIZE)]
            for idx, batch in enumerate(retry_batches):
                data, still_missing = fetch_batch(
                    page_id, platform, batch,
                    date_from_day, date_to_day, idx + 1, len(retry_batches)
                )
                all_data.extend(data)
                if still_missing:
                    context.log.error(f"  ❌ [{page_id}|{date_str}] Vẫn thiếu {len(still_missing)} ad_ids sau Round 2")

        # Gắn metadata
        for record in all_data:
            record["date"]     = date_sql
            record["page_id"]  = page_id
            record["platform"] = platform

        context.log.info(f"  ✅ [{page_id}|{date_str}] {len(all_data)} records")
        return all_data

    # =========================
    # BƯỚC 4: FETCH SONG SONG (OUTER)
    # =========================
    context.log.info("BƯỚC 4: Fetch insight song song (outer × inner)...")

    combos = [
        (page_id, platform, group["ad_id"].tolist(), date_str)
        for date_str in DATE_LIST
        for (page_id, platform), group in df_ads.groupby(["page_id", "platform"])
    ]

    context.log.info(f"⚡ {len(combos)} combos (page × ngày) | OUTER={OUTER_WORKERS} | INNER={INNER_WORKERS}")

    all_records = []
    completed   = 0

    with ThreadPoolExecutor(max_workers=OUTER_WORKERS) as executor:
        future_to_combo = {
            executor.submit(fetch_insight_for_combo, page_id, platform, ad_ids, date_str): (page_id, platform, date_str)
            for page_id, platform, ad_ids, date_str in combos
        }
        for future in as_completed(future_to_combo):
            completed += 1
            page_id, platform, date_str = future_to_combo[future]
            try:
                records = future.result()
                if records:
                    all_records.extend(records)
            except Exception as e:
                context.log.error(f"💥 [{page_id}|{date_str}] Unhandled: {e}")

            if completed % 5 == 0 or completed == len(combos):
                context.log.info(f"⏳ Tiến độ: {completed}/{len(combos)} combos ({100 * completed // len(combos)}%)")

    if not all_records:
        raise Exception("❌ Không có data insight nào được trả về!")

    # =========================
    # BƯỚC 5: XỬ LÝ DỮ LIỆU
    # =========================
    context.log.info("BƯỚC 5: Xử lý dữ liệu...")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df      = pd.DataFrame(all_records)
    df      = df.drop(columns=["ads_preview_url"], errors="ignore")

    # Ép kiểu
    int_cols = [
        "impressions", "reach", "link_click", "results", "post_comments",
        "messaging_conversation_started_7d", "messaging_first_reply",
        "lead_events", "purchases", "meta_purchase",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    float_cols = [
        "spend", "daily_budget", "budget_remaining", "lifetime_budget",
        "ctr", "cpm", "cpc", "cost_per_result", "purchase_roas",
        "purchases_conversion_value", "meta_purchase_value", "lead_events_value",
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    df["last_date_update"] = now_str

    # Sắp xếp cột
    priority_cols = [
        "date", "page_id", "platform", "ad_id", "adset_id", "account_id",
        "name", "status", "ad_status",
        "spend", "daily_budget", "budget_remaining", "lifetime_budget",
        "impressions", "reach", "link_click", "ctr", "cpm", "cpc",
        "results", "cost_per_result",
        "messaging_conversation_started_7d", "messaging_first_reply",
        "post_comments", "lead_events", "lead_events_value",
        "purchases", "purchases_conversion_value", "purchase_roas",
        "meta_purchase", "meta_purchase_value",
        "optimization_goal", "currency",
        "last_date_update",
    ]
    cols       = [c for c in priority_cols if c in df.columns]
    other_cols = [c for c in df.columns if c not in cols]
    df         = df[cols + other_cols]

    # Coverage report
    expected = set(df_ads["ad_id"].tolist())
    got      = set(df["ad_id"].tolist()) if "ad_id" in df.columns else set()
    missing  = expected - got
    coverage = len(got) / len(expected) * 100 if expected else 100
    context.log.info(f"📊 Coverage: {len(got)}/{len(expected)} ad_ids ({coverage:.1f}%)")
    if missing:
        context.log.warning(f"⚠️ Missing {len(missing)} ad_ids: {list(missing)[:20]}")

    context.log.info(f"✅ Tổng: {len(df)} records")

    # =========================
    # BƯỚC 6: CẬP NHẬT SQL SERVER
    # =========================
    context.log.info("BƯỚC 6: Cập nhật SQL Server...")

    DTYPE_MAP = {
        "date"                              : satypes.NVARCHAR(10),
        "page_id"                           : satypes.NVARCHAR(50),
        "platform"                          : satypes.NVARCHAR(20),
        "ad_id"                             : satypes.NVARCHAR(50),
        "adset_id"                          : satypes.NVARCHAR(50),
        "account_id"                        : satypes.NVARCHAR(50),
        "name"                              : satypes.NVARCHAR(500),
        "status"                            : satypes.NVARCHAR(50),
        "ad_status"                         : satypes.NVARCHAR(50),
        "optimization_goal"                 : satypes.NVARCHAR(100),
        "currency"                          : satypes.NVARCHAR(10),
        "spend"                             : satypes.FLOAT,
        "daily_budget"                      : satypes.FLOAT,
        "budget_remaining"                  : satypes.FLOAT,
        "lifetime_budget"                   : satypes.FLOAT,
        "ctr"                               : satypes.FLOAT,
        "cpm"                               : satypes.FLOAT,
        "cpc"                               : satypes.FLOAT,
        "cost_per_result"                   : satypes.FLOAT,
        "purchase_roas"                     : satypes.FLOAT,
        "purchases_conversion_value"        : satypes.FLOAT,
        "meta_purchase_value"               : satypes.FLOAT,
        "lead_events_value"                 : satypes.FLOAT,
        "impressions"                       : satypes.BIGINT,
        "reach"                             : satypes.BIGINT,
        "link_click"                        : satypes.BIGINT,
        "results"                           : satypes.BIGINT,
        "post_comments"                     : satypes.BIGINT,
        "messaging_conversation_started_7d" : satypes.BIGINT,
        "messaging_first_reply"             : satypes.BIGINT,
        "lead_events"                       : satypes.BIGINT,
        "purchases"                         : satypes.BIGINT,
        "meta_purchase"                     : satypes.BIGINT,
        "last_date_update"                  : satypes.NVARCHAR(30),
    }

    valid_cols = [c for c in df.columns if c in DTYPE_MAP]
    df_insert  = df[valid_cols]
    dtype_map  = {k: v for k, v in DTYPE_MAP.items() if k in df_insert.columns}

    with engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}
            WHERE [date] BETWEEN :date_from AND :date_to
        """), {"date_from": DATE_FROM_SQL, "date_to": DATE_TO_SQL})
        context.log.info(f"✅ Đã xóa data cũ từ {DATE_FROM_SQL} → {DATE_TO_SQL}")

        df_insert.to_sql(
            name      = TABLE_NAME,
            schema    = SCHEMA_NAME,
            con       = conn,
            if_exists = "append",
            index     = False,
            dtype     = dtype_map,
        )

    context.log.info(f"🎯 Đã insert {len(df_insert)} records vào [{SCHEMA_NAME}].[{TABLE_NAME}]!")
    context.log.info(f"📊 Pages : {df['page_id'].nunique()}")
    context.log.info(f"📊 Ads   : {df['ad_id'].nunique() if 'ad_id' in df.columns else 'N/A'}")
    if "spend" in df.columns:
        context.log.info(f"💰 Tổng spend      : {df['spend'].sum():,.0f}")
    if "impressions" in df.columns:
        context.log.info(f"👁  Tổng impressions: {df['impressions'].sum():,.0f}")
    if "results" in df.columns:
        context.log.info(f"🎯 Tổng results    : {df['results'].sum():,.0f}")
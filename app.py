import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from supabase import create_client


SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def read_center_stock() -> pd.DataFrame:
    sheet_id = st.secrets["sheets"]["sheet_id"]
    worksheet_name = st.secrets["sheets"]["center_stock"]

    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )
    ws = gspread.authorize(creds).open_by_key(sheet_id).worksheet(worksheet_name)

    rows: List[Dict[str, Any]] = ws.get_all_records(default_blank=None)
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=REQUIRED_COLS)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"구글시트 컬럼 누락: {missing} (필요: {REQUIRED_COLS})")

    df = df[REQUIRED_COLS].copy()
    df["style_code"] = df["style_code"].astype("string").str.strip()
    df["sku"] = df["sku"].astype("string").str.strip()
    df["center"] = df["center"].astype("string").str.strip()
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce").astype("Int64")
    return df.where(pd.notnull(df), None)


def sync_center_stock(replace_all: bool = True, chunk_size: int = 500) -> int:
    supabase = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
    rows = read_center_stock().to_dict(orient="records")

    if replace_all:
        supabase.table("center_stock").delete().neq("id", -1).execute()

    inserted = 0
    for i in range(0, len(rows), chunk_size):
        batch = rows[i : i + chunk_size]
        if not batch:
            continue
        res = supabase.table("center_stock").insert(batch).execute()
        data = getattr(res, "data", None)
        inserted += len(data) if isinstance(data, list) else len(batch)
    return inserted


st.set_page_config(page_title="center_stock sync", layout="wide")
st.title("center_stock 동기화 (Google Sheets → Supabase)")

replace_all = st.checkbox("전체 삭제 후 재삽입(덮어쓰기)", value=True)
chunk_size = st.number_input("배치 크기", min_value=50, max_value=2000, value=500, step=50)

if st.button("실행", type="primary"):
    with st.spinner("구글시트 읽는 중..."):
        df = read_center_stock()
    st.dataframe(df.head(50), use_container_width=True)
    st.caption(f"총 {len(df):,}행")

    with st.spinner("Supabase 적재 중..."):
        inserted = sync_center_stock(replace_all=replace_all, chunk_size=int(chunk_size))
    st.success(f"완료: {inserted:,}행 적재")



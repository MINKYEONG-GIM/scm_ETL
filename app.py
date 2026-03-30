from typing import Any, Dict, List

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from supabase import create_client


CENTER_STOCK_COLS = ["style_code", "sku", "center", "stock_qty"]
REORDER_COLS = ["style_code", "sku", "factory", "lead_time", "minimum_capacity"]


def _auto_chunk_size(n_rows: int) -> int:
    if n_rows <= 0:
        return 100
    if n_rows <= 100:
        return n_rows
    if n_rows <= 1000:
        return 250
    return 500


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
        return pd.DataFrame(columns=CENTER_STOCK_COLS)

    missing = [c for c in CENTER_STOCK_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"구글시트 컬럼 누락: {missing} (필요: {CENTER_STOCK_COLS})")

    df = df[CENTER_STOCK_COLS].copy()
    df["style_code"] = df["style_code"].astype("string").str.strip()
    df["sku"] = df["sku"].astype("string").str.strip()
    df["center"] = df["center"].astype("string").str.strip()
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce").astype("Int64")
    return df.where(pd.notnull(df), None)


def sync_center_stock(replace_all: bool = True) -> int:
    supabase = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
    rows = read_center_stock().to_dict(orient="records")
    chunk_size = _auto_chunk_size(len(rows))

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


def read_reorder() -> pd.DataFrame:
    sheet_id = st.secrets["sheets"]["sheet_id"]
    worksheet_name = st.secrets["sheets"]["reorder"]

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
        return pd.DataFrame(columns=REORDER_COLS)

    missing = [c for c in REORDER_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"구글시트 컬럼 누락: {missing} (필요: {REORDER_COLS})")

    df = df[REORDER_COLS].copy()
    df["style_code"] = df["style_code"].astype("string").str.strip()
    df["sku"] = df["sku"].astype("string").str.strip()
    df["factory"] = df["factory"].astype("string").str.strip()
    df["lead_time"] = pd.to_numeric(df["lead_time"], errors="coerce").astype("Int64")
    df["minimum_capacity"] = pd.to_numeric(df["minimum_capacity"], errors="coerce").astype("Int64")
    return df.where(pd.notnull(df), None)


def sync_reorder(replace_all: bool = True) -> int:
    supabase = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
    rows = read_reorder().to_dict(orient="records")
    chunk_size = _auto_chunk_size(len(rows))

    if replace_all:
        supabase.table("reorder").delete().neq("id", -1).execute()

    inserted = 0
    for i in range(0, len(rows), chunk_size):
        batch = rows[i : i + chunk_size]
        if not batch:
            continue
        res = supabase.table("reorder").insert(batch).execute()
        data = getattr(res, "data", None)
        inserted += len(data) if isinstance(data, list) else len(batch)
    return inserted


st.set_page_config(page_title="scm_etl sync", layout="wide")
st.title("Google Sheets → Supabase 동기화")

replace_all = st.checkbox("전체 삭제 후 재삽입(덮어쓰기)", value=True)

tab_center_stock, tab_reorder = st.tabs(["center_stock", "reorder"])

with tab_center_stock:
    if st.button("center_stock 실행", type="primary"):
        with st.spinner("구글시트 읽는 중..."):
            df = read_center_stock()
        st.dataframe(df.head(50), use_container_width=True)
        st.caption(f"총 {len(df):,}행")

        with st.spinner("Supabase 적재 중..."):
            inserted = sync_center_stock(replace_all=replace_all)
        st.success(f"완료: {inserted:,}행 적재")

with tab_reorder:
    if st.button("reorder 실행", type="primary"):
        with st.spinner("구글시트 읽는 중..."):
            df = read_reorder()
        st.dataframe(df.head(50), use_container_width=True)
        st.caption(f"총 {len(df):,}행")

        with st.spinner("Supabase 적재 중..."):
            inserted = sync_reorder(replace_all=replace_all)
        st.success(f"완료: {inserted:,}행 적재")


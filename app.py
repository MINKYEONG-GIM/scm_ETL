import json
import os
import re
import time
from typing import Dict, List

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from supabase import create_client


SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def make_unique_headers(headers: List[str]) -> List[str]:
    seen = {}
    result = []
    for h in headers:
        col = str(h).strip() or "unnamed"
        seen[col] = seen.get(col, 0) + 1
        result.append(col if seen[col] == 1 else f"{col}_{seen[col]}")
    return result


def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds_dict = dict(st.secrets["gcp_service_account"])
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def load_sheet_as_df(sheet_id: str, worksheet_name: str) -> pd.DataFrame:
    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = make_unique_headers(values[0])
    rows = values[1:] if len(values) > 1 else []
    if not rows:
        return pd.DataFrame(columns=headers)

    max_cols = len(headers)
    normalized_rows = []
    for row in rows:
        r = list(row)
        if len(r) < max_cols:
            r += [""] * (max_cols - len(r))
        elif len(r) > max_cols:
            r = r[:max_cols]
        normalized_rows.append(r)
    return pd.DataFrame(normalized_rows, columns=headers)


def get_spreadsheet(sheet_id: str):
    client = get_gspread_client()
    return client.open_by_key(sheet_id)


def get_available_sheet_names(sh) -> List[str]:
    return [ws.title for ws in sh.worksheets()]


def load_sheet_as_df_from_spreadsheet(sh, worksheet_name: str, fallback_first: bool = False) -> pd.DataFrame:
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        if not fallback_first:
            available = ", ".join(get_available_sheet_names(sh))
            raise ValueError(f"시트 '{worksheet_name}'를 찾지 못했습니다. 사용 가능 시트: [{available}]")
        worksheets = sh.worksheets()
        if not worksheets:
            raise ValueError("스프레드시트에 워크시트가 없습니다.")
        ws = worksheets[0]
        st.warning(f"'{worksheet_name}' 시트를 찾지 못해 첫 번째 시트('{ws.title}')를 사용합니다.")

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    headers = make_unique_headers(values[0])
    rows = values[1:] if len(values) > 1 else []
    if not rows:
        return pd.DataFrame(columns=headers)

    max_cols = len(headers)
    normalized_rows = []
    for row in rows:
        r = list(row)
        if len(r) < max_cols:
            r += [""] * (max_cols - len(r))
        elif len(r) > max_cols:
            r = r[:max_cols]
        normalized_rows.append(r)
    return pd.DataFrame(normalized_rows, columns=headers)


def normalize_value(value):
    # pandas/numpy scalar -> python scalar, NaN -> None
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return value


def clean_number(value) -> float:
    if pd.isna(value):
        return 0.0
    s = str(value).strip().replace(",", "")
    if s == "":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def parse_yearweek_to_date(yearweek: str) -> pd.Timestamp:
    s = str(yearweek).strip()
    if not re.match(r"^\d{4}-\d{1,2}$", s):
        return pd.NaT
    y, w = s.split("-")
    return pd.to_datetime(f"{int(y)}-W{int(w):02d}-1", format="%G-W%V-%u", errors="coerce")


def prepare_final_df(final_df: pd.DataFrame) -> pd.DataFrame:
    required = ["CALDAY", "PLANT", "MATERIAL", "SALE"]
    missing = [c for c in required if c not in final_df.columns]
    if missing:
        raise ValueError(f"final 시트 컬럼 누락: {missing}")

    df = final_df.copy()
    df["sku"] = df["MATERIAL"].astype(str).str.strip()
    df["sty"] = df["sku"].str[:10]
    df["style_code"] = df["sty"]
    df["plant"] = df["PLANT"].astype(str).str.strip()
    df["item_code"] = df["sku"].astype(str).str[2:4].fillna("")
    df["sale"] = df["SALE"].apply(clean_number)
    calday = df["CALDAY"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df["date"] = pd.to_datetime(calday, format="%Y%m%d", errors="coerce")
    return df


def plc_item_weekly(plc_df: pd.DataFrame, item_code: str) -> pd.DataFrame:
    if "아이템코드" not in plc_df.columns:
        raise ValueError("plc db 시트에 '아이템코드' 컬럼이 없습니다.")

    tmp = plc_df.copy()
    tmp["아이템코드"] = tmp["아이템코드"].astype(str).str.strip()
    matched = tmp[tmp["아이템코드"] == str(item_code).strip()]
    if matched.empty:
        return pd.DataFrame(columns=["year_week", "week_start", "sales"])
    row = matched.iloc[0]

    week_cols = [c for c in tmp.columns if re.match(r"^\d{4}-\d{1,2}$", str(c).strip())]
    records = []
    for col in week_cols:
        d = parse_yearweek_to_date(col)
        if pd.isna(d):
            continue
        records.append(
            {
                "year_week": str(col).strip(),
                "week_start": d,
                "sales": clean_number(row[col]),
            }
        )
    if not records:
        return pd.DataFrame(columns=["year_week", "week_start", "sales"])
    return pd.DataFrame(records).sort_values("week_start").reset_index(drop=True)


def forecast_weekly_from_ratio(plc_weekly: pd.DataFrame, final_item_df: pd.DataFrame) -> pd.DataFrame:
    if plc_weekly.empty:
        return pd.DataFrame(columns=["year_week", "forecast_qty", "is_peak_week"])

    base = plc_weekly.copy()
    base["week_no"] = base["week_start"].dt.isocalendar().week.astype(int)
    base["sales"] = pd.to_numeric(base["sales"], errors="coerce").fillna(0.0)
    total = float(base["sales"].sum())
    if total <= 0:
        return pd.DataFrame(columns=["year_week", "forecast_qty", "is_peak_week"])

    ratio_by_week = (base.groupby("week_no")["sales"].sum() / total).to_dict()
    last_by_week = base.groupby("week_no")["sales"].sum().to_dict()

    this_year = int(pd.Timestamp.today().year)
    cur_week = int(pd.Timestamp.today().isocalendar().week)
    fi = final_item_df.dropna(subset=["date"]).copy()
    fi["week_no"] = fi["date"].dt.isocalendar().week.astype(int)
    fi["iso_year"] = fi["date"].dt.isocalendar().year.astype(int)
    fi = fi[fi["iso_year"] == this_year]
    this_by_week = fi.groupby("week_no")["sale"].sum().to_dict()

    this_to_date = float(sum(v for w, v in this_by_week.items() if int(w) <= cur_week))
    last_to_date = float(sum(v for w, v in last_by_week.items() if int(w) <= cur_week))
    if last_to_date <= 0:
        return pd.DataFrame(columns=["year_week", "forecast_qty", "is_peak_week"])

    expected_total = total * (this_to_date / last_to_date)
    remaining_total = max(0.0, expected_total - this_to_date)
    rem_weeks = sorted([w for w in ratio_by_week.keys() if int(w) > cur_week])
    if not rem_weeks:
        return pd.DataFrame(columns=["year_week", "forecast_qty", "is_peak_week"])
    rem_ratio_sum = float(sum(ratio_by_week[w] for w in rem_weeks))
    if rem_ratio_sum <= 0:
        return pd.DataFrame(columns=["year_week", "forecast_qty", "is_peak_week"])

    peak_week = int(base.sort_values(["sales", "week_no"], ascending=[False, True]).iloc[0]["week_no"])
    rows = []
    for w in rem_weeks:
        qty = float(round(remaining_total * (ratio_by_week[w] / rem_ratio_sum), 2))
        rows.append(
            {
                "year_week": f"{this_year}-{int(w):02d}",
                "forecast_qty": qty,
                "is_peak_week": bool(int(w) == peak_week),
            }
        )
    return pd.DataFrame(rows)


def to_monthly_from_weekly(weekly_df: pd.DataFrame) -> pd.DataFrame:
    if weekly_df.empty:
        return pd.DataFrame(columns=["year_month", "forecast_qty", "is_peak_month"])
    tmp = weekly_df.copy()
    tmp["date"] = tmp["year_week"].apply(parse_yearweek_to_date)
    tmp["year_month"] = pd.to_datetime(tmp["date"]).dt.strftime("%Y-%m")
    m = tmp.groupby("year_month", as_index=False)["forecast_qty"].sum().sort_values("year_month")
    if m.empty:
        return pd.DataFrame(columns=["year_month", "forecast_qty", "is_peak_month"])
    peak_ym = str(m.sort_values(["forecast_qty", "year_month"], ascending=[False, True]).iloc[0]["year_month"])
    m["is_peak_month"] = m["year_month"].astype(str) == peak_ym
    return m


def create_forecast_run(row: Dict) -> int:
    data = {
        "SKU": normalize_value(row["sku"]),
        "style_code": normalize_value(row.get("style_code") or row.get("sty")),
        "plant": normalize_value(row.get("plant")),
        "shape_type": normalize_value(row.get("shape_type")),
        "shape_reason": normalize_value(row.get("shape_reason")),
        "peak_week": normalize_value(row.get("peak_week")),
        "peak_month": normalize_value(row.get("peak_month")),
        "season_start_week": normalize_value(row.get("season_start_week")),
        "season_end_week": normalize_value(row.get("season_end_week")),
    }
    res = supabase.table("sku_forecast_run").insert(data).execute()
    if not res.data:
        raise ValueError("sku_forecast_run insert 실패: 결과 data가 비어 있습니다.")
    return res.data[0]["id"]


def insert_monthly(run_id: int, df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        sty = normalize_value(r.get("style_code")) or normalize_value(r.get("sty"))
        rows.append(
            {
                "forecast_run_id": run_id,
                "sku": normalize_value(r["sku"]),
                "sty": sty,
                "year_month": normalize_value(r["year_month"]),
                "forecast_qty": int(normalize_value(r["forecast_qty"]) or 0),
                "stage": normalize_value(r.get("stage")),
                "is_peak_month": bool(normalize_value(r.get("is_peak_month", False)) or False),
            }
        )
    if rows:
        supabase.table("sku_monthly_forecast").insert(rows).execute()


def insert_weekly(run_id: int, df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        sty = normalize_value(r.get("style_code")) or normalize_value(r.get("sty"))
        rows.append(
            {
                "forecast_run_id": run_id,
                "sku": normalize_value(r["sku"]),
                "sty": sty,
                "year_week": normalize_value(r["year_week"]),
                "forecast_qty": float(normalize_value(r["forecast_qty"]) or 0),
                "stage": normalize_value(r.get("stage")),
                "is_peak_week": bool(normalize_value(r.get("is_peak_week", False)) or False),
            }
        )
    if rows:
        supabase.table("sku_weekly_forecast").insert(rows).execute()


def save_to_supabase(run_info: Dict, monthly_df: pd.DataFrame, weekly_df: pd.DataFrame):
    run_id = create_forecast_run(run_info)
    insert_monthly(run_id, monthly_df)
    insert_weekly(run_id, weekly_df)
    print("saved:", run_id)


def run_from_google_sheet(progress_bar=None, status_placeholder=None):
    sheets_cfg = dict(st.secrets["sheets"])
    sheet_id = sheets_cfg["sheet_id"]
    final_sheet = sheets_cfg.get("final_sheet", "final")
    plc_sheet = sheets_cfg.get("plc_sheet", "plc db")
    center_stock_sheet = sheets_cfg.get("center_stock_sheet", "center_stock")
    reorder_sheet = sheets_cfg.get("reorder_sheet", "reorder")

    sh = get_spreadsheet(sheet_id)
    final_df = load_sheet_as_df_from_spreadsheet(sh, final_sheet, fallback_first=False)
    plc_df = load_sheet_as_df_from_spreadsheet(sh, plc_sheet, fallback_first=False)
    # optional sheets (for future extension)
    try:
        _center_stock_df = load_sheet_as_df_from_spreadsheet(sh, center_stock_sheet, fallback_first=False)
    except Exception:
        _center_stock_df = pd.DataFrame()
    try:
        _reorder_df = load_sheet_as_df_from_spreadsheet(sh, reorder_sheet, fallback_first=False)
    except Exception:
        _reorder_df = pd.DataFrame()

    if final_df.empty:
        raise ValueError(f"'{final_sheet}' 시트가 비어 있습니다.")
    if plc_df.empty:
        raise ValueError(f"'{plc_sheet}' 시트가 비어 있습니다.")

    final_prepared = prepare_final_df(final_df)
    run_df = (
        final_prepared[["sku", "sty", "style_code", "plant", "item_code"]]
        .dropna(subset=["sku"])
        .drop_duplicates(subset=["sku", "style_code", "plant"])
        .reset_index(drop=True)
    )
    if run_df.empty:
        raise ValueError("final 시트에서 처리할 SKU가 없습니다.")

    success = 0
    fail = 0
    logs = []

    total = len(run_df)
    started_at = time.time()
    for idx, run_row in run_df.iterrows():
        try:
            sku = normalize_value(run_row["sku"])
            if status_placeholder is not None:
                elapsed = time.time() - started_at
                done = idx + 1
                avg_sec = elapsed / done if done > 0 else 0.0
                remain_sec = max(0.0, avg_sec * (total - done))
                status_placeholder.info(
                    f"진행 중: {done}/{total} - SKU {sku} | "
                    f"경과 {int(elapsed)}초 | 예상 남은 {int(remain_sec)}초"
                )
            sty = normalize_value(run_row["style_code"])
            plant = normalize_value(run_row["plant"])
            item_code = normalize_value(run_row["item_code"])
            final_item_df = final_prepared[
                (final_prepared["sku"].astype(str) == str(sku))
                & (final_prepared["style_code"].astype(str) == str(sty))
            ].copy()
            plc_weekly = plc_item_weekly(plc_df, str(item_code))
            weekly_fc = forecast_weekly_from_ratio(plc_weekly, final_item_df)
            monthly_fc = to_monthly_from_weekly(weekly_fc)

            if weekly_fc.empty:
                raise ValueError("주차 예측 데이터가 비어 있습니다.")

            run_info = {
                "sku": sku,
                "style_code": sty,
                "plant": plant,
                "shape_type": None,
                "shape_reason": None,
                "peak_week": None,
                "peak_month": None,
                "season_start_week": None,
                "season_end_week": None,
            }

            weekly_fc["sku"] = sku
            weekly_fc["sty"] = sty
            weekly_fc["stage"] = None
            monthly_fc["sku"] = sku
            monthly_fc["sty"] = sty
            monthly_fc["stage"] = None

            save_to_supabase(run_info, monthly_fc, weekly_fc)
            success += 1
            logs.append({"sku": sku, "status": "success", "message": ""})
        except Exception as e:
            fail += 1
            logs.append({"sku": normalize_value(run_row.get("sku")), "status": "fail", "message": str(e)})
        finally:
            if progress_bar is not None and total > 0:
                progress_bar.progress((idx + 1) / total)

    return success, fail, pd.DataFrame(logs)


def main():
    st.set_page_config(page_title="Sheet -> Supabase ETL", layout="wide")
    st.title("Google Sheet -> Supabase 저장")
    st.write("버튼을 누르면 시트 데이터를 읽어 3개 테이블에 저장합니다.")

    if st.button("실행하기", type="primary", use_container_width=True):
        progress_bar = st.progress(0.0)
        status_placeholder = st.empty()
        with st.spinner("구글 시트 읽는 중 / Supabase 저장 중..."):
            try:
                success, fail, log_df = run_from_google_sheet(
                    progress_bar=progress_bar,
                    status_placeholder=status_placeholder,
                )
                progress_bar.progress(1.0)
                status_placeholder.success("처리 완료")
                st.success(f"완료: 성공 {success}건 / 실패 {fail}건")
                st.dataframe(log_df, use_container_width=True, hide_index=True)
            except Exception as e:
                status_placeholder.error("중단됨")
                st.error(f"실행 실패: {e}")


if __name__ == "__main__":
    main()

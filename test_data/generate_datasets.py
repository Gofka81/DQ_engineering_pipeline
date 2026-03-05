"""
Generate 6 diverse test datasets for LLM enrichment testing.

Covers: e-commerce, medical, finance, IoT, retail, CRM.
Each dataset has intentional DQ issues targeting specific LLM behaviours:
  - leave_null vs drop_column distinction
  - skewed numeric (median vs mean)
  - sentinel strings (null_pct=0, invalid_count>0)
  - correlated intentional nulls (ATM withdrawals, unconverted prospects)
  - ID columns -> drop_row
  - abbreviated column names -> rename

Run: python test_data/generate_datasets.py
"""

import uuid
import numpy as np
import pandas as pd
from pathlib import Path

OUT = Path(__file__).parent
RNG = np.random.default_rng(42)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _null_mask(n: int, pct: float) -> np.ndarray:
    """Boolean mask with `pct` fraction True (= will become NaN)."""
    mask = np.zeros(n, dtype=bool)
    idx  = RNG.choice(n, size=int(n * pct), replace=False)
    mask[idx] = True
    return mask


def _save(df: pd.DataFrame, name: str) -> None:
    path = OUT / name
    df.to_csv(path, index=False)
    print(f"  {name:<40} {len(df):>5} rows  {len(df.columns):>2} cols")


# ---------------------------------------------------------------------------
# Dataset 1 — E-commerce Orders (small, narrow, skewed + sentinel + IDs)
# ---------------------------------------------------------------------------

def make_ecomm_orders(n: int = 100) -> pd.DataFrame:
    """
    Tests: ID rename (ord_id/cust_id), skewed txn_amt (median >> mean),
    sentinel disc_pct ("N/A" in numeric col → null_pct=0 invalid_count>0).
    """
    ord_ids  = [str(uuid.uuid4()) for _ in range(n)]
    cust_ids = [f"C{RNG.integers(1000, 9999)}" for _ in range(n)]
    ord_dts  = pd.date_range("2024-01-01", periods=n, freq="1D").strftime("%Y-%m-%d").tolist()

    # Right-skewed: lognormal — mean will be ~3-4x median
    txn_amt = RNG.lognormal(mean=4.0, sigma=1.2, size=n).clip(5, 50000).round(2)

    # disc_pct: mostly 0-40%, but 12 rows replaced with sentinel "N/A"
    disc_pct = RNG.uniform(0, 40, size=n).round(1).astype(object)
    sentinel_idx = RNG.choice(n, size=12, replace=False)
    for i in sentinel_idx:
        disc_pct[i] = "N/A"

    pmt_mthd_vals = ["card", "paypal", "bnpl", "cash"]
    pmt_mthd = RNG.choice(pmt_mthd_vals, size=n, p=[0.55, 0.25, 0.15, 0.05]).astype(object)
    rtn_flg  = RNG.choice([True, False], size=n, p=[0.1, 0.9])

    df = pd.DataFrame({
        "ord_id":   ord_ids,
        "cust_id":  cust_ids,
        "ord_dt":   ord_dts,
        "txn_amt":  txn_amt,
        "disc_pct": disc_pct,
        "pmt_mthd": pmt_mthd,
        "rtn_flg":  rtn_flg,
    })

    # Inject nulls
    df.loc[_null_mask(n, 0.08), "ord_id"]   = None
    df.loc[_null_mask(n, 0.05), "txn_amt"]  = None
    df.loc[_null_mask(n, 0.10), "pmt_mthd"] = None

    return df


# ---------------------------------------------------------------------------
# Dataset 2 — Medical Lab Results (medium, medium-wide, leave_null + skewed)
# ---------------------------------------------------------------------------

def make_medical_labs(n: int = 300) -> pd.DataFrame:
    """
    Tests: leave_null for adv_evt_dt (75% null = intentional clinical absence),
    bimodal glc_mg (healthy vs diabetic → mean >> median),
    abbreviated column renames (pat_id, diag_cd, trt_prot, age_yrs, glc_mg).
    """
    pat_ids  = [f"PAT-{i:05d}" for i in range(1, n + 1)]
    visit_dts = pd.date_range("2023-01-01", periods=n, freq="1D").strftime("%Y-%m-%d").tolist()

    age_yrs   = RNG.integers(18, 85, size=n)

    # BMI: normally distributed
    bmi = RNG.normal(26.5, 4.5, size=n).clip(15, 55).round(1)

    # glc_mg: bimodal — healthy (70%) + diabetic (30%)
    healthy  = RNG.normal(95, 15, size=int(n * 0.70)).clip(60, 130)
    diabetic = RNG.normal(280, 60, size=n - int(n * 0.70)).clip(140, 600)
    glc_mg   = np.concatenate([healthy, diabetic])
    RNG.shuffle(glc_mg)
    glc_mg   = glc_mg.round(1)

    systolic  = RNG.integers(100, 180, size=n)
    diastolic = RNG.integers(60, 110, size=n)

    diag_codes = ["E11.9", "I10", "J45", "M79.3", "F32.1"]
    diag_cd    = RNG.choice(diag_codes, size=n).astype(object)

    trt_vals = ["protocol_A", "protocol_B", "protocol_C", "watchful_wait"]
    trt_prot = RNG.choice(trt_vals, size=n, p=[0.35, 0.25, 0.20, 0.20]).astype(object)

    # adv_evt_dt: 75% intentionally null (no adverse event)
    has_event = RNG.random(size=n) < 0.25
    base_date = pd.Timestamp("2023-01-01")
    adv_evt_dt = np.where(
        has_event,
        [(base_date + pd.Timedelta(days=int(d))).strftime("%Y-%m-%d")
         for d in RNG.integers(30, 365, size=n)],
        None,
    )

    df = pd.DataFrame({
        "pat_id":     pat_ids,
        "visit_dt":   visit_dts,
        "age_yrs":    age_yrs,
        "bmi":        bmi,
        "glc_mg":     glc_mg,
        "systolic":   systolic,
        "diastolic":  diastolic,
        "diag_cd":    diag_cd,
        "trt_prot":   trt_prot,
        "adv_evt_dt": adv_evt_dt,
    })

    # Inject nulls
    df.loc[_null_mask(n, 0.06),  "bmi"]     = None
    df.loc[_null_mask(n, 0.15),  "glc_mg"]  = None
    df.loc[_null_mask(n, 0.08),  "diag_cd"] = None
    df.loc[_null_mask(n, 0.10),  "trt_prot"] = None

    return df


# ---------------------------------------------------------------------------
# Dataset 3 — Financial Transactions (medium, correlated nulls, leave_null)
# ---------------------------------------------------------------------------

def make_finance_txn(n: int = 350) -> pd.DataFrame:
    """
    Tests: correlated leave_null (merchant_nm/merchant_cat null exactly where
    txn_typ="atm"), rename (txn_id, acct_num, txn_typ, txn_amt, chnl_cd).
    LLM must read sample rows to infer the ATM → no merchant pattern.
    """
    txn_ids  = [str(uuid.uuid4()) for _ in range(n)]
    acct_nums = [f"ACC-{RNG.integers(10000000, 99999999)}" for _ in range(n)]
    txn_dts   = pd.date_range("2024-01-01", periods=n, freq="12h").strftime("%Y-%m-%d").tolist()

    # txn_typ — ~28% are "atm"
    txn_typ_vals = ["purchase", "withdrawal", "transfer", "refund", "atm"]
    txn_typ = RNG.choice(txn_typ_vals, size=n, p=[0.40, 0.15, 0.10, 0.07, 0.28]).astype(object)
    is_atm = txn_typ == "atm"

    # txn_amt: mix of positive/negative (withdrawals are negative)
    txn_amt = RNG.lognormal(mean=4.5, sigma=1.0, size=n).round(2)
    txn_amt[txn_typ == "withdrawal"] *= -1
    txn_amt[is_atm] *= -1

    # merchant_nm/cat: null exactly for ATM rows
    merchants = ["Amazon", "Starbucks", "Shell", "Walmart", "Netflix",
                 "Uber", "McDonald's", "Apple Store", "IKEA", "Spotify"]
    cats      = ["retail", "food", "fuel", "retail", "entertainment",
                 "transport", "food", "electronics", "home", "entertainment"]

    merchant_nm  = RNG.choice(merchants, size=n).astype(object)
    merchant_cat = np.array([cats[merchants.index(m)] for m in merchant_nm], dtype=object)
    merchant_nm[is_atm]  = None
    merchant_cat[is_atm] = None

    chnl_cd_vals = ["online", "pos", "atm", "mobile"]
    chnl_cd = RNG.choice(chnl_cd_vals, size=n, p=[0.30, 0.35, 0.20, 0.15]).astype(object)
    is_flagged = (RNG.random(size=n) < 0.02)

    df = pd.DataFrame({
        "txn_id":       txn_ids,
        "acct_num":     acct_nums,
        "txn_dt":       txn_dts,
        "txn_amt":      txn_amt,
        "txn_typ":      txn_typ,
        "merchant_nm":  merchant_nm,
        "merchant_cat": merchant_cat,
        "chnl_cd":      chnl_cd,
        "is_flagged":   is_flagged,
    })

    # Additional nulls
    df.loc[_null_mask(n, 0.05), "txn_id"]  = None
    df.loc[_null_mask(n, 0.08), "chnl_cd"] = None
    df.loc[_null_mask(n, 0.03), "txn_dt"]  = None

    return df


# ---------------------------------------------------------------------------
# Dataset 4 — IoT Sensor Readings (large, sentinel strings, real outliers)
# ---------------------------------------------------------------------------

def make_iot_sensors(n: int = 1200) -> pd.DataFrame:
    """
    Tests: sentinel "-999" in temp_c (invalid_count>0, null_pct small),
    real outliers in vibr_mm (genuine fault readings, not DQ errors),
    right-skewed pres_bar, large row count (sample is capped at 25 rows).
    """
    sensor_ids = [f"SNS-{RNG.integers(1, 51):03d}" for _ in range(n)]
    loc_codes  = [f"LOC-{RNG.integers(1, 11):02d}" for _ in range(n)]
    read_dts   = pd.date_range("2024-01-01", periods=n, freq="1h").strftime("%Y-%m-%d %H:%M").tolist()

    # temp_c: normal around 22°C; then inject sentinel "-999" for offline sensors
    temp_c = RNG.normal(22, 3, size=n).round(2).astype(object)
    null_temp = _null_mask(n, 0.04)
    sentinel_temp = _null_mask(n, 0.03) & ~null_temp
    temp_c[null_temp] = None
    temp_c[sentinel_temp] = "-999"

    # pres_bar: exponential → mean >> median (right skew)
    pres_bar = RNG.exponential(scale=1.8, size=n).clip(0.1, 30).round(3)

    # hum_pct: normal, bounded
    hum_pct = RNG.normal(55, 15, size=n).clip(0, 100).round(1)

    # vibr_mm: right-skewed + real outliers (genuine fault spikes)
    vibr_base    = RNG.exponential(scale=0.8, size=n).round(3)
    fault_idx    = RNG.choice(n, size=80, replace=False)
    vibr_base[fault_idx] = RNG.uniform(15, 50, size=80).round(3)  # fault readings
    vibr_mm = vibr_base

    status_vals = ["ok", "warn", "fault", "offline"]
    status_cd   = RNG.choice(status_vals, size=n, p=[0.70, 0.15, 0.10, 0.05]).astype(object)

    df = pd.DataFrame({
        "sensor_id": sensor_ids,
        "loc_cd":    loc_codes,
        "read_ts":   read_dts,
        "temp_c":    temp_c,
        "pres_bar":  pres_bar,
        "hum_pct":   hum_pct,
        "vibr_mm":   vibr_mm,
        "status_cd": status_cd,
    })

    # Inject nulls
    df.loc[_null_mask(n, 0.08), "pres_bar"]  = None
    df.loc[_null_mask(n, 0.06), "hum_pct"]   = None
    df.loc[_null_mask(n, 0.05), "vibr_mm"]   = None
    df.loc[_null_mask(n, 0.12), "status_cd"] = None

    return df


# ---------------------------------------------------------------------------
# Dataset 5 — Retail Product Catalog (medium-large, wide, drop_column)
# ---------------------------------------------------------------------------

def make_retail_catalog(n: int = 500) -> pd.DataFrame:
    """
    Tests: sub_cat_cd at 72% null → drop_column confirmation,
    margin_pct sentinel "42%" strings (invalid_count heavy, null_pct=0),
    wide schema (13 cols) with many abbreviations.
    """
    skus = [f"SKU-{RNG.integers(100000, 999999)}" for _ in range(n)]

    adj  = ["Premium", "Classic", "Ultra", "Pro", "Lite", "Sport", "Eco"]
    nouns = ["Jacket", "Shoe", "Bag", "Watch", "Shirt", "Pants", "Cap"]
    prod_nm = [f"{RNG.choice(adj)} {RNG.choice(nouns)}" for _ in range(n)]

    brands = ["NIKE", "ADCS", "PUMA", "RLPH", "GCCI", "ZMRA", "H&M", "ZARA"]
    brand_cd = RNG.choice(brands, size=n, p=[0.20, 0.18, 0.12, 0.10, 0.08, 0.12, 0.10, 0.10]).astype(object)

    cats = ["FTWR", "APRL", "ELEC", "ACCSS", "SPORT"]
    cat_cd = RNG.choice(cats, size=n, p=[0.30, 0.25, 0.15, 0.15, 0.15]).astype(object)

    # sub_cat_cd: 72% null (too sparse → drop_column)
    sub_cats = ["SNKR", "JCKT", "PHNE", "WTCH", "HLMT"]
    sub_cat_cd = RNG.choice(sub_cats, size=n).astype(object)
    sub_cat_cd[_null_mask(n, 0.72)] = None

    unit_cost  = RNG.uniform(10, 200, size=n).round(2)
    unit_price = (unit_cost * RNG.uniform(1.4, 3.5, size=n)).round(2)

    # margin_pct: 60% stored as "42%" strings (invalid) — sentinel issue
    margin_float = ((unit_price - unit_cost) / unit_price * 100).round(1)
    margin_pct   = margin_float.astype(object)
    pct_idx      = RNG.choice(n, size=int(n * 0.60), replace=False)
    for i in pct_idx:
        margin_pct[i] = f"{margin_float[i]:.0f}%"

    stock_qty  = RNG.integers(0, 500, size=n).astype(object)
    out_idx    = RNG.choice(n, size=15, replace=False)
    for i in out_idx:
        stock_qty[i] = "OUT"  # sentinel for out-of-stock

    reorder_pt = RNG.integers(10, 100, size=n).astype(object)

    supplier_cd_vals = [f"SUP-{i:03d}" for i in range(1, 41)]
    supplier_cd = RNG.choice(supplier_cd_vals, size=n).astype(object)

    launch_dts = pd.date_range("2020-01-01", periods=n, freq="3D").strftime("%Y-%m-%d").tolist()
    disc_flag  = RNG.choice([True, False], size=n, p=[0.08, 0.92])

    df = pd.DataFrame({
        "sku":         skus,
        "prod_nm":     prod_nm,
        "brand_cd":    brand_cd,
        "cat_cd":      cat_cd,
        "sub_cat_cd":  sub_cat_cd,
        "unit_cost":   unit_cost,
        "unit_price":  unit_price,
        "margin_pct":  margin_pct,
        "stock_qty":   stock_qty,
        "reorder_pt":  reorder_pt,
        "supplier_cd": supplier_cd,
        "launch_dt":   launch_dts,
        "disc_flag":   disc_flag,
    })

    # Inject nulls
    df.loc[_null_mask(n, 0.08), "brand_cd"]    = None
    df.loc[_null_mask(n, 0.06), "cat_cd"]      = None
    df.loc[_null_mask(n, 0.15), "supplier_cd"] = None
    df.loc[_null_mask(n, 0.10), "launch_dt"]   = None
    df.loc[_null_mask(n, 0.05), "unit_cost"]   = None
    df.loc[_null_mask(n, 0.03), "reorder_pt"]  = None

    return df


# ---------------------------------------------------------------------------
# Dataset 6 — CRM Contacts (very large, correlated leave_null, bimodal)
# ---------------------------------------------------------------------------

def make_crm_contacts(n: int = 2000) -> pd.DataFrame:
    """
    Tests: correlated leave_null (conv_dt null for unconverted prospects,
    visible via sales_stg in sample rows), bimodal opp_val (mean >> median),
    email + phone pattern columns, very large row count.
    """
    cntct_ids = [str(uuid.uuid4()) for _ in range(n)]

    acq_vals  = ["organic", "paid_search", "referral", "trade_show", "cold_outbound"]
    acq_src   = RNG.choice(acq_vals, size=n, p=[0.30, 0.25, 0.20, 0.15, 0.10]).astype(object)

    companies = ["Acme Corp", "Globex", "Initech", "Umbrella Ltd", "Stark Industries",
                 "Wayne Enterprises", "Dunder Mifflin", "Vandelay", "Sterling Cooper", "Cyberdyne"]
    cmpny_nm  = RNG.choice(companies, size=n).astype(object)

    # email: 18% null, 5% malformed
    first_names = ["james", "sarah", "michael", "lisa", "david", "emma", "chris", "anna"]
    domains     = ["company.com", "corp.io", "biz.net", "enterprise.org"]
    emails = np.array([
        f"{RNG.choice(first_names)}.{RNG.integers(100, 999)}@{RNG.choice(domains)}"
        for _ in range(n)
    ], dtype=object)
    malformed = ["plaintext", "john@", "@nodomain.com", "two@@at.com", "no-at-sign"]
    mal_idx = RNG.choice(n, size=int(n * 0.05), replace=False)
    for i in mal_idx:
        emails[i] = RNG.choice(malformed)
    emails[_null_mask(n, 0.18)] = None

    # ph_num: 25% null, rest have inconsistent formats
    def _phone():
        d = RNG.integers(2000000000, 9999999999)
        fmt = RNG.choice(["dashes", "parens", "plain"])
        if fmt == "dashes":
            return f"+1-{str(d)[:3]}-{str(d)[3:6]}-{str(d)[6:]}"
        elif fmt == "parens":
            return f"({str(d)[:3]}) {str(d)[3:6]}-{str(d)[6:]}"
        return str(d)

    ph_num = np.array([_phone() for _ in range(n)], dtype=object)
    ph_num[_null_mask(n, 0.25)] = None

    lead_scr = RNG.uniform(0, 100, size=n).round(1)

    # sales_stg — 55% are unconverted (prospect/qualified)
    stg_vals = ["prospect", "qualified", "proposal", "closed_won", "closed_lost"]
    sales_stg = RNG.choice(stg_vals, size=n, p=[0.35, 0.20, 0.15, 0.20, 0.10]).astype(object)

    # conv_dt: null for prospect + qualified (correlated)
    is_unconverted = np.isin(sales_stg, ["prospect", "qualified"])
    base_date = pd.Timestamp("2023-01-01")
    conv_dt = np.where(
        ~is_unconverted,
        [(base_date + pd.Timedelta(days=int(d))).strftime("%Y-%m-%d")
         for d in RNG.integers(1, 730, size=n)],
        None,
    )

    # opp_val: bimodal — 0 for unconverted, lognormal for converted
    opp_val = np.zeros(n)
    converted_mask = ~is_unconverted
    opp_val[converted_mask] = np.exp(
        RNG.normal(10, 1.5, size=converted_mask.sum())
    ).round(2)
    opp_val[_null_mask(n, 0.04)] = np.nan

    mgr_names = [f"Manager_{i}" for i in range(1, 31)]
    acct_mgr  = RNG.choice(mgr_names, size=n).astype(object)

    reg_vals = ["EMEA", "APAC", "AMER", "LATAM"]
    reg_cd   = RNG.choice(reg_vals, size=n, p=[0.30, 0.25, 0.30, 0.15]).astype(object)

    lst_cntct_dts = pd.date_range("2024-01-01", periods=n, freq="8h").strftime("%Y-%m-%d").tolist()

    df = pd.DataFrame({
        "cntct_id":      cntct_ids,
        "acq_src":       acq_src,
        "cmpny_nm":      cmpny_nm,
        "email":         emails,
        "ph_num":        ph_num,
        "lead_scr":      lead_scr,
        "conv_dt":       conv_dt,
        "opp_val":       opp_val,
        "sales_stg":     sales_stg,
        "acct_mgr":      acct_mgr,
        "reg_cd":        reg_cd,
        "lst_cntct_dt":  lst_cntct_dts,
    })

    # Additional nulls
    df.loc[_null_mask(n, 0.03), "cntct_id"]     = None
    df.loc[_null_mask(n, 0.09), "acq_src"]      = None
    df.loc[_null_mask(n, 0.06), "sales_stg"]    = None
    df.loc[_null_mask(n, 0.07), "reg_cd"]       = None
    df.loc[_null_mask(n, 0.08), "lst_cntct_dt"] = None

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Generating test datasets...\n")
    print(f"  {'file':<40} {'rows':>5}  {'cols':>4}")
    print("  " + "-" * 52)

    _save(make_ecomm_orders(),   "ecomm_orders_small.csv")
    _save(make_medical_labs(),   "medical_labs_medium.csv")
    _save(make_finance_txn(),    "finance_txn_medium.csv")
    _save(make_iot_sensors(),    "iot_sensors_large.csv")
    _save(make_retail_catalog(), "retail_catalog_wide.csv")
    _save(make_crm_contacts(),   "crm_contacts_xlarge.csv")

    print("\nDone. Files saved to test_data/")

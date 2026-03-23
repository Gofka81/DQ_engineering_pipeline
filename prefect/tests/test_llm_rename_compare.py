"""
Rename prompt comparison — tests current production rename prompt (B) against
the same prompt with an added QUALITY GATE self-verification block (C).

Both approaches use the existing split architecture (strategy call already
separate in production). Only the rename call prompt differs.

Approach B (current prod):  sparse format — return only renamed columns
Approach C (gate):          same prompt + QUALITY GATE self-verification block

Usage:
    LLM_API_KEY=<key> .venv/bin/python3 prefect/tests/test_llm_rename_compare.py [fragment]

    With no argument: runs all datasets.
    With a name fragment: runs matching datasets only (e.g. "opaque" or "iot").
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from groq import Groq

import flows.llm_enrichment as llm_mod
from flows.dq_logic import build_recommendations, profile_dataframe, score_profile

logging.disable(logging.CRITICAL)


class _Tee:
    """Write to both the real stdout and an output file simultaneously."""

    def __init__(self, filepath: Path) -> None:
        self._file   = filepath.open("w", encoding="utf-8")
        self._stdout = sys.stdout

    def write(self, data: str) -> None:
        self._stdout.write(data)
        self._file.write(data)

    def flush(self) -> None:
        self._stdout.flush()
        self._file.flush()

    def close(self) -> None:
        self._file.close()


# ---------------------------------------------------------------------------
# Paths + datasets
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).parent.parent.parent
TEST_DATA  = REPO_ROOT / "test_data"

DATASETS = [
    ("ecomm_orders_small.csv",    "E-commerce Orders",       "skewed txn_amt, sentinel disc_pct, ID renames"),
    ("medical_labs_medium.csv",   "Medical Lab Results",     "leave_null adv_evt_dt, bimodal glc_mg"),
    ("finance_txn_medium.csv",    "Financial Transactions",  "correlated leave_null merchant_nm/cat (ATM)"),
    ("iot_sensors_large.csv",     "IoT Sensor Readings",     "sentinel temp_c '-999', real outliers vibr_mm"),
    ("retail_catalog_wide.csv",   "Retail Product Catalog",  "drop_column sub_cat_cd, sentinel margin_pct '42%'"),
    ("crm_contacts_xlarge.csv",   "CRM Contacts",            "correlated leave_null conv_dt, bimodal opp_val"),
    ("opaque_fields.csv",         "Opaque Fields",           "all columns f001–f008, rename inferred purely from sample values"),
]

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

# Approach B — current production prompt (imported directly so any edits to
# production are automatically reflected here).
_RENAME_SYSTEM_B = llm_mod._RENAME_SYSTEM

# Approach C — identical to B + explicit quality gate self-verification block.
_RENAME_SYSTEM_C = _RENAME_SYSTEM_B.rstrip("\\") + """

QUALITY GATE:
Before producing output, verify each rename against ALL of these rules:
- every key in your output exactly matches an input column name — no invented keys
- every non-null value is a valid snake_case Python identifier (letters, digits, underscores only)
- sample values concretely confirm the implied meaning — if uncertain, output null or omit
- you are only expanding abbreviations, never dropping or reordering parts of the name
- well-known acronyms (sku, uuid, url, api, sql, ip, atm, pos) are left unexpanded
- columns with opaque names (single letters, generic codes, numeric suffixes) must be omitted unless sample values make the domain unambiguous
- output is a single flat JSON object — no nested keys, no markdown fences\
"""

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

W = 76

def _header(title: str) -> None:
    print(f"\n{'=' * W}")
    print(f"  {title}")
    print(f"{'=' * W}")

def _section(title: str) -> None:
    print(f"\n{'─' * W}")
    print(f"  {title}")
    print(f"{'─' * W}")


# ---------------------------------------------------------------------------
# Single rename call helper
# ---------------------------------------------------------------------------

def _rename_call(
    base: dict,
    df: pd.DataFrame,
    api_key: str,
    system_prompt: str,
    label: str,
    max_tokens: int = 512,
) -> tuple[dict[str, str], int, int, str]:
    """
    One rename API call with the given system prompt.
    Returns (renames, prompt_tokens, completion_tokens, raw_response).
    renames: {old_col: new_name} for columns being renamed.
    """
    known_columns = set(base["columns"].keys())
    col_type_map  = {col: cd["type"] for col, cd in base["columns"].items()}

    lines = []
    for col in base["columns"]:
        col_type = col_type_map[col]
        if col in df.columns:
            samples = df[col].dropna().head(3).astype(str).tolist()
            sample_str = ", ".join(samples) if samples else "(no values)"
        else:
            sample_str = "(no values)"
        lines.append(f"{col} ({col_type}): {sample_str}")

    rename_raw = ""
    renames: dict[str, str] = {}
    rename_in = rename_out = 0

    try:
        client = Groq(api_key=api_key)
        resp = client.chat.completions.create(
            model=llm_mod._MODEL_RENAME,
            max_tokens=max_tokens,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": "\n".join(lines)},
            ],
        )
        rename_raw = resp.choices[0].message.content.strip()
        rename_in  = resp.usage.prompt_tokens
        rename_out = resp.usage.completion_tokens

        clean = rename_raw
        if clean.startswith("```"):
            fence_lines = clean.splitlines()
            fence_lines = fence_lines[1:] if fence_lines else fence_lines
            if fence_lines and fence_lines[-1].strip() == "```":
                fence_lines = fence_lines[:-1]
            clean = "\n".join(fence_lines).strip()

        parsed = json.loads(clean)
        if not isinstance(parsed, dict):
            raise ValueError(f"expected dict, got {type(parsed)}")

        errors = []
        for old, new in parsed.items():
            if old not in known_columns:
                errors.append(f"unknown column '{old}'")
                continue
            if new is None:
                continue  # explicit keep-as-is
            if not isinstance(new, str) or not new.isidentifier():
                errors.append(f"invalid identifier '{new}' for '{old}'")
            else:
                renames[old] = new

        if errors:
            print(f"    [{label}] validation warnings: {'; '.join(errors)}")

    except Exception as e:
        print(f"    [{label}] call failed: {e}")
        rename_raw = f"ERROR: {e}"

    return renames, rename_in, rename_out, rename_raw


# ---------------------------------------------------------------------------
# Per-dataset comparison
# ---------------------------------------------------------------------------

def run_dataset(
    fname: str,
    label: str,
    description: str,
    api_key: str,
) -> dict:
    path = TEST_DATA / fname
    if not path.exists():
        print(f"  [SKIP] {fname} not found — run test_data/generate_datasets.py first")
        return {}

    df       = pd.read_csv(path)
    profile  = profile_dataframe(df)
    dq_score = score_profile(profile)
    base     = build_recommendations(df, profile, dq_score)

    _header(f"{label}  |  {fname}")
    print(f"  Description : {description}")
    print(f"  Shape       : {len(df)} rows × {len(df.columns)} cols")
    print(f"  Columns     : {', '.join(df.columns.tolist())}")

    # Approach B — production prompt, no gate
    renames_b, b_in, b_out, b_raw = _rename_call(
        base, df, api_key, _RENAME_SYSTEM_B, label="B"
    )
    time.sleep(1.0)

    # Approach C — same prompt + quality gate
    renames_c, c_in, c_out, c_raw = _rename_call(
        base, df, api_key, _RENAME_SYSTEM_C, label="C", max_tokens=512
    )

    # --- Rename comparison table ---
    _section("RENAME COMPARISON  (B = current prod | C = prod + quality gate)")
    all_cols = sorted(set(renames_b) | set(renames_c))

    if not all_cols:
        print("  Neither approach suggested any renames.")
    else:
        col_w  = max(len(c) for c in all_cols) + 2
        name_w = 28
        print(f"  {'Column':<{col_w}}  {'B (current)':<{name_w}}  {'C (gate)':<{name_w}}  Status")
        print(f"  {'─'*col_w}  {'─'*name_w}  {'─'*name_w}  {'─'*14}")

        for col in all_cols:
            b_name = renames_b.get(col, "—")
            c_name = renames_c.get(col, "—")

            if col in renames_b and col in renames_c:
                status = "AGREE" if b_name == c_name else "DISAGREE"
            elif col in renames_b:
                status = "B-only  ← C rejected"
            else:
                status = "C-only  ← B missed"

            print(f"  {col:<{col_w}}  {b_name:<{name_w}}  {c_name:<{name_w}}  {status}")

    agree    = sum(1 for c in all_cols if c in renames_b and c in renames_c and renames_b[c] == renames_c[c])
    disagree = sum(1 for c in all_cols if c in renames_b and c in renames_c and renames_b[c] != renames_c[c])
    b_only   = sum(1 for c in all_cols if c in renames_b and c not in renames_c)
    c_only   = sum(1 for c in all_cols if c not in renames_b and c in renames_c)

    # --- Raw responses ---
    _section("RENAME CALL B — RAW RESPONSE")
    print(b_raw if b_raw else "(no response)")

    _section("RENAME CALL C — RAW RESPONSE (quality gate)")
    print(c_raw if c_raw else "(no response)")

    # --- Token comparison ---
    _section("TOKEN COMPARISON")
    b_total = b_in + b_out
    c_total = c_in + c_out
    delta   = c_total - b_total
    delta_pct = (delta / b_total * 100) if b_total else 0

    print(f"  B (no gate)  :  in={b_in:>5}  out={b_out:>4}  total={b_total:>5}")
    print(f"  C (gate)     :  in={c_in:>5}  out={c_out:>4}  total={c_total:>5}  ({delta_pct:+.0f}% vs B)")

    return {
        "label":     label,
        "cols":      len(df.columns),
        "b_renames": len(renames_b),
        "c_renames": len(renames_c),
        "agree":     agree,
        "disagree":  disagree,
        "b_only":    b_only,
        "c_only":    c_only,
        "b_total":   b_total,
        "c_total":   c_total,
        "delta_pct": delta_pct,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    api_key = os.getenv("LLM_API_KEY", "").strip()
    if not api_key:
        env_file = REPO_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.strip().startswith("LLM_API_KEY"):
                    api_key = line.split("=", 1)[1].strip()
                    break
    if not api_key:
        print("ERROR: LLM_API_KEY not set. Export it or add to .env")
        sys.exit(1)

    os.environ["LLM_API_KEY"] = api_key

    results_dir = Path(__file__).parent / "rename_compare_results"
    results_dir.mkdir(exist_ok=True)
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = results_dir / f"{ts}.txt"
    tee     = _Tee(outfile)
    sys.stdout = tee
    print(f"  Saving output to: {outfile}")

    fragment = sys.argv[1].lower() if len(sys.argv) > 1 else ""
    datasets = [
        (f, l, d) for f, l, d in DATASETS
        if not fragment or fragment in f.lower() or fragment in l.lower()
    ]
    if not datasets:
        print(f"No datasets matched '{fragment}'")
        sys.exit(1)

    # Print both prompts once for reference
    _header("APPROACH B — CURRENT PRODUCTION RENAME PROMPT")
    print(_RENAME_SYSTEM_B)
    _header("APPROACH C — SAME PROMPT WITH QUALITY GATE")
    print(_RENAME_SYSTEM_C)

    results = []
    for i, (fname, label, desc) in enumerate(datasets):
        result = run_dataset(fname, label, desc, api_key)
        if result:
            results.append(result)
        if i < len(datasets) - 1:
            time.sleep(2.0)  # rate limit buffer between datasets

    if len(results) > 1:
        _header("SUMMARY — ALL DATASETS")
        hdr = (
            f"  {'Dataset':<28} {'cols':>4} "
            f"{'B_ren':>5} {'C_ren':>5} "
            f"{'agree':>5} {'disagr':>6} "
            f"{'B-only':>6} {'C-only':>6} "
            f"{'B_tok':>6} {'C_tok':>6} {'delta':>7}"
        )
        print(hdr)
        print("  " + "─" * (len(hdr) - 2))
        for r in results:
            print(
                f"  {r['label']:<28} {r['cols']:>4} "
                f"{r['b_renames']:>5} {r['c_renames']:>5} "
                f"{r['agree']:>5} {r['disagree']:>6} "
                f"{r['b_only']:>6} {r['c_only']:>6} "
                f"{r['b_total']:>6} {r['c_total']:>6} {r['delta_pct']:>+6.0f}%"
            )
        print("  " + "─" * (len(hdr) - 2))
        print(
            "\n  agree = both suggest same name  |  disagr = same col, different name"
            "  |  B/C-only = only that approach renamed"
        )

    print(f"\n  Results saved to: {outfile}")
    sys.stdout = tee._stdout
    tee.close()


if __name__ == "__main__":
    main()

"""
Model comparison — runs enrich_recommendations() with two enrichment models on all
test datasets and prints a side-by-side diff of strategies, renames, and transform hints.

Only _MODEL_ENRICHMENT is swapped per run. _MODEL_RENAME and _MODEL_TRANSFORM stay fixed
at their production values (scout-17b and llama-3.3-70b respectively).

Default models:
  --old  meta-llama/llama-4-scout-17b-16e-instruct  (previous — too small, caused hallucinated hints)
  --new  llama-3.3-70b-versatile                    (current production enrichment model)

Usage:
    LLM_API_KEY=<key> .venv/bin/python3 prefect/tests/test_model_compare.py [fragment] [--old MODEL] [--new MODEL]

    fragment   optional dataset name/label substring filter (e.g. "crm", "iot")
    --old      enrichment model ID to use as the baseline  (default: llama-4-scout-17b-16e-instruct)
    --new      enrichment model ID to compare against      (default: llama-3.3-70b-versatile)

Examples:
    # default comparison, all datasets
    .venv/bin/python3 prefect/tests/test_model_compare.py

    # only IoT dataset
    .venv/bin/python3 prefect/tests/test_model_compare.py iot

    # try a different candidate
    .venv/bin/python3 prefect/tests/test_model_compare.py --new deepseek-r1-distill-llama-70b

Available Groq models worth testing for enrichment:
    llama-3.3-70b-versatile                    current production (276 t/s)
    deepseek-r1-distill-llama-70b              strong reasoning
    meta-llama/llama-4-scout-17b-16e-instruct  fast, 500K context (rename model)
"""

import argparse
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
from flows.llm_enrichment import enrich_recommendations

logging.disable(logging.CRITICAL)

MODEL_OLD = "meta-llama/llama-4-scout-17b-16e-instruct"  # previous — caused hallucinated hints
MODEL_NEW = "llama-3.3-70b-versatile"                  # current production enrichment model

# ---------------------------------------------------------------------------
# Output tee
# ---------------------------------------------------------------------------

class _Tee:
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
# Capturing Groq wrapper
# ---------------------------------------------------------------------------

class _CapturingGroq:
    def __init__(self, client: Groq) -> None:
        self._client = client
        self.last_prompt_tokens     = 0
        self.last_completion_tokens = 0

    class _Chat:
        def __init__(self, outer: "_CapturingGroq") -> None:
            self._outer = outer

        class _Completions:
            def __init__(self, outer: "_CapturingGroq") -> None:
                self._outer = outer

            def create(self, **kwargs):
                resp = self._outer._client.chat.completions.create(**kwargs)
                self._outer.last_prompt_tokens     = resp.usage.prompt_tokens
                self._outer.last_completion_tokens = resp.usage.completion_tokens
                return resp

        @property
        def completions(self):
            return self._Completions(self._outer)

    @property
    def chat(self):
        return self._Chat(self)


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
    ("opaque_fields.csv",         "Opaque Fields",           "all columns f001-f008, rename inferred purely from sample values"),
]

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
# Run one model
# ---------------------------------------------------------------------------

def _run_model(
    model_id: str,
    profile: dict,
    base: dict,
    df: pd.DataFrame,
    api_key: str,
) -> tuple[dict, int, int, float]:
    """Returns (enriched, in_tok, out_tok, latency_sec)."""
    real_client = Groq(api_key=api_key)
    inst_client = _CapturingGroq(real_client)

    original_model  = llm_mod._MODEL_ENRICHMENT
    original_runner = llm_mod._runner
    captured: list[tuple[int, int]] = []

    def _patched(client, llm_profile, base_recs, previous_output, validation_error, attempt):
        result = original_runner(inst_client, llm_profile, base_recs, previous_output, validation_error, attempt)
        captured.append((inst_client.last_prompt_tokens, inst_client.last_completion_tokens))
        return result

    llm_mod._MODEL_ENRICHMENT = model_id
    llm_mod._runner           = _patched
    t0 = time.time()
    try:
        enriched = enrich_recommendations(profile, base, df)
    finally:
        llm_mod._MODEL_ENRICHMENT = original_model
        llm_mod._runner           = original_runner

    latency = time.time() - t0
    in_tok  = sum(t[0] for t in captured)
    out_tok = sum(t[1] for t in captured)
    return enriched, in_tok, out_tok, latency


# ---------------------------------------------------------------------------
# Per-dataset comparison
# ---------------------------------------------------------------------------

def _short(model_id: str) -> str:
    """Return the last path segment of a model ID for use in column headers."""
    return model_id.split("/")[-1]


def run_dataset(
    fname: str,
    label: str,
    description: str,
    api_key: str,
    model_old: str,
    model_new: str,
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

    enriched_old, old_in, old_out, old_lat = _run_model(model_old, profile, base, df, api_key)
    time.sleep(5.0)
    enriched_new, new_in, new_out, new_lat = _run_model(model_new, profile, base, df, api_key)

    cols_old = enriched_old.get("columns", {})
    cols_new = enriched_new.get("columns", {})
    all_cols = sorted(set(cols_old) | set(cols_new))

    # --- Renames ---
    renames_old = {c: d["rename_to"] for c, d in cols_old.items() if d.get("rename_to")}
    renames_new = {c: d["rename_to"] for c, d in cols_new.items() if d.get("rename_to")}
    all_ren     = sorted(set(renames_old) | set(renames_new))

    _section("RENAME COMPARISON")
    if not all_ren:
        print("  Neither model suggested any renames.")
    else:
        col_w  = max(len(c) for c in all_ren) + 2
        nw     = 28
        lbl_o  = f"Old ({_short(model_old)[:24]})"
        lbl_n  = f"New ({_short(model_new)[:24]})"
        print(f"  {'Column':<{col_w}}  {lbl_o:<{nw}}  {lbl_n:<{nw}}  Status")
        print(f"  {'─'*col_w}  {'─'*nw}  {'─'*nw}  {'─'*12}")
        for col in all_ren:
            ov = renames_old.get(col, "—")
            nv = renames_new.get(col, "—")
            if col in renames_old and col in renames_new:
                status = "AGREE" if ov == nv else "DISAGREE"
            elif col in renames_old:
                status = "OLD-ONLY"
            else:
                status = "NEW-ONLY"
            print(f"  {col:<{col_w}}  {ov:<{nw}}  {nv:<{nw}}  {status}")

    ren_agree    = sum(1 for c in all_ren if c in renames_old and c in renames_new and renames_old[c] == renames_new[c])
    ren_disagree = sum(1 for c in all_ren if c in renames_old and c in renames_new and renames_old[c] != renames_new[c])
    ren_old_only = sum(1 for c in all_ren if c in renames_old and c not in renames_new)
    ren_new_only = sum(1 for c in all_ren if c not in renames_old and c in renames_new)
    print(f"\n  agree={ren_agree}  disagree={ren_disagree}  old-only={ren_old_only}  new-only={ren_new_only}")

    # --- Strategy disagreements ---
    def _strat(cols_dict, col):
        mv = cols_dict.get(col, {}).get("missing_values") or {}
        return mv.get("strategy") if isinstance(mv, dict) else None

    strat_diffs = [
        c for c in all_cols
        if _strat(cols_old, c) != _strat(cols_new, c)
    ]

    _section("STRATEGY DISAGREEMENTS")
    if not strat_diffs:
        print("  Both models agree on all missing_values strategies.")
    else:
        col_w = max(len(c) for c in strat_diffs) + 2
        sw    = 18
        print(f"  {'Column':<{col_w}}  {'Old strategy':<{sw}}  {'New strategy':<{sw}}")
        print(f"  {'─'*col_w}  {'─'*sw}  {'─'*sw}")
        for col in strat_diffs:
            ov = _strat(cols_old, col) or "—"
            nv = _strat(cols_new, col) or "—"
            print(f"  {col:<{col_w}}  {ov:<{sw}}  {nv:<{sw}}")

    # --- Transform hints ---
    hints_old = {c: d["transform_hint"] for c, d in cols_old.items() if d.get("transform_hint")}
    hints_new = {c: d["transform_hint"] for c, d in cols_new.items() if d.get("transform_hint")}
    all_hint  = sorted(set(hints_old) | set(hints_new))

    _section("TRANSFORM HINTS")
    if not all_hint:
        print("  Neither model suggested any transform hints.")
    else:
        for col in all_hint:
            ov = hints_old.get(col)
            nv = hints_new.get(col)
            if ov == nv:
                status = "AGREE"
            elif ov and not nv:
                status = "OLD-ONLY"
            elif nv and not ov:
                status = "NEW-ONLY"
            else:
                status = "DISAGREE"
            print(f"\n  {col}  [{status}]")
            if ov:
                print(f"    OLD: {ov}")
            if nv and nv != ov:
                print(f"    NEW: {nv}")

    # --- Notes (only disagreements) ---
    notes_old = {c: d.get("note") for c, d in cols_old.items() if d.get("note")}
    notes_new = {c: d.get("note") for c, d in cols_new.items() if d.get("note")}
    note_diffs = sorted(
        c for c in set(notes_old) | set(notes_new)
        if notes_old.get(c) != notes_new.get(c)
    )

    _section("NOTE DISAGREEMENTS")
    if not note_diffs:
        print("  Both models agree on all notes (or both omitted).")
    else:
        for col in note_diffs:
            ov = notes_old.get(col, "—")
            nv = notes_new.get(col, "—")
            print(f"\n  {col}")
            print(f"    OLD: {ov}")
            print(f"    NEW: {nv}")

    # --- Token + latency ---
    old_total = old_in + old_out
    new_total = new_in + new_out
    delta_pct = ((new_total - old_total) / old_total * 100) if old_total else 0

    lbl_o = f"Old ({_short(model_old)[:22]})"
    lbl_n = f"New ({_short(model_new)[:22]})"
    w = max(len(lbl_o), len(lbl_n)) + 2
    _section("TOKEN + LATENCY COMPARISON")
    print(f"  {lbl_o:<{w}}  in={old_in:>5}  out={old_out:>4}  total={old_total:>5}  latency={old_lat:.1f}s")
    print(f"  {lbl_n:<{w}}  in={new_in:>5}  out={new_out:>4}  total={new_total:>5}  latency={new_lat:.1f}s  ({delta_pct:+.0f}%)")

    return {
        "label":        label,
        "cols":         len(df.columns),
        "old_renames":  len(renames_old),
        "new_renames":  len(renames_new),
        "ren_agree":    ren_agree,
        "ren_disagree": ren_disagree,
        "ren_old_only": ren_old_only,
        "ren_new_only": ren_new_only,
        "strat_diffs":  len(strat_diffs),
        "hint_old":     len(hints_old),
        "hint_new":     len(hints_new),
        "old_total":    old_total,
        "new_total":    new_total,
        "old_lat":      old_lat,
        "new_lat":      new_lat,
        "delta_pct":    delta_pct,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare two Groq models on the DQ enrichment task across test datasets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "fragment",
        nargs="?",
        default="",
        help="Optional dataset name/label substring filter (e.g. 'crm', 'iot').",
    )
    parser.add_argument(
        "--old",
        default=MODEL_OLD,
        metavar="MODEL_ID",
        help=f"Baseline model ID (default: {MODEL_OLD})",
    )
    parser.add_argument(
        "--new",
        default=MODEL_NEW,
        metavar="MODEL_ID",
        help=f"Candidate model ID (default: {MODEL_NEW})",
    )
    args = parser.parse_args()

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

    results_dir = Path(__file__).parent / "model_compare_results"
    results_dir.mkdir(exist_ok=True)
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = results_dir / f"{ts}_{_short(args.old)}_vs_{_short(args.new)}.txt"
    tee     = _Tee(outfile)
    sys.stdout = tee
    print(f"  Saving output to: {outfile}")
    print(f"  Old model: {args.old}")
    print(f"  New model: {args.new}")

    fragment = args.fragment.lower()
    datasets = [
        (f, l, d) for f, l, d in DATASETS
        if not fragment or fragment in f.lower() or fragment in l.lower()
    ]
    if not datasets:
        print(f"No datasets matched '{fragment}'")
        sys.exit(1)

    results = []
    for i, (fname, label, desc) in enumerate(datasets):
        result = run_dataset(fname, label, desc, api_key, args.old, args.new)
        if result:
            results.append(result)
        if i < len(datasets) - 1:
            time.sleep(15.0)  # rate limit buffer between datasets

    if len(results) > 1:
        _header("SUMMARY — ALL DATASETS")
        hdr = (
            f"  {'Dataset':<26} {'cols':>4} "
            f"{'ren_old':>7} {'ren_new':>7} {'ren_agr':>7} {'ren_dis':>7} "
            f"{'strat_D':>7} {'hnt_old':>7} {'hnt_new':>7} "
            f"{'old_tok':>7} {'new_tok':>7} {'delta':>7} "
            f"{'old_lat':>7} {'new_lat':>7}"
        )
        print(hdr)
        print("  " + "─" * (len(hdr) - 2))
        for r in results:
            print(
                f"  {r['label']:<26} {r['cols']:>4} "
                f"{r['old_renames']:>7} {r['new_renames']:>7} {r['ren_agree']:>7} {r['ren_disagree']:>7} "
                f"{r['strat_diffs']:>7} {r['hint_old']:>7} {r['hint_new']:>7} "
                f"{r['old_total']:>7} {r['new_total']:>7} {r['delta_pct']:>+6.0f}% "
                f"{r['old_lat']:>6.1f}s {r['new_lat']:>6.1f}s"
            )
        print("  " + "─" * (len(hdr) - 2))
        print(
            "\n  Key: ren_agr=rename agree  ren_dis=rename disagree  "
            "strat_D=strategy disagree  hnt=transform_hint count"
        )

    print(f"\n  Results saved to: {outfile}")
    sys.stdout = tee._stdout
    tee.close()


if __name__ == "__main__":
    main()

"""
Live LLM enrichment test — full prompt logging + token analytics.

Runs each test dataset through the full pipeline and prints:
  - System prompt (once)
  - Per-dataset: user prompt sent, raw LLM response, token counts, diff

Usage:
    LLM_API_KEY=<key> .venv/bin/python3 prefect/tests/test_llm_live.py [dataset_name]

    With no argument: runs all datasets.
    With a name fragment: runs matching datasets only (e.g. "iot" or "medical").

Requires:
    - LLM_API_KEY in environment or .env file
    - test_data/*.csv generated via test_data/generate_datasets.py
"""

import json
import logging
import os
import sys
import time
from pathlib import Path

# Allow running from repo root or prefect/
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from groq import Groq

import flows.llm_enrichment as llm_mod
from flows.dq_logic import build_recommendations, profile_dataframe, score_profile
from flows.llm_enrichment import (
    _RUNNER_SYSTEM,
    _RUNNER_USER,
    _build_llm_profile,
    enrich_recommendations,
)

logging.disable(logging.CRITICAL)   # suppress internal INFO logs; we print our own

# ---------------------------------------------------------------------------
# Datasets
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
]

# ---------------------------------------------------------------------------
# Groq client instrumentation — capture usage without modifying production code
# ---------------------------------------------------------------------------

class _CapturingGroq:
    """Wraps Groq client to intercept the last API response usage."""

    def __init__(self, client: Groq) -> None:
        self._client = client
        self.last_prompt_tokens     = 0
        self.last_completion_tokens = 0
        self.last_raw_response      = ""

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
                self._outer.last_raw_response      = resp.choices[0].message.content.strip()
                return resp

        @property
        def completions(self):
            return self._Completions(self._outer)

    @property
    def chat(self):
        return self._Chat(self)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

W = 72  # line width

def _header(title: str) -> None:
    print(f"\n{'=' * W}")
    print(f"  {title}")
    print(f"{'=' * W}")

def _section(title: str) -> None:
    print(f"\n{'─' * W}")
    print(f"  {title}")
    print(f"{'─' * W}")

def _box(label: str, content: str, max_lines: int = 0) -> None:
    """Print labelled content block, optionally truncating."""
    print(f"\n[ {label} ]")
    lines = content.splitlines()
    if max_lines and len(lines) > max_lines:
        shown = lines[:max_lines]
        print("\n".join(shown))
        print(f"  ... ({len(lines) - max_lines} more lines truncated)")
    else:
        print(content)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_dataset(fname: str, label: str, description: str,
                api_key: str, show_system: bool = False) -> dict:
    path = TEST_DATA / fname
    if not path.exists():
        print(f"  [SKIP] {fname} not found — run test_data/generate_datasets.py first")
        return {}

    df       = pd.read_csv(path)
    profile  = profile_dataframe(df)
    dq_score = score_profile(profile)
    base     = build_recommendations(df, profile, dq_score)

    llm_profile_str = _build_llm_profile(profile, df)
    base_json_str   = json.dumps(base, indent=2, default=str)
    user_prompt     = _RUNNER_USER.format(
        profile=llm_profile_str,
        base_recs_json=base_json_str,
    )

    _header(f"{label}  |  {fname}")
    print(f"  Description : {description}")
    print(f"  Shape       : {len(df)} rows × {len(df.columns)} cols")
    print(f"  DQ score    : {dq_score:.1f}")
    print(f"  Columns     : {', '.join(df.columns.tolist())}")

    if show_system:
        _section("SYSTEM PROMPT")
        print(_RUNNER_SYSTEM)

    _section("USER PROMPT")
    _box("Profile CSV + baseline sent to LLM", user_prompt)

    # Instrument client
    real_client = Groq(api_key=api_key)
    inst_client = _CapturingGroq(real_client)

    # Patch runner to inject our capturing client
    original_runner = llm_mod._runner
    raw_response: list[str] = []

    def _patched_runner(client, llm_profile, base_recs,
                        previous_output, validation_error, attempt):
        result = original_runner(inst_client, llm_profile, base_recs,
                                 previous_output, validation_error, attempt)
        raw_response.append(inst_client.last_raw_response)
        return result

    llm_mod._runner = _patched_runner
    t0       = time.time()
    enriched = enrich_recommendations(profile, base, df)
    elapsed  = time.time() - t0
    llm_mod._runner = original_runner

    _section("RAW LLM RESPONSE")
    print(raw_response[-1] if raw_response else "(no response captured — fallback used)")

    # Token counts
    in_tok  = inst_client.last_prompt_tokens
    out_tok = inst_client.last_completion_tokens

    _section("TOKEN ANALYTICS")
    print(f"  Prompt tokens     : {in_tok:>6}")
    print(f"  Completion tokens : {out_tok:>6}")
    print(f"  Total             : {in_tok + out_tok:>6}")
    print(f"  Latency           : {elapsed:.2f}s")
    print(f"  Tokens/sec (out)  : {out_tok / elapsed:.0f}")

    # Diff summary
    _section("ENRICHMENT DIFF  (baseline → enriched)")
    any_change = False
    for col, cd in enriched["columns"].items():
        base_cd   = base["columns"][col]
        mv        = cd.get("missing_values")
        base_mv   = base_cd.get("missing_values")
        note      = cd.get("note") or ""
        rename    = cd.get("rename_to")
        nullable  = cd.get("nullable")
        base_null = base_cd.get("nullable")

        changed = (mv != base_mv) or rename or (nullable != base_null)
        if not changed:
            continue

        any_change = True
        mv_str  = f"{mv['strategy']}(v={mv.get('value','')})" if mv else "—"
        base_mv_str = (
            f"{base_mv['strategy']}(v={base_mv.get('value','')})" if base_mv else "null"
        )
        rn_str  = f"  rename→ {rename}" if rename else ""
        nl_str  = f"  nullable={nullable}" if nullable != base_null else ""
        mv_diff = f"  missing: {base_mv_str} → {mv_str}" if mv != base_mv else ""

        print(f"\n  {col}{rn_str}{nl_str}{mv_diff}")
        if note:
            print(f"    note: {note[:100]}")

    if not any_change:
        print("  — nothing changed")

    return {
        "label":   label,
        "rows":    len(df),
        "cols":    len(df.columns),
        "dq":      dq_score,
        "in_tok":  in_tok,
        "out_tok": out_tok,
        "elapsed": elapsed,
    }


def main() -> None:
    # Load API key
    api_key = os.getenv("LLM_API_KEY", "").strip()
    if not api_key:
        # Try .env in repo root
        env_file = REPO_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.strip().startswith("LLM_API_KEY"):
                    api_key = line.split("=", 1)[1].strip()
                    break
    if not api_key:
        print("ERROR: LLM_API_KEY not set. Export it or add to .env")
        sys.exit(1)

    # Make the key visible to enrich_recommendations() which does os.getenv() internally
    os.environ["LLM_API_KEY"] = api_key

    # Filter by name fragment if provided
    fragment = sys.argv[1].lower() if len(sys.argv) > 1 else ""
    datasets = [
        (f, l, d) for f, l, d in DATASETS
        if not fragment or fragment in f.lower() or fragment in l.lower()
    ]

    if not datasets:
        print(f"No datasets matched '{fragment}'")
        sys.exit(1)

    # Print system prompt once at the top
    _header("SYSTEM PROMPT  (same for all datasets)")
    print(_RUNNER_SYSTEM)

    results = []
    for i, (fname, label, desc) in enumerate(datasets):
        result = run_dataset(fname, label, desc, api_key, show_system=False)
        if result:
            results.append(result)
        if i < len(datasets) - 1:
            time.sleep(1.5)  # avoid rate limits between datasets

    if len(results) > 1:
        # Summary table
        _header("SUMMARY — ALL DATASETS")
        hdr = f"  {'Dataset':<28} {'rows':>5} {'cols':>4} {'dq':>6} {'in_tok':>7} {'out_tok':>8} {'total':>7} {'time':>7}"
        print(hdr)
        print("  " + "─" * (len(hdr) - 2))
        for r in results:
            print(
                f"  {r['label']:<28} {r['rows']:>5} {r['cols']:>4} {r['dq']:>6.1f}"
                f" {r['in_tok']:>7} {r['out_tok']:>8} {r['in_tok']+r['out_tok']:>7}"
                f" {r['elapsed']:>6.1f}s"
            )
        print("  " + "─" * (len(hdr) - 2))
        total_in  = sum(r["in_tok"]  for r in results)
        total_out = sum(r["out_tok"] for r in results)
        print(
            f"  {'TOTAL':<28} {'':>5} {'':>4} {'':>6}"
            f" {total_in:>7} {total_out:>8} {total_in+total_out:>7}"
        )


if __name__ == "__main__":
    main()

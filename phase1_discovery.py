import os
import json
import asyncio
from pathlib import Path
import datetime

from dotenv import load_dotenv
from browser_use import Agent, ChatGoogle

from common import (
    CompanyInfo, read_rows, write_rows, sniff_dialect_and_header,
    OUT_COL_ADDRESS, OUT_COL_POSTCODE, OUT_COL_COMPANY_NAME, OUT_COL_WEBSITE,
    OUT_COL_EMAIL, OUT_COL_PHONE, OUT_COL_GOVUK_URL, OUT_COL_SOURCE_URL,
    OUT_COL_CONFIDENCE, OUT_COL_NOTES, ensure_col_exact, extract_company_from_address,
    clean_phone, dedupe_keep_order, ensure_phone_cols, fill_phone_cols
)

# -----------------------------
# Config (Phase 1)
# -----------------------------
INPUT_CSV = os.getenv("INPUT_CSV", "input.csv")
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "output.core.csv")

# ✅ Autosave controls
PARTIAL_EVERY = int(os.getenv("PARTIAL_EVERY", "20"))
PARTIAL_CSV = os.getenv("PARTIAL_CSV", "output.core.partial.csv")

MAX_STEPS = int(os.getenv("MAX_STEPS", "120"))
ROW_RETRIES = int(os.getenv("ROW_RETRIES", "5"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.8"))
RETRY_START_SLEEP = float(os.getenv("RETRY_START_SLEEP", "1.0"))
RETRY_MAX_SLEEP = float(os.getenv("RETRY_MAX_SLEEP", "12.0"))

load_dotenv(override=True)

RUN_ID = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
RUN_DIR = Path("runs") / ("phase1-" + RUN_ID)
RUN_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_JSONL = RUN_DIR / "checkpoint.jsonl"

def build_task(address: str, postcode: str, seed_company: str) -> str:
    return f"""
You are a precise UK business lookup agent. Follow this order EXACTLY.

PHASE 0 — GOV.UK FIRST (hard gate)
- Search on google (any browser): "{seed_company} {address} {postcode} gov.co.uk" (literal text 'gov.co.uk')
- find their gov.uk company page on the search results
- Capture:
  • 'company_name' as canonical (include Limited/Ltd)
  • status (must be Active to proceed; if dissolved/insolvent, note it and continue ONLY if there is a trading site+general inbox)
  • save page URL in 'govuk_url'

PHASE A — COMPANY CORE INFO
1) Search on google (any browser) for "{{company_name}} {postcode}":
   - capture main phone,main wesite and main email try to navigate to the company website if possible.
2) If you find the official website (strict navigation order):
   - Click: "Contact"/"Contact us"/"Contacts"/"Get in touch"/"Support"
   - If missing: open footer and collect mailto: links.
   - If still nothing, try paths: /contact, /contact-us, /about, /company, /support
   - If still nothing, on-site search or query: site:{{domain}} (contact OR enquiries OR "info@")
   - Extract:
     • 'website' (homepage)
     • 'email' ONE general inbox (lowercase): prefer info@ → enquiries@ → contact@ → hello@ → admin@ → office@ → reception@ → team@ → sales@
     • main/general phone (prefer website over GBP)
3) If no website: phone may come from Google Business Profile / Yell / Thomson (directories) — NEVER take email from directories.
4) Set 'source_url' to the site contact page if possible (else homepage, GBP, or directory in that order).

OUTPUT (single JSON object):
company_name, post_code, website, email, numbers, govuk_url, source_url, confidence, notes
"""

async def run_for_row(llm: ChatGoogle, address: str, postcode: str, seed_company: str) -> CompanyInfo | None:
    agent = Agent(
        task=build_task(address, postcode, seed_company),
        llm=llm,
        output_model_schema=CompanyInfo,
        max_failures=6,
        step_timeout=240,
        max_actions_per_step=60,
    )
    history = await agent.run(max_steps=MAX_STEPS)

    data = getattr(history, "structured_output", None)
    if data is not None:
        try:
            if hasattr(data, "model_dump"):
                return CompanyInfo(**data.model_dump())
            if isinstance(data, dict):
                return CompanyInfo(**data)
        except Exception:
            pass

    final = None
    if hasattr(history, "final_result"):
        try:
            final = history.final_result()
        except Exception:
            final = None

    if isinstance(final, str) and final.strip():
        try:
            return CompanyInfo(**json.loads(final))
        except Exception:
            return CompanyInfo(notes=final.strip())
    return None

def attempt_has_core(info: CompanyInfo | None) -> bool:
    if not info:
        return False
    has_site_inbox = bool(info.website and info.email)
    has_phone = bool(info.numbers and len(info.numbers) > 0)
    has_identity = bool(info.company_name and info.govuk_url)
    # Prefer contactability; allow identity as success so we don't loop endlessly on rare cases.
    return has_site_inbox or has_phone or has_identity

def write_checkpoint(record: dict):
    try:
        with open(CHECKPOINT_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _write_partial_snapshot(dialect, base_header, processed_rows, phone_lists, path: str):
    """
    Write a consistent partial CSV snapshot:
    - expands phone columns to fit numbers seen so far
    - fills numbers into the correct columns
    """
    # Copy header so we don't mutate the live header yet
    header = list(base_header)
    max_phones_so_far = max((len(x) for x in phone_lists), default=0)
    ensure_phone_cols(header, max_phones_so_far)

    # Prepare rows with phones filled (copy so we don't mutate live rows)
    rows_copy = [list(r) + [""] * (len(header) - len(r)) for r in processed_rows]
    for r, nums in zip(rows_copy, phone_lists):
        fill_phone_cols(r, header, nums)

    write_rows(path, dialect, header, rows_copy)

async def main():
    if not os.getenv("GOOGLE_API_KEY"):
        raise RuntimeError("GOOGLE_API_KEY not found in environment. Put it in your .env")

    if not Path(INPUT_CSV).exists():
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

    rows = read_rows(INPUT_CSV)
    if not rows:
        print("No data rows found.")
        return

    dialect, has_header = sniff_dialect_and_header(INPUT_CSV)
    if not has_header:
        raise ValueError("Your file appears to have no header row. Need ADDRESS and POSTCODE.")

    header = rows[0]
    data_rows = rows[1:]

    # Build output header
    output_header = header[:]
    idx_address = ensure_col_exact(output_header, OUT_COL_ADDRESS)
    idx_pc = ensure_col_exact(output_header, OUT_COL_POSTCODE)
    idx_name = ensure_col_exact(output_header, OUT_COL_COMPANY_NAME)
    idx_site = ensure_col_exact(output_header, OUT_COL_WEBSITE)
    idx_email = ensure_col_exact(output_header, OUT_COL_EMAIL)
    idx_phone = ensure_col_exact(output_header, OUT_COL_PHONE)
    idx_govuk = ensure_col_exact(output_header, OUT_COL_GOVUK_URL)
    idx_src = ensure_col_exact(output_header, OUT_COL_SOURCE_URL)
    idx_conf = ensure_col_exact(output_header, OUT_COL_CONFIDENCE)
    idx_notes = ensure_col_exact(output_header, OUT_COL_NOTES)

    processed_rows = []
    phone_lists: list[list[str]] = []

    llm = ChatGoogle(model="gemini-flash-latest")

    print(f"🔎 Phase 1: processing {len(data_rows)} row(s) from {INPUT_CSV} ...")

    for i, row in enumerate(data_rows, start=1):
        row = row + [""] * (len(output_header) - len(row))
        address = row[idx_address].strip() if len(row) > idx_address else ""
        postcode = row[idx_pc].strip() if len(row) > idx_pc else ""

        if not address and not postcode:
            processed_rows.append(row)
            phone_lists.append([])
            print(f"[{i}/{len(data_rows)}] → Skipped blank row")
            # checkpoint for skipped
            write_checkpoint({
                "row_index": i,
                "input": {"address": address, "postcode": postcode},
                "output": None,
                "note": "skipped blank row"
            })
            # autosave if threshold hit
            if PARTIAL_EVERY > 0 and i % PARTIAL_EVERY == 0:
                try:
                    _write_partial_snapshot(dialect, output_header, processed_rows, phone_lists, PARTIAL_CSV)
                    print(f"💾 Autosave snapshot → {PARTIAL_CSV} ({i} rows)")
                except Exception as e:
                    print(f"⚠️ Autosave failed: {e}")
            continue

        inferred_company = extract_company_from_address(address) or address[:140]
        info = None

        sleep_s = RETRY_START_SLEEP
        for attempt in range(1, ROW_RETRIES + 1):
            print(f"[{i}/{len(data_rows)}] 🔁 Attempt {attempt}/{ROW_RETRIES} :: {inferred_company} {postcode}")
            try:
                info = await run_for_row(llm, address, postcode, inferred_company)
            except Exception as e:
                print(f"   ↳ agent error attempt {attempt}: {e}")
                info = None

            if attempt_has_core(info):
                print(f"   ↳ success on attempt {attempt}")
                break

            import random
            jitter = random.uniform(0.0, 0.4 * sleep_s)
            to_sleep = min(RETRY_MAX_SLEEP, sleep_s + jitter)
            print(f"   ↳ retrying after {to_sleep:.1f}s")
            await asyncio.sleep(to_sleep)
            sleep_s = min(RETRY_MAX_SLEEP, sleep_s * RETRY_BACKOFF_BASE)

        # Apply results
        nums = []
        if info:
            row[idx_address] = row[idx_address] or address or ""
            row[idx_pc] = row[idx_pc] or info.post_code or postcode or ""
            if info.company_name:
                row[idx_name] = row[idx_name] or info.company_name.strip()
            if info.website:
                row[idx_site] = row[idx_site] or info.website.strip()
            if info.email:
                row[idx_email] = row[idx_email] or (info.email or "").strip().lower()
            if info.govuk_url:
                row[idx_govuk] = row[idx_govuk] or info.govuk_url.strip()
            if info.source_url:
                row[idx_src] = row[idx_src] or info.source_url.strip()
            if info.confidence is not None and not row[idx_conf]:
                row[idx_conf] = f"{info.confidence:.2f}"
            existing_notes = row[idx_notes] or ""
            note = (info.notes or "").strip()
            attempts_note = f"Phase1 attempts: {attempt}/{ROW_RETRIES}"
            combined = "; ".join([s for s in [existing_notes, note, attempts_note] if s]).strip("; ")
            row[idx_notes] = combined

            if info.numbers:
                nums = [clean_phone(n) for n in info.numbers if isinstance(n, str)]
                nums = [n for n in nums if len(n) >= 7]
                nums = dedupe_keep_order(nums)

        phone_lists.append(nums)
        processed_rows.append(row)

        # per-row checkpoint (best-effort)
        try:
            write_checkpoint({
                "row_index": i,
                "input": {"address": address, "postcode": postcode, "seed_company": inferred_company},
                "output": {
                    "company_name": row[idx_name] or None,
                    "website": row[idx_site] or None,
                    "email": row[idx_email] or None,
                    "numbers": nums or None,
                    "govuk_url": row[idx_govuk] or None,
                    "source_url": row[idx_src] or None,
                    "confidence": row[idx_conf] or None,
                    "notes": row[idx_notes] or None,
                }
            })
        except Exception:
            pass

        # ✅ AUTOSAVE every N rows
        if PARTIAL_EVERY > 0 and i % PARTIAL_EVERY == 0:
            try:
                _write_partial_snapshot(dialect, output_header, processed_rows, phone_lists, PARTIAL_CSV)
                print(f"💾 Autosave snapshot → {PARTIAL_CSV} ({i} rows)")
            except Exception as e:
                print(f"⚠️ Autosave failed: {e}")

    # Expand header with phone columns as needed and write phones into rows
    max_phones = max((len(x) for x in phone_lists), default=0)
    ensure_phone_cols(output_header, max_phones)

    # Now write phone numbers into rows
    for row, nums in zip(processed_rows, phone_lists):
        fill_phone_cols(row, output_header, nums)

    write_rows(OUTPUT_CSV, dialect, output_header, processed_rows)
    print(f"✅ Phase 1 done. Wrote {len(processed_rows)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())

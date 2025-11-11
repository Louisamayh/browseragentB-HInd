import os
import json
import asyncio
from pathlib import Path
import datetime

from dotenv import load_dotenv
from browser_use import Agent, ChatGoogle

from common import (
    read_rows, write_rows, sniff_dialect_and_header,
    OUT_COL_COMPANY_NAME, OUT_COL_GOVUK_URL,
    OUT_COL_CONTACT_NAME, OUT_COL_CONTACT_TITLE,
    OUT_COL_CONTACT_LINKEDIN, OUT_COL_CONTACT_EMAIL,
    OUT_COL_SOURCE_URL, OUT_COL_CONFIDENCE, OUT_COL_NOTES,
    ensure_col_exact, ensure_phone_cols,
)

# Load .env FIRST so programmatic env set by main.py isn't overridden.
load_dotenv(override=False)

# -----------------------------
# Config (Phase 2)
# -----------------------------
INPUT_CSV = os.getenv("INPUT_CSV_PHASE2", "output.core.csv")
OUTPUT_CSV = os.getenv("OUTPUT_CSV_PHASE2", "output.with_contacts.csv")

# Autosave config
PARTIAL_EVERY = int(os.getenv("PARTIAL_EVERY", "20"))
PARTIAL_CSV = os.getenv("PARTIAL_CSV_PHASE2", "output.with_contacts.partial.csv")

MAX_STEPS = int(os.getenv("MAX_STEPS", "120"))
ROW_RETRIES = max(1, int(os.getenv("ROW_RETRIES", "5")))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.8"))
RETRY_START_SLEEP = float(os.getenv("RETRY_START_SLEEP", "1.0"))
RETRY_MAX_SLEEP = float(os.getenv("RETRY_MAX_SLEEP", "12.0"))

# 1..3 contacts maximum; will return fewer if fewer exist
TARGET_CONTACTS = int(os.getenv("TARGET_CONTACTS", "3"))
TARGET_CONTACTS = max(1, min(3, TARGET_CONTACTS))

RUN_ID = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
RUN_DIR = Path("runs") / ("phase2-" + RUN_ID)
RUN_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_JSONL = RUN_DIR / "checkpoint.jsonl"

# -----------------------------
# Helpers: ensure multi-contact columns
# -----------------------------
def ensure_contact_cols(header, max_contacts: int):
    """
    Ensure columns for up to `max_contacts` contacts.
    Contact #1 uses base names; #2/#3 get " 2"/" 3" suffixes.
    """
    idx = {}
    for k in range(1, max_contacts + 1):
        suffix = "" if k == 1 else f" {k}"
        idx[k] = {
            "name": ensure_col_exact(header, f"{OUT_COL_CONTACT_NAME}{suffix}"),
            "title": ensure_col_exact(header, f"{OUT_COL_CONTACT_TITLE}{suffix}"),
            "linkedin": ensure_col_exact(header, f"{OUT_COL_CONTACT_LINKEDIN}{suffix}"),
            "email": ensure_col_exact(header, f"{OUT_COL_CONTACT_EMAIL}{suffix}"),
        }
    return idx

# -----------------------------
# Agent prompt (collect 1..3 officers)
# -----------------------------
def build_task(company_name: str, govuk_url: str | None, max_contacts: int) -> str:
    """
    Builds an agent prompt to find a company's officers on Companies House.
    If no valid gov.uk URL is provided, the agent must perform a Google search.
    """

    return f"""
You are a precise UK contact finder.

STEP 1 — Locate the correct Companies House page (gov.uk)
If a URL is provided, open it first:
  - Provided URL: {govuk_url or "None"}
  - If it 404s, redirects, or doesn't clearly match the company, continue with Google search below.

Otherwise (or if the above fails):
  1) Go to https://www.google.com
  2) Search exactly: "{company_name} site:gov.uk"
  3) Click the result whose title and snippet best match the company name and location.

STEP 2 — People extraction
1) On the Companies House page, open the “People” section.
2) Identify ALL CURRENT (non-resigned) officers, up to {max_contacts} total.
3) Prioritise senior roles: Managing Director, Director, Owner, CFO, Finance Director, Head of Finance, COO, Operations Director.
4) For each person, capture:
   - contact_name
   - contact_title

STEP 3 — LinkedIn refinement
For each person:
  - Search Google: site:linkedin.com/in "<PERSON_NAME>" "{company_name}"
  - Confirm they’re tied to this company before saving the LinkedIn URL.
  - Only set contact_email if a personal, public email appears.

STEP 4 — Source & confidence
- For each person, include source_url (gov.uk or LinkedIn page used).
- Provide a confidence score (0.0–1.0) and a short note (e.g., "Current Director on gov.uk; LinkedIn confirmed").

RETURN FORMAT (JSON object):
{{
  "contacts": [
    {{
      "contact_name": string,
      "contact_title": string,
      "contact_linkedin": string | null,
      "contact_email": string | null,
      "source_url": string | null,
      "confidence": number | null,
      "notes": string | null
    }},
    ...
  ]
}}
Only include as many contacts as you found (1..{max_contacts}). Do NOT include resigned officers.
"""

async def run_for_company(llm: ChatGoogle, company_name: str, govuk_url: str | None) -> list[dict]:
    """
    Returns a list of contact dicts with keys:
    contact_name, contact_title, contact_linkedin, contact_email, source_url, confidence, notes
    """
    agent = Agent(
        task=build_task(company_name, govuk_url, TARGET_CONTACTS),
        llm=llm,
        # No output_model_schema: we expect a custom {"contacts":[...]} payload
        max_failures=6,
        step_timeout=240,
        max_actions_per_step=60,
    )
    history = await agent.run(max_steps=MAX_STEPS)

    # Try to parse final JSON
    payload = None
    data = getattr(history, "structured_output", None)
    if isinstance(data, dict) and "contacts" in data:
        payload = data
    else:
        final = None
        if hasattr(history, "final_result"):
            try:
                final = history.final_result()
            except Exception:
                final = None
        if isinstance(final, str) and final.strip():
            try:
                payload = json.loads(final)
            except Exception:
                payload = None

    contacts = []
    if isinstance(payload, dict):
        c = payload.get("contacts", [])
        if isinstance(c, list):
            for item in c:
                if not isinstance(item, dict):
                    continue
                name = (item.get("contact_name") or "").strip()
                title = (item.get("contact_title") or "").strip()
                if not name or not title:
                    continue
                contacts.append({
                    "contact_name": name,
                    "contact_title": title,
                    "contact_linkedin": (item.get("contact_linkedin") or "").strip() or "",
                    "contact_email": (item.get("contact_email") or "").strip().lower() or "",
                    "source_url": (item.get("source_url") or "").strip() or "",
                    "confidence": item.get("confidence", None),
                    "notes": (item.get("notes") or "").strip() or "",
                })

    # Cap to TARGET_CONTACTS
    return contacts[:TARGET_CONTACTS]

def write_checkpoint(record: dict):
    try:
        with open(CHECKPOINT_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass

# Periodic partial snapshot writer
def write_partial_snapshot(dialect, header: list[str], rows: list[list[str]], path: str):
    """
    Writes a consistent snapshot CSV using the current header+rows.
    """
    try:
        write_rows(path, dialect, header, rows)
        print(f"💾 Autosave snapshot → {path} ({len(rows)} rows)")
    except Exception as e:
        print(f"WARN: autosave snapshot failed: {e}")

# -----------------------------
# Main
# -----------------------------
async def main():
    if not os.getenv("GOOGLE_API_KEY"):
        raise RuntimeError("GOOGLE_API_KEY not found in environment. Put it in your .env")

    if not Path(INPUT_CSV).exists():
        raise FileNotFoundError(f"Phase 2 input not found: {INPUT_CSV}")

    rows = read_rows(INPUT_CSV)
    if not rows:
        print("No data rows found.")
        return

    dialect, has_header = sniff_dialect_and_header(INPUT_CSV)
    if not has_header:
        raise ValueError("Input CSV must have a header row.")

    header = rows[0]
    data_rows = rows[1:]

    # Build output header (preserve all existing columns; ensure up to TARGET_CONTACTS contact columns)
    output_header = header[:]
    idx_name = ensure_col_exact(output_header, OUT_COL_COMPANY_NAME)
    idx_govuk = ensure_col_exact(output_header, OUT_COL_GOVUK_URL)
    idx_src = ensure_col_exact(output_header, OUT_COL_SOURCE_URL)
    idx_conf = ensure_col_exact(output_header, OUT_COL_CONFIDENCE)
    idx_notes = ensure_col_exact(output_header, OUT_COL_NOTES)

    contact_cols = ensure_contact_cols(output_header, TARGET_CONTACTS)

    processed_rows = []

    llm = ChatGoogle(model="gemini-flash-latest")
    print(f"👤 Phase 2: up to {TARGET_CONTACTS} contacts per company from {INPUT_CSV} ...")

    for i, row in enumerate(data_rows, start=1):
        row = row + [""] * (len(output_header) - len(row))
        company_name = (row[idx_name] or "").strip()
        govuk_url = (row[idx_govuk] or "").strip() or None

        if not company_name:
            processed_rows.append(row)
            print(f"[{i}/{len(data_rows)}] → Skipped (missing company_name)")

            # checkpoint + periodic autosave even on skipped rows
            write_checkpoint({
                "row_index": i,
                "company_name": company_name,
                "govuk_url": govuk_url,
                "result": None,
                "note": "Skipped (missing company_name)"
            })
            if PARTIAL_EVERY > 0 and (i % PARTIAL_EVERY == 0):
                write_partial_snapshot(dialect, output_header, processed_rows, PARTIAL_CSV)
            continue

        contacts: list[dict] = []
        sleep_s = RETRY_START_SLEEP

        for attempt in range(1, ROW_RETRIES + 1):
            print(f"[{i}/{len(data_rows)}] 🔁 Attempt {attempt}/{ROW_RETRIES} :: {company_name}")
            try:
                contacts = await run_for_company(llm, company_name, govuk_url)
            except Exception as e:
                print(f"   ↳ agent error attempt {attempt}: {e}")
                contacts = []

            if contacts:  # success if we got at least one
                print(f"   ↳ success with {len(contacts)} contact(s) on attempt {attempt}")
                break
            else:
                print("   ↳ no contacts found on this attempt; will retry")

            import random
            jitter = random.uniform(0.0, 0.4 * sleep_s)
            to_sleep = min(RETRY_MAX_SLEEP, sleep_s + jitter)
            print(f"   ↳ retrying after {to_sleep:.1f}s")
            await asyncio.sleep(to_sleep)
            sleep_s = min(RETRY_MAX_SLEEP, sleep_s * RETRY_BACKOFF_BASE)

        # Write contacts into columns, up to TARGET_CONTACTS
        for slot, contact in enumerate(contacts[:TARGET_CONTACTS], start=1):
            row[contact_cols[slot]["name"]] = row[contact_cols[slot]["name"]] or contact["contact_name"]
            row[contact_cols[slot]["title"]] = row[contact_cols[slot]["title"]] or contact["contact_title"]
            if contact.get("contact_linkedin"):
                row[contact_cols[slot]["linkedin"]] = row[contact_cols[slot]["linkedin"]] or contact["contact_linkedin"]
            if contact.get("contact_email"):
                row[contact_cols[slot]["email"]] = row[contact_cols[slot]["email"]] or contact["contact_email"]

            # meta (set once if empty)
            if contact.get("source_url") and not row[idx_src]:
                row[idx_src] = contact["source_url"]
            if contact.get("confidence") is not None and not row[idx_conf]:
                try:
                    row[idx_conf] = f"{float(contact['confidence']):.2f}"
                except Exception:
                    pass
            # notes — append attempt count
            attempts_note = f"Phase2 attempts: {attempt}/{ROW_RETRIES}"
            note_bits = [row[idx_notes] or "", contact.get("notes") or "", attempts_note]
            row[idx_notes] = "; ".join([s for s in note_bits if s]).strip("; ")

        processed_rows.append(row)

        # per-row checkpoint and periodic autosave
        try:
            write_checkpoint({
                "row_index": i,
                "company_name": company_name,
                "govuk_url": govuk_url,
                "contacts_found": len(contacts),
                "contacts": contacts,
            })
        except Exception:
            pass

        if PARTIAL_EVERY > 0 and (i % PARTIAL_EVERY == 0):
            write_partial_snapshot(dialect, output_header, processed_rows, PARTIAL_CSV)

    # Keep phone columns valid if present (no-op if already exist)
    ensure_phone_cols(output_header, 1)

    write_rows(OUTPUT_CSV, dialect, output_header, processed_rows)
    print(f"✅ Phase 2 done. Wrote {len(processed_rows)} rows to {OUTPUT_CSV}")

    # Final snapshot to partial (mirrors final file)
    try:
        write_rows(PARTIAL_CSV, dialect, output_header, processed_rows)
        print(f"💾 Final snapshot also written to {PARTIAL_CSV}")
    except Exception:
        pass

if __name__ == "__main__":
    asyncio.run(main())

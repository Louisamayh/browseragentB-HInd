import os
import csv
import re
import json
import asyncio
from dataclasses import dataclass
from typing import Optional, List, Tuple

from pydantic import BaseModel, Field

# -----------------------------
# Structured output schema
# -----------------------------
class CompanyInfo(BaseModel):
    # Core
    company_name: Optional[str] = Field(None)
    post_code: Optional[str] = Field(None)
    website: Optional[str] = Field(None)
    email: Optional[str] = Field(None)  # general inbox
    numbers: Optional[List[str]] = Field(None)  # most general first
    # Contacts
    contact_name: Optional[str] = Field(None)
    contact_title: Optional[str] = Field(None)
    contact_linkedin: Optional[str] = Field(None)
    contact_email: Optional[str] = Field(None)
    # Gov.uk
    govuk_url: Optional[str] = Field(None)
    # Meta
    source_url: Optional[str] = Field(None)
    confidence: Optional[float] = Field(None)
    notes: Optional[str] = Field(None)

# -----------------------------
# CSV helpers
# -----------------------------
def _normalize_header_cell(cell: str) -> str:
    if cell is None:
        return ""
    s = str(cell).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s

def sniff_dialect_and_header(path: str) -> Tuple[csv.Dialect, bool]:
    with open(path, "rb") as f:
        sample = f.read(8192)

    sniffer = csv.Sniffer()
    try:
        decoded = sample.decode("utf-8", errors="ignore")
        dialect = sniffer.sniff(decoded)
        has_header = sniffer.has_header(decoded)
        return dialect, has_header
    except Exception:
        pass

    try:
        text = sample.decode("utf-8", errors="ignore")
        first_line = text.splitlines()[0] if text.splitlines() else ""
        comma_count = first_line.count(",")
        tab_count = first_line.count("\t")

        class _D(csv.Dialect):
            delimiter = "," if comma_count >= tab_count else "\t"
            quotechar = '"'
            doublequote = True
            skipinitialspace = True
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return _D(), bool(re.search(r"[A-Za-z]", first_line))
    except Exception:
        class _D(csv.Dialect):
            delimiter = ","
            quotechar = '"'
            doublequote = True
            skipinitialspace = True
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return _D(), True

def read_rows(path: str) -> List[List[str]]:
    dialect, _ = sniff_dialect_and_header(path)
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f, dialect)
        rows = [row for row in reader if row and any(c.strip() for c in row)]
    return rows

def write_rows(path: str, dialect: csv.Dialect, header: List[str], rows: List[List[str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(
            f,
            delimiter=dialect.delimiter,
            quotechar='"',
            doublequote=True,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writerow(header)
        for r in rows:
            writer.writerow(r)

# -----------------------------
# Common columns (keep names identical between phases)
# -----------------------------
OUT_COL_ADDRESS = "ADDRESS"
OUT_COL_POSTCODE = "POSTCODE"
OUT_COL_COMPANY_NAME = "COMPANY NAME"
OUT_COL_WEBSITE = "WEBSITE"
OUT_COL_EMAIL = "GENERAL EMAIL"
OUT_COL_PHONE = "PHONE NUMBER"  # base; expand to PHONE NUMBER 2, ...
OUT_COL_CONTACT_NAME = "DIRECT CONTACT"
OUT_COL_CONTACT_TITLE = "JOBTITLE"
OUT_COL_CONTACT_LINKEDIN = "LINKEDIN"
OUT_COL_CONTACT_EMAIL = "EMAIL"
OUT_COL_GOVUK_URL = "GOV.UK URL"
OUT_COL_SOURCE_URL = "source_url"
OUT_COL_CONFIDENCE = "confidence"
OUT_COL_NOTES = "notes"
# New: keep Phase 2 contact source separate from Phase 1 source
OUT_COL_CONTACT_SOURCE_URL = "contact_source_url"

# -----------------------------
# Utilities
# -----------------------------
_phone_clean_re = re.compile(r"[^\d+]")

def clean_phone(p: str) -> str:
    return _phone_clean_re.sub("", (p or "").strip())

def dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items or []:
        if not x:
            continue
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

_company_lead_trash = re.compile(
    r"^(unit|suite|apt|apartment|flat|office|room|floor|level|block|building|bldg|dept|department|the)\s+\w+\.?[,]?\s*",
    re.I,
)

def extract_company_from_address(address: str) -> str:
    if not address:
        return ""
    cand = address.strip()
    cand = re.split(r"\s+-\s+|,|\n|\r", cand)[0]
    cand = _company_lead_trash.sub("", cand).strip()
    cand = re.sub(r"^[\d]+[A-Za-z\-]*\s*", "", cand).strip()
    return cand[:140].strip()

def ensure_col_exact(header: List[str], name: str) -> int:
    if name in header:
        return header.index(name)
    header.append(name)
    return len(header) - 1

def ensure_phone_cols(header: List[str], max_phones: int) -> None:
    if OUT_COL_PHONE not in header:
        header.append(OUT_COL_PHONE)
    if max_phones > 1:
        for k in range(2, max_phones + 1):
            col_name = f"{OUT_COL_PHONE} {k}"
            if col_name not in header:
                header.append(col_name)

def fill_phone_cols(row: List[str], header: List[str], numbers: List[str]) -> None:
    number_base_idx = header.index(OUT_COL_PHONE)
    max_phones = 1
    for k, val in enumerate(numbers or []):
        if k == 0:
            row[number_base_idx] = row[number_base_idx] or val
        else:
            col = f"{OUT_COL_PHONE} {k+1}"
            if col not in header:
                header.append(col)
            idx = header.index(col)
            if len(row) <= idx:
                row += [""] * (idx + 1 - len(row))
            row[idx] = val
            max_phones = max(max_phones, k + 1)
    ensure_phone_cols(header, max_phones)

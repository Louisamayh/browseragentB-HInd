import csv
import os
from pathlib import Path

def write_csv(path: str, header: list, rows: list, dialect: csv.Dialect = None):
    """
    Basic CSV writer helper. Creates parent folders and writes a header + rows.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(
            f,
            delimiter=dialect.delimiter if dialect else ",",
            quotechar='"',
            doublequote=True,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writerow(header)
        for r in rows:
            writer.writerow(r)


def append_row_safe(path: str, header: list, row: list, dialect: csv.Dialect = None):
    """
    Crash-safe append: creates file with header if missing, appends one row,
    then flushes + fsync so bytes hit disk even on power loss.
    """
    p = Path(path)
    file_exists = p.exists()
    p.parent.mkdir(parents=True, exist_ok=True)

    with open(p, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(
            f,
            delimiter=dialect.delimiter if dialect else ",",
            quotechar='"',
            doublequote=True,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
        )
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            # some filesystems ignore fsync
            pass
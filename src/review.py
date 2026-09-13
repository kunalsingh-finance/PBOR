"""Persist analyst review evidence without changing calculated control results.

The journal is independent of the pipeline's replaceable financial tables. A
review describes work performed; clearing a break requires corrected inputs and
a new reconciliation run. This local journal does not authenticate reviewers.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import sqlite3

import pandas as pd

REVIEW_STATUSES = ("Unreviewed", "Investigating", "Escalated", "Evidence recorded")
OWNER_MAX_LENGTH = 120
NOTE_MIN_LENGTH = 10
NOTE_MAX_LENGTH = 2000
REVIEW_COLUMNS = ["review_id", "exception_key", "owner", "review_status", "note", "updated_at"]

_IDENTITY_COLUMNS = (
    "asof_date", "portfolio_id", "account_id", "security_id", "currency", "break_type",
)
_OBSERVATION_COLUMNS = (
    "internal_quantity", "external_quantity", "internal_price", "external_price",
    "internal_market_value", "external_market_value", "internal_cash", "external_cash",
)


def _text_value(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _number_value(value: object) -> str | None:
    text = _text_value(value)
    if text is None:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"Exception observation must be numeric: {text!r}") from exc
    if not number.is_finite():
        raise ValueError("Exception observations must be finite numbers or missing values.")
    # SQLite/pandas may load an integer as a float. Both must identify the same
    # observation; differences in actual source values must produce new keys.
    if number == 0:
        return "0"
    text = format(number, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def exception_key(row: Mapping[str, object]) -> str:
    """Hash the account, instrument, date, break type and observed source values.

    Presentation and review fields are excluded. Missing values are canonicalized
    across pandas and SQLite, and numerically equal observations have equal keys.
    A changed observation starts an unreviewed exception while retaining history.
    """
    identity = {column: _text_value(row.get(column)) for column in _IDENTITY_COLUMNS}
    for column in ("asof_date", "portfolio_id", "account_id", "break_type"):
        if identity[column] is None:
            raise ValueError(f"Exception identity requires {column}.")
    if identity["security_id"] is None and identity["currency"] is None:
        raise ValueError("Exception identity requires a security_id or currency.")
    try:
        date = pd.Timestamp(identity["asof_date"])
        if pd.isna(date):
            raise ValueError("Missing date")
        identity["asof_date"] = date.date().isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError("Exception asof_date must be a valid date.") from exc
    observations = {column: _number_value(row.get(column)) for column in _OBSERVATION_COLUMNS}
    payload = {"version": 1, "identity": identity, "observations": observations}
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _connect(db_path: str | Path, *, writable: bool = False) -> sqlite3.Connection:
    path = Path(db_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Portfolio database not found: {path}. Run the pipeline first.")
    # URI mode also prevents accidentally creating a new database if the file is
    # removed between the existence check and the connection attempt.
    return sqlite3.connect(f"{path.as_uri()}?mode={'rw' if writable else 'ro'}", uri=True, timeout=10)


def _has_journal(conn: sqlite3.Connection) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'exception_reviews'"
    ).fetchone() is not None


def load_review_history(db_path: str | Path) -> pd.DataFrame:
    """Return all review events in append order, without creating a journal."""
    with closing(_connect(db_path)) as conn:
        if not _has_journal(conn):
            return pd.DataFrame(columns=REVIEW_COLUMNS)
        return pd.read_sql_query(
            "SELECT review_id, exception_key, owner, review_status, note, updated_at "
            "FROM exception_reviews ORDER BY review_id ASC", conn,
        )


def load_reviews(db_path: str | Path) -> pd.DataFrame:
    """Return the latest saved review per exception, newest append first."""
    with closing(_connect(db_path)) as conn:
        if not _has_journal(conn):
            return pd.DataFrame(columns=REVIEW_COLUMNS)
        return pd.read_sql_query(
            "SELECT review_id, exception_key, owner, review_status, note, updated_at "
            "FROM exception_reviews WHERE review_id IN "
            "(SELECT MAX(review_id) FROM exception_reviews GROUP BY exception_key) "
            "ORDER BY review_id DESC", conn,
        )


def save_review(
    db_path: str | Path,
    exception_id: str,
    owner: str,
    review_status: str,
    note: str,
) -> None:
    """Append a validated review event with a server-generated UTC timestamp.

    No source value, technical status, workflow status, or sign-off table is
    modified. A subsequent save adds history rather than overwriting an event.
    """
    if not isinstance(exception_id, str) or re.fullmatch(r"[0-9a-f]{64}", exception_id) is None:
        raise ValueError("Exception ID must be the key produced by exception_key().")
    if not isinstance(owner, str) or not owner.strip():
        raise ValueError("A review owner is required.")
    owner = owner.strip()
    if len(owner) > OWNER_MAX_LENGTH:
        raise ValueError(f"Owner must contain at most {OWNER_MAX_LENGTH} characters.")
    if review_status not in REVIEW_STATUSES:
        raise ValueError(f"Review status must be one of: {', '.join(REVIEW_STATUSES)}.")
    if not isinstance(note, str):
        raise ValueError("A review note is required.")
    note = note.strip()
    if len(note) < NOTE_MIN_LENGTH or not any(character.isalnum() for character in note):
        raise ValueError(f"Describe the review in a note of at least {NOTE_MIN_LENGTH} characters.")
    if len(note) > NOTE_MAX_LENGTH:
        raise ValueError(f"Review note must contain at most {NOTE_MAX_LENGTH} characters.")
    timestamp = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    with closing(_connect(db_path, writable=True)) as conn, conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS exception_reviews ("
            "review_id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "exception_key TEXT NOT NULL, owner TEXT NOT NULL, "
            "review_status TEXT NOT NULL CHECK (review_status IN "
            "('Unreviewed', 'Investigating', 'Escalated', 'Evidence recorded')), "
            "note TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS exception_reviews_key_idx "
            "ON exception_reviews (exception_key, review_id)"
        )
        for operation in ("UPDATE", "DELETE"):
            conn.execute(
                f"CREATE TRIGGER IF NOT EXISTS exception_reviews_no_{operation.lower()} "
                f"BEFORE {operation} ON exception_reviews "
                "BEGIN SELECT RAISE(ABORT, 'Review history is append-only'); END"
            )
        conn.execute(
            "INSERT INTO exception_reviews (exception_key, owner, review_status, note, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (exception_id, owner, review_status, note, timestamp),
        )

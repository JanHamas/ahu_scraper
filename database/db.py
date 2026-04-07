# database/db.py
import sqlite3
import threading
from pathlib import Path
from scraper.logger import get_logger

log = get_logger()

SCHEMA = """
CREATE TABLE IF NOT EXISTS company_types (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS provinces (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS cities (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    province_id INTEGER NOT NULL REFERENCES provinces(id),
    UNIQUE(name, province_id)
);

CREATE TABLE IF NOT EXISTS companies (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    data_id    TEXT UNIQUE NOT NULL,
    name       TEXT NOT NULL,
    phone      TEXT DEFAULT '',
    address    TEXT DEFAULT '',
    city_id    INTEGER REFERENCES cities(id),
    type_id    INTEGER REFERENCES company_types(id),
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS keywords (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword      TEXT UNIQUE NOT NULL,
    status       TEXT DEFAULT 'pending',
    result_count INTEGER DEFAULT 0,
    scraped_at   TEXT
);

CREATE TABLE IF NOT EXISTS keyword_companies (
    keyword_id INTEGER NOT NULL REFERENCES keywords(id),
    company_id INTEGER NOT NULL REFERENCES companies(id),
    PRIMARY KEY (keyword_id, company_id)
);

CREATE INDEX IF NOT EXISTS idx_companies_data_id ON companies(data_id);
CREATE INDEX IF NOT EXISTS idx_companies_name    ON companies(name);
CREATE INDEX IF NOT EXISTS idx_companies_type    ON companies(type_id);
CREATE INDEX IF NOT EXISTS idx_keywords_status   ON keywords(status);
CREATE INDEX IF NOT EXISTS idx_cities_province   ON cities(province_id);
"""


class DBHandler:
    """
    Thread-safe SQLite handler for multi-worker scraping.

    Key design decisions:
    - Single shared connection with WAL mode (safe for concurrent reads,
      serialized writes via threading.Lock).
    - _lock wraps every write path so get-or-create → insert sequences
      are atomic and never interleaved across workers.
    - No commit() inside private helpers — the public method that holds
      the lock owns the transaction and commits once at the end.
    - foreign_keys=ON is set inside a begin/commit block so it survives
      the implicit COMMIT that executescript() issues.
    """

    def __init__(self, db_path):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        # executescript issues an implicit COMMIT — run schema first
        self.conn.executescript(SCHEMA)
        # PRAGMAs after schema
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.commit()
        log.info(f"[DB] Connected to {db_path}")

    # ── Keywords ───────────────────────────────────────────────────────────────

    def seed_keywords(self, keywords: list[str]) -> int:
        """Bulk-insert keywords, skipping duplicates. Returns count inserted."""
        with self._lock:
            before = self._count("keywords")
            self.conn.executemany(
                "INSERT OR IGNORE INTO keywords (keyword) VALUES (?)",
                [(k,) for k in keywords],
            )
            self.conn.commit()
            inserted = self._count("keywords") - before
        log.info(f"[DB] Seeded {inserted} new keywords ({self._count('keywords')} total)")
        return inserted

    def get_pending_keywords(self) -> list[str]:
        """Return all pending keywords in alphabetical order."""
        rows = self.conn.execute(
            "SELECT keyword FROM keywords WHERE status='pending' ORDER BY keyword"
        ).fetchall()
        return [r["keyword"] for r in rows]

    def mark_keyword(self, keyword: str, status: str, result_count: int = 0) -> None:
        with self._lock:
            self.conn.execute(
                """UPDATE keywords
                   SET status=?, result_count=?, scraped_at=datetime('now')
                   WHERE keyword=?""",
                (status, result_count, keyword),
            )
            self.conn.commit()
        log.debug(f"[DB] Keyword '{keyword}' → {status} ({result_count} results)")

    def add_expanded_keywords(self, keywords: list[str]) -> int:
        """Add overflow-expanded keywords (4-letter, 5-letter) as pending."""
        return self.seed_keywords(keywords)

    def get_keyword_id(self, keyword: str) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM keywords WHERE keyword=?", (keyword,)
        ).fetchone()
        return row["id"] if row else None

    # ── Companies ──────────────────────────────────────────────────────────────

    def upsert_company(self, company: dict, keyword: str | None = None) -> int | None:
        """
        Atomically resolve all foreign keys, insert the company if new,
        and link it to the keyword.

        The entire sequence runs under _lock so no two workers can
        interleave their get-or-create calls and produce FK violations.

        Returns:
            company_id (int) if a new row was inserted.
            None if the company already existed OR on error.
        """
        with self._lock:
            try:
                type_id = self._get_or_create(
                    "company_types", "name",
                    company.get("company_type") or "Unknown",
                )
                province_id = self._get_or_create(
                    "provinces", "name",
                    company.get("province") or "Unknown",
                )
                city_id = self._get_or_create_city(
                    company.get("city") or "Unknown",
                    province_id,
                )

                cur = self.conn.execute(
                    """INSERT OR IGNORE INTO companies
                       (data_id, name, phone, address, city_id, type_id)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        company["data_id"],
                        company["name"],
                        company.get("phone", ""),
                        company.get("address", ""),
                        city_id,
                        type_id,
                    ),
                )

                # lastrowid is 0 when INSERT OR IGNORE skips an existing row
                is_new = cur.lastrowid and cur.lastrowid > 0
                company_id = cur.lastrowid if is_new else self._get_company_id(company["data_id"])

                if keyword and company_id:
                    kw_id = self.get_keyword_id(keyword)
                    if kw_id:
                        self.conn.execute(
                            "INSERT OR IGNORE INTO keyword_companies VALUES (?, ?)",
                            (kw_id, company_id),
                        )

                self.conn.commit()
                # Return id only for new inserts so callers can count "saved"
                return company_id if is_new else None

            except Exception as e:
                self.conn.rollback()
                log.error(
                    f"[DB] upsert_company error for data_id={company.get('data_id')}: {e}"
                )
                return None

    def get_total_companies(self) -> int:
        return self._count("companies")

    def company_exists(self, data_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM companies WHERE data_id=?", (data_id,)
        ).fetchone()
        return row is not None

    # ── Stats ──────────────────────────────────────────────────────────────────

    def print_stats(self) -> None:
        total    = self._count("companies")
        done     = self._count_keywords("done")
        pending  = self._count_keywords("pending")
        overflow = self._count_keywords("overflow")
        failed   = self._count_keywords("failed")
        log.info(
            f"[DB] Stats → Companies: {total} | "
            f"Keywords done: {done} | pending: {pending} | "
            f"overflow: {overflow} | failed: {failed}"
        )

    # ── Internal helpers (call only while holding _lock) ──────────────────────

    def _get_or_create(self, table: str, col: str, value: str) -> int:
        """
        Return the id for `value` in `table`, inserting a new row if needed.
        Does NOT commit — the caller (upsert_company) commits once for the
        whole transaction.
        """
        value = value.strip() if value else "Unknown"
        if not value:
            value = "Unknown"

        row = self.conn.execute(
            f"SELECT id FROM {table} WHERE {col}=?", (value,)
        ).fetchone()
        if row:
            return row["id"]

        cur = self.conn.execute(
            f"INSERT INTO {table} ({col}) VALUES (?)", (value,)
        )
        return cur.lastrowid

    def _get_or_create_city(self, city: str, province_id: int) -> int:
        """
        Return the id for (city, province_id), inserting if needed.
        Does NOT commit — caller commits.
        """
        city = city.strip() if city else "Unknown"
        if not city:
            city = "Unknown"

        row = self.conn.execute(
            "SELECT id FROM cities WHERE name=? AND province_id=?",
            (city, province_id),
        ).fetchone()
        if row:
            return row["id"]

        cur = self.conn.execute(
            "INSERT INTO cities (name, province_id) VALUES (?, ?)",
            (city, province_id),
        )
        return cur.lastrowid

    def _get_company_id(self, data_id: str) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM companies WHERE data_id=?", (data_id,)
        ).fetchone()
        return row["id"] if row else None

    def _count(self, table: str) -> int:
        return self.conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

    def _count_keywords(self, status: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM keywords WHERE status=?", (status,)
        ).fetchone()[0]

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def close(self) -> None:
        self.conn.close()
        log.info("[DB] Connection closed")
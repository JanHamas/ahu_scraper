import sqlite3
from pathlib import Path
from scraper.logger import get_logger

log = get_logger()

# Schema
SCHEMA = """
-- Lookup: company legal types
CREATE TABLE IF NOT EXISTS company_types (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- Lookup: provinces
CREATE TABLE IF NOT EXISTS provinces (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- Lookup: cities (linked to province)
CREATE TABLE IF NOT EXISTS cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    province_id INTEGER NOT NULL REFERENCES province(id)
    UNIQUE(name, province_id)
);

-- Core: companies
-- data_id = the 'data-id' attribute from AHU HTML = internal NBRS-style ID
CREATE TABLE IF NOT EXISTS companies(
    id          INTEGER PRIMARY KEY AUTOINREMENT,
    data_id     TEXT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    phone       TEXT DEFAULT '',
    city_id     INTEGER REFERENCES cities(id),
    type_id     INTEGER REFERENCES company_types(id),
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now')),
    
);

-- Track every keyword searched + its status
CREATE TABLE IF NOT EXISTS keywords (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT UNIQUE NOT NULL,
    status  TEXT DEFAULT 'pending',
    result_count   INTEGER DEFAULT 0,
    scraped_at TEXT
);

-- status values:
pending     -> not yet scraped
done        -> successfully scraped
overflow    -> result count > limit, expanded to sub-keywords
singleton   -> only 1 result, discarded per task rules
empty       -> 0 results
failed      -> scrape error, will retry

-- Many-to-Many: which companies were found by which keywords
CREATE TABLE IF NOT EXISTS keyword_companies (
    keyword_id INTEGER NOT NULL REPERENCES keywords(id)
    company_id  INTEGER NOT NULL REFERENCES companies(id)
    PRIMARY KEY (keyword_id, company_id)
);

-- Indexes for last lookups
CREATE INDEX IF NOT EXISTS idx_companies_data ON companies(data_id)
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name)
CREATE INDEX IF NOT EXISTS idx_companies_type ON companies(type_id)
CREATE INDEX IF NOT EXISTS idx_keywords_status ON keywords(status)
CREATE INDEX IF NOT EXISTS idx_cities_prvince  ON cities(province_id)
"""

class DBHandler:
    """
    Thread-safe SQlite handler with WAL mode.
    All lookups use get-or-create pattern to maintain normlization.
    
    """
    def __init__(self, db_path):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        # WAL = Write-Ahead Logging - safe for concurrent reads during write

        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.commit()
        log.info(f"[DB] Connected to {db_path}")

    # Keywords
    def seed_keywords(self, keywords: list[str]) -> int:
        """ Bulk insert keywords - skips existing ones. Returns count inserted."""
        before = self._count("keywords")
        self.conn.executemany(
            "INSERT OR IGNORE INTO keywords (keyword) VALUE (?)",
            [(k,) for k in keywords]
        )
        self.conn.commit()
        inserted = self._count("keywords") - before
        log.info(f"[DB] Seeded {inserted} new keywords ({self._count('keywords')} total)")
        return inserted
    
    def get_pending_keywords(self) -> list[str]:
        """ Return all keywords not yet processed, in alhpabetical order"""
        rows = self.conn.execute(
            "SELECT keyword FROM keywords WHERE status='pending' ORDER BY keyword"
        ).fetchall()
        return [r["keyword"] for r in rows]
    
    def mark_keyword(self, keyword: str, status: str, result_count: int = 0) -> None:
        self.conn.execute(
            """ UPDATE keywords
                SET status=?, result_count=?, scraped_at=datetime('now')
                WHERE keyword=?""",
                (status, result_count, keyword)     

        )
        self.conn.commit()
        log.debug(f"[DB] Keyword '{keyword}' -> {status} ({result_count}) results")
    
    def add_expanded_keywords(self, keywords: list[str]) -> int:
        """Add overflow-expanded keywords (4-letter, 5-letter) as pending"""
        return self.seed_keywords(keywords)
    
    def get_keyword_id(self, keyword: str) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM keywords WHERE keyword=?", (keyword,)
        ).fetchone()
        return row["id"] if row else None
    
    # Companies
    def upsert_company(self, company: dict, keyword: str | None = None) -> int | None:
        """
        Insert company if data_id not seen before.
        Optionally links to the keyword that found it.
        Returns company id (new or existing).
        """
        type_id = self._get_or_create("company_types", "name", company.get("company_type", "Unknown"))
        province_id = self._get_or_create("province", "name", company.get("province", "") or "Unknown")
        city_id = self._get_or_create_city(
            company.get("city", "") or "Unknown", province_id
        )

        try:
            cur = self.conn.execute(
                """ INSERT OR IGNORE INTO companies
                    (data_id, name, phone, address, city_id, type_id)
                    VALUES (?, ?, ?, ?, ?, ?)"""
                (
                    company["data_id"],
                    company["name"],
                    company.get("phone", ""),
                    company.get("address", ""),
                    city_id,
                    type_id

                )
            )
            self.conn.commit()

            # Get the company id (new insert or existing)
            company_id = cur.lastrowid or self._get_company_id(company["data_id"])

            # Link keyword -> company if provided
            if keyword and company_id:
                kw_id = self.get_keyword_id(keyword)
                if kw_id:
                    self.conn.execute(
                        "INSERT OR IGNORE INTO keyword_companies VALUES (?, ?)",
                        (kw_id, company_id)
                    )
                    self.conn.commit()
            return company_id
        
        except Exception as e:
            log.error(f"[DB] upsert_company error for data_id={company.get('data_id')}: {e}")
            return None
        
    def get_total_companies(self) -> int:
        return self._count("companies")
    
    def company_exists(self, data_id, str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM companies WHERE data_id=?", (data_id)
        ).fetchone()
        return row is not None
    
    # Status
    def print_stats(self) -> None:
        total = self._count("companies")
        done = self._count_keywords("done")
        pending = self._count_keywords("pending")
        overflow = self._count_keywords("overflow")
        failed = self._count_keywords("failed")
        log.info(
            f"[DB] Stats -> Companies: {total} | "
            f"Keywords done: {done} | pending: {pending}"
            f"overflow: {overflow} | failed: {failed}"
        )
    
    # Internal helper
    def _get_or_create(self, table: str, col: str, value: str) -> int:
        value = value.strip() if value else "Unknown"
        if not value:
            value = "Unknown"
        row = self.conn.execute(
            f"SELECT id FROM {table} WHERE {col}=?", (value,)
        ).fetchone()
        if row:
            return row["id"]
        cur = self.conn.execute(f"INSERT INTO {table} ({col}) VALUES (?)", (value,))
        self.conn.commit()
        return cur.lastrowid
    
    def _get_or_create_city(self, city: str, province_id: int) -> int:
        city = city.strip() if city else "Unknown"
        row = self.conn.execute(
            "SELECT id FROM cities WHERE name=? AND province_id=?", (city, province_id)
        ).fetchone()
        if row:
            return row["id"]
        cur = self.conn.execute(
            "INSERT INTO cities (name, province_id) VALUES (?, ?)", (city, province_id)
        )
        self.conn.commit()
        return cur.lastrowid
    
    def _get_company_id(self, data_id: str) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM companies WHERE data_id=?", (data_id,)
        ).fetchone()
        return row["id"] if row else None
    
    def _count(self, table: str) -> int:
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    
    def _count_keyword(self, status: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM keywords WHERE status=?", (status,)
        ).fetchone()[0]
    
    def close(self) -> None:
        self.conn.close()
        log.info("[DB] Connection closed")
    



import json
import sqlite3
import time
from pathlib import Path
from typing import Tuple, Dict

class DB:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")
        self.conn.execute("PRAGMA temp_store = MEMORY;")
        self._init()

    def _init(self):
        cur = self.conn.cursor()
        # Очередь обхода
        cur.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            url TEXT PRIMARY KEY,
            kind TEXT CHECK(kind IN ('catalog','category','product')) NOT NULL,
            status TEXT CHECK(status IN ('pending','done','error')) NOT NULL DEFAULT 'pending',
            tries INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            updated_at REAL
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_queue_kind_status ON queue(kind, status);")

        # Категории
        cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            url TEXT PRIMARY KEY,
            name TEXT,
            discovered_at REAL
        );
        """)

        # Найденные карточки на списковых страницах
        cur.execute("""
        CREATE TABLE IF NOT EXISTS product_discovery (
            url TEXT PRIMARY KEY,
            title TEXT,
            category_url TEXT,
            discovered_at REAL
        );
        """)

        # Итоговые товары
        cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            url TEXT PRIMARY KEY,
            title TEXT,
            price REAL,
            currency TEXT,
            part_number TEXT,
            brand TEXT,
            stock INTEGER,
            prod_id TEXT,
            app_id TEXT,
            alt_sku TEXT,
            category_url TEXT,
            attrs_json TEXT,
            fitment_json TEXT,
            discovered_at REAL,
            scraped_at REAL
        );
        """)

        # Замки для конкурентной выборки задач
        cur.execute("""
        CREATE TABLE IF NOT EXISTS locks (
            url TEXT PRIMARY KEY,
            ts REAL
        );
        """)
        self.conn.commit()

    def _ts(self) -> float:
        return time.time()

    # очередь
    def seed_catalog(self, url: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO queue(url, kind, status, updated_at) VALUES(?, 'catalog', 'pending', ?)",
            (url, self._ts()),
        )
        self.conn.commit()

    def upsert_queue(self, url: str, kind: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO queue(url, kind, status, updated_at) VALUES(?, ?, 'pending', ?)",
            (url, kind, self._ts()),
        )
        self.conn.commit()

    def count_pending(self) -> int:
        (n,) = self.conn.execute("SELECT COUNT(*) FROM queue WHERE status='pending'").fetchone()
        return int(n)

    def mark_done(self, url: str):
        self.conn.execute(
            "UPDATE queue SET status='done', updated_at=? WHERE url=?",
            (self._ts(), url)
        )
        self.conn.execute("DELETE FROM locks WHERE url=?", (url,))
        self.conn.commit()

    def mark_error(self, url: str, err: str):
        self.conn.execute(
            "UPDATE queue SET status='error', tries=tries+1, last_error=?, updated_at=? WHERE url=?",
            (err[:1000], self._ts(), url)
        )
        self.conn.execute("DELETE FROM locks WHERE url=?", (url,))
        self.conn.commit()

    def reserve_next(self):
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        row = cur.execute("""
            SELECT url, kind FROM queue
            WHERE status='pending'
              AND url NOT IN (SELECT url FROM locks)
            ORDER BY CASE kind WHEN 'catalog' THEN 0 WHEN 'category' THEN 1 ELSE 2 END,
                     updated_at ASC
            LIMIT 1;
        """).fetchone()
        if not row:
            self.conn.execute("COMMIT;")
            return None
        url = row["url"]
        cur.execute("INSERT OR IGNORE INTO locks(url, ts) VALUES(?, ?)", (url, self._ts()))
        self.conn.execute("COMMIT;")
        return (row["url"], row["kind"])

    # категории/продукты
    def insert_category(self, url: str, name: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO categories(url, name, discovered_at) VALUES(?,?,?)",
            (url, name, self._ts())
        )
        self.conn.commit()

    def insert_product_discovery(self, url: str, title: str, category_url: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO product_discovery(url, title, category_url, discovered_at) VALUES(?,?,?,?)",
            (url, title, category_url, self._ts())
        )
        self.conn.commit()

    def product_exists(self, url: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM products WHERE url=? LIMIT 1", (url,)).fetchone()
        return bool(row)

    def upsert_product(self, data: dict):
        self.conn.execute("""
        INSERT INTO products(
            url, title, price, currency, part_number, brand, stock,
            prod_id, app_id, alt_sku, category_url,
            attrs_json, fitment_json, discovered_at, scraped_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(url) DO UPDATE SET
            title=excluded.title,
            price=excluded.price,
            currency=excluded.currency,
            part_number=excluded.part_number,
            brand=excluded.brand,
            stock=excluded.stock,
            prod_id=excluded.prod_id,
            app_id=excluded.app_id,
            alt_sku=excluded.alt_sku,
            category_url=excluded.category_url,
            attrs_json=excluded.attrs_json,
            fitment_json=excluded.fitment_json,
            scraped_at=excluded.scraped_at;
        """, (
            data.get("url"),
            data.get("title"),
            data.get("price"),
            data.get("currency"),
            data.get("part_number"),
            data.get("brand"),
            data.get("stock"),
            data.get("prod_id"),
            data.get("app_id"),
            data.get("alt_sku"),
            data.get("category_url"),
            json.dumps(data.get("attrs") or {}, ensure_ascii=False),
            json.dumps(data.get("fitment") or [], ensure_ascii=False),
            data.get("discovered_at") or self._ts(),
            data.get("scraped_at") or self._ts(),
        ))
        self.conn.commit()

    # Прогресс
    def category_counts(self) -> Dict[str, int]:
        (total,)  = self.conn.execute("SELECT COUNT(*) FROM queue WHERE kind='category'").fetchone()
        (done,)   = self.conn.execute("SELECT COUNT(*) FROM queue WHERE kind='category' AND status='done'").fetchone()
        (pend,)   = self.conn.execute("SELECT COUNT(*) FROM queue WHERE kind='category' AND status='pending'").fetchone()
        (error,)  = self.conn.execute("SELECT COUNT(*) FROM queue WHERE kind='category' AND status='error'").fetchone()
        return {"total": int(total), "done": int(done), "pending": int(pend), "error": int(error)}

    def product_counts_for_category(self, category_url: str) -> Dict[str, int]:
        (total,) = self.conn.execute(
            "SELECT COUNT(*) FROM product_discovery WHERE category_url=?",
            (category_url,)
        ).fetchone()
        (done,) = self.conn.execute(
            """
            SELECT COUNT(*) FROM queue
            WHERE kind='product' AND status='done'
              AND url IN (SELECT url FROM product_discovery WHERE category_url=?)
            """, (category_url,)
        ).fetchone()
        (pend,) = self.conn.execute(
            """
            SELECT COUNT(*) FROM queue
            WHERE kind='product' AND status='pending'
              AND url IN (SELECT url FROM product_discovery WHERE category_url=?)
            """, (category_url,)
        ).fetchone()
        (error,) = self.conn.execute(
            """
            SELECT COUNT(*) FROM queue
            WHERE kind='product' AND status='error'
              AND url IN (SELECT url FROM product_discovery WHERE category_url=?)
            """, (category_url,)
        ).fetchone()
        return {"total": int(total), "done": int(done), "pending": int(pend), "error": int(error)}

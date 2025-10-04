# cleanup_and_requeue.py
import sys
import sqlite3
import time
from pathlib import Path


def now_ts() -> float:
    return time.time()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    return conn


def find_empty_products(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("""
        SELECT url
        FROM products
        WHERE
            (title IS NULL OR trim(title) = '' OR title IN ('USD','EUR','GBP'))
            OR (
                price IS NULL
                AND IFNULL(trim(part_number),'') = ''
                AND IFNULL(trim(brand),'') = ''
                AND (attrs_json IS NULL OR attrs_json = '' OR attrs_json = '{}')
            )
    """).fetchall()
    return [r["url"] for r in rows]


def delete_products(conn: sqlite3.Connection, urls: list[str]) -> int:
    if not urls:
        return 0
    conn.executemany("DELETE FROM products WHERE url = ?", [(u,) for u in urls])
    return conn.total_changes


def ensure_queue_pending_for_products(conn: sqlite3.Connection, urls: list[str], ts: float) -> None:
    for u in urls:
        conn.execute("""
            UPDATE queue
               SET status='pending', tries=0, last_error=NULL, updated_at=?
             WHERE url=? AND status!='pending'
        """, (ts, u))
        conn.execute("""
            INSERT OR IGNORE INTO queue(url, kind, status, updated_at)
            VALUES(?, 'product', 'pending', ?)
        """, (u, ts))
        conn.execute("DELETE FROM locks WHERE url=?", (u,))


def requeue_all_errors(conn: sqlite3.Connection, ts: float) -> int:
    err_rows = conn.execute("SELECT url FROM queue WHERE status='error'").fetchall()
    urls = [r["url"] for r in err_rows]

    conn.execute("""
        UPDATE queue
           SET status='pending', tries=0, last_error=NULL, updated_at=?
         WHERE status='error'
    """, (ts,))
    if urls:
        conn.executemany("DELETE FROM locks WHERE url=?", [(u,) for u in urls])
    return len(urls)


def main():
    db_path = Path("partsgeek.sqlite3")

    conn = connect(db_path)
    try:
        ts = now_ts()

        # удаляем пустые продукты
        empty_urls = find_empty_products(conn)
        print(f"[i] Empty products found: {len(empty_urls)}")

        conn.execute("BEGIN IMMEDIATE;")
        deleted_before = conn.total_changes
        delete_products(conn, empty_urls)

        ensure_queue_pending_for_products(conn, empty_urls, ts)
        conn.commit()
        deleted_count = conn.total_changes - deleted_before
        print(f"[✓] Deleted products: {deleted_count}; re-queued: {len(empty_urls)}")

        # все ошибки в pending
        conn.execute("BEGIN IMMEDIATE;")
        moved = requeue_all_errors(conn, ts)
        conn.commit()
        print(f"[✓] Re-queued from error: {moved}")
        print("[✓] Done.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

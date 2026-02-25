import json
import time
from typing import Any, Dict, Optional, Tuple, List

import aiosqlite

DB_PATH = "/opt/ps_factory/var/ps_factory.db"


async def _open_db() -> aiosqlite.Connection:
    """
    Abre conexión SQLite con pragmas recomendados.
    IMPORTANTE: esta función YA devuelve una conexión lista (ya awaited),
    así que NO debe usarse con `async with await ...`.
    """
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.execute("PRAGMA journal_mode=WAL;")
    db.row_factory = aiosqlite.Row
    return db


def _now_ts() -> int:
    return int(time.time())


# ---------------------------
# Posts
# ---------------------------

async def create_post(post_id: str, topic: str, bitcoin_anchor: str) -> None:
    """
    Crea el post si no existe. No falla si ya existe.
    """
    db = await _open_db()
    try:
        await db.execute(
            """
            INSERT OR IGNORE INTO posts (id, topic, bitcoin_anchor, status)
            VALUES (?, ?, ?, COALESCE((SELECT status FROM posts WHERE id=?), 'draft'))
            """,
            (post_id, topic, bitcoin_anchor, post_id),
        )
        # Si ya existía, refrescamos topic/anchor si estaban vacíos
        await db.execute(
            """
            UPDATE posts
            SET topic = COALESCE(NULLIF(topic,''), ?),
                bitcoin_anchor = COALESCE(NULLIF(bitcoin_anchor,''), ?)
            WHERE id=?
            """,
            (topic, bitcoin_anchor, post_id),
        )
        await db.commit()
    finally:
        await db.close()


async def get_post(post_id: str) -> Optional[Dict[str, Any]]:
    db = await _open_db()
    try:
        cur = await db.execute("SELECT * FROM posts WHERE id=?", (post_id,))
        row = await cur.fetchone()
        await cur.close()
        return dict(row) if row else None
    finally:
        await db.close()


async def set_draft_message_ref(post_id: str, draft_chat_id: int, draft_message_id: int) -> None:
    db = await _open_db()
    try:
        await db.execute(
            "UPDATE posts SET draft_chat_id=?, draft_message_id=?, status='draft' WHERE id=?",
            (int(draft_chat_id), int(draft_message_id), post_id),
        )
        await db.commit()
    finally:
        await db.close()


async def approve_post(
    post_id: str,
    approver: str,
    approved_chat_id: int,
    approved_message_id: int,
    approved_at: int,
) -> None:
    db = await _open_db()
    try:
        await db.execute(
            """
            UPDATE posts
            SET status='approved',
                approved_by=?,
                approved_at=?,
                approved_chat_id=?,
                approved_message_id=?
            WHERE id=?
            """,
            (approver, int(approved_at), int(approved_chat_id), int(approved_message_id), post_id),
        )
        await db.commit()
    finally:
        await db.close()


async def get_last_post_id() -> Optional[str]:
    """
    Devuelve el ID del último post insertado en posts (por rowid DESC).
    """
    db = await _open_db()
    try:
        cur = await db.execute("SELECT id FROM posts ORDER BY rowid DESC LIMIT 1")
        row = await cur.fetchone()
        await cur.close()
        return str(row["id"]) if row else None
    finally:
        await db.close()


# ---------------------------
# Versions
# ---------------------------

async def add_version(post_id: str, version: int, content: Dict[str, Any], model: Optional[str] = None) -> None:
    content_json = json.dumps(content, ensure_ascii=False)
    db = await _open_db()
    try:
        await db.execute(
            """
            INSERT INTO post_versions (post_id, version, model, content_json)
            VALUES (?, ?, ?, ?)
            """,
            (post_id, int(version), model, content_json),
        )
        await db.commit()
    finally:
        await db.close()


async def get_latest_version_number(post_id: str) -> int:
    db = await _open_db()
    try:
        cur = await db.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM post_versions WHERE post_id=?",
            (post_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row["v"]) if row else 0
    finally:
        await db.close()


async def get_latest_version(post_id: str) -> Optional[Tuple[int, Dict[str, Any]]]:
    """
    Devuelve (version, content_dict) de la última versión.
    """
    db = await _open_db()
    try:
        cur = await db.execute(
            """
            SELECT version, content_json
            FROM post_versions
            WHERE post_id=?
            ORDER BY version DESC
            LIMIT 1
            """,
            (post_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        return int(row["version"]), json.loads(row["content_json"])
    finally:
        await db.close()


async def list_versions(post_id: str) -> List[int]:
    """
    Devuelve lista de versiones disponibles para un post (DESC).
    """
    db = await _open_db()
    try:
        cur = await db.execute(
            "SELECT version FROM post_versions WHERE post_id = ? ORDER BY version DESC",
            (post_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [int(r["version"]) for r in rows]
    finally:
        await db.close()


async def get_version(post_id: str, version: int) -> Optional[Tuple[int, Any]]:
    """
    Devuelve (version, content) para una versión específica o None.
    """
    db = await _open_db()
    try:
        cur = await db.execute(
            "SELECT version, content_json FROM post_versions WHERE post_id = ? AND version = ? LIMIT 1",
            (post_id, int(version)),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        return int(row["version"]), json.loads(row["content_json"])
    finally:
        await db.close()


async def list_recent_latest_posts(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Devuelve posts recientes con su última versión (content_json parseado).
    Útil para anti-duplicado semántico/candidato.
    """
    db = await _open_db()
    try:
        cur = await db.execute(
            """
            SELECT p.id AS post_id, p.status, p.created_at AS post_created_at,
                   v.version, v.content_json, v.created_at AS version_created_at
            FROM posts p
            JOIN post_versions v
              ON v.post_id = p.id
             AND v.version = (
                SELECT MAX(v2.version)
                FROM post_versions v2
                WHERE v2.post_id = p.id
             )
            ORDER BY p.created_at DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = await cur.fetchall()
        await cur.close()

        out: List[Dict[str, Any]] = []
        for r in rows:
            content = {}
            try:
                content = json.loads(r["content_json"] or "{}")
            except Exception:
                content = {}
            out.append(
                {
                    "post_id": r["post_id"],
                    "status": r["status"],
                    "post_created_at": r["post_created_at"],
                    "version": int(r["version"]),
                    "version_created_at": r["version_created_at"],
                    "content": content,
                }
            )
        return out
    finally:
        await db.close()


# ---------------------------
# Events
# ---------------------------

async def log_event(post_id: str, event_type: str, meta: Optional[Dict[str, Any]] = None) -> None:
    meta_json = json.dumps(meta or {}, ensure_ascii=False)
    db = await _open_db()
    try:
        await db.execute(
            "INSERT INTO post_events (post_id, event_type, meta_json) VALUES (?, ?, ?)",
            (post_id, event_type, meta_json),
        )
        await db.commit()
    finally:
        await db.close()


# ---------------------------
# KV Store
# ---------------------------

async def kv_get(key: str) -> Optional[str]:
    db = await _open_db()
    try:
        cur = await db.execute("SELECT v FROM kv_store WHERE k = ?", (key,))
        row = await cur.fetchone()
        await cur.close()
        return row["v"] if row else None
    finally:
        await db.close()


async def kv_set(key: str, value: str) -> None:
    ts = _now_ts()
    db = await _open_db()
    try:
        await db.execute(
            """
            INSERT INTO kv_store (k, v, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at
            """,
            (key, value, ts),
        )
        await db.commit()
    finally:
        await db.close()


# ---------------------------
# Radar candidates
# ---------------------------

async def upsert_radar_candidate(c: dict) -> None:
    """
    Inserta/actualiza un candidato de radar.
    Espera que c tenga estas llaves:
      candidate_id, run_id, source, title, summary?,
      evidence_json, scores_json, total_score, created_at
    """
    db = await _open_db()
    try:
        await db.execute(
            """
            INSERT INTO radar_candidates (
              candidate_id, run_id, source, title, summary,
              evidence_json, scores_json, total_score, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(candidate_id) DO UPDATE SET
              run_id=excluded.run_id,
              source=excluded.source,
              title=excluded.title,
              summary=excluded.summary,
              evidence_json=excluded.evidence_json,
              scores_json=excluded.scores_json,
              total_score=excluded.total_score,
              created_at=excluded.created_at
            """,
            (
                c["candidate_id"],
                c["run_id"],
                c["source"],
                c["title"],
                c.get("summary"),
                c["evidence_json"],
                c["scores_json"],
                float(c["total_score"]),
                int(c["created_at"]),
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def get_radar_candidate(candidate_id: str) -> Optional[dict]:
    db = await _open_db()
    try:
        cur = await db.execute("SELECT * FROM radar_candidates WHERE candidate_id = ?", (candidate_id,))
        row = await cur.fetchone()
        await cur.close()
        return dict(row) if row else None
    finally:
        await db.close()


async def list_radar_candidates_by_run(run_id: str) -> List[dict]:
    db = await _open_db()
    try:
        cur = await db.execute(
            "SELECT * FROM radar_candidates WHERE run_id = ? ORDER BY total_score DESC",
            (run_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]
    finally:
        await db.close()

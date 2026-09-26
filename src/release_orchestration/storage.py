"""发布编排服务的 SQLite 模式和事务辅助。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS release_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('planner','operator','risk','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_profiles (
    profile_id TEXT PRIMARY KEY,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_profiles_model
ON model_profiles(model_name, created_at);

CREATE TABLE IF NOT EXISTS model_catalog (
    model_name TEXT PRIMARY KEY,
    maintained_profile_id TEXT NOT NULL REFERENCES model_profiles(profile_id),
    revision INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capacity_pools (
    pool_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('serving','fallback')),
    total_gpu_hours TEXT NOT NULL,
    reserved_gpu_hours TEXT NOT NULL DEFAULT '0',
    revision INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','suspended')),
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS release_plans (
    plan_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES model_profiles(profile_id),
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    profile_sha256 TEXT NOT NULL,
    catalog_revision INTEGER NOT NULL,
    definition_json TEXT NOT NULL,
    plan_sha256 TEXT NOT NULL,
    canary_percent TEXT NOT NULL,
    peak_gpu_hours TEXT NOT NULL,
    required_fallback_gpu_hours TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'draft'
        CHECK(state IN ('draft','confirmed','in_progress','completed','rolled_back','invalidated')),
    revision INTEGER NOT NULL DEFAULT 1,
    current_phase_seq INTEGER,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL,
    confirmed_at TEXT,
    started_at TEXT,
    closed_at TEXT,
    closed_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_plans_model_state
ON release_plans(model_name, state);

CREATE TABLE IF NOT EXISTS release_phases (
    plan_id TEXT NOT NULL REFERENCES release_plans(plan_id),
    seq INTEGER NOT NULL,
    phase TEXT NOT NULL CHECK(phase IN ('warmup','canary','scale_up','converge')),
    offset_minutes INTEGER NOT NULL,
    capacity_gpu_hours TEXT NOT NULL,
    gate_json TEXT NOT NULL,
    sources_json TEXT,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','active','passed')),
    last_blocking_json TEXT,
    activated_at TEXT,
    closed_at TEXT,
    PRIMARY KEY(plan_id, seq)
);

CREATE TABLE IF NOT EXISTS capacity_reservations (
    reservation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id TEXT NOT NULL REFERENCES release_plans(plan_id),
    pool_id TEXT NOT NULL REFERENCES capacity_pools(pool_id),
    purpose TEXT NOT NULL CHECK(purpose IN ('serving','fallback')),
    gpu_hours TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'held' CHECK(state IN ('held','released')),
    created_at TEXT NOT NULL,
    released_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_reservations_plan
ON capacity_reservations(plan_id, state);

CREATE TABLE IF NOT EXISTS release_ledger (
    ledger_key TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES release_plans(plan_id),
    action TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metric_summaries (
    summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id TEXT NOT NULL REFERENCES release_plans(plan_id),
    phase_seq INTEGER NOT NULL,
    metrics_json TEXT NOT NULL,
    gate_passed INTEGER NOT NULL CHECK(gate_passed IN (0,1)),
    blocking_json TEXT NOT NULL,
    fallback_available_gpu_hours TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    recorded_by TEXT NOT NULL REFERENCES release_users(user_id),
    recorded_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metrics_plan_phase
ON metric_summaries(plan_id, phase_seq, summary_id);

CREATE TABLE IF NOT EXISTS traffic_samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id TEXT NOT NULL REFERENCES release_plans(plan_id),
    phase_seq INTEGER NOT NULL,
    observed_gpu_hours TEXT NOT NULL,
    note TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    recorded_by TEXT NOT NULL REFERENCES release_users(user_id),
    recorded_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_traffic_plan_phase
ON traffic_samples(plan_id, phase_seq, sample_id);

CREATE TABLE IF NOT EXISTS release_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS release_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_release_audit_entity
ON release_audit_events(entity_type, entity_id, event_id);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    # check_same_thread=False 配合 API 层的请求锁，允许 ThreadingHTTPServer 共享连接。
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=10, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    initialize(connection)
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)


@contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def row_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return None if row is None else dict(row)

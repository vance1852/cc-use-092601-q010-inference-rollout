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
    role TEXT NOT NULL CHECK(role IN ('planner','operator','capacity','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_profiles (
    profile_id TEXT PRIMARY KEY,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    vram_gb_per_replica TEXT NOT NULL,
    cold_start_seconds INTEGER NOT NULL,
    tenant_priority INTEGER NOT NULL,
    replica_qps TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capacity_pools (
    pool_id TEXT PRIMARY KEY,
    gpu_count INTEGER NOT NULL,
    vram_gb_per_gpu TEXT NOT NULL,
    protected_gpu INTEGER NOT NULL DEFAULT 0,
    preemption_priority INTEGER NOT NULL DEFAULT 100,
    reserved_gpu INTEGER NOT NULL DEFAULT 0 CHECK(reserved_gpu >= 0),
    revision INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','maintenance','retired')),
    created_at TEXT NOT NULL,
    CHECK(protected_gpu < gpu_count),
    CHECK(reserved_gpu <= gpu_count)
);

CREATE TABLE IF NOT EXISTS release_plans (
    plan_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES model_profiles(profile_id),
    profile_sha256 TEXT NOT NULL,
    pool_id TEXT NOT NULL REFERENCES capacity_pools(pool_id),
    pool_revision INTEGER NOT NULL,
    tenant_priority INTEGER NOT NULL,
    peak_qps TEXT NOT NULL,
    curve_json TEXT NOT NULL,
    policy_json TEXT NOT NULL,
    phases_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'draft'
        CHECK(state IN ('draft','confirmed','in_progress','completed','rolled_back','invalidated')),
    current_phase INTEGER NOT NULL DEFAULT 0,
    reserved_gpu INTEGER NOT NULL DEFAULT 0,
    protected_margin_gpu INTEGER NOT NULL DEFAULT 0,
    capacity_released_gpu INTEGER NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_release_plans_pool_state
ON release_plans(pool_id, state);

CREATE TABLE IF NOT EXISTS phase_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id TEXT NOT NULL REFERENCES release_plans(plan_id),
    phase_seq INTEGER NOT NULL,
    phase_kind TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    verdict TEXT NOT NULL CHECK(verdict IN ('passed','blocked')),
    blocking_json TEXT NOT NULL,
    margin_gpu INTEGER NOT NULL,
    submitted_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_phase_metrics_plan
ON phase_metrics(plan_id, phase_seq, metric_id);

CREATE TABLE IF NOT EXISTS rollback_events (
    event_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES release_plans(plan_id),
    phase_seq INTEGER NOT NULL,
    phase_kind TEXT NOT NULL,
    anomaly TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    released_gpu INTEGER NOT NULL,
    response_json TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rollback_events_plan
ON rollback_events(plan_id, created_at);

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
    # check_same_thread=False：HTTP 服务在worker线程中处理请求，
    # 由 JsonApplication 的锁保证同一连接上的请求串行。
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

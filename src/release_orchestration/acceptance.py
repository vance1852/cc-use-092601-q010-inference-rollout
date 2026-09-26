"""贯通模型画像、冻结资源、分阶段发布、门槛推进与异常回退的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import InvalidState
from .service import ReleaseService


GOOD_METRICS = {
    "error_rate_percent": "0.4",
    "p99_latency_ms": 420,
    "cold_start_p99_seconds": 95,
    "vram_headroom_percent": "35",
    "observed_qps": "1000",
    "sampled_at": "2026-09-24T09:00:00Z",
}


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = ReleaseService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
    for user_id, role in (("plan", "planner"), ("ops", "operator"), ("cap", "capacity"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    service.create_pool("cap", {
        "pool_id": "infer-pool-1", "gpu_count": 48, "vram_gb_per_gpu": "80",
        "protected_gpu": 4, "preemption_priority": 50,
    })
    service.create_profile("plan", {
        "profile_id": "profile-qwen3-32b", "model_name": "qwen3-32b", "model_version": "2026.09-rc4",
        "vram_gb_per_replica": "40", "cold_start_seconds": 90, "tenant_priority": 10,
        "replica_qps": "250",
    })
    curve = [
        {"percent": "5", "hold_minutes": 30},
        {"percent": "25", "hold_minutes": 30},
        {"percent": "60", "hold_minutes": 20},
        {"percent": "100", "hold_minutes": 0},
    ]
    policy = {
        "max_error_rate_percent": "1.5",
        "max_p99_latency_ms": 800,
        "min_vram_headroom_percent": "10",
        "min_rollback_margin_gpu": 2,
    }
    # 计划 A：完整走完预热、灰度、扩容、收敛。
    plan_a = service.create_plan("plan", {
        "plan_id": "rel-2026-001", "profile_id": "profile-qwen3-32b", "pool_id": "infer-pool-1",
        "peak_qps": "20000", "traffic_curve": curve, "rollback_policy": policy,
        "idempotency_key": "rel-2026-001-key",
    })
    service.confirm_plan("plan", "rel-2026-001", 1)
    service.start_plan("ops", "rel-2026-001", 2)
    revision = 3
    for _ in range(4):
        outcome = service.advance_plan("ops", "rel-2026-001", revision, dict(GOOD_METRICS))
        revision = outcome["revision"]
    detail_a = service.plan_detail("audit", "rel-2026-001")
    # 计划 B：灰度被门槛阻断一次，随后异常回退并验证重复回调幂等。
    service.create_plan("plan", {
        "plan_id": "rel-2026-002", "profile_id": "profile-qwen3-32b", "pool_id": "infer-pool-1",
        "peak_qps": "4000", "traffic_curve": curve, "rollback_policy": policy,
        "idempotency_key": "rel-2026-002-key",
    })
    service.confirm_plan("plan", "rel-2026-002", 1)
    service.start_plan("ops", "rel-2026-002", 2)
    blocked = service.advance_plan("ops", "rel-2026-002", 3, dict(GOOD_METRICS, error_rate_percent="3.2"))
    passed = service.advance_plan("ops", "rel-2026-002", 3, dict(GOOD_METRICS))
    rollback = service.rollback_plan("ops", "rel-2026-002", {
        "event_id": "evt-alarm-7701", "anomaly": "p99 延迟突增触发回退",
        "traffic_evidence": {"window": "2026-09-24T09:30:00Z/2026-09-24T09:35:00Z", "error_rate_percent": "4.1", "observed_qps": "980"},
    })
    replay = service.rollback_plan("ops", "rel-2026-002", {
        "event_id": "evt-alarm-7701", "anomaly": "p99 延迟突增触发回退",
        "traffic_evidence": {"window": "2026-09-24T09:30:00Z/2026-09-24T09:35:00Z", "error_rate_percent": "4.1", "observed_qps": "980"},
    })
    detail_b = service.plan_detail("audit", "rel-2026-002")
    # 计划 C：草稿期间资源池维护版本变化，计划失效且无法再确认。
    service.create_plan("plan", {
        "plan_id": "rel-2026-003", "profile_id": "profile-qwen3-32b", "pool_id": "infer-pool-1",
        "peak_qps": "1000", "traffic_curve": curve, "rollback_policy": policy,
        "idempotency_key": "rel-2026-003-key",
    })
    maintenance = service.maintain_pool("cap", "infer-pool-1", {
        "gpu_count": 44, "vram_gb_per_gpu": "80", "protected_gpu": 4,
        "preemption_priority": 50, "state": "active", "expected_revision": 1,
        "reason": "下架 4 卡进行固件维护",
    })
    confirm_blocked = False
    try:
        service.confirm_plan("plan", "rel-2026-003", 1)
    except InvalidState:
        confirm_blocked = True
    detail_c = service.plan_detail("audit", "rel-2026-003")
    pool = service.get_pool("infer-pool-1")
    result = {
        "status": "ok",
        "plan_a": {"state": detail_a["state"], "phases": len(detail_a["phases"]), "reserved_gpu": detail_a["reserved_gpu"]},
        "plan_b": {
            "state": detail_b["state"],
            "gate_blocked": not blocked["advanced"] and len(blocked["blocking_reasons"]) > 0,
            "advanced_after_fix": passed["advanced"],
            "released_gpu": rollback["released_gpu"],
            "replay_released_gpu": replay["released_gpu"],
            "replayed": replay["replayed"],
            "evidence_preserved": rollback["evidence_preserved"] and len(detail_b["metric_history"]) > 0,
        },
        "plan_c": {
            "state": detail_c["state"],
            "confirm_blocked": confirm_blocked,
            "invalidated_by_maintenance": maintenance["invalidated_plans"] == ["rel-2026-003"],
        },
        "pool": {"reserved_gpu": pool["reserved_gpu"], "revision": pool["revision"], "free_gpu": pool["free_gpu"]},
        "audit": service.audit_chain("audit"),
        "workspace": workspace.name,
    }
    connection.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行容量预演与发布编排离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

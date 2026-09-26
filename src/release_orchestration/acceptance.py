"""贯通模型画像、容量预演、分阶段发布、回退与版本失效的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .service import ReleaseService


POLICY = {
    "max_error_rate_percent": "1.5",
    "max_p95_latency_ms": "800",
    "min_success_rate_percent": "99",
    "min_fallback_gpu_hours": "3000",
}
PASSING_METRICS = {"error_rate_percent": "0.4", "p95_latency_ms": "620", "success_rate_percent": "99.6"}


def _profile(profile_id: str, version: str) -> dict[str, object]:
    return {
        "profile_id": profile_id,
        "model_name": "llm-x",
        "model_version": version,
        "vram_gb_per_replica": "80",
        "cold_start_seconds": 240,
        "tenant_priority": 10,
        "fallback_capacity_gpu_hours": "4000",
    }


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = ReleaseService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
    for user_id, role in (("plan", "planner"), ("ops", "operator"), ("risk", "risk"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    service.create_pool("ops", {"pool_id": "serving-a", "name": "在线服务池A", "kind": "serving", "total_gpu_hours": "12000"})
    service.create_pool("ops", {"pool_id": "serving-b", "name": "在线服务池B", "kind": "serving", "total_gpu_hours": "20000"})
    service.create_pool("ops", {"pool_id": "fallback-1", "name": "故障回退池", "kind": "fallback", "total_gpu_hours": "10000"})
    service.register_profile("plan", _profile("profile-v3.2.1", "v3.2.1"))

    # 计划 A：预热 → 灰度 → 扩容 → 收敛，全部门槛满足后完成并释放全部预留。
    service.create_plan("plan", {
        "plan_id": "plan-a",
        "profile_id": "profile-v3.2.1",
        "canary_percent": "25",
        "traffic_curve": [
            {"offset_minutes": 0, "target_gpu_hours": "2000"},
            {"offset_minutes": 30, "target_gpu_hours": "8000"},
            {"offset_minutes": 90, "target_gpu_hours": "20000"},
            {"offset_minutes": 180, "target_gpu_hours": "12000"},
        ],
        "rollback_policy": POLICY,
        "idempotency_key": "plan-key-a",
    })
    confirmed = service.confirm_plan("plan", "plan-a", 1)
    started = service.start_plan("ops", "plan-a", confirmed["revision"])
    revision = started["revision"]
    for sequence in (1, 2, 3, 4):
        service.record_traffic("ops", "plan-a", {
            "phase_seq": sequence,
            "observed_gpu_hours": "1500",
            "note": "灰度流量采样",
            "idempotency_key": f"traffic-a-{sequence}",
        })
        service.submit_metrics("ops", "plan-a", {
            "phase_seq": sequence,
            **PASSING_METRICS,
            "idempotency_key": f"metrics-a-{sequence}",
        })
        revision = service.advance_plan("ops", "plan-a", revision)["revision"]
    plan_a = service.plan_detail("audit", "plan-a")

    # 计划 B：执行中触发异常回退，重复回调不重复扣减容量，流量证据保留。
    service.create_plan("plan", {
        "plan_id": "plan-b",
        "profile_id": "profile-v3.2.1",
        "traffic_curve": [
            {"offset_minutes": 0, "target_gpu_hours": "1000"},
            {"offset_minutes": 60, "target_gpu_hours": "4000"},
        ],
        "rollback_policy": POLICY,
        "idempotency_key": "plan-key-b",
    })
    service.confirm_plan("plan", "plan-b", 1)
    service.start_plan("ops", "plan-b", 2)
    service.record_traffic("ops", "plan-b", {"phase_seq": 1, "observed_gpu_hours": "980", "idempotency_key": "traffic-b-1"})
    service.submit_metrics("ops", "plan-b", {"phase_seq": 1, **PASSING_METRICS, "idempotency_key": "metrics-b-1"})
    rollback = service.rollback_plan("risk", "plan-b", {"reason": "P95 延迟突增触发回退", "idempotency_key": "rb-b-1"})
    replay = service.rollback_plan("risk", "plan-b", {"reason": "P95 延迟突增触发回退", "idempotency_key": "rb-b-1"})
    plan_b = service.plan_detail("audit", "plan-b")

    # 计划 C：新版本画像登记后，旧计划自动失效。
    service.create_plan("plan", {
        "plan_id": "plan-c",
        "profile_id": "profile-v3.2.1",
        "traffic_curve": [
            {"offset_minutes": 0, "target_gpu_hours": "500"},
            {"offset_minutes": 30, "target_gpu_hours": "1500"},
        ],
        "rollback_policy": POLICY,
        "idempotency_key": "plan-key-c",
    })
    service.register_profile("plan", _profile("profile-v3.2.2", "v3.2.2"))
    plan_c = service.plan_detail("audit", "plan-c")

    pools = [service.get_pool(pool_id) for pool_id in ("serving-a", "serving-b", "fallback-1")]
    result = {
        "status": "ok",
        "plan_a": {"state": plan_a["state"], "phases": [phase["state"] for phase in plan_a["phases"]]},
        "plan_b": {
            "state": plan_b["state"],
            "released_gpu_hours": rollback["released_gpu_hours"],
            "replay_identical": replay == rollback,
            "evidence": rollback["evidence"],
        },
        "plan_c": {"state": plan_c["state"], "closed_reason": plan_c["closed_reason"]},
        "pools_reserved": {pool["pool_id"]: pool["reserved_gpu_hours"] for pool in pools},
        "maintained": service.model_version_info("llm-x"),
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

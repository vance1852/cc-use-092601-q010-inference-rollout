from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from release_orchestration.api import JsonApplication
from release_orchestration.acceptance import run as acceptance_run
from release_orchestration.clock import FrozenClock
from release_orchestration.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from release_orchestration.models import CurvePoint
from release_orchestration.planning import allocate_sources, build_phase_specs, evaluate_gate
from release_orchestration.service import ReleaseService


POLICY = {
    "max_error_rate_percent": "1.5",
    "max_p95_latency_ms": "800",
    "min_success_rate_percent": "99",
    "min_fallback_gpu_hours": "3000",
}
PASSING = {"error_rate_percent": "0.4", "p95_latency_ms": "620", "success_rate_percent": "99.6"}
CURVE = [
    {"offset_minutes": 0, "target_gpu_hours": "2000"},
    {"offset_minutes": 30, "target_gpu_hours": "8000"},
    {"offset_minutes": 90, "target_gpu_hours": "20000"},
    {"offset_minutes": 180, "target_gpu_hours": "12000"},
]


def profile_payload(profile_id: str = "profile-v1", version: str = "v1.0") -> dict[str, object]:
    return {
        "profile_id": profile_id,
        "model_name": "llm-x",
        "model_version": version,
        "vram_gb_per_replica": "80",
        "cold_start_seconds": 240,
        "tenant_priority": 10,
        "fallback_capacity_gpu_hours": "4000",
    }


def plan_payload(plan_id: str = "plan-1", key: str = "plan-key-1", **overrides) -> dict[str, object]:
    payload: dict[str, object] = {
        "plan_id": plan_id,
        "profile_id": "profile-v1",
        "canary_percent": "25",
        "traffic_curve": CURVE,
        "rollback_policy": dict(POLICY),
        "idempotency_key": key,
    }
    payload.update(overrides)
    return payload


class PlanningTests(unittest.TestCase):
    def test_phase_specs_follow_traffic_curve(self) -> None:
        curve = [CurvePoint(0, Decimal("2000")), CurvePoint(30, Decimal("8000")),
                 CurvePoint(90, Decimal("20000")), CurvePoint(180, Decimal("12000"))]
        phases = build_phase_specs(curve, Decimal("25"), 240, Decimal("4000"), dict(POLICY))
        self.assertEqual([phase["phase"] for phase in phases], ["warmup", "canary", "scale_up", "converge"])
        self.assertEqual([phase["capacity_gpu_hours"] for phase in phases],
                         [Decimal("2000.000"), Decimal("5000.000"), Decimal("20000.000"), Decimal("12000.000")])
        self.assertEqual(phases[1]["offset_minutes"], 45)
        self.assertEqual(phases[0]["gate"]["min_warmup_seconds"], "240")
        self.assertEqual(phases[3]["gate"]["required_fallback_gpu_hours"], "4000.000")

    def test_phase_specs_reject_zero_canary(self) -> None:
        curve = [CurvePoint(0, Decimal("1000")), CurvePoint(10, Decimal("1000"))]
        with self.assertRaises(ValueError):
            build_phase_specs(curve, Decimal("0.00001"), 60, Decimal("0"), dict(POLICY))

    def test_gate_reports_all_blocking_reasons(self) -> None:
        result = evaluate_gate(
            {"error_rate_percent": Decimal("2"), "p95_latency_ms": Decimal("900"),
             "success_rate_percent": Decimal("98")},
            {"max_error_rate_percent": Decimal("1.5"), "max_p95_latency_ms": Decimal("800"),
             "min_success_rate_percent": Decimal("99")},
            Decimal("1000"),
            Decimal("4000"),
        )
        self.assertFalse(result["passed"])
        self.assertEqual(
            [reason["code"] for reason in result["blocking_reasons"]],
            ["error_rate_above_threshold", "p95_latency_above_threshold",
             "success_rate_below_threshold", "fallback_margin_insufficient"],
        )

    def test_allocate_sources_is_deterministic_and_reports_shortfall(self) -> None:
        pools = [
            {"pool_id": "b", "available_gpu_hours": Decimal("5000")},
            {"pool_id": "a", "available_gpu_hours": Decimal("3000")},
        ]
        allocations = allocate_sources(pools, Decimal("7000"))
        self.assertEqual(allocations, [
            {"pool_id": "a", "gpu_hours": "3000.000"},
            {"pool_id": "b", "gpu_hours": "4000.000"},
        ])
        with self.assertRaises(ValueError):
            allocate_sources(pools, Decimal("9000"))


class ReleaseServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = ReleaseService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("ops", "operator"), ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_pool("ops", {"pool_id": "serving-a", "name": "服务池A", "kind": "serving", "total_gpu_hours": "12000"})
        self.service.create_pool("ops", {"pool_id": "serving-b", "name": "服务池B", "kind": "serving", "total_gpu_hours": "20000"})
        self.service.create_pool("ops", {"pool_id": "fallback-1", "name": "回退池", "kind": "fallback", "total_gpu_hours": "10000"})
        self.service.register_profile("plan", profile_payload())

    def tearDown(self) -> None:
        self.connection.close()

    def reserved(self, pool_id: str) -> str:
        return self.service.get_pool(pool_id)["reserved_gpu_hours"]

    def create_and_confirm(self, plan_id: str = "plan-1", key: str = "plan-key-1", **overrides) -> dict[str, object]:
        self.service.create_plan("plan", plan_payload(plan_id, key, **overrides))
        return self.service.confirm_plan("plan", plan_id, 1)

    def start(self, plan_id: str) -> int:
        return self.service.start_plan("ops", plan_id, 2)["revision"]

    def submit_passing(self, plan_id: str, seq: int) -> dict[str, object]:
        return self.service.submit_metrics("ops", plan_id, {
            "phase_seq": seq, **PASSING, "idempotency_key": f"metrics-{plan_id}-{seq}",
        })

    # ------------------------------------------------------------------
    # 模型画像
    # ------------------------------------------------------------------

    def test_profile_is_immutable(self) -> None:
        with self.assertRaises(Conflict):
            self.service.register_profile("plan", profile_payload())
        changed = profile_payload("profile-v2", "v1.0")
        with self.assertRaises(Conflict):
            self.service.register_profile("plan", changed)

    def test_new_version_supersedes_maintained_pointer(self) -> None:
        self.service.register_profile("plan", profile_payload("profile-v2", "v2.0"))
        info = self.service.model_version_info("llm-x")
        self.assertEqual(info["maintained_profile_id"], "profile-v2")
        self.assertEqual(info["catalog_revision"], 2)
        self.assertFalse(self.service.get_profile("profile-v1")["maintained"])

    # ------------------------------------------------------------------
    # 计划创建
    # ------------------------------------------------------------------

    def test_plan_creation_is_idempotent_and_generates_phases(self) -> None:
        first = self.service.create_plan("plan", plan_payload())
        second = self.service.create_plan("plan", plan_payload())
        self.assertEqual(first, second)
        self.assertEqual([phase["phase"] for phase in first["phases"]],
                         ["warmup", "canary", "scale_up", "converge"])
        self.assertEqual(first["peak_gpu_hours"], "20000.000")
        self.assertEqual(first["required_fallback_gpu_hours"], "4000.000")
        changed = plan_payload(traffic_curve=[{"offset_minutes": 0, "target_gpu_hours": "100"},
                                              {"offset_minutes": 10, "target_gpu_hours": "200"}])
        with self.assertRaises(Conflict):
            self.service.create_plan("plan", changed)

    def test_plan_requires_maintained_profile(self) -> None:
        self.service.register_profile("plan", profile_payload("profile-v2", "v2.0"))
        with self.assertRaises(InvalidState):
            self.service.create_plan("plan", plan_payload("plan-old", "key-old"))
        with self.assertRaises(NotFound):
            self.service.create_plan("plan", plan_payload("plan-x", "key-x", profile_id="profile-none"))

    def test_plan_validates_curve_and_policy(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.create_plan("plan", plan_payload("p1", "k1", traffic_curve=[
                {"offset_minutes": 0, "target_gpu_hours": "100"},
            ]))
        with self.assertRaises(ValidationFailed):
            self.service.create_plan("plan", plan_payload("p2", "k2", rollback_policy={
                **POLICY, "min_success_rate_percent": "101",
            }))

    # ------------------------------------------------------------------
    # 确认与原子预留
    # ------------------------------------------------------------------

    def test_confirm_freezes_sources_and_reserves_capacity(self) -> None:
        confirmed = self.create_and_confirm()
        self.assertEqual(confirmed["serving_sources"], [
            {"pool_id": "serving-a", "gpu_hours": "12000.000"},
            {"pool_id": "serving-b", "gpu_hours": "8000.000"},
        ])
        self.assertEqual(confirmed["fallback_sources"], [{"pool_id": "fallback-1", "gpu_hours": "4000.000"}])
        self.assertEqual(self.reserved("serving-a"), "12000.000")
        self.assertEqual(self.reserved("serving-b"), "8000.000")
        self.assertEqual(self.reserved("fallback-1"), "4000.000")
        detail = self.service.plan_detail("audit", "plan-1")
        self.assertTrue(all(phase["sources_frozen"] for phase in detail["phases"]))
        self.assertEqual(detail["phases"][0]["capacity_sources"]["serving"],
                         [{"pool_id": "serving-a", "gpu_hours": "2000.000"}])
        self.assertEqual(detail["rollback_scope"]["held_total_gpu_hours"], "24000.000")

    def test_failed_confirm_is_atomic(self) -> None:
        self.service.adjust_pool("ops", "serving-b", {"total_gpu_hours": "6000", "expected_revision": 1})
        self.service.create_plan("plan", plan_payload())
        with self.assertRaises(Conflict):
            self.service.confirm_plan("plan", "plan-1", 1)
        self.assertEqual(self.reserved("serving-a"), "0.000")
        self.assertEqual(self.reserved("serving-b"), "0.000")
        self.assertEqual(self.reserved("fallback-1"), "0.000")
        detail = self.service.plan_detail("audit", "plan-1")
        self.assertEqual(detail["state"], "draft")
        self.assertEqual(detail["rollback_scope"]["held_reservations"], [])
        count = self.connection.execute("SELECT count(*) FROM capacity_reservations").fetchone()[0]
        self.assertEqual(count, 0)
        ledger = self.connection.execute("SELECT count(*) FROM release_ledger").fetchone()[0]
        self.assertEqual(ledger, 0)
        self.service.adjust_pool("ops", "serving-b", {"total_gpu_hours": "20000", "expected_revision": 2})
        confirmed = self.service.confirm_plan("plan", "plan-1", 1)
        self.assertEqual(confirmed["state"], "confirmed")

    def test_confirm_checks_revision_and_state(self) -> None:
        self.service.create_plan("plan", plan_payload())
        with self.assertRaises(InvalidState):
            self.service.confirm_plan("plan", "plan-1", 2)
        self.service.confirm_plan("plan", "plan-1", 1)
        with self.assertRaises(InvalidState):
            self.service.confirm_plan("plan", "plan-1", 1)

    def test_pool_adjustment_respects_reservations(self) -> None:
        self.create_and_confirm()
        with self.assertRaises(Conflict):
            self.service.adjust_pool("ops", "serving-a", {"total_gpu_hours": "5000", "expected_revision": 1})
        with self.assertRaises(InvalidState):
            self.service.adjust_pool("ops", "serving-a", {"total_gpu_hours": "15000", "expected_revision": 9})

    # ------------------------------------------------------------------
    # 阶段门槛与推进
    # ------------------------------------------------------------------

    def test_advance_requires_metrics(self) -> None:
        self.create_and_confirm()
        revision = self.start("plan-1")
        verdict = self.service.advance_plan("ops", "plan-1", revision)
        self.assertFalse(verdict["advanced"])
        self.assertEqual(verdict["blocking_reasons"][0]["code"], "metrics_missing")

    def test_failing_metrics_block_advance_until_recovered(self) -> None:
        self.create_and_confirm()
        revision = self.start("plan-1")
        failing = self.service.submit_metrics("ops", "plan-1", {
            "phase_seq": 1, "error_rate_percent": "5", "p95_latency_ms": "620",
            "success_rate_percent": "99.6", "idempotency_key": "metrics-bad",
        })
        self.assertFalse(failing["gate_passed"])
        verdict = self.service.advance_plan("ops", "plan-1", revision)
        self.assertFalse(verdict["advanced"])
        self.assertEqual(verdict["blocking_reasons"][0]["code"], "error_rate_above_threshold")
        detail = self.service.plan_detail("audit", "plan-1")
        self.assertEqual(detail["phases"][0]["state"], "active")
        self.assertEqual(detail["phases"][0]["blocking_reasons"][0]["code"], "error_rate_above_threshold")
        self.submit_passing("plan-1", 1)
        advanced = self.service.advance_plan("ops", "plan-1", revision)
        self.assertTrue(advanced["advanced"])
        self.assertEqual(advanced["current_phase_seq"], 2)

    def test_fallback_margin_must_survive_to_advance(self) -> None:
        self.create_and_confirm()
        revision = self.start("plan-1")
        self.submit_passing("plan-1", 1)
        self.service.adjust_pool("ops", "fallback-1", {"state": "suspended", "expected_revision": 1})
        verdict = self.service.advance_plan("ops", "plan-1", revision)
        self.assertFalse(verdict["advanced"])
        self.assertEqual([reason["code"] for reason in verdict["blocking_reasons"]],
                         ["fallback_margin_insufficient"])
        detail = self.service.plan_detail("audit", "plan-1")
        self.assertFalse(detail["fallback"]["margin_ok"])
        self.service.adjust_pool("ops", "fallback-1", {"state": "active", "expected_revision": 2})
        advanced = self.service.advance_plan("ops", "plan-1", revision)
        self.assertTrue(advanced["advanced"])

    def test_full_release_completes_and_releases_all_capacity(self) -> None:
        self.create_and_confirm()
        revision = self.start("plan-1")
        for seq in (1, 2, 3):
            self.submit_passing("plan-1", seq)
            revision = self.service.advance_plan("ops", "plan-1", revision)["revision"]
        # 进入收敛阶段后，峰值与稳态差值已释放。
        self.assertEqual(self.reserved("serving-b"), "0.000")
        self.assertEqual(self.reserved("serving-a"), "12000.000")
        self.submit_passing("plan-1", 4)
        final = self.service.advance_plan("ops", "plan-1", revision)
        self.assertEqual(final["plan_state"], "completed")
        self.assertEqual(self.reserved("serving-a"), "0.000")
        self.assertEqual(self.reserved("fallback-1"), "0.000")
        detail = self.service.plan_detail("audit", "plan-1")
        self.assertEqual([phase["state"] for phase in detail["phases"]], ["passed"] * 4)
        self.assertFalse(detail["rollback_scope"]["reversible"])

    def test_metrics_and_traffic_only_accept_active_phase(self) -> None:
        self.create_and_confirm()
        self.start("plan-1")
        with self.assertRaises(InvalidState):
            self.service.submit_metrics("ops", "plan-1", {
                "phase_seq": 2, **PASSING, "idempotency_key": "metrics-wrong-phase",
            })
        with self.assertRaises(InvalidState):
            self.service.record_traffic("ops", "plan-1", {
                "phase_seq": 3, "observed_gpu_hours": "100", "idempotency_key": "traffic-wrong-phase",
            })

    def test_metrics_replay_is_idempotent(self) -> None:
        self.create_and_confirm()
        self.start("plan-1")
        payload = {"phase_seq": 1, **PASSING, "idempotency_key": "metrics-replay"}
        first = self.service.submit_metrics("ops", "plan-1", payload)
        second = self.service.submit_metrics("ops", "plan-1", payload)
        self.assertEqual(first, second)
        with self.assertRaises(Conflict):
            self.service.submit_metrics("ops", "plan-1", {**payload, "error_rate_percent": "0.9"})
        count = self.connection.execute("SELECT count(*) FROM metric_summaries").fetchone()[0]
        self.assertEqual(count, 1)

    # ------------------------------------------------------------------
    # 回退
    # ------------------------------------------------------------------

    def test_rollback_preserves_evidence_and_never_double_deducts(self) -> None:
        self.create_and_confirm()
        self.start("plan-1")
        self.service.record_traffic("ops", "plan-1", {
            "phase_seq": 1, "observed_gpu_hours": "1950", "note": "预热流量", "idempotency_key": "traffic-1",
        })
        self.submit_passing("plan-1", 1)
        rollback = self.service.rollback_plan("risk", "plan-1", {
            "reason": "错误率突增", "idempotency_key": "rb-1",
        })
        self.assertEqual(rollback["state"], "rolled_back")
        self.assertEqual(rollback["released_gpu_hours"], "24000.000")
        self.assertEqual(rollback["evidence"]["metric_summaries"], 1)
        self.assertEqual(rollback["evidence"]["traffic_samples"], 1)
        self.assertEqual(self.reserved("serving-a"), "0.000")
        self.assertEqual(self.reserved("serving-b"), "0.000")
        self.assertEqual(self.reserved("fallback-1"), "0.000")
        replay = self.service.rollback_plan("risk", "plan-1", {
            "reason": "错误率突增", "idempotency_key": "rb-1",
        })
        self.assertEqual(replay, rollback)
        self.assertEqual(self.reserved("serving-a"), "0.000")
        with self.assertRaises(InvalidState):
            self.service.rollback_plan("risk", "plan-1", {"reason": "重复告警", "idempotency_key": "rb-2"})
        self.assertEqual(self.reserved("fallback-1"), "0.000")
        detail = self.service.plan_detail("audit", "plan-1")
        self.assertEqual(detail["state"], "rolled_back")
        self.assertEqual(detail["rollback_scope"]["evidence"]["traffic_samples"], 1)
        self.assertEqual(detail["rollback_scope"]["held_total_gpu_hours"], "0.000")

    def test_rollback_replay_with_different_payload_conflicts(self) -> None:
        self.create_and_confirm()
        self.start("plan-1")
        self.service.rollback_plan("risk", "plan-1", {"reason": "异常", "idempotency_key": "rb-1"})
        with self.assertRaises(Conflict):
            self.service.rollback_plan("risk", "plan-1", {"reason": "另一个原因", "idempotency_key": "rb-1"})

    # ------------------------------------------------------------------
    # 版本失效
    # ------------------------------------------------------------------

    def test_version_change_invalidates_non_terminal_plans(self) -> None:
        small_curve = [
            {"offset_minutes": 0, "target_gpu_hours": "1000"},
            {"offset_minutes": 60, "target_gpu_hours": "4000"},
        ]
        self.service.create_plan("plan", plan_payload("plan-draft", "key-draft"))
        self.create_and_confirm("plan-confirmed", "key-confirmed", traffic_curve=small_curve)
        self.create_and_confirm("plan-running", "key-running", traffic_curve=small_curve)
        self.start("plan-running")
        self.service.record_traffic("ops", "plan-running", {
            "phase_seq": 1, "observed_gpu_hours": "500", "idempotency_key": "traffic-running",
        })
        result = self.service.register_profile("plan", profile_payload("profile-v2", "v2.0"))
        self.assertEqual(sorted(result["invalidated_plans"]),
                         ["plan-confirmed", "plan-draft", "plan-running"])
        for plan_id in ("plan-draft", "plan-confirmed", "plan-running"):
            detail = self.service.plan_detail("audit", plan_id)
            self.assertEqual(detail["state"], "invalidated")
            self.assertEqual(detail["closed_reason"], "version_superseded")
        self.assertEqual(self.reserved("serving-a"), "0.000")
        self.assertEqual(self.reserved("serving-b"), "0.000")
        self.assertEqual(self.reserved("fallback-1"), "0.000")
        running = self.service.plan_detail("audit", "plan-running")
        self.assertEqual(running["rollback_scope"]["evidence"]["traffic_samples"], 1)
        with self.assertRaises(InvalidState):
            self.service.start_plan("ops", "plan-confirmed", 2)
        with self.assertRaises(InvalidState):
            self.service.rollback_plan("risk", "plan-running", {"reason": "告警", "idempotency_key": "rb-x"})

    def test_version_change_keeps_terminal_plans(self) -> None:
        self.create_and_confirm("plan-done", "key-done")
        revision = self.start("plan-done")
        for seq in (1, 2, 3, 4):
            self.submit_passing("plan-done", seq)
            revision = self.service.advance_plan("ops", "plan-done", revision)["revision"]
        result = self.service.register_profile("plan", profile_payload("profile-v2", "v2.0"))
        self.assertEqual(result["invalidated_plans"], [])
        self.assertEqual(self.service.plan_detail("audit", "plan-done")["state"], "completed")

    # ------------------------------------------------------------------
    # 权限与审计
    # ------------------------------------------------------------------

    def test_role_separation(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.register_profile("ops", profile_payload("profile-x", "v9"))
        self.service.create_plan("plan", plan_payload())
        with self.assertRaises(Forbidden):
            self.service.confirm_plan("ops", "plan-1", 1)
        with self.assertRaises(Forbidden):
            self.service.advance_plan("plan", "plan-1", 1)
        with self.assertRaises(Forbidden):
            self.service.audit_chain("ops")

    def test_audit_chain_detects_tampering(self) -> None:
        self.create_and_confirm()
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.connection.execute("UPDATE release_audit_events SET payload_json='{}' WHERE event_id=1")
        self.assertFalse(self.service.audit_chain("audit")["valid"])


class ReleaseApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.service = ReleaseService(self.connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
        self.app = JsonApplication(self.service)
        for user_id, role in (("plan", "planner"), ("ops", "operator"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_pool("ops", {"pool_id": "serving-a", "name": "服务池A", "kind": "serving", "total_gpu_hours": "30000"})
        self.service.create_pool("ops", {"pool_id": "fallback-1", "name": "回退池", "kind": "fallback", "total_gpu_hours": "10000"})
        self.service.register_profile("plan", profile_payload())

    def tearDown(self) -> None:
        self.connection.close()

    def post(self, path: str, payload: dict[str, object], actor: str = "plan"):
        import json
        return self.app.handle("POST", path, {"X-Actor-Id": actor}, json.dumps(payload).encode())

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)

    def test_plan_detail_explains_sources_blocking_and_rollback_scope(self) -> None:
        created = self.post("/plans", plan_payload())
        self.assertEqual(created.status, 201)
        confirmed = self.post("/plans/plan-1/confirm", {"expected_revision": 1})
        self.assertEqual(confirmed.status, 200)
        started = self.post("/plans/plan-1/start", {"expected_revision": 2}, actor="ops")
        self.assertEqual(started.status, 200)
        blocked = self.post("/plans/plan-1/advance", {"expected_revision": 3}, actor="ops")
        self.assertEqual(blocked.status, 200)
        self.assertFalse(blocked.body["advanced"])
        detail = self.app.handle("GET", "/plans/plan-1", {"X-Actor-Id": "audit"})
        self.assertEqual(detail.status, 200)
        first_phase = detail.body["phases"][0]
        self.assertEqual(first_phase["capacity_sources"]["serving"],
                         [{"pool_id": "serving-a", "gpu_hours": "2000.000"}])
        self.assertEqual(first_phase["blocking_reasons"][0]["code"], "metrics_missing")
        self.assertTrue(detail.body["rollback_scope"]["reversible"])
        self.assertTrue(detail.body["fallback"]["margin_ok"])
        self.assertEqual(detail.body["rollback_scope"]["held_total_gpu_hours"], "24000.000")

    def test_error_shape_and_unknown_route(self) -> None:
        missing = self.app.handle("GET", "/plans/nope", {"X-Actor-Id": "audit"})
        self.assertEqual(missing.status, 404)
        self.assertEqual(missing.body["error"]["code"], "not_found")
        unknown = self.app.handle("GET", "/nope", {"X-Actor-Id": "audit"})
        self.assertEqual(unknown.status, 404)
        self.assertEqual(unknown.body["error"]["code"], "route_not_found")
        no_actor = self.app.handle("GET", "/plans/plan-1")
        self.assertEqual(no_actor.status, 422)


class AcceptanceFlowTests(unittest.TestCase):
    def test_offline_acceptance(self) -> None:
        from pathlib import Path

        result = acceptance_run(Path(__file__).resolve().parents[1])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["plan_a"]["state"], "completed")
        self.assertEqual(result["plan_a"]["phases"], ["passed"] * 4)
        self.assertEqual(result["plan_b"]["state"], "rolled_back")
        self.assertTrue(result["plan_b"]["replay_identical"])
        self.assertEqual(result["plan_b"]["evidence"]["traffic_samples"], 1)
        self.assertEqual(result["plan_c"]["state"], "invalidated")
        self.assertEqual(result["plan_c"]["closed_reason"], "version_superseded")
        self.assertEqual(set(result["pools_reserved"].values()), {"0.000"})
        self.assertTrue(result["audit"]["valid"])


if __name__ == "__main__":
    unittest.main()

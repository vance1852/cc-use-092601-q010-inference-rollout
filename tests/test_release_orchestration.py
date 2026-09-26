from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from release_orchestration.acceptance import run as acceptance_run
from release_orchestration.api import JsonApplication
from release_orchestration.clock import FrozenClock
from release_orchestration.errors import Conflict, Forbidden, InvalidState, ValidationFailed
from release_orchestration.models import MetricSummary, ModelProfile, RollbackPolicy, TrafficCurve
from release_orchestration.planning import (
    coverable_traffic_percent,
    evaluate_gates,
    generate_phases,
    gpu_for_replicas,
    replicas_for_percent,
    rollback_margin_gpu,
    warmup_min_hold_minutes,
)
from release_orchestration.service import ReleaseService


CURVE = [
    {"percent": "5", "hold_minutes": 30},
    {"percent": "25", "hold_minutes": 30},
    {"percent": "60", "hold_minutes": 20},
    {"percent": "100", "hold_minutes": 0},
]
POLICY = {
    "max_error_rate_percent": "1.5",
    "max_p99_latency_ms": 800,
    "min_vram_headroom_percent": "10",
    "min_rollback_margin_gpu": 2,
}
GOOD_METRICS = {
    "error_rate_percent": "0.4",
    "p99_latency_ms": 420,
    "cold_start_p99_seconds": 95,
    "vram_headroom_percent": "35",
    "observed_qps": "1000",
    "sampled_at": "2026-09-24T09:00:00Z",
}


def profile(**overrides):
    raw = {
        "profile_id": "profile-a",
        "model_name": "qwen3-32b",
        "model_version": "2026.09-rc4",
        "vram_gb_per_replica": "40",
        "cold_start_seconds": 90,
        "tenant_priority": 10,
        "replica_qps": "250",
    }
    raw.update(overrides)
    return raw


def plan_payload(plan_id="rel-1", peak_qps="20000", **overrides):
    raw = {
        "plan_id": plan_id,
        "profile_id": "profile-a",
        "pool_id": "pool-1",
        "peak_qps": peak_qps,
        "traffic_curve": [dict(point) for point in CURVE],
        "rollback_policy": dict(POLICY),
        "idempotency_key": f"{plan_id}-key",
    }
    raw.update(overrides)
    return raw


class PlanningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.profile = ModelProfile.from_dict(profile())
        self.curve = TrafficCurve.from_list([dict(point) for point in CURVE])
        self.policy = RollbackPolicy.from_dict(dict(POLICY))

    def test_replica_and_gpu_math_uses_vram(self) -> None:
        self.assertEqual(replicas_for_percent(Decimal("20000"), Decimal("5"), Decimal("250")), 4)
        self.assertEqual(replicas_for_percent(Decimal("20000"), Decimal("100"), Decimal("250")), 80)
        self.assertEqual(gpu_for_replicas(80, Decimal("40"), Decimal("80")), 40)
        self.assertEqual(gpu_for_replicas(3, Decimal("40"), Decimal("80")), 2)

    def test_warmup_hold_covers_cold_start_waves(self) -> None:
        self.assertEqual(warmup_min_hold_minutes(90, 4), 2)
        self.assertEqual(warmup_min_hold_minutes(90, 25), 5)

    def test_generate_phases_in_order_with_margin_requirements(self) -> None:
        phases = generate_phases(
            profile=self.profile, peak_qps=Decimal("20000"), curve=self.curve,
            policy=self.policy, vram_gb_per_gpu=Decimal("80"),
        )
        self.assertEqual([phase["kind"] for phase in phases], ["warmup", "canary", "scale_up", "convergence"])
        self.assertEqual([phase["required_gpu"] for phase in phases], [2, 10, 24, 40])
        self.assertEqual(phases[0]["min_hold_minutes"], 2)
        self.assertEqual(phases[0]["hold_minutes"], 30)
        self.assertEqual(phases[0]["rollback_margin_required_gpu"], 2)
        self.assertEqual(phases[1]["rollback_margin_required_gpu"], 10)
        self.assertEqual(phases[3]["rollback_margin_required_gpu"], 2)
        self.assertEqual(phases[0]["gates"]["max_cold_start_p99_seconds"], 135)

    def test_curve_must_ramp_to_full_convergence(self) -> None:
        bad = [dict(point) for point in CURVE]
        bad[3]["percent"] = "90"
        with self.assertRaises(ValidationFailed):
            TrafficCurve.from_list(bad)
        with self.assertRaises(ValidationFailed):
            TrafficCurve.from_list([{"percent": "50", "hold_minutes": 1}])

    def test_evaluate_gates_reports_all_blockers(self) -> None:
        phases = generate_phases(
            profile=self.profile, peak_qps=Decimal("20000"), curve=self.curve,
            policy=self.policy, vram_gb_per_gpu=Decimal("80"),
        )
        metrics = MetricSummary.from_dict(dict(GOOD_METRICS, error_rate_percent="2.5", vram_headroom_percent="5"))
        blockers = evaluate_gates(phases[0], metrics, margin_gpu=0)
        self.assertEqual([b["gate"] for b in blockers], ["error_rate", "vram_headroom", "rollback_margin"])
        self.assertEqual(evaluate_gates(phases[0], MetricSummary.from_dict(dict(GOOD_METRICS)), 46), [])

    def test_rollback_margin_counts_reservation_headroom(self) -> None:
        margin = rollback_margin_gpu(pool_gpu_count=48, pool_reserved_gpu=40, plan_reserved_gpu=40, phase_required_gpu=10)
        self.assertEqual(margin, 38)

    def test_coverable_traffic_percent(self) -> None:
        phases = generate_phases(
            profile=self.profile, peak_qps=Decimal("20000"), curve=self.curve,
            policy=self.policy, vram_gb_per_gpu=Decimal("80"),
        )
        self.assertEqual(coverable_traffic_percent(phases, 9), Decimal("5"))
        self.assertEqual(coverable_traffic_percent(phases, 40), Decimal("100"))
        self.assertEqual(coverable_traffic_percent(phases, 0), Decimal("0"))


class ReleaseServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = ReleaseService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("ops", "operator"), ("cap", "capacity"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_pool("cap", {
            "pool_id": "pool-1", "gpu_count": 48, "vram_gb_per_gpu": "80",
            "protected_gpu": 4, "preemption_priority": 50,
        })
        self.service.create_profile("plan", profile())

    def tearDown(self) -> None:
        self.connection.close()

    def create_plan(self, plan_id="rel-1", **overrides):
        return self.service.create_plan("plan", plan_payload(plan_id, **overrides))

    def confirm_and_start(self, plan_id="rel-1"):
        self.service.confirm_plan("plan", plan_id, 1)
        self.service.start_plan("ops", plan_id, 2)

    def test_profile_is_immutable_and_content_addressed(self) -> None:
        with self.assertRaises(Conflict):
            self.service.create_profile("plan", profile())
        with self.assertRaises(Conflict):
            self.service.create_profile("plan", profile(model_version="2026.09-rc5"))
        stored = self.service.get_profile("profile-a")
        self.assertEqual(stored["model_version"], "2026.09-rc4")
        self.assertTrue(stored["immutable"])
        self.assertEqual(len(stored["content_sha256"]), 64)

    def test_plan_creation_is_idempotent_and_rejects_payload_change(self) -> None:
        first = self.create_plan()
        second = self.create_plan()
        self.assertEqual(first, second)
        with self.assertRaises(Conflict):
            self.service.create_plan("plan", plan_payload("rel-1", peak_qps="21000"))
        self.assertEqual([phase["kind"] for phase in first["phases"]], ["warmup", "canary", "scale_up", "convergence"])
        self.assertEqual(first["pool_revision"], 1)

    def test_plan_exceeding_pool_total_is_rejected(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.create_plan(peak_qps="30000")

    def test_confirm_reserves_atomically_and_failure_leaves_pool_untouched(self) -> None:
        self.create_plan()
        confirmed = self.service.confirm_plan("plan", "rel-1", 1)
        self.assertEqual(confirmed["reserved_gpu"], 40)
        self.assertEqual(self.service.get_pool("pool-1")["reserved_gpu"], 40)
        self.create_plan("rel-2", peak_qps="7000")
        with self.assertRaises(Conflict):
            self.service.confirm_plan("plan", "rel-2", 1)
        self.assertEqual(self.service.get_pool("pool-1")["reserved_gpu"], 40)
        self.assertEqual(self.service.plan_detail("audit", "rel-2")["state"], "draft")

    def test_protected_margin_requires_high_tenant_priority(self) -> None:
        self.service.create_pool("cap", {
            "pool_id": "pool-2", "gpu_count": 10, "vram_gb_per_gpu": "80",
            "protected_gpu": 2, "preemption_priority": 5,
        })
        self.service.create_profile("plan", profile(profile_id="profile-low", tenant_priority=100, replica_qps="1000"))
        self.service.create_profile("plan", profile(profile_id="profile-high", tenant_priority=1, replica_qps="1000"))
        self.service.create_plan("plan", plan_payload("rel-base", profile_id="profile-high", pool_id="pool-2", peak_qps="7000"))
        self.service.confirm_plan("plan", "rel-base", 1)
        self.service.create_plan("plan", plan_payload("rel-low", profile_id="profile-low", pool_id="pool-2", peak_qps="11000"))
        with self.assertRaises(Conflict):
            self.service.confirm_plan("plan", "rel-low", 1)
        self.service.create_plan("plan", plan_payload("rel-high", profile_id="profile-high", pool_id="pool-2", peak_qps="11000"))
        confirmed = self.service.confirm_plan("plan", "rel-high", 1)
        self.assertEqual(confirmed["protected_margin_gpu"], 2)

    def test_maintenance_invalidates_drafts_and_blocks_confirm(self) -> None:
        self.create_plan()
        result = self.service.maintain_pool("cap", "pool-1", {
            "gpu_count": 44, "vram_gb_per_gpu": "80", "protected_gpu": 4,
            "preemption_priority": 50, "state": "maintenance", "expected_revision": 1,
            "reason": "固件维护",
        })
        self.assertEqual(result["invalidated_plans"], ["rel-1"])
        with self.assertRaises(InvalidState):
            self.service.confirm_plan("plan", "rel-1", 1)
        detail = self.service.plan_detail("audit", "rel-1")
        self.assertEqual(detail["state"], "invalidated")
        self.assertFalse(detail["pool"]["revision_matches"])
        self.assertEqual(detail["blocking_reasons"][0]["gate"], "plan")

    def test_maintenance_cannot_drop_below_reserved(self) -> None:
        self.create_plan()
        self.service.confirm_plan("plan", "rel-1", 1)
        with self.assertRaises(Conflict):
            self.service.maintain_pool("cap", "pool-1", {
                "gpu_count": 30, "vram_gb_per_gpu": "80", "protected_gpu": 4,
                "preemption_priority": 50, "state": "active", "expected_revision": 1,
            })
        with self.assertRaises(InvalidState):
            self.service.maintain_pool("cap", "pool-1", {
                "gpu_count": 48, "vram_gb_per_gpu": "80", "protected_gpu": 4,
                "preemption_priority": 50, "state": "active", "expected_revision": 7,
            })

    def test_advance_blocks_on_metrics_and_records_evidence(self) -> None:
        self.create_plan()
        self.confirm_and_start()
        outcome = self.service.advance_plan("ops", "rel-1", 3, dict(GOOD_METRICS, error_rate_percent="3.2"))
        self.assertFalse(outcome["advanced"])
        self.assertEqual(outcome["blocking_reasons"][0]["gate"], "error_rate")
        self.assertEqual(outcome["revision"], 3)
        detail = self.service.plan_detail("audit", "rel-1")
        self.assertEqual(detail["metric_history"][0]["verdict"], "blocked")
        passed = self.service.advance_plan("ops", "rel-1", 3, dict(GOOD_METRICS))
        self.assertTrue(passed["advanced"])
        self.assertEqual(passed["current_phase"], 2)

    def test_advance_blocks_when_rollback_margin_vanishes(self) -> None:
        self.create_plan()
        self.confirm_and_start()
        self.service.advance_plan("ops", "rel-1", 3, dict(GOOD_METRICS))
        self.service.advance_plan("ops", "rel-1", 4, dict(GOOD_METRICS))
        self.service.maintain_pool("cap", "pool-1", {
            "gpu_count": 44, "vram_gb_per_gpu": "80", "protected_gpu": 4,
            "preemption_priority": 50, "state": "active", "expected_revision": 1,
        })
        outcome = self.service.advance_plan("ops", "rel-1", 5, dict(GOOD_METRICS))
        self.assertFalse(outcome["advanced"])
        gates = [blocker["gate"] for blocker in outcome["blocking_reasons"]]
        self.assertEqual(gates, ["rollback_margin"])
        detail = self.service.plan_detail("audit", "rel-1")
        self.assertEqual(detail["blocking_reasons"][0]["gate"], "rollback_margin")
        self.assertTrue(detail["rollback_scope"]["available"])
        self.assertEqual(detail["rollback_scope"]["margin_gpu"], 20)
        self.assertEqual(detail["rollback_scope"]["required_margin_gpu"], 24)
        self.assertEqual(detail["rollback_scope"]["coverable_traffic_percent"], "25")

    def test_full_lifecycle_completes(self) -> None:
        self.create_plan()
        self.confirm_and_start()
        revision = 3
        for expected_phase in (2, 3, 4):
            outcome = self.service.advance_plan("ops", "rel-1", revision, dict(GOOD_METRICS))
            self.assertEqual(outcome["current_phase"], expected_phase)
            revision = outcome["revision"]
        outcome = self.service.advance_plan("ops", "rel-1", revision, dict(GOOD_METRICS))
        self.assertEqual(outcome["state"], "completed")
        detail = self.service.plan_detail("audit", "rel-1")
        self.assertTrue(all(phase["status"] == "passed" for phase in detail["phases"]))
        self.assertEqual(detail["rollback_scope"]["reason"], "plan_completed")
        with self.assertRaises(InvalidState):
            self.service.advance_plan("ops", "rel-1", outcome["revision"], dict(GOOD_METRICS))

    def test_rollback_preserves_evidence_and_is_idempotent(self) -> None:
        self.create_plan()
        self.confirm_and_start()
        self.service.advance_plan("ops", "rel-1", 3, dict(GOOD_METRICS))
        payload = {
            "event_id": "evt-1",
            "anomaly": "错误率突增",
            "traffic_evidence": {"window": "t0/t1", "observed_qps": "5000", "error_rate_percent": "4.4"},
        }
        first = self.service.rollback_plan("ops", "rel-1", payload)
        self.assertEqual(first["released_gpu"], 40)
        self.assertEqual(first["rolled_back_at_phase"], 2)
        self.assertFalse(first["replayed"])
        self.assertEqual(self.service.get_pool("pool-1")["reserved_gpu"], 0)
        replay = self.service.rollback_plan("ops", "rel-1", payload)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["released_gpu"], 40)
        self.assertEqual(self.service.get_pool("pool-1")["reserved_gpu"], 0)
        with self.assertRaises(InvalidState):
            self.service.rollback_plan("ops", "rel-1", dict(payload, event_id="evt-2"))
        self.assertEqual(self.service.get_pool("pool-1")["reserved_gpu"], 0)
        detail = self.service.plan_detail("audit", "rel-1")
        self.assertEqual(detail["state"], "rolled_back")
        self.assertEqual(detail["capacity_released_gpu"], 40)
        self.assertEqual(detail["rollback_events"][0]["event_id"], "evt-1")
        self.assertEqual(len(detail["metric_history"]), 1)
        self.assertEqual(detail["phases"][0]["status"], "passed")
        self.assertEqual(detail["phases"][1]["status"], "rolled_back")
        self.assertEqual(detail["phases"][1]["capacity_source"]["kind"], "released")
        self.assertEqual(detail["rollback_scope"]["reason"], "already_rolled_back")
        stored = self.connection.execute("SELECT evidence_json FROM rollback_events WHERE event_id='evt-1'").fetchone()
        evidence = json.loads(stored["evidence_json"])
        self.assertEqual(evidence["traffic_evidence"]["observed_qps"], "5000")
        self.assertEqual(len(evidence["phase_metrics"]), 1)

    def test_rollback_rejected_for_draft_and_completed(self) -> None:
        self.create_plan()
        with self.assertRaises(InvalidState):
            self.service.rollback_plan("ops", "rel-1", {"event_id": "evt-9", "anomaly": "x", "traffic_evidence": {"a": 1}})

    def test_detail_exposes_capacity_source_and_rollback_scope(self) -> None:
        self.create_plan()
        self.confirm_and_start()
        detail = self.service.plan_detail("audit", "rel-1")
        source = detail["phases"][0]["capacity_source"]
        self.assertEqual(source["kind"], "pool_reservation")
        self.assertEqual(source["reserved_gpu"], 40)
        self.assertEqual(source["active_gpu"], 2)
        self.assertEqual(source["headroom_gpu"], 38)
        scope = detail["rollback_scope"]
        self.assertTrue(scope["available"])
        self.assertEqual(scope["margin_gpu"], 46)
        self.assertEqual(scope["coverable_traffic_percent"], "100")
        self.assertEqual(scope["revertible_phases"], [])

    def test_permissions_are_enforced(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.create_profile("ops", profile(profile_id="profile-x"))
        self.create_plan()
        with self.assertRaises(Forbidden):
            self.service.confirm_plan("ops", "rel-1", 1)
        with self.assertRaises(Forbidden):
            self.service.audit_chain("plan")

    def test_audit_chain_detects_tampering(self) -> None:
        self.create_plan()
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.connection.execute("UPDATE release_audit_events SET payload_json='{}' WHERE event_id=1")
        self.assertFalse(self.service.audit_chain("audit")["valid"])


class ReleaseApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.service = ReleaseService(self.connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
        self.app = JsonApplication(self.service)
        self.service.create_user("plan", "plan", "planner")
        self.service.create_user("cap", "cap", "capacity")

    def tearDown(self) -> None:
        self.connection.close()

    def test_health_and_error_shape(self) -> None:
        self.assertEqual(self.app.handle("GET", "/health").status, 200)
        response = self.app.handle("POST", "/release-plans", {"X-Actor-Id": "plan"}, b"not-json")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")
        response = self.app.handle("POST", "/release-plans", {}, b"{}")
        self.assertEqual(response.status, 422)
        self.assertIn("X-Actor-Id", response.body["error"]["message"])

    def test_plan_lifecycle_over_http(self) -> None:
        headers = {"X-Actor-Id": "cap"}
        response = self.app.handle("POST", "/capacity-pools", headers, json.dumps({
            "pool_id": "pool-1", "gpu_count": 48, "vram_gb_per_gpu": "80",
            "protected_gpu": 4, "preemption_priority": 50,
        }).encode())
        self.assertEqual(response.status, 201)
        response = self.app.handle("POST", "/model-profiles", {"X-Actor-Id": "plan"}, json.dumps(profile()).encode())
        self.assertEqual(response.status, 201)
        response = self.app.handle("POST", "/release-plans", {"X-Actor-Id": "plan"}, json.dumps(plan_payload()).encode())
        self.assertEqual(response.status, 201)
        self.assertEqual(response.body["state"], "draft")
        response = self.app.handle("POST", "/release-plans/rel-1/confirm", {"X-Actor-Id": "plan"}, json.dumps({"expected_revision": 1}).encode())
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["reserved_gpu"], 40)
        response = self.app.handle("GET", "/release-plans/rel-1", {"X-Actor-Id": "plan"})
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["phases"][0]["capacity_source"]["kind"], "pool_reservation")
        self.assertIn("rollback_scope", response.body)
        response = self.app.handle("GET", "/release-plans/rel-1", {"X-Actor-Id": "cap"})
        self.assertEqual(response.status, 200)
        response = self.app.handle("POST", "/release-plans/rel-1/rollback", {"X-Actor-Id": "plan"}, json.dumps({"event_id": "e1", "anomaly": "x", "traffic_evidence": {"qps": 1}}).encode())
        self.assertEqual(response.status, 403)


class ReleaseAcceptanceTests(unittest.TestCase):
    def test_offline_acceptance(self) -> None:
        from pathlib import Path

        result = acceptance_run(Path(__file__).resolve().parents[1])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["plan_a"]["state"], "completed")
        self.assertEqual(result["plan_b"]["state"], "rolled_back")
        self.assertTrue(result["plan_b"]["replayed"])
        self.assertTrue(result["plan_b"]["evidence_preserved"])
        self.assertEqual(result["plan_b"]["released_gpu"], result["plan_b"]["replay_released_gpu"])
        self.assertTrue(result["plan_c"]["confirm_blocked"])
        self.assertTrue(result["audit"]["valid"])


if __name__ == "__main__":
    unittest.main()

"""容量预演与分阶段发布编排的事务用例。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from typing import Any, Mapping

from .clock import SystemClock, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import CapacityPool, MetricSummary, ModelProfile, RollbackPolicy, TrafficCurve, decimal_value, identifier
from .planning import (
    canonical_json,
    coverable_traffic_percent,
    decimal_text,
    digest,
    evaluate_gates,
    generate_phases,
    peak_gpu,
    rollback_margin_gpu,
)
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "planner": {"profile.write", "plan.write", "plan.confirm", "report.read"},
    "operator": {"plan.start", "plan.advance", "rollback.write", "report.read"},
    "capacity": {"pool.write", "maintenance.write", "report.read"},
    "auditor": {"report.read", "audit.read"},
}

ACTIVE_STATES = ("confirmed", "in_progress")


class ReleaseService:
    """在单个 SQLite 连接上提供容量预演与发布编排操作。"""

    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM release_users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound("用户不存在")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM release_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": self._now(),
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO release_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                entity_type,
                entity_id,
                event_type,
                actor_id,
                canonical_json(payload),
                previous_hash,
                event_hash,
                body["created_at"],
            ),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed("未知角色")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO release_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("用户已经存在") from exc
        return {"user_id": user_id.strip(), "role": role}

    # ------------------------------------------------------------------
    # 不可变模型画像
    # ------------------------------------------------------------------
    def create_profile(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "profile.write")
        profile = ModelProfile.from_dict(raw)
        definition = canonical_json(dict(raw))
        content_sha256 = hashlib.sha256(definition.encode("utf-8")).hexdigest()
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO model_profiles(profile_id,model_name,model_version,vram_gb_per_replica,"
                    "cold_start_seconds,tenant_priority,replica_qps,definition_json,content_sha256,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        profile.profile_id,
                        profile.model_name,
                        profile.model_version,
                        decimal_text(profile.vram_gb_per_replica),
                        profile.cold_start_seconds,
                        profile.tenant_priority,
                        decimal_text(profile.replica_qps),
                        definition,
                        content_sha256,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit(
                    "model_profile", profile.profile_id, "profile.created", actor_id,
                    {"sha256": content_sha256, "model_version": profile.model_version},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("模型画像编号或内容已经存在") from exc
        return {"profile_id": profile.profile_id, "sha256": content_sha256, "immutable": True}

    def get_profile(self, profile_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM model_profiles WHERE profile_id=?", (profile_id,)
        ).fetchone()
        if row is None:
            raise NotFound("模型画像不存在")
        result = dict(row)
        result["immutable"] = True
        return result

    def _profile_row(self, profile_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM model_profiles WHERE profile_id=?", (profile_id,)
        ).fetchone()
        if row is None:
            raise NotFound("模型画像不存在")
        return row

    # ------------------------------------------------------------------
    # 冻结资源池与维护版本
    # ------------------------------------------------------------------
    def create_pool(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "pool.write")
        pool = CapacityPool.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO capacity_pools(pool_id,gpu_count,vram_gb_per_gpu,protected_gpu,"
                    "preemption_priority,created_at) VALUES(?,?,?,?,?,?)",
                    (
                        pool.pool_id,
                        pool.gpu_count,
                        decimal_text(pool.vram_gb_per_gpu),
                        pool.protected_gpu,
                        pool.preemption_priority,
                        self._now(),
                    ),
                )
                self._audit("capacity_pool", pool.pool_id, "pool.created", actor_id, dict(raw))
        except sqlite3.IntegrityError as exc:
            raise Conflict("资源池编号已经存在") from exc
        return self.get_pool(pool.pool_id)

    def get_pool(self, pool_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM capacity_pools WHERE pool_id=?", (pool_id,)
        ).fetchone()
        if row is None:
            raise NotFound("资源池不存在")
        result = dict(row)
        result["free_gpu"] = row["gpu_count"] - row["reserved_gpu"]
        return result

    def _pool_row(self, pool_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM capacity_pools WHERE pool_id=?", (pool_id,)
        ).fetchone()
        if row is None:
            raise NotFound("资源池不存在")
        return row

    def maintain_pool(self, actor_id: str, pool_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        """登记维护版本：容量或状态变化使该池的草稿计划失效。"""
        self._require(actor_id, "maintenance.write")
        expected_revision = raw.get("expected_revision")
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int):
            raise ValidationFailed("expected_revision 必须是整数")
        updated = CapacityPool.from_dict({**raw, "pool_id": pool_id})
        state = raw.get("state", "active")
        if state not in {"active", "maintenance", "retired"}:
            raise ValidationFailed("state 必须是 active、maintenance 或 retired")
        reason = raw.get("reason", "")
        with transaction(self.connection, immediate=True):
            pool = self._pool_row(pool_id)
            if pool["revision"] != expected_revision:
                raise InvalidState("资源池维护版本与预期不符")
            if updated.gpu_count < pool["reserved_gpu"]:
                raise Conflict("维护后容量不能低于已预留容量")
            cursor = self.connection.execute(
                "UPDATE capacity_pools SET gpu_count=?,vram_gb_per_gpu=?,protected_gpu=?,"
                "preemption_priority=?,state=?,revision=revision+1 "
                "WHERE pool_id=? AND revision=?",
                (
                    updated.gpu_count,
                    decimal_text(updated.vram_gb_per_gpu),
                    updated.protected_gpu,
                    updated.preemption_priority,
                    state,
                    pool_id,
                    expected_revision,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidState("资源池维护版本与预期不符")
            invalidated = self.connection.execute(
                "SELECT plan_id FROM release_plans WHERE pool_id=? AND state='draft' ORDER BY plan_id",
                (pool_id,),
            ).fetchall()
            self.connection.execute(
                "UPDATE release_plans SET state='invalidated',revision=revision+1,updated_at=? "
                "WHERE pool_id=? AND state='draft'",
                (self._now(), pool_id),
            )
            self._audit(
                "capacity_pool", pool_id, "pool.maintained", actor_id,
                {"revision": expected_revision + 1, "state": state, "reason": reason},
            )
            for plan in invalidated:
                self._audit(
                    "release_plan", plan["plan_id"], "plan.invalidated", actor_id,
                    {"pool_revision": expected_revision + 1},
                )
        return {**self.get_pool(pool_id), "invalidated_plans": [row["plan_id"] for row in invalidated]}

    # ------------------------------------------------------------------
    # 发布计划：预演生成、原子确认、阶段推进与回退
    # ------------------------------------------------------------------
    def create_plan(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "plan.write")
        plan_id = identifier(raw.get("plan_id"), "plan_id")
        idempotency_key = identifier(raw.get("idempotency_key"), "idempotency_key")
        request_digest = digest(dict(raw))
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM release_idempotency "
            "WHERE scope='release-plan' AND idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同计划内容")
            return json.loads(stored["response_json"])
        profile_row = self._profile_row(identifier(raw.get("profile_id"), "profile_id"))
        pool = self._pool_row(identifier(raw.get("pool_id"), "pool_id"))
        if pool["state"] != "active":
            raise InvalidState("资源池当前不可用于新计划")
        profile = ModelProfile.from_dict(json.loads(profile_row["definition_json"]))
        peak_qps = decimal_value(raw.get("peak_qps"), "peak_qps", minimum=Decimal("0.01"))
        curve = TrafficCurve.from_list(raw.get("traffic_curve"))
        policy = RollbackPolicy.from_dict(raw.get("rollback_policy"))
        phases = generate_phases(
            profile=profile,
            peak_qps=peak_qps,
            curve=curve,
            policy=policy,
            vram_gb_per_gpu=Decimal(pool["vram_gb_per_gpu"]),
        )
        needed = peak_gpu(phases)
        if needed > pool["gpu_count"]:
            raise ValidationFailed("目标流量所需容量超过资源池总容量")
        response = {
            "plan_id": plan_id,
            "state": "draft",
            "revision": 1,
            "profile_id": profile.profile_id,
            "profile_sha256": profile_row["content_sha256"],
            "pool_id": pool["pool_id"],
            "pool_revision": pool["revision"],
            "tenant_priority": profile.tenant_priority,
            "peak_gpu": needed,
            "phases": phases,
        }
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO release_plans(plan_id,profile_id,profile_sha256,pool_id,pool_revision,"
                    "tenant_priority,peak_qps,curve_json,policy_json,phases_json,idempotency_key,"
                    "created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        plan_id,
                        profile.profile_id,
                        profile_row["content_sha256"],
                        pool["pool_id"],
                        pool["revision"],
                        profile.tenant_priority,
                        decimal_text(peak_qps),
                        canonical_json(raw.get("traffic_curve")),
                        canonical_json(raw.get("rollback_policy")),
                        canonical_json(phases),
                        idempotency_key,
                        actor_id,
                        self._now(),
                        self._now(),
                    ),
                )
                self.connection.execute(
                    "INSERT INTO release_idempotency(scope,idempotency_key,request_sha256,response_json,"
                    "created_at) VALUES('release-plan',?,?,?,?)",
                    (idempotency_key, request_digest, canonical_json(response), self._now()),
                )
                self._audit(
                    "release_plan", plan_id, "plan.created", actor_id,
                    {"pool_revision": pool["revision"], "peak_gpu": needed},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("计划编号或幂等键冲突") from exc
        return response

    def _plan_row(self, plan_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM release_plans WHERE plan_id=?", (plan_id,)
        ).fetchone()
        if row is None:
            raise NotFound("发布计划不存在")
        return row

    @staticmethod
    def _phases(plan: sqlite3.Row) -> list[dict[str, Any]]:
        return json.loads(plan["phases_json"])

    def confirm_plan(self, actor_id: str, plan_id: str, expected_revision: int) -> dict[str, Any]:
        """原子完成计划确认与资源预留；资源池维护版本变化后确认失败。"""
        self._require(actor_id, "plan.confirm")
        with transaction(self.connection, immediate=True):
            plan = self._plan_row(plan_id)
            if plan["state"] == "invalidated":
                raise InvalidState("计划已失效：资源池维护版本已变化")
            if plan["state"] != "draft":
                raise InvalidState("只有草稿计划可以确认")
            if plan["revision"] != expected_revision:
                raise InvalidState("计划版本与预期不符")
            pool = self._pool_row(plan["pool_id"])
            if pool["state"] != "active":
                raise InvalidState("资源池当前不可预留")
            if pool["revision"] != plan["pool_revision"]:
                raise InvalidState("资源池维护版本已变化，计划需要重新预演")
            phases = self._phases(plan)
            needed = peak_gpu(phases)
            free = pool["gpu_count"] - pool["reserved_gpu"]
            preempts = plan["tenant_priority"] <= pool["preemption_priority"]
            usable = free if preempts else free - pool["protected_gpu"]
            if needed > usable:
                raise Conflict("资源池可用容量不足，无法完成预留")
            protected_used = min(
                pool["protected_gpu"], max(0, needed - (free - pool["protected_gpu"]))
            ) if preempts else 0
            cursor = self.connection.execute(
                "UPDATE capacity_pools SET reserved_gpu=reserved_gpu+? "
                "WHERE pool_id=? AND revision=? AND reserved_gpu+?<=gpu_count",
                (needed, pool["pool_id"], pool["revision"], needed),
            )
            if cursor.rowcount != 1:
                raise Conflict("资源池容量竞争，预留未完成")
            cursor = self.connection.execute(
                "UPDATE release_plans SET state='confirmed',reserved_gpu=?,protected_margin_gpu=?,"
                "revision=revision+1,updated_at=? WHERE plan_id=? AND revision=? AND state='draft'",
                (needed, protected_used, self._now(), plan_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("计划版本与预期不符")
            self._audit(
                "release_plan", plan_id, "plan.confirmed", actor_id,
                {"reserved_gpu": needed, "protected_margin_gpu": protected_used},
            )
        return {
            "plan_id": plan_id,
            "state": "confirmed",
            "revision": expected_revision + 1,
            "reserved_gpu": needed,
            "protected_margin_gpu": protected_used,
        }

    def start_plan(self, actor_id: str, plan_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "plan.start")
        with transaction(self.connection, immediate=True):
            plan = self._plan_row(plan_id)
            if plan["state"] != "confirmed":
                raise InvalidState("只有已确认计划可以启动")
            if plan["revision"] != expected_revision:
                raise InvalidState("计划版本与预期不符")
            pool = self._pool_row(plan["pool_id"])
            if pool["state"] != "active":
                raise InvalidState("资源池当前不可启动发布")
            cursor = self.connection.execute(
                "UPDATE release_plans SET state='in_progress',current_phase=1,revision=revision+1,"
                "updated_at=? WHERE plan_id=? AND revision=? AND state='confirmed'",
                (self._now(), plan_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("计划版本与预期不符")
            self._audit("release_plan", plan_id, "plan.started", actor_id, {"phase": "warmup"})
        return {"plan_id": plan_id, "state": "in_progress", "current_phase": 1, "revision": expected_revision + 1}

    def _margin_for_phase(self, pool: sqlite3.Row, plan: sqlite3.Row, phase: Mapping[str, Any]) -> int:
        return rollback_margin_gpu(
            pool_gpu_count=pool["gpu_count"],
            pool_reserved_gpu=pool["reserved_gpu"],
            plan_reserved_gpu=plan["reserved_gpu"],
            phase_required_gpu=int(phase["required_gpu"]),
        )

    def advance_plan(
        self,
        actor_id: str,
        plan_id: str,
        expected_revision: int,
        metric_summary: Mapping[str, Any],
    ) -> dict[str, Any]:
        """评估当前阶段门槛：指标达标且回退余量存在时才推进。"""
        self._require(actor_id, "plan.advance")
        metrics = MetricSummary.from_dict(metric_summary)
        with transaction(self.connection, immediate=True):
            plan = self._plan_row(plan_id)
            if plan["state"] == "invalidated":
                raise InvalidState("计划已失效：资源池维护版本已变化")
            if plan["state"] != "in_progress":
                raise InvalidState("只有进行中的计划可以推进")
            if plan["revision"] != expected_revision:
                raise InvalidState("计划版本与预期不符")
            phases = self._phases(plan)
            phase = phases[plan["current_phase"] - 1]
            pool = self._pool_row(plan["pool_id"])
            margin = self._margin_for_phase(pool, plan, phase)
            blockers = evaluate_gates(phase, metrics, margin)
            verdict = "blocked" if blockers else "passed"
            self.connection.execute(
                "INSERT INTO phase_metrics(plan_id,phase_seq,phase_kind,summary_json,verdict,"
                "blocking_json,margin_gpu,submitted_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    plan_id,
                    phase["seq"],
                    phase["kind"],
                    canonical_json(dict(metric_summary)),
                    verdict,
                    canonical_json(blockers),
                    margin,
                    actor_id,
                    self._now(),
                ),
            )
            if blockers:
                self._audit(
                    "release_plan", plan_id, "plan.gate_blocked", actor_id,
                    {"phase": phase["kind"], "blockers": blockers},
                )
                return {
                    "plan_id": plan_id,
                    "advanced": False,
                    "state": plan["state"],
                    "current_phase": plan["current_phase"],
                    "revision": plan["revision"],
                    "blocking_reasons": blockers,
                }
            completed = plan["current_phase"] == len(phases)
            if completed:
                cursor = self.connection.execute(
                    "UPDATE release_plans SET state='completed',revision=revision+1,updated_at=? "
                    "WHERE plan_id=? AND revision=? AND state='in_progress'",
                    (self._now(), plan_id, expected_revision),
                )
            else:
                cursor = self.connection.execute(
                    "UPDATE release_plans SET current_phase=current_phase+1,revision=revision+1,"
                    "updated_at=? WHERE plan_id=? AND revision=? AND state='in_progress'",
                    (self._now(), plan_id, expected_revision),
                )
            if cursor.rowcount != 1:
                raise InvalidState("计划版本与预期不符")
            event_type = "plan.completed" if completed else "plan.phase_advanced"
            self._audit(
                "release_plan", plan_id, event_type, actor_id,
                {"phase": phase["kind"], "margin_gpu": margin},
            )
        return {
            "plan_id": plan_id,
            "advanced": True,
            "state": "completed" if completed else "in_progress",
            "current_phase": plan["current_phase"] if completed else plan["current_phase"] + 1,
            "revision": expected_revision + 1,
            "blocking_reasons": [],
        }

    def rollback_plan(self, actor_id: str, plan_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        """异常回退：保留已发生流量证据；同一事件重复回调不会重复扣减容量。"""
        self._require(actor_id, "rollback.write")
        event_id = identifier(raw.get("event_id"), "event_id")
        anomaly = raw.get("anomaly")
        if not isinstance(anomaly, str) or not anomaly.strip():
            raise ValidationFailed("anomaly 不能为空")
        evidence = raw.get("traffic_evidence")
        if not isinstance(evidence, Mapping) or not evidence:
            raise ValidationFailed("traffic_evidence 必须是非空对象")
        existing = self.connection.execute(
            "SELECT response_json FROM rollback_events WHERE event_id=?", (event_id,)
        ).fetchone()
        if existing is not None:
            return {**json.loads(existing["response_json"]), "replayed": True}
        try:
            with transaction(self.connection, immediate=True):
                plan = self._plan_row(plan_id)
                if plan["state"] not in ACTIVE_STATES:
                    raise InvalidState("计划当前状态不可回退")
                phases = self._phases(plan)
                current = plan["current_phase"]
                phase = phases[current - 1] if current >= 1 else phases[0]
                metrics_rows = self.connection.execute(
                    "SELECT phase_seq,phase_kind,summary_json,verdict,margin_gpu,created_at "
                    "FROM phase_metrics WHERE plan_id=? ORDER BY metric_id",
                    (plan_id,),
                ).fetchall()
                released = plan["reserved_gpu"]
                cursor = self.connection.execute(
                    "UPDATE release_plans SET state='rolled_back',reserved_gpu=0,"
                    "capacity_released_gpu=?,revision=revision+1,updated_at=? "
                    "WHERE plan_id=? AND state IN ('confirmed','in_progress') AND reserved_gpu=?",
                    (released, self._now(), plan_id, released),
                )
                if cursor.rowcount != 1:
                    raise InvalidState("计划当前状态不可回退")
                cursor = self.connection.execute(
                    "UPDATE capacity_pools SET reserved_gpu=reserved_gpu-? "
                    "WHERE pool_id=? AND reserved_gpu>=?",
                    (released, plan["pool_id"], released),
                )
                if cursor.rowcount != 1:
                    raise Conflict("资源池预留容量与计划不一致")
                evidence_record = {
                    "traffic_evidence": dict(evidence),
                    "state_before": plan["state"],
                    "rolled_back_at_phase": None if current == 0 else current,
                    "phase_metrics": [
                        {
                            "phase_seq": row["phase_seq"],
                            "phase_kind": row["phase_kind"],
                            "summary": json.loads(row["summary_json"]),
                            "verdict": row["verdict"],
                            "margin_gpu": row["margin_gpu"],
                            "created_at": row["created_at"],
                        }
                        for row in metrics_rows
                    ],
                }
                response = {
                    "plan_id": plan_id,
                    "state": "rolled_back",
                    "event_id": event_id,
                    "released_gpu": released,
                    "rolled_back_at_phase": None if current == 0 else current,
                    "phase_kind": None if current == 0 else phase["kind"],
                    "evidence_preserved": True,
                    "replayed": False,
                }
                self.connection.execute(
                    "INSERT INTO rollback_events(event_id,plan_id,phase_seq,phase_kind,anomaly,"
                    "evidence_json,released_gpu,response_json,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        event_id,
                        plan_id,
                        current,
                        phase["kind"],
                        anomaly.strip(),
                        canonical_json(evidence_record),
                        released,
                        canonical_json(response),
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit(
                    "release_plan", plan_id, "plan.rolled_back", actor_id,
                    {
                        "event_id": event_id,
                        "released_gpu": released,
                        "evidence_sha256": digest(evidence_record),
                    },
                )
        except sqlite3.IntegrityError:
            stored = self.connection.execute(
                "SELECT response_json FROM rollback_events WHERE event_id=?", (event_id,)
            ).fetchone()
            if stored is None:
                raise
            return {**json.loads(stored["response_json"]), "replayed": True}
        return response

    # ------------------------------------------------------------------
    # 计划详情：容量来源、阻断原因与可回退范围
    # ------------------------------------------------------------------
    def _phase_status(self, plan: sqlite3.Row, seq: int) -> str:
        state = plan["state"]
        current = plan["current_phase"]
        if state == "completed":
            return "passed"
        if state == "rolled_back":
            if seq < current:
                return "passed"
            if seq == current:
                return "rolled_back"
            return "pending"
        if state == "in_progress":
            if seq < current:
                return "passed"
            if seq == current:
                return "current"
            return "pending"
        return "pending"

    def _capacity_source(self, plan: sqlite3.Row, pool: sqlite3.Row, phase: Mapping[str, Any]) -> dict[str, Any]:
        base = {
            "pool_id": plan["pool_id"],
            "protected_margin_gpu": plan["protected_margin_gpu"],
            "pool_free_gpu": pool["gpu_count"] - pool["reserved_gpu"],
        }
        if plan["state"] in ("draft", "invalidated"):
            return {
                "kind": "unreserved",
                "would_reserve_gpu": peak_gpu(self._phases(plan)),
                **base,
            }
        if plan["state"] == "rolled_back":
            return {"kind": "released", "released_gpu": plan["capacity_released_gpu"], **base}
        return {
            "kind": "pool_reservation",
            "reserved_gpu": plan["reserved_gpu"],
            "active_gpu": int(phase["required_gpu"]),
            "headroom_gpu": max(0, plan["reserved_gpu"] - int(phase["required_gpu"])),
            **base,
        }

    def _live_blockers(self, plan: sqlite3.Row, pool: sqlite3.Row) -> list[dict[str, Any]]:
        phases = self._phases(plan)
        blockers: list[dict[str, Any]] = []
        state = plan["state"]
        if state == "invalidated":
            return [{"gate": "plan", "message": "计划已失效：资源池维护版本已变化"}]
        if state in ("draft", "confirmed") and pool["state"] != "active":
            blockers.append({
                "gate": "pool_state",
                "state": pool["state"],
                "message": "资源池不在可用状态",
            })
        if state == "draft":
            needed = peak_gpu(phases)
            free = pool["gpu_count"] - pool["reserved_gpu"]
            preempts = plan["tenant_priority"] <= pool["preemption_priority"]
            usable = free if preempts else free - pool["protected_gpu"]
            if needed > usable:
                blockers.append({
                    "gate": "capacity",
                    "required_gpu": needed,
                    "usable_gpu": usable,
                    "message": "资源池可用容量不足",
                })
        if state == "in_progress":
            phase = phases[plan["current_phase"] - 1]
            margin = self._margin_for_phase(pool, plan, phase)
            required = int(phase["rollback_margin_required_gpu"])
            if margin < required:
                blockers.append({
                    "gate": "rollback_margin",
                    "required_gpu": required,
                    "available_gpu": margin,
                    "message": "回退余量不足",
                })
        return blockers

    def _rollback_scope(
        self,
        plan: sqlite3.Row,
        pool: sqlite3.Row,
        passed_phases: list[int],
    ) -> dict[str, Any]:
        phases = self._phases(plan)
        state = plan["state"]
        reasons = {
            "draft": "not_confirmed",
            "invalidated": "plan_invalidated",
            "completed": "plan_completed",
            "rolled_back": "already_rolled_back",
        }
        available = state in ACTIVE_STATES
        current = plan["current_phase"] if plan["current_phase"] >= 1 else 1
        phase = phases[min(current, len(phases)) - 1]
        margin = self._margin_for_phase(pool, plan, phase)
        return {
            "available": available,
            "reason": None if available else reasons[state],
            "margin_gpu": margin,
            "required_margin_gpu": int(phase["rollback_margin_required_gpu"]),
            "coverable_traffic_percent": decimal_text(coverable_traffic_percent(phases, margin)),
            "revertible_phases": passed_phases,
            "capacity_released_gpu": plan["capacity_released_gpu"],
        }

    def plan_detail(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        plan = self._plan_row(plan_id)
        pool = self._pool_row(plan["pool_id"])
        profile = self._profile_row(plan["profile_id"])
        phases = self._phases(plan)
        metrics_rows = self.connection.execute(
            "SELECT * FROM phase_metrics WHERE plan_id=? ORDER BY metric_id", (plan_id,)
        ).fetchall()
        latest_verdict: dict[int, str] = {}
        passed_phases: list[int] = []
        for row in metrics_rows:
            latest_verdict[row["phase_seq"]] = row["verdict"]
        for row in metrics_rows:
            if row["verdict"] == "passed" and row["phase_seq"] not in passed_phases:
                passed_phases.append(row["phase_seq"])
        rollback_rows = self.connection.execute(
            "SELECT event_id,phase_seq,phase_kind,anomaly,released_gpu,created_at "
            "FROM rollback_events WHERE plan_id=? ORDER BY created_at,event_id",
            (plan_id,),
        ).fetchall()
        return {
            "plan_id": plan["plan_id"],
            "state": plan["state"],
            "revision": plan["revision"],
            "current_phase": plan["current_phase"],
            "profile": {
                "profile_id": profile["profile_id"],
                "model_name": profile["model_name"],
                "model_version": profile["model_version"],
                "sha256": plan["profile_sha256"],
            },
            "pool": {
                "pool_id": pool["pool_id"],
                "pinned_revision": plan["pool_revision"],
                "current_revision": pool["revision"],
                "revision_matches": pool["revision"] == plan["pool_revision"],
                "state": pool["state"],
            },
            "tenant_priority": plan["tenant_priority"],
            "peak_qps": plan["peak_qps"],
            "reserved_gpu": plan["reserved_gpu"],
            "capacity_released_gpu": plan["capacity_released_gpu"],
            "phases": [
                {
                    **phase,
                    "status": self._phase_status(plan, int(phase["seq"])),
                    "last_verdict": latest_verdict.get(int(phase["seq"])),
                    "capacity_source": self._capacity_source(plan, pool, phase),
                }
                for phase in phases
            ],
            "blocking_reasons": self._live_blockers(plan, pool),
            "rollback_scope": self._rollback_scope(plan, pool, passed_phases),
            "metric_history": [
                {
                    "phase_seq": row["phase_seq"],
                    "phase_kind": row["phase_kind"],
                    "summary": json.loads(row["summary_json"]),
                    "verdict": row["verdict"],
                    "blocking_reasons": json.loads(row["blocking_json"]),
                    "margin_gpu": row["margin_gpu"],
                    "created_at": row["created_at"],
                }
                for row in metrics_rows
            ],
            "rollback_events": [dict(row) for row in rollback_rows],
        }

    def audit_chain(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        rows = self.connection.execute(
            "SELECT * FROM release_audit_events ORDER BY event_id"
        ).fetchall()
        previous_hash = "0" * 64
        valid = True
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            calculated = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or row["event_hash"] != calculated:
                valid = False
                break
            previous_hash = row["event_hash"]
        return {"valid": valid, "events": len(rows), "head_hash": previous_hash}

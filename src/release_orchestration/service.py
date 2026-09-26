"""容量预演与分阶段发布编排的事务用例。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from typing import Any, Mapping

from .clock import SystemClock, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import (
    PHASE_NAMES,
    CapacityPoolInput,
    MetricSummaryInput,
    ModelProfile,
    PlanRequest,
    TrafficSampleInput,
    decimal_value,
    required_text,
)
from .planning import (
    allocate_sources,
    build_phase_specs,
    canonical_json,
    decimal_text,
    digest,
    evaluate_gate,
    quantize_volume,
)
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "planner": {"profile.write", "plan.write", "plan.confirm", "report.read"},
    "operator": {
        "pool.write", "plan.start", "metrics.write", "traffic.write",
        "plan.advance", "plan.rollback", "report.read",
    },
    "risk": {"pool.write", "plan.rollback", "report.read"},
    "auditor": {"report.read", "audit.read"},
}

TERMINAL_STATES = ("completed", "rolled_back", "invalidated")


class ReleaseService:
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
    # 模型画像与维护版本
    # ------------------------------------------------------------------

    def register_profile(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "profile.write")
        profile = ModelProfile.from_dict(raw)
        definition = canonical_json(profile.definition())
        content_sha256 = hashlib.sha256(canonical_json(profile.content()).encode("utf-8")).hexdigest()
        if self.connection.execute(
            "SELECT 1 FROM model_profiles WHERE profile_id=?", (profile.profile_id,)
        ).fetchone() is not None:
            raise Conflict("模型画像不可变，编号已经存在")
        if self.connection.execute(
            "SELECT 1 FROM model_profiles WHERE content_sha256=?", (content_sha256,)
        ).fetchone() is not None:
            raise Conflict("相同内容的模型画像已经登记")
        now = self._now()
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "INSERT INTO model_profiles(profile_id,model_name,model_version,definition_json,"
                "content_sha256,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (
                    profile.profile_id,
                    profile.model_name,
                    profile.model_version,
                    definition,
                    content_sha256,
                    actor_id,
                    now,
                ),
            )
            catalog = self.connection.execute(
                "SELECT revision FROM model_catalog WHERE model_name=?", (profile.model_name,)
            ).fetchone()
            if catalog is None:
                catalog_revision = 1
                self.connection.execute(
                    "INSERT INTO model_catalog(model_name,maintained_profile_id,revision,updated_at) "
                    "VALUES(?,?,1,?)",
                    (profile.model_name, profile.profile_id, now),
                )
            else:
                catalog_revision = int(catalog["revision"]) + 1
                self.connection.execute(
                    "UPDATE model_catalog SET maintained_profile_id=?,revision=revision+1,updated_at=? "
                    "WHERE model_name=?",
                    (profile.profile_id, now, profile.model_name),
                )
            # 版本变化使旧计划失效：同一事务内作废旧计划并释放其预留。
            superseded = self.connection.execute(
                "SELECT plan_id FROM release_plans WHERE model_name=? AND profile_id<>? "
                "AND state NOT IN ('completed','rolled_back','invalidated') ORDER BY plan_id",
                (profile.model_name, profile.profile_id),
            ).fetchall()
            invalidated = [row["plan_id"] for row in superseded]
            for plan_id in invalidated:
                self._close_plan(plan_id, "invalidated", "version_superseded")
                self._audit(
                    "plan", plan_id, "plan.invalidated", actor_id,
                    {"superseded_by": profile.profile_id, "catalog_revision": catalog_revision},
                )
            self._audit(
                "profile", profile.profile_id, "profile.registered", actor_id,
                {
                    "model_name": profile.model_name,
                    "model_version": profile.model_version,
                    "catalog_revision": catalog_revision,
                    "invalidated_plans": invalidated,
                },
            )
        return {
            "profile_id": profile.profile_id,
            "model_name": profile.model_name,
            "model_version": profile.model_version,
            "content_sha256": content_sha256,
            "catalog_revision": catalog_revision,
            "invalidated_plans": invalidated,
        }

    def get_profile(self, profile_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM model_profiles WHERE profile_id=?", (profile_id,)
        ).fetchone()
        if row is None:
            raise NotFound("模型画像不存在")
        catalog = self.connection.execute(
            "SELECT maintained_profile_id,revision FROM model_catalog WHERE model_name=?",
            (row["model_name"],),
        ).fetchone()
        return {
            **json.loads(row["definition_json"]),
            "content_sha256": row["content_sha256"],
            "maintained": catalog is not None and catalog["maintained_profile_id"] == row["profile_id"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    def model_version_info(self, model_name: str) -> dict[str, Any]:
        catalog = self.connection.execute(
            "SELECT * FROM model_catalog WHERE model_name=?", (model_name,)
        ).fetchone()
        if catalog is None:
            raise NotFound("模型没有维护版本")
        profile = self.connection.execute(
            "SELECT model_version,content_sha256 FROM model_profiles WHERE profile_id=?",
            (catalog["maintained_profile_id"],),
        ).fetchone()
        return {
            "model_name": model_name,
            "maintained_profile_id": catalog["maintained_profile_id"],
            "model_version": profile["model_version"],
            "content_sha256": profile["content_sha256"],
            "catalog_revision": catalog["revision"],
            "updated_at": catalog["updated_at"],
        }

    # ------------------------------------------------------------------
    # 容量池
    # ------------------------------------------------------------------

    def create_pool(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "pool.write")
        pool = CapacityPoolInput.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO capacity_pools(pool_id,name,kind,total_gpu_hours,reserved_gpu_hours,"
                    "created_by,created_at) VALUES(?,?,?,?,'0',?,?)",
                    (
                        pool.pool_id,
                        pool.name,
                        pool.kind,
                        decimal_text(quantize_volume(pool.total_gpu_hours)),
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit("pool", pool.pool_id, "pool.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("容量池编号已经存在") from exc
        return self.get_pool(pool.pool_id)

    def _pool_row(self, pool_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM capacity_pools WHERE pool_id=?", (pool_id,)
        ).fetchone()
        if row is None:
            raise NotFound("容量池不存在")
        return row

    def get_pool(self, pool_id: str) -> dict[str, Any]:
        row = self._pool_row(pool_id)
        total = Decimal(row["total_gpu_hours"])
        reserved = Decimal(row["reserved_gpu_hours"])
        return {
            "pool_id": row["pool_id"],
            "name": row["name"],
            "kind": row["kind"],
            "state": row["state"],
            "total_gpu_hours": decimal_text(quantize_volume(total)),
            "reserved_gpu_hours": decimal_text(quantize_volume(reserved)),
            "available_gpu_hours": decimal_text(quantize_volume(total - reserved)),
            "revision": row["revision"],
        }

    def adjust_pool(self, actor_id: str, pool_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "pool.write")
        pool = self._pool_row(pool_id)
        expected = raw.get("expected_revision")
        if isinstance(expected, bool) or not isinstance(expected, int) or expected != pool["revision"]:
            raise InvalidState("容量池版本已变化，请重新读取后再调整")
        new_total = Decimal(pool["total_gpu_hours"])
        if "total_gpu_hours" in raw:
            new_total = quantize_volume(
                decimal_value(raw.get("total_gpu_hours"), "total_gpu_hours", minimum=Decimal("0"))
            )
        new_state = pool["state"]
        if "state" in raw:
            new_state = required_text(raw.get("state"), "state", 16)
            if new_state not in ("active", "suspended"):
                raise ValidationFailed("state 必须是 active 或 suspended")
        if new_total < Decimal(pool["reserved_gpu_hours"]):
            raise Conflict("容量池总量不能低于已预留容量")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE capacity_pools SET total_gpu_hours=?,state=?,revision=revision+1 "
                "WHERE pool_id=? AND revision=?",
                (decimal_text(new_total), new_state, pool_id, expected),
            )
            if cursor.rowcount != 1:
                raise InvalidState("容量池版本已变化，请重新读取后再调整")
            self._audit(
                "pool", pool_id, "pool.adjusted", actor_id,
                {"total_gpu_hours": decimal_text(new_total), "state": new_state},
            )
        return self.get_pool(pool_id)

    # ------------------------------------------------------------------
    # 发布计划
    # ------------------------------------------------------------------

    def _plan_row(self, plan_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM release_plans WHERE plan_id=?", (plan_id,)
        ).fetchone()
        if row is None:
            raise NotFound("发布计划不存在")
        return row

    def create_plan(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "plan.write")
        request = PlanRequest.from_dict(raw)
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM release_idempotency "
            "WHERE scope='plan' AND idempotency_key=?",
            (request.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同计划内容")
            return json.loads(stored["response_json"])
        profile = self.connection.execute(
            "SELECT * FROM model_profiles WHERE profile_id=?", (request.profile_id,)
        ).fetchone()
        if profile is None:
            raise NotFound("模型画像不存在")
        catalog = self.connection.execute(
            "SELECT maintained_profile_id,revision FROM model_catalog WHERE model_name=?",
            (profile["model_name"],),
        ).fetchone()
        if catalog is None or catalog["maintained_profile_id"] != profile["profile_id"]:
            raise InvalidState("模型画像不是当前维护版本，请基于最新画像重新制定计划")
        profile_definition = json.loads(profile["definition_json"])
        required_fallback = max(
            Decimal(str(profile_definition["fallback_capacity_gpu_hours"])),
            request.policy.min_fallback_gpu_hours,
        )
        try:
            phases = build_phase_specs(
                request.curve,
                request.canary_percent,
                int(profile_definition["cold_start_seconds"]),
                required_fallback,
                request.policy.thresholds(),
            )
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        peak = max(phase["capacity_gpu_hours"] for phase in phases)
        plan_sha256 = hashlib.sha256(canonical_json(raw).encode("utf-8")).hexdigest()
        now = self._now()
        response = {
            "plan_id": request.plan_id,
            "profile_id": request.profile_id,
            "model_name": profile["model_name"],
            "model_version": profile["model_version"],
            "state": "draft",
            "revision": 1,
            "peak_gpu_hours": decimal_text(quantize_volume(peak)),
            "required_fallback_gpu_hours": decimal_text(quantize_volume(required_fallback)),
            "phases": [
                {
                    "seq": phase["seq"],
                    "phase": phase["phase"],
                    "offset_minutes": phase["offset_minutes"],
                    "capacity_gpu_hours": decimal_text(phase["capacity_gpu_hours"]),
                    "gate": phase["gate"],
                }
                for phase in phases
            ],
        }
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO release_plans(plan_id,profile_id,model_name,model_version,profile_sha256,"
                    "catalog_revision,definition_json,plan_sha256,canary_percent,peak_gpu_hours,"
                    "required_fallback_gpu_hours,idempotency_key,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        request.plan_id,
                        request.profile_id,
                        profile["model_name"],
                        profile["model_version"],
                        profile["content_sha256"],
                        catalog["revision"],
                        canonical_json(raw),
                        plan_sha256,
                        decimal_text(request.canary_percent),
                        decimal_text(quantize_volume(peak)),
                        decimal_text(quantize_volume(required_fallback)),
                        request.idempotency_key,
                        actor_id,
                        now,
                    ),
                )
                for phase in phases:
                    self.connection.execute(
                        "INSERT INTO release_phases(plan_id,seq,phase,offset_minutes,capacity_gpu_hours,"
                        "gate_json) VALUES(?,?,?,?,?,?)",
                        (
                            request.plan_id,
                            phase["seq"],
                            phase["phase"],
                            phase["offset_minutes"],
                            decimal_text(phase["capacity_gpu_hours"]),
                            canonical_json(phase["gate"]),
                        ),
                    )
                self.connection.execute(
                    "INSERT INTO release_idempotency(scope,idempotency_key,request_sha256,response_json,"
                    "created_at) VALUES('plan',?,?,?,?)",
                    (request.idempotency_key, request_digest, canonical_json(response), now),
                )
                self._audit(
                    "plan", request.plan_id, "plan.created", actor_id,
                    {"profile_id": request.profile_id, "plan_sha256": plan_sha256},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("计划编号或幂等键冲突") from exc
        return response

    def confirm_plan(self, actor_id: str, plan_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "plan.confirm")
        plan = self._plan_row(plan_id)
        if plan["state"] != "draft" or plan["revision"] != expected_revision:
            raise InvalidState("计划不是当前草稿版本")
        serving_need = Decimal(plan["peak_gpu_hours"])
        fallback_need = Decimal(plan["required_fallback_gpu_hours"])
        now = self._now()
        # 计划确认与资源预留在同一事务中原子完成；任何一步失败都整体回滚。
        with transaction(self.connection, immediate=True):
            pools = self.connection.execute(
                "SELECT * FROM capacity_pools WHERE state='active' ORDER BY pool_id"
            ).fetchall()
            by_kind = {"serving": [], "fallback": []}
            for row in pools:
                by_kind[row["kind"]].append({
                    "pool_id": row["pool_id"],
                    "available_gpu_hours": Decimal(row["total_gpu_hours"]) - Decimal(row["reserved_gpu_hours"]),
                })
            try:
                serving_alloc = allocate_sources(by_kind["serving"], serving_need)
            except ValueError as exc:
                raise Conflict(f"在线服务容量不足，缺口 {str(exc).split(':', 1)[1]} GPU时") from exc
            try:
                fallback_alloc = allocate_sources(by_kind["fallback"], fallback_need)
            except ValueError as exc:
                raise Conflict(f"故障回退容量不足，缺口 {str(exc).split(':', 1)[1]} GPU时") from exc
            reservations: list[dict[str, Any]] = []
            for purpose, allocations in (("serving", serving_alloc), ("fallback", fallback_alloc)):
                for item in allocations:
                    pool = self.connection.execute(
                        "SELECT * FROM capacity_pools WHERE pool_id=? AND state='active'",
                        (item["pool_id"],),
                    ).fetchone()
                    if pool is None:
                        raise Conflict("容量池在确认期间发生变化")
                    take = Decimal(item["gpu_hours"])
                    new_reserved = quantize_volume(Decimal(pool["reserved_gpu_hours"]) + take)
                    if new_reserved > Decimal(pool["total_gpu_hours"]):
                        raise Conflict("容量池在确认期间发生变化")
                    cursor = self.connection.execute(
                        "UPDATE capacity_pools SET reserved_gpu_hours=? WHERE pool_id=? AND revision=?",
                        (decimal_text(new_reserved), item["pool_id"], pool["revision"]),
                    )
                    if cursor.rowcount != 1:
                        raise Conflict("容量池在确认期间发生变化")
                    cursor = self.connection.execute(
                        "INSERT INTO capacity_reservations(plan_id,pool_id,purpose,gpu_hours,created_at) "
                        "VALUES(?,?,?,?,?)",
                        (plan_id, item["pool_id"], purpose, decimal_text(quantize_volume(take)), now),
                    )
                    reservations.append({
                        "reservation_id": int(cursor.lastrowid),
                        "pool_id": item["pool_id"],
                        "purpose": purpose,
                        "gpu_hours": decimal_text(quantize_volume(take)),
                    })
            phases = self.connection.execute(
                "SELECT seq,capacity_gpu_hours FROM release_phases WHERE plan_id=? ORDER BY seq",
                (plan_id,),
            ).fetchall()
            for phase in phases:
                phase_sources = {
                    "serving": allocate_sources(by_kind["serving"], Decimal(phase["capacity_gpu_hours"])),
                    "fallback": fallback_alloc,
                }
                self.connection.execute(
                    "UPDATE release_phases SET sources_json=? WHERE plan_id=? AND seq=?",
                    (canonical_json(phase_sources), plan_id, phase["seq"]),
                )
            cursor = self.connection.execute(
                "UPDATE release_plans SET state='confirmed',revision=revision+1,confirmed_at=? "
                "WHERE plan_id=? AND state='draft' AND revision=?",
                (now, plan_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("计划不是当前草稿版本")
            self.connection.execute(
                "INSERT INTO release_ledger(ledger_key,plan_id,action,payload_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (
                    f"plan:{plan_id}:reserve",
                    plan_id,
                    "reserve",
                    canonical_json({"reservations": reservations}),
                    now,
                ),
            )
            self._audit(
                "plan", plan_id, "plan.confirmed", actor_id,
                {"serving_sources": serving_alloc, "fallback_sources": fallback_alloc},
            )
        return {
            "plan_id": plan_id,
            "state": "confirmed",
            "revision": expected_revision + 1,
            "serving_sources": serving_alloc,
            "fallback_sources": fallback_alloc,
            "reservations": reservations,
        }

    def start_plan(self, actor_id: str, plan_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "plan.start")
        plan = self._plan_row(plan_id)
        if plan["state"] != "confirmed" or plan["revision"] != expected_revision:
            raise InvalidState("计划不是当前已确认版本")
        now = self._now()
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE release_plans SET state='in_progress',current_phase_seq=1,started_at=?,"
                "revision=revision+1 WHERE plan_id=? AND state='confirmed' AND revision=?",
                (now, plan_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("计划不是当前已确认版本")
            self.connection.execute(
                "UPDATE release_phases SET state='active',activated_at=? WHERE plan_id=? AND seq=1",
                (now, plan_id),
            )
            self._audit("plan", plan_id, "plan.started", actor_id, {"phase_seq": 1})
        return {"plan_id": plan_id, "state": "in_progress", "current_phase_seq": 1, "revision": expected_revision + 1}

    # ------------------------------------------------------------------
    # 指标、流量证据与阶段推进
    # ------------------------------------------------------------------

    def _fallback_available(self, plan_id: str) -> Decimal:
        pools = self.connection.execute(
            "SELECT total_gpu_hours,reserved_gpu_hours FROM capacity_pools "
            "WHERE kind='fallback' AND state='active'"
        ).fetchall()
        free = sum(
            (max(Decimal("0"), Decimal(row["total_gpu_hours"]) - Decimal(row["reserved_gpu_hours"])) for row in pools),
            Decimal("0"),
        )
        held = self.connection.execute(
            "SELECT r.gpu_hours FROM capacity_reservations r "
            "JOIN capacity_pools p ON p.pool_id=r.pool_id "
            "WHERE r.plan_id=? AND r.purpose='fallback' AND r.state='held' AND p.state='active'",
            (plan_id,),
        ).fetchall()
        retained = sum((Decimal(row["gpu_hours"]) for row in held), Decimal("0"))
        return quantize_volume(free + retained)

    @staticmethod
    def _gate_thresholds(gate: Mapping[str, Any]) -> dict[str, Decimal]:
        return {
            "max_error_rate_percent": Decimal(str(gate["max_error_rate_percent"])),
            "max_p95_latency_ms": Decimal(str(gate["max_p95_latency_ms"])),
            "min_success_rate_percent": Decimal(str(gate["min_success_rate_percent"])),
        }

    def _active_phase(self, plan: sqlite3.Row, phase_seq: int) -> sqlite3.Row:
        phase = self.connection.execute(
            "SELECT * FROM release_phases WHERE plan_id=? AND seq=?",
            (plan["plan_id"], phase_seq),
        ).fetchone()
        if phase is None:
            raise NotFound("计划阶段不存在")
        if plan["state"] != "in_progress" or plan["current_phase_seq"] != phase_seq or phase["state"] != "active":
            raise InvalidState("阶段当前不接受新的指标或流量证据")
        return phase

    def submit_metrics(self, actor_id: str, plan_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "metrics.write")
        metrics = MetricSummaryInput.from_dict(raw)
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM release_idempotency "
            "WHERE scope='metrics' AND idempotency_key=?",
            (metrics.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同指标内容")
            return json.loads(stored["response_json"])
        plan = self._plan_row(plan_id)
        phase = self._active_phase(plan, metrics.phase_seq)
        gate = json.loads(phase["gate_json"])
        fallback_required = Decimal(gate["required_fallback_gpu_hours"])
        fallback_available = self._fallback_available(plan_id)
        result = evaluate_gate(
            {
                "error_rate_percent": metrics.error_rate_percent,
                "p95_latency_ms": metrics.p95_latency_ms,
                "success_rate_percent": metrics.success_rate_percent,
            },
            self._gate_thresholds(gate),
            fallback_available,
            fallback_required,
        )
        now = self._now()
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO metric_summaries(plan_id,phase_seq,metrics_json,gate_passed,blocking_json,"
                "fallback_available_gpu_hours,idempotency_key,recorded_by,recorded_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    plan_id,
                    metrics.phase_seq,
                    canonical_json(metrics.metrics()),
                    1 if result["passed"] else 0,
                    canonical_json(result["blocking_reasons"]),
                    decimal_text(fallback_available),
                    metrics.idempotency_key,
                    actor_id,
                    now,
                ),
            )
            summary_id = int(cursor.lastrowid)
            self.connection.execute(
                "UPDATE release_phases SET last_blocking_json=? WHERE plan_id=? AND seq=?",
                (canonical_json(result["blocking_reasons"]), plan_id, metrics.phase_seq),
            )
            response = {
                "summary_id": summary_id,
                "plan_id": plan_id,
                "phase_seq": metrics.phase_seq,
                "gate_passed": result["passed"],
                "blocking_reasons": result["blocking_reasons"],
                "fallback_available_gpu_hours": decimal_text(fallback_available),
                "fallback_required_gpu_hours": decimal_text(fallback_required),
            }
            self.connection.execute(
                "INSERT INTO release_idempotency(scope,idempotency_key,request_sha256,response_json,"
                "created_at) VALUES('metrics',?,?,?,?)",
                (metrics.idempotency_key, request_digest, canonical_json(response), now),
            )
            self._audit(
                "plan", plan_id, "metrics.recorded", actor_id,
                {"phase_seq": metrics.phase_seq, "summary_id": summary_id, "gate_passed": result["passed"]},
            )
        return response

    def record_traffic(self, actor_id: str, plan_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "traffic.write")
        sample = TrafficSampleInput.from_dict(raw)
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM release_idempotency "
            "WHERE scope='traffic' AND idempotency_key=?",
            (sample.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同流量内容")
            return json.loads(stored["response_json"])
        plan = self._plan_row(plan_id)
        self._active_phase(plan, sample.phase_seq)
        now = self._now()
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO traffic_samples(plan_id,phase_seq,observed_gpu_hours,note,idempotency_key,"
                "recorded_by,recorded_at) VALUES(?,?,?,?,?,?,?)",
                (
                    plan_id,
                    sample.phase_seq,
                    decimal_text(quantize_volume(sample.observed_gpu_hours)),
                    sample.note,
                    sample.idempotency_key,
                    actor_id,
                    now,
                ),
            )
            sample_id = int(cursor.lastrowid)
            response = {
                "sample_id": sample_id,
                "plan_id": plan_id,
                "phase_seq": sample.phase_seq,
                "observed_gpu_hours": decimal_text(quantize_volume(sample.observed_gpu_hours)),
            }
            self.connection.execute(
                "INSERT INTO release_idempotency(scope,idempotency_key,request_sha256,response_json,"
                "created_at) VALUES('traffic',?,?,?,?)",
                (sample.idempotency_key, request_digest, canonical_json(response), now),
            )
            self._audit(
                "plan", plan_id, "traffic.recorded", actor_id,
                {"phase_seq": sample.phase_seq, "sample_id": sample_id},
            )
        return response

    def advance_plan(self, actor_id: str, plan_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "plan.advance")
        plan = self._plan_row(plan_id)
        if plan["state"] != "in_progress" or plan["revision"] != expected_revision:
            raise InvalidState("计划不是当前执行中版本")
        phase = self.connection.execute(
            "SELECT * FROM release_phases WHERE plan_id=? AND seq=?",
            (plan_id, plan["current_phase_seq"]),
        ).fetchone()
        latest = self.connection.execute(
            "SELECT * FROM metric_summaries WHERE plan_id=? AND phase_seq=? "
            "ORDER BY summary_id DESC LIMIT 1",
            (plan_id, phase["seq"]),
        ).fetchone()
        gate = json.loads(phase["gate_json"])
        fallback_required = Decimal(gate["required_fallback_gpu_hours"])
        fallback_available = self._fallback_available(plan_id)
        if latest is None:
            blocking = [{"code": "metrics_missing", "message": "当前阶段还没有指标摘要，不能推进"}]
        else:
            metrics = {key: Decimal(value) for key, value in json.loads(latest["metrics_json"]).items()}
            blocking = evaluate_gate(
                metrics,
                self._gate_thresholds(gate),
                fallback_available,
                fallback_required,
            )["blocking_reasons"]
        now = self._now()
        if blocking:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "UPDATE release_phases SET last_blocking_json=? WHERE plan_id=? AND seq=?",
                    (canonical_json(blocking), plan_id, phase["seq"]),
                )
                self._audit(
                    "plan", plan_id, "plan.advance_blocked", actor_id,
                    {"phase_seq": phase["seq"], "blocking_reasons": blocking},
                )
            return {
                "plan_id": plan_id,
                "advanced": False,
                "phase_seq": phase["seq"],
                "phase": phase["phase"],
                "blocking_reasons": blocking,
                "fallback_available_gpu_hours": decimal_text(fallback_available),
                "fallback_required_gpu_hours": decimal_text(fallback_required),
            }
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE release_phases SET state='passed',closed_at=?,last_blocking_json='[]' "
                "WHERE plan_id=? AND seq=? AND state='active'",
                (now, plan_id, phase["seq"]),
            )
            if phase["seq"] < len(PHASE_NAMES):
                next_seq = phase["seq"] + 1
                trim_entries: list[dict[str, Any]] = []
                if PHASE_NAMES[next_seq - 1] == "converge":
                    next_phase = self.connection.execute(
                        "SELECT capacity_gpu_hours FROM release_phases WHERE plan_id=? AND seq=?",
                        (plan_id, next_seq),
                    ).fetchone()
                    trim = quantize_volume(Decimal(plan["peak_gpu_hours"]) - Decimal(next_phase["capacity_gpu_hours"]))
                    if trim > Decimal("0"):
                        trim_entries = self._converge_trim(plan_id, trim)
                self.connection.execute(
                    "UPDATE release_phases SET state='active',activated_at=? WHERE plan_id=? AND seq=?",
                    (now, plan_id, next_seq),
                )
                cursor = self.connection.execute(
                    "UPDATE release_plans SET current_phase_seq=?,revision=revision+1 "
                    "WHERE plan_id=? AND state='in_progress' AND revision=?",
                    (next_seq, plan_id, expected_revision),
                )
                if cursor.rowcount != 1:
                    raise InvalidState("计划不是当前执行中版本")
                self._audit(
                    "plan", plan_id, "plan.advanced", actor_id,
                    {"from_seq": phase["seq"], "to_seq": next_seq, "converge_trim": trim_entries},
                )
                return {
                    "plan_id": plan_id,
                    "advanced": True,
                    "plan_state": "in_progress",
                    "current_phase_seq": next_seq,
                    "revision": expected_revision + 1,
                }
            released = self._final_release(plan_id)
            cursor = self.connection.execute(
                "UPDATE release_plans SET state='completed',closed_at=?,closed_reason='completed',"
                "revision=revision+1 WHERE plan_id=? AND state='in_progress' AND revision=?",
                (now, plan_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("计划不是当前执行中版本")
            self._audit(
                "plan", plan_id, "plan.completed", actor_id,
                {"released": released, "evidence": self._evidence(plan_id)},
            )
            return {
                "plan_id": plan_id,
                "advanced": True,
                "plan_state": "completed",
                "current_phase_seq": None,
                "revision": expected_revision + 1,
            }

    # ------------------------------------------------------------------
    # 回退与容量台账
    # ------------------------------------------------------------------

    def _pool_release(self, pool_id: str, amount: Decimal) -> None:
        pool = self._pool_row(pool_id)
        new_reserved = quantize_volume(Decimal(pool["reserved_gpu_hours"]) - amount)
        if new_reserved < Decimal("0"):
            raise InvalidState("容量池预留账目异常，拒绝重复扣减")
        self.connection.execute(
            "UPDATE capacity_pools SET reserved_gpu_hours=? WHERE pool_id=?",
            (decimal_text(new_reserved), pool_id),
        )

    def _final_release(self, plan_id: str) -> list[dict[str, Any]]:
        ledger_key = f"plan:{plan_id}:final-release"
        existing = self.connection.execute(
            "SELECT payload_json FROM release_ledger WHERE ledger_key=?", (ledger_key,)
        ).fetchone()
        if existing is not None:
            return json.loads(existing["payload_json"])["entries"]
        rows = self.connection.execute(
            "SELECT * FROM capacity_reservations WHERE plan_id=? AND state='held' "
            "ORDER BY reservation_id",
            (plan_id,),
        ).fetchall()
        now = self._now()
        entries: list[dict[str, Any]] = []
        for row in rows:
            amount = Decimal(row["gpu_hours"])
            self.connection.execute(
                "UPDATE capacity_reservations SET state='released',released_at=? WHERE reservation_id=?",
                (now, row["reservation_id"]),
            )
            self._pool_release(row["pool_id"], amount)
            entries.append({
                "reservation_id": row["reservation_id"],
                "pool_id": row["pool_id"],
                "purpose": row["purpose"],
                "gpu_hours": decimal_text(quantize_volume(amount)),
            })
        self.connection.execute(
            "INSERT INTO release_ledger(ledger_key,plan_id,action,payload_json,created_at) "
            "VALUES(?,?,?,?,?)",
            (ledger_key, plan_id, "final-release", canonical_json({"entries": entries}), now),
        )
        return entries

    def _converge_trim(self, plan_id: str, trim: Decimal) -> list[dict[str, Any]]:
        ledger_key = f"plan:{plan_id}:converge-trim"
        existing = self.connection.execute(
            "SELECT payload_json FROM release_ledger WHERE ledger_key=?", (ledger_key,)
        ).fetchone()
        if existing is not None:
            return json.loads(existing["payload_json"])["entries"]
        rows = self.connection.execute(
            "SELECT * FROM capacity_reservations WHERE plan_id=? AND purpose='serving' AND state='held' "
            "ORDER BY reservation_id DESC",
            (plan_id,),
        ).fetchall()
        now = self._now()
        remaining = trim
        entries: list[dict[str, Any]] = []
        for row in rows:
            if remaining <= Decimal("0"):
                break
            amount = Decimal(row["gpu_hours"])
            take = min(amount, remaining)
            left = quantize_volume(amount - take)
            if left == Decimal("0"):
                self.connection.execute(
                    "UPDATE capacity_reservations SET gpu_hours='0',state='released',released_at=? "
                    "WHERE reservation_id=?",
                    (now, row["reservation_id"]),
                )
            else:
                self.connection.execute(
                    "UPDATE capacity_reservations SET gpu_hours=? WHERE reservation_id=?",
                    (decimal_text(left), row["reservation_id"]),
                )
            self._pool_release(row["pool_id"], take)
            entries.append({
                "reservation_id": row["reservation_id"],
                "pool_id": row["pool_id"],
                "purpose": "serving",
                "gpu_hours": decimal_text(quantize_volume(take)),
            })
            remaining = quantize_volume(remaining - take)
        self.connection.execute(
            "INSERT INTO release_ledger(ledger_key,plan_id,action,payload_json,created_at) "
            "VALUES(?,?,?,?,?)",
            (ledger_key, plan_id, "converge-trim", canonical_json({"entries": entries}), now),
        )
        return entries

    def _evidence(self, plan_id: str) -> dict[str, Any]:
        metric_ids = [
            row["summary_id"]
            for row in self.connection.execute(
                "SELECT summary_id FROM metric_summaries WHERE plan_id=? ORDER BY summary_id",
                (plan_id,),
            ).fetchall()
        ]
        sample_ids = [
            row["sample_id"]
            for row in self.connection.execute(
                "SELECT sample_id FROM traffic_samples WHERE plan_id=? ORDER BY sample_id",
                (plan_id,),
            ).fetchall()
        ]
        return {
            "metric_summary_ids": metric_ids,
            "traffic_sample_ids": sample_ids,
            "metric_summaries": len(metric_ids),
            "traffic_samples": len(sample_ids),
        }

    def _close_plan(self, plan_id: str, closed_state: str, reason: str) -> list[dict[str, Any]]:
        released = self._final_release(plan_id)
        now = self._now()
        self.connection.execute(
            "UPDATE release_phases SET closed_at=? WHERE plan_id=? AND closed_at IS NULL",
            (now, plan_id),
        )
        self.connection.execute(
            "UPDATE release_plans SET state=?,revision=revision+1,closed_at=?,closed_reason=? "
            "WHERE plan_id=?",
            (closed_state, now, reason, plan_id),
        )
        return released

    def rollback_plan(self, actor_id: str, plan_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "plan.rollback")
        key = required_text(raw.get("idempotency_key"), "idempotency_key", 64)
        reason = required_text(raw.get("reason"), "reason")
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM release_idempotency "
            "WHERE scope='rollback' AND idempotency_key=?",
            (key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同回退内容")
            return json.loads(stored["response_json"])
        plan = self._plan_row(plan_id)
        if plan["state"] in TERMINAL_STATES:
            raise InvalidState("计划已处于终态，不能重复回退扣减容量")
        with transaction(self.connection, immediate=True):
            released = self._close_plan(plan_id, "rolled_back", "rollback")
            evidence = self._evidence(plan_id)
            response = {
                "plan_id": plan_id,
                "state": "rolled_back",
                "reason": reason,
                "released_gpu_hours": decimal_text(quantize_volume(sum(
                    (Decimal(entry["gpu_hours"]) for entry in released), Decimal("0")
                ))),
                "released": released,
                "evidence": evidence,
            }
            self.connection.execute(
                "INSERT INTO release_idempotency(scope,idempotency_key,request_sha256,response_json,"
                "created_at) VALUES('rollback',?,?,?,?)",
                (key, request_digest, canonical_json(response), self._now()),
            )
            self._audit(
                "plan", plan_id, "plan.rolled_back", actor_id,
                {"reason": reason, "released": released, "evidence": evidence},
            )
        return response

    # ------------------------------------------------------------------
    # 计划说明视图
    # ------------------------------------------------------------------

    def plan_detail(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        plan = self._plan_row(plan_id)
        catalog = self.connection.execute(
            "SELECT maintained_profile_id FROM model_catalog WHERE model_name=?",
            (plan["model_name"],),
        ).fetchone()
        maintained = catalog is not None and catalog["maintained_profile_id"] == plan["profile_id"]
        phases = self.connection.execute(
            "SELECT * FROM release_phases WHERE plan_id=? ORDER BY seq", (plan_id,)
        ).fetchall()
        phase_views: list[dict[str, Any]] = []
        for phase in phases:
            latest = self.connection.execute(
                "SELECT metrics_json,gate_passed,blocking_json,recorded_at FROM metric_summaries "
                "WHERE plan_id=? AND phase_seq=? ORDER BY summary_id DESC LIMIT 1",
                (plan_id, phase["seq"]),
            ).fetchone()
            sources = None if phase["sources_json"] is None else json.loads(phase["sources_json"])
            phase_views.append({
                "seq": phase["seq"],
                "phase": phase["phase"],
                "state": phase["state"],
                "offset_minutes": phase["offset_minutes"],
                "capacity_gpu_hours": phase["capacity_gpu_hours"],
                "capacity_sources": sources,
                "sources_frozen": sources is not None,
                "gate": json.loads(phase["gate_json"]),
                "latest_metrics": None if latest is None else {
                    **json.loads(latest["metrics_json"]),
                    "gate_passed": bool(latest["gate_passed"]),
                    "recorded_at": latest["recorded_at"],
                },
                "blocking_reasons": json.loads(phase["last_blocking_json"] or "[]"),
            })
        held = self.connection.execute(
            "SELECT reservation_id,pool_id,purpose,gpu_hours FROM capacity_reservations "
            "WHERE plan_id=? AND state='held' ORDER BY reservation_id",
            (plan_id,),
        ).fetchall()
        gate = json.loads(phases[0]["gate_json"])
        fallback_required = Decimal(gate["required_fallback_gpu_hours"])
        fallback_available = self._fallback_available(plan_id)
        evidence = self._evidence(plan_id)
        return {
            "plan_id": plan["plan_id"],
            "state": plan["state"],
            "revision": plan["revision"],
            "model_name": plan["model_name"],
            "model_version": plan["model_version"],
            "profile_id": plan["profile_id"],
            "maintained_version": maintained,
            "closed_reason": plan["closed_reason"],
            "current_phase_seq": plan["current_phase_seq"],
            "peak_gpu_hours": plan["peak_gpu_hours"],
            "canary_percent": plan["canary_percent"],
            "phases": phase_views,
            "fallback": {
                "required_gpu_hours": decimal_text(quantize_volume(fallback_required)),
                "available_gpu_hours": decimal_text(fallback_available),
                "margin_ok": fallback_available >= fallback_required,
            },
            "rollback_scope": {
                "reversible": plan["state"] in ("confirmed", "in_progress"),
                "held_reservations": [
                    {
                        "reservation_id": row["reservation_id"],
                        "pool_id": row["pool_id"],
                        "purpose": row["purpose"],
                        "gpu_hours": row["gpu_hours"],
                    }
                    for row in held
                ],
                "held_total_gpu_hours": decimal_text(quantize_volume(sum(
                    (Decimal(row["gpu_hours"]) for row in held), Decimal("0")
                ))),
                "evidence": evidence,
            },
            "created_by": plan["created_by"],
            "created_at": plan["created_at"],
            "confirmed_at": plan["confirmed_at"],
            "started_at": plan["started_at"],
            "closed_at": plan["closed_at"],
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

"""发布编排领域输入契约。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")
POOL_KINDS = {"serving", "fallback"}
PHASE_NAMES = ("warmup", "canary", "scale_up", "converge")


def required_text(value: object, field: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field} 不能超过 {maximum} 个字符")
    return result


def identifier(value: object, field: str) -> str:
    result = required_text(value, field, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field} 格式不正确")
    return result


def decimal_value(
    value: object,
    field: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal:
    if isinstance(value, bool):
        raise ValidationFailed(f"{field} 必须是数值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationFailed(f"{field} 必须是十进制数值") from exc
    if not result.is_finite():
        raise ValidationFailed(f"{field} 必须是有限数值")
    if minimum is not None and result < minimum:
        raise ValidationFailed(f"{field} 不能小于 {minimum}")
    if maximum is not None and result > maximum:
        raise ValidationFailed(f"{field} 不能大于 {maximum}")
    return result


def positive_integer(value: object, field: str, *, maximum: int = 86400) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= maximum:
        raise ValidationFailed(f"{field} 必须是 1 到 {maximum} 的整数")
    return value


def optional_mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationFailed(f"{field} 必须是对象")
    return value


@dataclass(frozen=True, slots=True)
class ModelProfile:
    """不可变模型画像：显存、冷启动、租户优先级和故障回退容量。"""

    profile_id: str
    model_name: str
    model_version: str
    vram_gb_per_replica: Decimal
    cold_start_seconds: int
    tenant_priority: int
    fallback_capacity_gpu_hours: Decimal

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ModelProfile":
        priority = raw.get("tenant_priority")
        if isinstance(priority, bool) or not isinstance(priority, int) or not 1 <= priority <= 999:
            raise ValidationFailed("tenant_priority 必须是 1 到 999 的整数")
        return cls(
            profile_id=identifier(raw.get("profile_id"), "profile_id"),
            model_name=identifier(raw.get("model_name"), "model_name"),
            model_version=required_text(raw.get("model_version"), "model_version", 64),
            vram_gb_per_replica=decimal_value(
                raw.get("vram_gb_per_replica"), "vram_gb_per_replica", minimum=Decimal("0.1")
            ),
            cold_start_seconds=positive_integer(raw.get("cold_start_seconds"), "cold_start_seconds"),
            tenant_priority=priority,
            fallback_capacity_gpu_hours=decimal_value(
                raw.get("fallback_capacity_gpu_hours"),
                "fallback_capacity_gpu_hours",
                minimum=Decimal("0"),
            ),
        )

    def definition(self) -> dict[str, Any]:
        return {"profile_id": self.profile_id, **self.content()}

    def content(self) -> dict[str, Any]:
        """参与内容摘要的画像字段；编号本身不参与，保证相同内容只能登记一次。"""

        return {
            "model_name": self.model_name,
            "model_version": self.model_version,
            "vram_gb_per_replica": str(self.vram_gb_per_replica),
            "cold_start_seconds": self.cold_start_seconds,
            "tenant_priority": self.tenant_priority,
            "fallback_capacity_gpu_hours": str(self.fallback_capacity_gpu_hours),
        }


@dataclass(frozen=True, slots=True)
class CapacityPoolInput:
    pool_id: str
    name: str
    kind: str
    total_gpu_hours: Decimal

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "CapacityPoolInput":
        kind = required_text(raw.get("kind"), "kind", 24)
        if kind not in POOL_KINDS:
            raise ValidationFailed("kind 必须是 serving 或 fallback")
        return cls(
            pool_id=identifier(raw.get("pool_id"), "pool_id"),
            name=required_text(raw.get("name"), "name"),
            kind=kind,
            total_gpu_hours=decimal_value(
                raw.get("total_gpu_hours"), "total_gpu_hours", minimum=Decimal("0.001")
            ),
        )


@dataclass(frozen=True, slots=True)
class CurvePoint:
    offset_minutes: int
    target_gpu_hours: Decimal


def traffic_curve(value: object) -> tuple[CurvePoint, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValidationFailed("traffic_curve 必须是数组")
    if not 2 <= len(value) <= 24:
        raise ValidationFailed("traffic_curve 必须包含 2 到 24 个采样点")
    points: list[CurvePoint] = []
    previous_offset = -1
    for index, item in enumerate(value):
        field = f"traffic_curve[{index}]"
        if not isinstance(item, Mapping):
            raise ValidationFailed(f"{field} 必须是对象")
        offset = item.get("offset_minutes")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValidationFailed(f"{field}.offset_minutes 必须是非负整数")
        if offset <= previous_offset:
            raise ValidationFailed("traffic_curve 的 offset_minutes 必须严格递增")
        target = decimal_value(
            item.get("target_gpu_hours"), f"{field}.target_gpu_hours", minimum=Decimal("0.001")
        )
        points.append(CurvePoint(offset, target))
        previous_offset = offset
    return tuple(points)


@dataclass(frozen=True, slots=True)
class RollbackPolicy:
    """回退策略：阶段门槛指标与最小回退余量。"""

    max_error_rate_percent: Decimal
    max_p95_latency_ms: Decimal
    min_success_rate_percent: Decimal
    min_fallback_gpu_hours: Decimal

    @classmethod
    def from_dict(cls, raw: object) -> "RollbackPolicy":
        mapping = optional_mapping(raw, "rollback_policy")
        return cls(
            max_error_rate_percent=decimal_value(
                mapping.get("max_error_rate_percent"),
                "rollback_policy.max_error_rate_percent",
                minimum=Decimal("0"),
                maximum=Decimal("100"),
            ),
            max_p95_latency_ms=decimal_value(
                mapping.get("max_p95_latency_ms"),
                "rollback_policy.max_p95_latency_ms",
                minimum=Decimal("1"),
            ),
            min_success_rate_percent=decimal_value(
                mapping.get("min_success_rate_percent"),
                "rollback_policy.min_success_rate_percent",
                minimum=Decimal("0"),
                maximum=Decimal("100"),
            ),
            min_fallback_gpu_hours=decimal_value(
                mapping.get("min_fallback_gpu_hours"),
                "rollback_policy.min_fallback_gpu_hours",
                minimum=Decimal("0"),
            ),
        )

    def thresholds(self) -> dict[str, str]:
        return {
            "max_error_rate_percent": str(self.max_error_rate_percent),
            "max_p95_latency_ms": str(self.max_p95_latency_ms),
            "min_success_rate_percent": str(self.min_success_rate_percent),
        }


@dataclass(frozen=True, slots=True)
class PlanRequest:
    plan_id: str
    profile_id: str
    canary_percent: Decimal
    curve: tuple[CurvePoint, ...]
    policy: RollbackPolicy
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "PlanRequest":
        canary = decimal_value(
            raw.get("canary_percent", 25),
            "canary_percent",
            minimum=Decimal("0.1"),
            maximum=Decimal("100"),
        )
        return cls(
            plan_id=identifier(raw.get("plan_id"), "plan_id"),
            profile_id=identifier(raw.get("profile_id"), "profile_id"),
            canary_percent=canary,
            curve=traffic_curve(raw.get("traffic_curve")),
            policy=RollbackPolicy.from_dict(raw.get("rollback_policy")),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class MetricSummaryInput:
    phase_seq: int
    error_rate_percent: Decimal
    p95_latency_ms: Decimal
    success_rate_percent: Decimal
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "MetricSummaryInput":
        seq = raw.get("phase_seq")
        if isinstance(seq, bool) or not isinstance(seq, int) or not 1 <= seq <= len(PHASE_NAMES):
            raise ValidationFailed(f"phase_seq 必须是 1 到 {len(PHASE_NAMES)} 的整数")
        return cls(
            phase_seq=seq,
            error_rate_percent=decimal_value(
                raw.get("error_rate_percent"), "error_rate_percent", minimum=Decimal("0"), maximum=Decimal("100")
            ),
            p95_latency_ms=decimal_value(
                raw.get("p95_latency_ms"), "p95_latency_ms", minimum=Decimal("0")
            ),
            success_rate_percent=decimal_value(
                raw.get("success_rate_percent"), "success_rate_percent", minimum=Decimal("0"), maximum=Decimal("100")
            ),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )

    def metrics(self) -> dict[str, str]:
        return {
            "error_rate_percent": str(self.error_rate_percent),
            "p95_latency_ms": str(self.p95_latency_ms),
            "success_rate_percent": str(self.success_rate_percent),
        }


@dataclass(frozen=True, slots=True)
class TrafficSampleInput:
    phase_seq: int
    observed_gpu_hours: Decimal
    note: str
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "TrafficSampleInput":
        seq = raw.get("phase_seq")
        if isinstance(seq, bool) or not isinstance(seq, int) or not 1 <= seq <= len(PHASE_NAMES):
            raise ValidationFailed(f"phase_seq 必须是 1 到 {len(PHASE_NAMES)} 的整数")
        note = raw.get("note", "")
        if not isinstance(note, str) or len(note) > 256:
            raise ValidationFailed("note 不能超过 256 个字符")
        return cls(
            phase_seq=seq,
            observed_gpu_hours=decimal_value(
                raw.get("observed_gpu_hours"), "observed_gpu_hours", minimum=Decimal("0")
            ),
            note=note.strip(),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )

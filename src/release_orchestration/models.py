"""容量预演与发布编排的领域输入契约。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .clock import parse_utc
from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")
PHASE_KINDS = ("warmup", "canary", "scale_up", "convergence")
POOL_STATES = {"active", "maintenance", "retired"}


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


def integer_value(
    value: object,
    field: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationFailed(f"{field} 必须是整数")
    if minimum is not None and value < minimum:
        raise ValidationFailed(f"{field} 不能小于 {minimum}")
    if maximum is not None and value > maximum:
        raise ValidationFailed(f"{field} 不能大于 {maximum}")
    return value


def percent_value(value: object, field: str, *, allow_zero: bool = False) -> Decimal:
    lower = Decimal("0") if allow_zero else Decimal("0.0001")
    return decimal_value(value, field, minimum=lower, maximum=Decimal("100"))


@dataclass(frozen=True, slots=True)
class ModelProfile:
    """不可变模型画像：显存、冷启动、租户优先级与单副本吞吐。"""

    profile_id: str
    model_name: str
    model_version: str
    vram_gb_per_replica: Decimal
    cold_start_seconds: int
    tenant_priority: int
    replica_qps: Decimal

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ModelProfile":
        priority = integer_value(raw.get("tenant_priority"), "tenant_priority", minimum=1, maximum=999)
        return cls(
            profile_id=identifier(raw.get("profile_id"), "profile_id"),
            model_name=required_text(raw.get("model_name"), "model_name", 128),
            model_version=required_text(raw.get("model_version"), "model_version", 64),
            vram_gb_per_replica=decimal_value(
                raw.get("vram_gb_per_replica"), "vram_gb_per_replica",
                minimum=Decimal("0.1"), maximum=Decimal("1000"),
            ),
            cold_start_seconds=integer_value(
                raw.get("cold_start_seconds"), "cold_start_seconds", minimum=1, maximum=86400,
            ),
            tenant_priority=priority,
            replica_qps=decimal_value(
                raw.get("replica_qps"), "replica_qps",
                minimum=Decimal("0.01"), maximum=Decimal("1000000"),
            ),
        )


@dataclass(frozen=True, slots=True)
class CapacityPool:
    """冻结资源池：卡数、单卡显存、保护余量与维护版本。"""

    pool_id: str
    gpu_count: int
    vram_gb_per_gpu: Decimal
    protected_gpu: int
    preemption_priority: int

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "CapacityPool":
        gpu_count = integer_value(raw.get("gpu_count"), "gpu_count", minimum=1, maximum=1000000)
        protected = integer_value(raw.get("protected_gpu", 0), "protected_gpu", minimum=0)
        if protected >= gpu_count:
            raise ValidationFailed("protected_gpu 必须小于 gpu_count")
        return cls(
            pool_id=identifier(raw.get("pool_id"), "pool_id"),
            gpu_count=gpu_count,
            vram_gb_per_gpu=decimal_value(
                raw.get("vram_gb_per_gpu"), "vram_gb_per_gpu",
                minimum=Decimal("1"), maximum=Decimal("2048"),
            ),
            protected_gpu=protected,
            preemption_priority=integer_value(
                raw.get("preemption_priority", 100), "preemption_priority", minimum=1, maximum=999,
            ),
        )


@dataclass(frozen=True, slots=True)
class CurvePoint:
    percent: Decimal
    hold_minutes: int


@dataclass(frozen=True, slots=True)
class TrafficCurve:
    """目标流量曲线：四个递增 waypoint 依次映射预热、灰度、扩容、收敛。"""

    points: tuple[CurvePoint, CurvePoint, CurvePoint, CurvePoint]

    @classmethod
    def from_list(cls, raw: object) -> "TrafficCurve":
        if not isinstance(raw, list) or len(raw) != 4:
            raise ValidationFailed("traffic_curve 必须包含 4 个阶段点（预热、灰度、扩容、收敛）")
        points: list[CurvePoint] = []
        for index, item in enumerate(raw):
            if not isinstance(item, Mapping):
                raise ValidationFailed("traffic_curve 阶段点必须是对象")
            points.append(CurvePoint(
                percent=percent_value(item.get("percent"), f"traffic_curve[{index}].percent"),
                hold_minutes=integer_value(
                    item.get("hold_minutes", 0), f"traffic_curve[{index}].hold_minutes",
                    minimum=0, maximum=10080,
                ),
            ))
        percents = [point.percent for point in points]
        if any(percents[index] >= percents[index + 1] for index in range(3)):
            raise ValidationFailed("traffic_curve 流量百分比必须严格递增")
        if percents[0] > Decimal("25"):
            raise ValidationFailed("预热阶段流量不能超过 25%")
        if percents[1] > Decimal("50"):
            raise ValidationFailed("灰度阶段流量不能超过 50%")
        if percents[2] >= Decimal("100"):
            raise ValidationFailed("扩容阶段流量必须低于 100%")
        if percents[3] != Decimal("100"):
            raise ValidationFailed("收敛阶段流量必须达到 100%")
        return cls(points=(points[0], points[1], points[2], points[3]))


@dataclass(frozen=True, slots=True)
class RollbackPolicy:
    """回退策略：阶段门槛指标与最小回退余量。"""

    max_error_rate_percent: Decimal
    max_p99_latency_ms: int
    max_cold_start_p99_seconds: int | None
    min_vram_headroom_percent: Decimal
    min_rollback_margin_gpu: int

    @classmethod
    def from_dict(cls, raw: object) -> "RollbackPolicy":
        if not isinstance(raw, Mapping):
            raise ValidationFailed("rollback_policy 必须是对象")
        cold_start = raw.get("max_cold_start_p99_seconds")
        return cls(
            max_error_rate_percent=decimal_value(
                raw.get("max_error_rate_percent"), "max_error_rate_percent",
                minimum=Decimal("0"), maximum=Decimal("100"),
            ),
            max_p99_latency_ms=integer_value(
                raw.get("max_p99_latency_ms"), "max_p99_latency_ms", minimum=1, maximum=3600000,
            ),
            max_cold_start_p99_seconds=None if cold_start is None else integer_value(
                cold_start, "max_cold_start_p99_seconds", minimum=1, maximum=86400,
            ),
            min_vram_headroom_percent=decimal_value(
                raw.get("min_vram_headroom_percent", "10"), "min_vram_headroom_percent",
                minimum=Decimal("0"), maximum=Decimal("100"),
            ),
            min_rollback_margin_gpu=integer_value(
                raw.get("min_rollback_margin_gpu", 0), "min_rollback_margin_gpu",
                minimum=0, maximum=1000000,
            ),
        )


@dataclass(frozen=True, slots=True)
class MetricSummary:
    """阶段推进时提交的指标摘要。"""

    error_rate_percent: Decimal
    p99_latency_ms: int
    cold_start_p99_seconds: int
    vram_headroom_percent: Decimal
    observed_qps: Decimal
    sampled_at: str

    @classmethod
    def from_dict(cls, raw: object) -> "MetricSummary":
        if not isinstance(raw, Mapping):
            raise ValidationFailed("metric_summary 必须是对象")
        sampled_at = required_text(raw.get("sampled_at"), "metric_summary.sampled_at", 40)
        try:
            parse_utc(sampled_at, "metric_summary.sampled_at")
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        return cls(
            error_rate_percent=decimal_value(
                raw.get("error_rate_percent"), "metric_summary.error_rate_percent",
                minimum=Decimal("0"), maximum=Decimal("100"),
            ),
            p99_latency_ms=integer_value(
                raw.get("p99_latency_ms"), "metric_summary.p99_latency_ms", minimum=0, maximum=3600000,
            ),
            cold_start_p99_seconds=integer_value(
                raw.get("cold_start_p99_seconds"), "metric_summary.cold_start_p99_seconds",
                minimum=0, maximum=86400,
            ),
            vram_headroom_percent=decimal_value(
                raw.get("vram_headroom_percent"), "metric_summary.vram_headroom_percent",
                minimum=Decimal("0"), maximum=Decimal("100"),
            ),
            observed_qps=decimal_value(
                raw.get("observed_qps"), "metric_summary.observed_qps", minimum=Decimal("0"),
            ),
            sampled_at=sampled_at,
        )

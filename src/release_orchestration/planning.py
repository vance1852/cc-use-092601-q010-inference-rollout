"""确定性的阶段生成、门槛评估与容量来源计算。"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping, Sequence

from .models import CurvePoint


ZERO = Decimal("0")
HUNDRED = Decimal("100")


def quantize_volume(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def build_phase_specs(
    curve: Sequence[CurvePoint],
    canary_percent: Decimal,
    cold_start_seconds: int,
    required_fallback_gpu_hours: Decimal,
    thresholds: Mapping[str, str],
) -> list[dict[str, Any]]:
    """由目标流量曲线确定性地派生预热、灰度、扩容、收敛四个阶段。"""

    first = curve[0]
    final = curve[-1]
    peak_point = curve[0]
    for point in curve:
        if point.target_gpu_hours > peak_point.target_gpu_hours:
            peak_point = point
    canary_capacity = quantize_volume(peak_point.target_gpu_hours * canary_percent / HUNDRED)
    if canary_capacity <= ZERO:
        raise ValueError("canary_percent 相对峰值过小，灰度阶段容量为零")
    canary_offset = first.offset_minutes + (peak_point.offset_minutes - first.offset_minutes) // 2
    gate = {
        **thresholds,
        "required_fallback_gpu_hours": decimal_text(quantize_volume(required_fallback_gpu_hours)),
    }
    specs = [
        ("warmup", first.offset_minutes, first.target_gpu_hours),
        ("canary", canary_offset, canary_capacity),
        ("scale_up", peak_point.offset_minutes, peak_point.target_gpu_hours),
        ("converge", final.offset_minutes, final.target_gpu_hours),
    ]
    phases: list[dict[str, Any]] = []
    for sequence, (name, offset, capacity) in enumerate(specs, start=1):
        phase_gate = dict(gate)
        if name == "warmup":
            phase_gate["min_warmup_seconds"] = str(cold_start_seconds)
        phases.append({
            "seq": sequence,
            "phase": name,
            "offset_minutes": offset,
            "capacity_gpu_hours": quantize_volume(capacity),
            "gate": phase_gate,
        })
    return phases


def evaluate_gate(
    metrics: Mapping[str, Decimal],
    thresholds: Mapping[str, Decimal],
    fallback_available: Decimal,
    fallback_required: Decimal,
) -> dict[str, Any]:
    """指标摘要与回退余量的门槛判定，返回阻断原因列表。"""

    reasons: list[dict[str, str]] = []
    if metrics["error_rate_percent"] > thresholds["max_error_rate_percent"]:
        reasons.append({
            "code": "error_rate_above_threshold",
            "message": f"错误率 {decimal_text(metrics['error_rate_percent'])}% 高于门槛 "
                       f"{decimal_text(thresholds['max_error_rate_percent'])}%",
        })
    if metrics["p95_latency_ms"] > thresholds["max_p95_latency_ms"]:
        reasons.append({
            "code": "p95_latency_above_threshold",
            "message": f"P95 延迟 {decimal_text(metrics['p95_latency_ms'])}ms 高于门槛 "
                       f"{decimal_text(thresholds['max_p95_latency_ms'])}ms",
        })
    if metrics["success_rate_percent"] < thresholds["min_success_rate_percent"]:
        reasons.append({
            "code": "success_rate_below_threshold",
            "message": f"成功率 {decimal_text(metrics['success_rate_percent'])}% 低于门槛 "
                       f"{decimal_text(thresholds['min_success_rate_percent'])}%",
        })
    if fallback_available < fallback_required:
        reasons.append({
            "code": "fallback_margin_insufficient",
            "message": f"回退余量 {decimal_text(quantize_volume(fallback_available))} GPU时 低于要求 "
                       f"{decimal_text(quantize_volume(fallback_required))} GPU时",
        })
    return {"passed": not reasons, "blocking_reasons": reasons}


def allocate_sources(
    pools: Iterable[Mapping[str, Any]],
    amount: Decimal,
) -> list[dict[str, str]]:
    """按 pool_id 顺序在容量池间确定性地切分需求，返回每池承担量。"""

    if amount < ZERO:
        raise ValueError("需求容量不能为负数")
    remaining = quantize_volume(amount)
    allocations: list[dict[str, str]] = []
    for pool in sorted(pools, key=lambda item: str(item["pool_id"])):
        if remaining == ZERO:
            break
        available = Decimal(str(pool["available_gpu_hours"]))
        if available <= ZERO:
            continue
        take = min(available, remaining)
        allocations.append({"pool_id": str(pool["pool_id"]), "gpu_hours": decimal_text(quantize_volume(take))})
        remaining = quantize_volume(remaining - take)
    if remaining > ZERO:
        raise ValueError(f"capacity_shortfall:{decimal_text(remaining)}")
    return allocations

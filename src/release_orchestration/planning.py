"""确定性的容量预演计算：阶段生成、门槛评估与可回退范围。"""

from __future__ import annotations

import hashlib
import json
import math
from decimal import Decimal, ROUND_HALF_UP
from typing import Mapping, Sequence

from .models import MetricSummary, ModelProfile, RollbackPolicy, TrafficCurve


ZERO = Decimal("0")
HUNDRED = Decimal("100")
WARMUP_WAVE_REPLICAS = 10
PHASE_KINDS = ("warmup", "canary", "scale_up", "convergence")
PHASE_LABELS = {
    "warmup": "预热",
    "canary": "灰度",
    "scale_up": "扩容",
    "convergence": "收敛",
}


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def replicas_for_percent(peak_qps: Decimal, percent: Decimal, replica_qps: Decimal) -> int:
    """按目标流量百分比计算所需副本数（至少 1 个）。"""
    if replica_qps <= ZERO:
        raise ValueError("replica_qps 必须大于零")
    needed = peak_qps * percent / HUNDRED / replica_qps
    return max(1, math.ceil(needed))


def gpu_for_replicas(replicas: int, vram_gb_per_replica: Decimal, vram_gb_per_gpu: Decimal) -> int:
    """按显存占用折算所需 GPU 卡数。"""
    if replicas < 0:
        raise ValueError("副本数不能为负数")
    if vram_gb_per_gpu <= ZERO:
        raise ValueError("单卡显存必须大于零")
    return math.ceil(Decimal(replicas) * vram_gb_per_replica / vram_gb_per_gpu)


def warmup_min_hold_minutes(cold_start_seconds: int, replicas: int) -> int:
    """冷启动决定的预热最短时长：每波最多并行拉起 WARMUP_WAVE_REPLICAS 个副本。"""
    if cold_start_seconds <= 0:
        raise ValueError("冷启动时间必须大于零")
    waves = max(1, math.ceil(max(1, replicas) / WARMUP_WAVE_REPLICAS))
    return math.ceil(waves * cold_start_seconds / 60)


def generate_phases(
    *,
    profile: ModelProfile,
    peak_qps: Decimal,
    curve: TrafficCurve,
    policy: RollbackPolicy,
    vram_gb_per_gpu: Decimal,
) -> list[dict[str, object]]:
    """基于模型画像、目标流量曲线与回退策略生成四个发布阶段。

    每个阶段给出容量需求、门槛指标与推进所需的回退余量；
    预热阶段时长取曲线驻留与冷启动推导时长的较大者。
    """
    if peak_qps <= ZERO:
        raise ValueError("peak_qps 必须大于零")
    max_cold_start = policy.max_cold_start_p99_seconds
    if max_cold_start is None:
        max_cold_start = math.ceil(Decimal(profile.cold_start_seconds) * Decimal("1.5"))
    gates = {
        "max_error_rate_percent": decimal_text(policy.max_error_rate_percent),
        "max_p99_latency_ms": policy.max_p99_latency_ms,
        "max_cold_start_p99_seconds": max_cold_start,
        "min_vram_headroom_percent": decimal_text(policy.min_vram_headroom_percent),
    }
    phases: list[dict[str, object]] = []
    for index, (kind, point) in enumerate(zip(PHASE_KINDS, curve.points), start=1):
        replicas = replicas_for_percent(peak_qps, point.percent, profile.replica_qps)
        required_gpu = gpu_for_replicas(replicas, profile.vram_gb_per_replica, vram_gb_per_gpu)
        hold_minutes = point.hold_minutes
        phase: dict[str, object] = {
            "seq": index,
            "kind": kind,
            "label": PHASE_LABELS[kind],
            "traffic_percent": decimal_text(point.percent),
            "required_replicas": replicas,
            "required_gpu": required_gpu,
            "gates": gates,
        }
        if kind == "warmup":
            min_hold = warmup_min_hold_minutes(profile.cold_start_seconds, replicas)
            phase["min_hold_minutes"] = min_hold
            hold_minutes = max(hold_minutes, min_hold)
        if kind == "convergence":
            margin_required = policy.min_rollback_margin_gpu
        else:
            margin_required = max(required_gpu, policy.min_rollback_margin_gpu)
        phase["hold_minutes"] = hold_minutes
        phase["rollback_margin_required_gpu"] = margin_required
        phases.append(phase)
    return phases


def peak_gpu(phases: Sequence[Mapping[str, object]]) -> int:
    return max(int(phase["required_gpu"]) for phase in phases)


def rollback_margin_gpu(
    *,
    pool_gpu_count: int,
    pool_reserved_gpu: int,
    plan_reserved_gpu: int,
    phase_required_gpu: int,
) -> int:
    """回退余量 = 资源池空闲容量 + 本计划预留中尚未投入当前阶段的部分。"""
    headroom = max(0, plan_reserved_gpu - phase_required_gpu)
    return pool_gpu_count - pool_reserved_gpu + headroom


def evaluate_gates(
    phase: Mapping[str, object],
    metrics: MetricSummary,
    margin_gpu: int,
) -> list[dict[str, object]]:
    """评估阶段门槛，返回全部阻断原因（空列表表示可以推进）。"""
    gates = phase["gates"]
    blockers: list[dict[str, object]] = []
    max_error = Decimal(str(gates["max_error_rate_percent"]))
    if metrics.error_rate_percent > max_error:
        blockers.append({
            "gate": "error_rate",
            "limit": decimal_text(max_error),
            "observed": decimal_text(metrics.error_rate_percent),
            "message": "错误率超过门槛",
        })
    max_latency = int(gates["max_p99_latency_ms"])
    if metrics.p99_latency_ms > max_latency:
        blockers.append({
            "gate": "p99_latency",
            "limit": max_latency,
            "observed": metrics.p99_latency_ms,
            "message": "p99 延迟超过门槛",
        })
    max_cold = int(gates["max_cold_start_p99_seconds"])
    if metrics.cold_start_p99_seconds > max_cold:
        blockers.append({
            "gate": "cold_start",
            "limit": max_cold,
            "observed": metrics.cold_start_p99_seconds,
            "message": "冷启动 p99 超过门槛",
        })
    min_headroom = Decimal(str(gates["min_vram_headroom_percent"]))
    if metrics.vram_headroom_percent < min_headroom:
        blockers.append({
            "gate": "vram_headroom",
            "limit": decimal_text(min_headroom),
            "observed": decimal_text(metrics.vram_headroom_percent),
            "message": "显存余量低于门槛",
        })
    margin_required = int(phase["rollback_margin_required_gpu"])
    if margin_gpu < margin_required:
        blockers.append({
            "gate": "rollback_margin",
            "required_gpu": margin_required,
            "available_gpu": margin_gpu,
            "message": "回退余量不足",
        })
    return blockers


def coverable_traffic_percent(phases: Sequence[Mapping[str, object]], margin_gpu: int) -> Decimal:
    """当前回退余量能够完整承接回退的最高流量百分比。"""
    coverable = ZERO
    for phase in phases:
        if int(phase["required_gpu"]) <= margin_gpu:
            coverable = max(coverable, Decimal(str(phase["traffic_percent"])))
    return coverable

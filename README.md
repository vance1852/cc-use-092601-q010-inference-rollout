# 实现推理流量容量预演与分阶段发布基础平台

本项目是一套可离线运行的 Python 服务端平台，用于管理数据中心、互联通道、加速卡资源批次、租户预约、容量分配、交付情景与硬件稳定性准入。业务状态、幂等结果和审计事件保存在 SQLite 中，适合调度、质量、风险和审计人员在单个 Linux 应用容器内协作。

## 目录

- `src/compute_fabric/`：站点、通道、资源库存、预约、容量分配和情景分析；
- `src/release_orchestration/`：不可变模型画像、容量预演、分阶段发布编排与回退；
- `src/accelerator_lab/`：加速卡测点导入、排除复核、分析任务租约和准入决定；
- `src/silicon_qualification/`：AI 加速芯片批次、测量、分析与质量审批；
- `fixtures/`：离线验收使用的结构化协议与测点；
- `tests/`：核心规则、错误边界、事务、API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m compute_fabric.acceptance --workspace .
PYTHONPATH=src python3 -m release_orchestration.acceptance --workspace .
PYTHONPATH=src python3 -m accelerator_lab.acceptance --workspace .
PYTHONPATH=src python3 -m silicon_qualification.acceptance
```

这些命令使用临时 SQLite 数据库完成站点、资源、预约、分配、测点分析和芯片准入流程，不访问外部网络。发布编排验收额外覆盖：模型画像登记、容量池冻结预留、预热/灰度/扩容/收敛四阶段门槛推进、异常回退的幂等重放以及版本变化导致的计划失效。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m compute_fabric.api --database compute.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m accelerator_lab.api --database lab.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m silicon_qualification.api --database silicon.sqlite3 --host 127.0.0.1 --port 8082
PYTHONPATH=src python3 -m release_orchestration.api --database release.sqlite3 --host 127.0.0.1 --port 8083
```

服务均提供 `GET /health`，其余接口使用 JSON。进程重启后可以继续查询 SQLite 中的业务状态和审计历史。

## 发布编排接口

`release_orchestration` 服务把容量预演固化为一条可审计的状态机：`草稿 → 已确认 → 执行中 → 已完成 / 已回退 / 已失效`。

- `POST /profiles` 登记不可变模型画像（显存、冷启动、租户优先级、故障回退容量），内容摘要唯一，重复登记即冲突；同一模型登记新画像会使旧版本的非终态计划原子失效并释放其预留。
- `POST /plans` 基于维护版本画像、目标流量曲线和回退策略生成预热、灰度、扩容、收敛四个阶段，计划创建按幂等键去重。
- `POST /plans/{id}/confirm` 在同一事务内完成计划确认与容量池预留，任何一步失败都整体回滚，并冻结每阶段的容量来源。
- `POST /plans/{id}/metrics` 提交阶段指标摘要；`POST /plans/{id}/advance` 只有在指标满足门槛且回退余量仍存在时才推进，否则返回阻断原因（`metrics_missing`、`error_rate_above_threshold`、`p95_latency_above_threshold`、`success_rate_below_threshold`、`fallback_margin_insufficient`）。
- `POST /plans/{id}/traffic` 追加流量证据；`POST /plans/{id}/rollback` 触发回退，保留已发生的指标与流量证据，容量台账保证重复回调不会重复扣减容量。
- `GET /plans/{id}` 说明每阶段的容量来源（`capacity_sources`）、阻断原因（`blocking_reasons`）和当前可回退范围（`rollback_scope`：持有预留、可逆性与证据计数）。

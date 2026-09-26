# 实现推理流量容量预演与分阶段发布基础平台

本项目是一套可离线运行的 Python 服务端平台，用于管理数据中心、互联通道、加速卡资源批次、租户预约、容量分配、交付情景与硬件稳定性准入。业务状态、幂等结果和审计事件保存在 SQLite 中，适合调度、质量、风险和审计人员在单个 Linux 应用容器内协作。

## 目录

- `src/compute_fabric/`：站点、通道、资源库存、预约、容量分配和情景分析；
- `src/release_orchestration/`：不可变模型画像、冻结资源池、容量预演与预热/灰度/扩容/收敛分阶段发布编排；
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

这些命令使用临时 SQLite 数据库完成站点、资源、预约、分配、测点分析和芯片准入流程，不访问外部网络。

## 容量预演与发布编排

`release_orchestration` 把情景分析的总量预测落成可执行的分阶段发布：

- 团队提交**不可变模型画像**（显存占用、冷启动时间、租户优先级、单副本吞吐，内容寻址不可篡改）、**目标流量曲线**（四个递增 waypoint）与**回退策略**（指标门槛与最小回退余量）；
- 系统基于冻结资源池与维护版本生成**预热、灰度、扩容、收敛**四个阶段，预热时长取曲线驻留与冷启动推导时长的较大者；
- 计划确认与资源预留在单个事务中原子完成；资源池维护版本变化会使未确认的草稿计划失效；
- 每阶段提交指标摘要后，仅当全部门槛达标且回退余量仍存在时才推进，否则返回全部阻断原因；
- 异常回退保留已发生的流量证据（指标历史与回调证据），同一事件重复回调幂等，不会重复扣减容量；
- 计划详情接口说明每阶段的容量来源（预留/未预留/已释放）、当前阻断原因与可回退范围（余量、可承接回退的流量百分比、可回退阶段）。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m compute_fabric.api --database compute.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m accelerator_lab.api --database lab.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m silicon_qualification.api --database silicon.sqlite3 --host 127.0.0.1 --port 8082
PYTHONPATH=src python3 -m release_orchestration.api --database release.sqlite3 --host 127.0.0.1 --port 8083
```

服务均提供 `GET /health`，其余接口使用 JSON。进程重启后可以继续查询 SQLite 中的业务状态和审计历史。

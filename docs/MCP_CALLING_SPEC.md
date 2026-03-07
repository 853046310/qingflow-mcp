# Qingflow MCP 调用规范（v0.7.0）

本规范描述 `qingflow-mcp` 的公开 canonical 协议。面向智能体时，只使用 `listTools()` 暴露出来的 10 个工具；不要再让模型直接走 legacy 工具。

## 1. 公开工具面

仅暴露以下工具：

- `qf_tool_spec_get`
- `qf_form_get`
- `qf_field_resolve`
- `qf_value_probe`
- `qf.query.plan`
- `qf.query.rows`
- `qf.query.record`
- `qf.query.aggregate`
- `qf.query.export`
- `qf.records.mutate`

原则：

1. `qf.query.plan` 是唯一公开 planner。
2. `qf.query.rows / record / aggregate / export / qf.records.mutate` 是 execute-only 工具。
3. execute 工具必须吃 `plan_id`，不再让模型自己重新拼执行参数。

## 2. 标准链路：Plan -> Execute

标准调用链路：

1. 先调用 `qf.query.plan`
2. 读取返回中的：
   - `data.plan_id`
   - `data.plan.executor_tool`
   - `data.plan.executor_args`
   - `data.ready`
   - `data.blockers`
3. `data.ready=true` 时，直接执行 `data.plan.executor_tool`，参数原样使用 `data.plan.executor_args`
4. 不要在 execute 阶段改写 query

planner 返回结构核心字段：

```json
{
  "ok": true,
  "data": {
    "kind": "aggregate",
    "plan_id": "plan_xxx",
    "normalized_query": { "app_key": "21b3d559" },
    "plan": {
      "plan_id": "plan_xxx",
      "kind": "aggregate",
      "executor_tool": "qf.query.aggregate",
      "executor_args": { "plan_id": "plan_xxx" },
      "ready": true,
      "blockers": []
    },
    "estimate": { "page_size": 50 },
    "ready": true,
    "blockers": []
  }
}
```

## 3. Execute 输入契约

### 3.1 `qf.query.rows`

必填：

- `plan_id`

可选：

- `query`

规则：

- `query` 仅用于 drift 校验
- 如果传了 `query`，它必须与 planner 产出的 `normalized_query` 完全一致
- 不一致返回 `PLAN_DRIFT`

### 3.2 `qf.query.record`

必填：

- `plan_id`

可选：

- `query`

### 3.3 `qf.query.aggregate`

必填：

- `plan_id`

可选：

- `query`

### 3.4 `qf.query.export`

必填：

- `plan_id`

可选：

- `query`

### 3.5 `qf.records.mutate`

必填：

- `plan_id`

可选：

- `action`

## 4. Canonical 查询 DSL

planner 输入使用 canonical DSL：

```json
{
  "kind": "aggregate",
  "query": {
    "app_key": "21b3d559",
    "where": [
      { "field": 6564644, "op": "contains", "value": "北斗", "match": "normalized" },
      { "field": 6299264, "op": "between", "from": "2025-01-01", "to": "2025-12-31" }
    ],
    "group_by": [9500572],
    "metrics": [
      { "op": "count" },
      { "column": 6302833, "op": "sum" }
    ],
    "strict_full": true
  }
}
```

规则：

1. 公开读工具只接受 `where`，不接受 legacy `filters`
2. runtime alias 被禁止：
   - 顶层：`from` / `to` / `dateFrom` / `dateTo`
   - `filters[]` 内：`searchKey` / `searchKeys` / `from` / `to` / `dateFrom` / `dateTo`
3. planner 可以做 loose normalization，但不会吞并危险 alias
4. execute 阶段不做纠偏

## 5. 匹配语义

支持显式 `match` / `match_mode`：

- `exact`
- `normalized`
- `contains`
- `prefix`
- `fuzzy`

`qf_value_probe` 返回：

```json
{
  "candidates": [
    {
      "value": "北斗",
      "display_value": "北斗",
      "observed_count": 337,
      "score": 0.98,
      "match": "exact",
      "matched_texts": ["北斗"]
    }
  ],
  "matched_values": ["北斗"]
}
```

canonical execute 工具在 `evidence.match_evidence` 中回显最终候选值，用于解释“到底匹配到了什么”。

## 6. Aggregate 业务口径

业务摘要层只保留 3 个 canonical count：

```json
{
  "summary": {
    "counts": {
      "source_record_count": 370,
      "group_assignment_count": 405,
      "metric_nonnull_record_count": 395
    },
    "primary_metric_total": 12272931.75,
    "primary_metric_missing_count": 10
  }
}
```

含义：

- `source_record_count`：过滤后唯一记录数；默认回答“多少单/多少条”时只读它
- `group_assignment_count`：分组展开后的 assignment 数
- `metric_nonnull_record_count`：主指标列非空记录数
- `primary_metric_missing_count`：主指标缺失记录数

不要再把以下字段当业务总量：

- `completeness.*`
- `metrics_by_column.*.nonnull_record_count`
- 任意 group-level count

## 7. Completeness 仅表达技术状态

公开 canonical execute 工具的 `completeness` 只表达“是否可以下最终结论”：

```json
{
  "is_complete": true,
  "raw_scan_complete": true,
  "scan_limit_hit": false,
  "fetched_pages": 19,
  "requested_pages": 50,
  "actual_scanned_pages": 19,
  "scanned_pages": 19,
  "scan_limit": 50,
  "has_more": false,
  "next_page_token": null,
  "stop_reason": "source_exhausted",
  "output_truncated": false,
  "omitted_items": 0,
  "omitted_chars": 0
}
```

规则：

1. `completeness` 不再承载业务总量。
2. `strict_full=true` 时，只要：
   - `raw_scan_complete=false`，或
   - `output_truncated=true`
   就直接返回 `INCOMPLETE_RESULT`。
3. `is_complete=false` 的结果不能被智能体当成最终分析结论。

## 8. 错误协议

所有失败统一返回：

```json
{
  "ok": false,
  "error": {
    "code": "PLAN_DRIFT",
    "message": "...",
    "fix_hint": "...",
    "retryable": true
  },
  "error_code": "PLAN_DRIFT",
  "message": "...",
  "fix_hint": "...",
  "details": {}
}
```

当前重点错误码：

- `FORBIDDEN_RUNTIME_ALIAS`
- `VALIDATION_ERROR`
- `PLAN_REQUIRED`
- `PLAN_NOT_READY`
- `PLAN_DRIFT`
- `INCOMPLETE_RESULT`
- `INVALID_FIELD_REF`
- `UNKNOWN_FIELD_VALUE`
- `UNSUPPORTED_MATCH_MODE`
- `UPSTREAM_TIMEOUT`
- `UPSTREAM_API_ERROR`
- `INTERNAL_ERROR`

编排建议：

1. `PLAN_REQUIRED` / `PLAN_NOT_READY`：回到 `qf.query.plan`
2. `PLAN_DRIFT`：不要改写 execute 参数，直接复用 planner 返回的 `plan_id`
3. `INCOMPLETE_RESULT`：继续翻页或扩大扫描预算
4. `FORBIDDEN_RUNTIME_ALIAS` / `VALIDATION_ERROR`：修参数
5. `UPSTREAM_TIMEOUT` / `UPSTREAM_API_ERROR`：缩小查询规模或停止并告知用户

## 9. 对智能体的硬规则

1. 新会话先看 `listTools()` 或 `qf_tool_spec_get`
2. 字段不确定时先 `qf_form_get` / `qf_field_resolve`
3. 字段值不确定时先 `qf_value_probe`
4. 正式执行前先 `qf.query.plan`
5. execute 时只传 planner 返回的 `plan_id`
6. 如果 `is_complete=false`，禁止输出“完整分析”

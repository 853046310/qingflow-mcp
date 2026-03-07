# Qingflow MCP 调用规范（v0.4.x）

本规范用于智能体、前端编排层、后端服务统一接入 `qingflow-mcp`。

## 1. 总体约定

1. 所有工具返回可解析 JSON（成功时在 `structuredContent`，失败时在 `content[0].text` 的 JSON 字符串）。
2. 读工具以“可证明”为目标，支持两种输出模式：
   - `output_profile=compact`（默认）：仅返回核心数据与 `next_page_token`
   - `output_profile=verbose`：额外返回 `evidence`、`meta` 等可审计字段
   - 例外：`qf_query(summary)` 与 `qf_records_aggregate` 即使在 `compact` 模式也会返回 `completeness`，因为它们直接承载统计结论
   - `error_code` / `fix_hint`：在 `verbose` 成功响应中为 `null`，失败时给出结构化错误与修复建议
3. 统计结论必须看 `is_complete`（`verbose` 模式）：
   - `is_complete=true` 才能直接用于最终统计结论
   - `is_complete=false` 只能视为样本/部分结果
4. 默认硬限制：
   - 行上限默认 `200`（可由 `max_rows` 或 `max_items` 下调，最大仍是 `200`）
   - `select_columns` 最大 `2`
   - `max_columns` 最大 `2`
   - 导出工具 `max_rows` 最大 `10000`（`QINGFLOW_EXPORT_MAX_ROWS`）
5. 参数契约（P1）：
   - 正式执行工具遵循严格 JSON 契约：number 就是 number，array 就是 array，object 就是 object，boolean 就是 boolean
   - `additionalProperties=false` 的工具会拒绝未知字段
   - 不要把数组、对象、数字、布尔写成字符串
   - 若模型暂时拿不准参数形状，先调用 `qf_query_plan` 做预检，再执行正式工具
6. 超时保护（P0）：
   - 默认单次上游请求超时：`QINGFLOW_REQUEST_TIMEOUT_MS=18000`
   - 默认工具执行预算：`QINGFLOW_EXECUTION_BUDGET_MS=20000`
   - 默认 `scan_max_pages=10`，避免在 25s tool timeout 场景下超时。

## 2. 完整性协议（completeness）

读工具返回的 `completeness` 至少包含以下基础字段：

- `result_amount`: 服务端已知总条数
- `returned_items`: 本次返回条数
- `fetched_pages`: 本次实际拉取页数
- `requested_pages`: 本次请求期望拉取页数
- `actual_scanned_pages`: 本次实际扫描页数
- `has_more`: 是否还有下一页
- `next_page_token`: 下一页 token（无则 `null`）
- `is_complete`: 是否完整
- `partial`: 是否部分结果（`!is_complete`）
- `omitted_items`: 由于限流/截断未返回的条数
- `omitted_chars`: 由于大小保护省略的字符量估算

其中 `qf_query(summary)` / `qf_records_aggregate` 还会返回扩展字段，用于区分“源数据没扫全”和“输出被裁剪”：

- `raw_scan_complete`: 底层源数据是否已扫全
- `scan_limit_hit`: 是否因为扫描预算/执行预算命中上限而提前停止
- `scanned_pages`: 实际扫描页数
- `scan_limit`: 本次扫描页上限
- `output_page_complete`: 当前输出层是否完整
- `raw_next_page_token`: 底层源扫描续拉 token；对 `qf_query(summary)` / `qf_records_aggregate` 来说，它会携带累计状态，续拉时必须保持查询参数不变
- `output_next_page_token`: 输出层分页 token（当前一般为 `null`）
- `stop_reason`: 停止原因（如 `source_exhausted` / `execution_budget` / `adaptive_budget`）

判定规则：

- `raw_scan_complete=false`：不能把统计结果当全量结论
- `output_page_complete=false`：说明输出被裁剪（例如 `max_rows` / `max_groups`），但不一定代表底层源数据没扫全
- `is_complete = raw_scan_complete && output_page_complete`

## 3. 证据链协议（evidence，`output_profile=verbose`）

读工具在 `verbose` 模式返回：

- `query_id`: 本次查询唯一 ID
- `app_key`: 查询应用
- `filters`: 生效过滤条件
- `selected_columns`: 生效列集合
- `time_range`: 生效时间范围（可能为 `null`）
- `source_pages`: 本次读取到的原始页码列表

## 4. 严格完整模式（strict_full）

1. 当 `strict_full=true` 且底层源数据未扫全时，工具不返回成功结果，而返回错误：
   - `code = "NEED_MORE_DATA"`
   - `status = "need_more_data"`
   - `details.completeness`
   - `details.evidence`
2. 当 `strict_full=false`（或未开启）时，可返回部分结果，并由上层根据 `completeness` 决策。
3. 对 `qf_query(summary)` 来说，`strict_full` 只约束“源数据必须扫全”；`rows` 受 `max_rows` 裁剪不会触发 `NEED_MORE_DATA`，但会把 `output_page_complete` 标成 `false`。

## 5. 分页规范（确定性）

1. `page_num` 与 `page_token` 互斥。
2. 首次调用建议只传 `page_num`（或不传，默认 1）。
3. 后续翻页优先使用上次返回的 `next_page_token`。
4. 建议显式传：
   - `requested_pages`
   - `scan_max_pages`

## 6. 工具清单与调用规则

## 6.1 `qf_apps_list`

用途：列出工作区应用。

关键入参：
- 可选：`keyword`, `limit`, `offset`, `favourite`, `user_id`

## 6.2 `qf_form_get`

用途：读取应用表单字段元数据（用于字段映射）。

关键入参：
- 必填：`app_key`
- 可选：`include_raw`, `force_refresh`, `user_id`

## 6.3 `qf_field_resolve`

用途：把自然语言字段名/别名映射到稳定 `que_id`。

关键入参：
- 必填：`app_key` + (`query` 或 `queries`)
- 可选：`top_k`, `fuzzy`

## 6.3.1 `qf_value_probe`

用途：探测某个字段的常见值/候选值，并显式返回匹配模式与命中证据。

关键入参：
- 必填：`app_key`, `field`
- 可选：`query`, `match_mode(exact|normalized|contains|prefix|fuzzy)`, `limit`, `scan_max_pages`, `page_size`

返回核心：
- `field`: 已解析字段元数据（`que_id` / `que_title` / `que_type`）
- `requested_match_mode` / `effective_match_mode`
- `provider_translation`
- `candidates[]`: `value` / `display_value` / `count` / `match_strength` / `matched_texts` / `matched_as`
- `matched_values`

适用场景：
- 智能体不确定“北斗/麒麟/追高”是不是某个字段的真实取值
- 执行正式查询前，需要先确认字段值候选和匹配模式

## 6.4 `qf_query_plan`

用途：预检查询参数、字段映射、页数预算与完整性风险；这是唯一允许“先纠偏再执行”的工具。

关键入参：
- 必填：`tool_name`
- 可选：`args`

调用要求：
- 当你不确定字段 id、参数类型、扫描预算是否够用时，先调用 `qf_query_plan`
- 正式工具不要依赖字符串化参数容错；如果参数不是原生 JSON，MCP 边界会直接拒绝

用途：执行前预检（参数归一化、必填检查、字段映射、扫描规模估算）。

关键入参：
- 必填：`tool`
- 可选：`arguments`, `resolve_fields`, `probe`

返回核心：
- `normalized_arguments`
- `validation`（`valid`/`missing_required`/`warnings`）
- `field_mapping`
- `estimate`（页规模和命中上限风险）
- `ready_for_final_conclusion`（当前计划是否适合直接产出最终结论）
- `final_conclusion_blockers` / `recommended_next_actions`

## 6.5 `qf_records_list`

用途：多条记录列表查询（推荐用于数据拉取）。

关键入参：
- 必填：`app_key`, `select_columns`
- 可选：
  - 过滤排序：`filters`, `sort`, `mode`, `type`, `keyword`, `query_logic`
  - 时间过滤：`time_range`
  - 行列限制：`max_rows` / `max_items`（默认 200）、`max_columns`
  - 分页：`page_num` / `page_token`, `page_size`, `requested_pages`, `scan_max_pages`
  - 完整性策略：`strict_full`
  - 输出模式：`output_profile`（`compact`|`verbose`，默认 `compact`）

约束：
- `include_answers=false` 不允许
- `select_columns <= 2`
- `max_columns <= 2`
- `max_rows/max_items <= 200`

## 6.6 `qf_record_get`

用途：单条记录详情查询。

关键入参：
- 必填：`apply_id`, `select_columns`
- 可选：`max_columns`, `output_profile`（默认 `compact`）

约束：
- `select_columns <= 2`
- `max_columns <= 2`

## 6.7 `qf_records_batch_get`

用途：按 `apply_ids` 批量拉详情，输出扁平 `rows`。

关键入参：
- 必填：`app_key`, `apply_ids`, `select_columns`
- 可选：`max_columns`, `output_profile`

## 6.8 `qf_export_csv` / `qf_export_json`

用途：把查询结果写文件，避免大结果直接回传导致上下文爆炸。

关键入参：
- 必填：`app_key`, `select_columns`
- 可选：
  - 查询参数同 `qf_records_list`
  - 导出参数：`file_name`, `export_dir`
  - `max_rows`（最大 `10000`）

返回核心：
- `file_path`, `file_size_bytes`, `row_count`, `columns`, `preview`

## 6.9 `qf_query`

用途：统一读入口，按 `query_mode` 路由到 list/record/summary。

路由规则：
- `query_mode=auto` 时：
  - 有 `apply_id` -> `record`
  - 有汇总参数（如 `amount_column`/`time_range`/`scan_max_pages`）-> `summary`
  - 否则 -> `list`

各模式关键要求：
- `list`：必填 `app_key`, `select_columns`
- `record`：必填 `apply_id`, `select_columns`
- `summary`：必填 `app_key`, `select_columns`

说明：
- `list` 模式中 `time_range` 会自动下推为筛选条件。
- `summary` 默认 `strict_full=true`。
- `output_profile` 默认 `compact`，但 `summary` 模式下仍会返回 `completeness`，避免把部分统计当成全量。

## 6.10 `qf_records_aggregate`

用途：通用聚合工具（把统计计算下沉到 MCP）。

关键入参：
- 必填：`app_key`, `group_by`
- 可选：
  - 指标：`amount_column` / `amount_columns`, `metrics(count|sum|avg|min|max)`, `time_bucket(day|week|month)`, `stat_policy`
  - 过滤：`filters`, `time_range`
  - 分页扫描：`page_num` / `page_token`, `page_size`, `requested_pages`, `scan_max_pages`
  - 结果规模：`max_groups`
  - 完整性：`strict_full`

返回核心：
- `summary`: 总数/总金额
- `groups`: 分组统计（count、amount、占比 + 可选 metrics）
- `completeness`: 无论 `compact/verbose` 都返回，用于判断统计是否可直接下结论
- `evidence`: `output_profile=verbose` 时返回

## 6.11 `qf_record_create` / `qf_record_update` / `qf_operation_get`

用途：写入与异步结果查询。

简要规则：
- create/update 支持 `answers` 与 `fields` 两种写法
- 若返回 `request_id`，通过 `qf_operation_get` 查询最终状态

## 7. 错误协议

失败统一为 JSON（在 MCP `isError=true` 文本里）：

- 通用：
  - `ok=false`
  - `error_code`
  - `message`
  - `fix_hint`
  - 可选 `err_code`, `err_msg`, `http_status`, `details`, `next_page_token`
- 不完整失败（严格模式）：
  - `code="NEED_MORE_DATA"`
  - `status="need_more_data"`
  - `error_code="NEED_MORE_DATA"`
  - `fix_hint` 提示续拉方式
  - `details.completeness`
  - `details.evidence`

## 8. 推荐调用流程

1. 用 `qf_form_get` 获取字段映射。
2. 用 `qf_records_list` 或 `qf_query(list)` 拉列表（带 `select_columns`）。
3. 若需要跨页全量统计：
   - 首选 `qf_records_aggregate` 或 `qf_query(summary)`
   - 开启 `strict_full=true`
4. 若返回 `NEED_MORE_DATA`：按 `raw_next_page_token`（兼容旧字段 `next_page_token`）继续调用直到 `raw_scan_complete=true`。
5. 只要出现以下任一条件，就禁止输出“完整分析”：
   - `is_complete=false`
   - `raw_scan_complete=false`
   - `scan_limit_hit=true`

# Qingflow MCP 调用规范（v0.3.x）

本规范用于智能体、前端编排层、后端服务统一接入 `qingflow-mcp`。

## 1. 总体约定

1. 所有工具返回可解析 JSON（成功时在 `structuredContent`，失败时在 `content[0].text` 的 JSON 字符串）。
2. 读工具以“可证明”为目标，统一返回：
   - `completeness`：完整性信息
   - `evidence`：证据链信息
   - `error_code` / `fix_hint`：成功时为 `null`，失败时给出结构化错误与修复建议
   - `next_page_token`：顶层续拉 token（无则 `null`）
3. 统计结论必须看 `is_complete`：
   - `is_complete=true` 才能直接用于最终统计结论
   - `is_complete=false` 只能视为样本/部分结果
4. 默认硬限制：
   - 行上限默认 `200`（可由 `max_rows` 或 `max_items` 下调，最大仍是 `200`）
   - `select_columns` 最大 `10`
   - `max_columns` 最大 `10`
5. 参数容错（P0）：
   - 支持字符串化 JSON 自动反序列化（如 `select_columns` / `filters` / `group_by`）
   - 数字字符串自动转 number（如 `max_rows: \"50\"`）
   - 布尔字符串自动转 boolean（如 `strict_full: \"true\"`）

## 2. 完整性协议（completeness）

读工具（`qf_records_list` / `qf_query(list|summary)` / `qf_records_aggregate` / `qf_record_get`）会返回：

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

## 3. 证据链协议（evidence）

读工具会返回：

- `query_id`: 本次查询唯一 ID
- `app_key`: 查询应用
- `filters`: 生效过滤条件
- `selected_columns`: 生效列集合
- `time_range`: 生效时间范围（可能为 `null`）
- `source_pages`: 本次读取到的原始页码列表

## 4. 严格完整模式（strict_full）

1. 当 `strict_full=true` 且结果不完整时，工具不返回成功结果，而返回错误：
   - `code = "NEED_MORE_DATA"`
   - `status = "need_more_data"`
   - `details.completeness`
   - `details.evidence`
2. 当 `strict_full=false`（或未开启）时，可返回部分结果，并由上层根据 `is_complete` 决策。

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

## 6.3 `qf_records_list`

用途：多条记录列表查询（推荐用于数据拉取）。

关键入参：
- 必填：`app_key`, `select_columns`
- 可选：
  - 过滤排序：`filters`, `sort`, `mode`, `type`, `keyword`, `query_logic`
  - 时间过滤：`time_range`
  - 行列限制：`max_rows` / `max_items`（默认 200）、`max_columns`
  - 分页：`page_num` / `page_token`, `page_size`, `requested_pages`, `scan_max_pages`
  - 完整性策略：`strict_full`

约束：
- `include_answers=false` 不允许
- `select_columns <= 10`
- `max_columns <= 10`
- `max_rows/max_items <= 200`

## 6.4 `qf_record_get`

用途：单条记录详情查询。

关键入参：
- 必填：`apply_id`, `select_columns`
- 可选：`max_columns`

约束：
- `select_columns <= 10`
- `max_columns <= 10`

## 6.5 `qf_query`

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

## 6.6 `qf_records_aggregate`

用途：通用聚合工具（把统计计算下沉到 MCP）。

关键入参：
- 必填：`app_key`, `group_by`
- 可选：
  - 指标：`amount_column`, `stat_policy`
  - 过滤：`filters`, `time_range`
  - 分页扫描：`page_num` / `page_token`, `page_size`, `requested_pages`, `scan_max_pages`
  - 结果规模：`max_groups`
  - 完整性：`strict_full`

返回核心：
- `summary`: 总数/总金额
- `groups`: 分组统计（count、amount、占比）
- `completeness` + `evidence`

## 6.7 `qf_record_create` / `qf_record_update` / `qf_operation_get`

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
4. 若返回 `NEED_MORE_DATA`：按 `next_page_token` 继续调用直到 `is_complete=true`。

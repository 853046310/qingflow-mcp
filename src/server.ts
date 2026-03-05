#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js"
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js"
import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js"
import { randomUUID } from "node:crypto"
import { z } from "zod"

import { QingflowApiError, QingflowClient, type QingflowResponse } from "./qingflow-client.js"

const MODE_TO_TYPE = {
  todo: 1,
  done: 2,
  mine_approved: 3,
  mine_rejected: 4,
  mine_draft: 5,
  mine_need_improve: 6,
  mine_processing: 7,
  all: 8,
  all_approved: 9,
  all_rejected: 10,
  all_processing: 11,
  cc: 12
} as const

type ModeKey = keyof typeof MODE_TO_TYPE

type JsonPrimitive = string | number | boolean | null

interface FormField {
  queId?: unknown
  queTitle?: unknown
  queType?: unknown
  subQuestionBaseInfos?: unknown
}

interface FormCacheEntry {
  expiresAt: number
  data: QingflowResponse<unknown>
}

interface FieldIndex {
  byId: Map<string, FormField>
  byTitle: Map<string, FormField[]>
}

interface ListQueryState {
  query_id: string
  app_key: string
  selected_columns: string[]
  filters: Array<Record<string, unknown>>
  time_range: {
    column: string
    from: string | null
    to: string | null
    timezone: string | null
  } | null
}

interface ContinuationTokenPayload {
  app_key: string
  next_page_num: number
  page_size: number
}

class NeedMoreDataError extends Error {
  public readonly code = "NEED_MORE_DATA"
  public readonly details: Record<string, unknown>

  constructor(message: string, details: Record<string, unknown>) {
    super(message)
    this.name = "NeedMoreDataError"
    this.details = details
  }
}

class InputValidationError extends Error {
  public readonly errorCode: string
  public readonly fixHint: string
  public readonly details: Record<string, unknown> | null

  constructor(params: {
    message: string
    errorCode: string
    fixHint: string
    details?: Record<string, unknown>
  }) {
    super(params.message)
    this.name = "InputValidationError"
    this.errorCode = params.errorCode
    this.fixHint = params.fixHint
    this.details = params.details ?? null
  }
}

const FORM_CACHE_TTL_MS = Number(process.env.QINGFLOW_FORM_CACHE_TTL_MS ?? "300000")
const formCache = new Map<string, FormCacheEntry>()
const DEFAULT_PAGE_SIZE = 50
const DEFAULT_SCAN_MAX_PAGES = 50
const DEFAULT_ROW_LIMIT = 200
const MAX_LIST_ITEMS_BYTES = toPositiveInt(process.env.QINGFLOW_LIST_MAX_ITEMS_BYTES) ?? 400000

const accessToken = process.env.QINGFLOW_ACCESS_TOKEN
const baseUrl = process.env.QINGFLOW_BASE_URL

if (!accessToken) {
  throw new Error("QINGFLOW_ACCESS_TOKEN is required")
}

if (!baseUrl) {
  throw new Error("QINGFLOW_BASE_URL is required")
}

const client = new QingflowClient({
  accessToken,
  baseUrl
})

const server = new McpServer({
  name: "qingflow-mcp",
  version: "0.3.1"
})

const jsonPrimitiveSchema = z.union([z.string(), z.number(), z.boolean(), z.null()])
const answerValueSchema = z.union([
  jsonPrimitiveSchema,
  z
    .object({
      value: z.unknown().optional(),
      dataValue: z.unknown().optional(),
      id: z.union([z.string(), z.number()]).optional(),
      email: z.string().optional(),
      optionId: z.union([z.string(), z.number()]).optional(),
      otherInfo: z.string().optional(),
      queId: z.union([z.string(), z.number()]).optional(),
      valueStr: z.string().optional(),
      matchValue: z.unknown().optional(),
      ordinal: z.union([z.number(), z.string()]).optional(),
      pluginValue: z.unknown().optional()
    })
    .passthrough()
])

const answerInputSchema = z
  .object({
    que_id: z.union([z.string().min(1), z.number().int()]).optional(),
    queId: z.union([z.string().min(1), z.number().int()]).optional(),
    que_title: z.string().optional(),
    queTitle: z.string().optional(),
    que_type: z.unknown().optional(),
    queType: z.unknown().optional(),
    value: answerValueSchema.optional(),
    values: z.array(answerValueSchema).optional(),
    table_values: z.array(z.array(z.unknown())).optional(),
    tableValues: z.array(z.array(z.unknown())).optional()
  })
  .passthrough()
  .refine((value) => Boolean(value.que_id ?? value.queId), {
    message: "answer item requires que_id or queId"
  })
  .refine(
    (value) =>
      value.value !== undefined ||
      value.values !== undefined ||
      value.table_values !== undefined ||
      value.tableValues !== undefined,
    {
      message: "answer item requires value(s) or table_values"
    }
  )

const fieldValueSchema: z.ZodType<unknown> = z.union([
  jsonPrimitiveSchema,
  z.array(z.unknown()),
  z.record(z.unknown())
])

const apiMetaSchema = z.object({
  provider_err_code: z.number(),
  provider_err_msg: z.string().nullable(),
  base_url: z.string()
})

const completenessSchema = z.object({
  result_amount: z.number().int().nonnegative(),
  returned_items: z.number().int().nonnegative(),
  fetched_pages: z.number().int().nonnegative(),
  requested_pages: z.number().int().positive(),
  actual_scanned_pages: z.number().int().nonnegative(),
  has_more: z.boolean(),
  next_page_token: z.string().nullable(),
  is_complete: z.boolean(),
  partial: z.boolean(),
  omitted_items: z.number().int().nonnegative(),
  omitted_chars: z.number().int().nonnegative()
})

const evidenceSchema = z.object({
  query_id: z.string(),
  app_key: z.string(),
  filters: z.array(z.record(z.unknown())),
  selected_columns: z.array(z.string()),
  time_range: z
    .object({
      column: z.string(),
      from: z.string().nullable(),
      to: z.string().nullable(),
      timezone: z.string().nullable()
    })
    .nullable(),
  source_pages: z.array(z.number().int().positive())
})

const queryContractFields = {
  completeness: completenessSchema,
  evidence: z.record(z.unknown()),
  error_code: z.null(),
  fix_hint: z.null(),
  next_page_token: z.string().nullable()
}

const appSchema = z.object({
  appKey: z.string(),
  appName: z.string()
})

const fieldSummarySchema = z.object({
  que_id: z.union([z.number(), z.string(), z.null()]),
  que_title: z.string().nullable(),
  que_type: z.unknown(),
  has_sub_fields: z.boolean(),
  sub_field_count: z.number().int().nonnegative()
})

const recordItemSchema = z.object({
  apply_id: z.union([z.string(), z.number(), z.null()]),
  app_key: z.string().nullable(),
  apply_num: z.union([z.number(), z.string(), z.null()]),
  apply_time: z.string().nullable(),
  last_update_time: z.string().nullable(),
  answers: z.array(z.unknown()).optional()
})

const operationResultSchema = z.object({
  request_id: z.string(),
  operation_result: z.unknown()
})

const appsInputSchema = z.object({
  user_id: z.string().min(1).optional(),
  favourite: z.union([z.literal(0), z.literal(1)]).optional(),
  keyword: z.string().min(1).optional(),
  limit: z.number().int().positive().max(500).optional(),
  offset: z.number().int().nonnegative().optional()
})

const appsSuccessOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    total_apps: z.number().int().nonnegative(),
    returned_apps: z.number().int().nonnegative(),
    limit: z.number().int().positive(),
    offset: z.number().int().nonnegative(),
    apps: z.array(appSchema)
  }),
  meta: apiMetaSchema
})
const appsOutputSchema = appsSuccessOutputSchema

const formInputSchema = z.object({
  app_key: z.string().min(1),
  user_id: z.string().min(1).optional(),
  force_refresh: z.boolean().optional(),
  include_raw: z.boolean().optional()
})

const formSuccessOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    app_key: z.string(),
    total_fields: z.number().int().nonnegative(),
    field_summaries: z.array(fieldSummarySchema),
    form: z.unknown().optional()
  }),
  meta: apiMetaSchema
})
const formOutputSchema = formSuccessOutputSchema

const listInputSchema = z
  .preprocess(
    normalizeListInput,
    z.object({
      app_key: z.string().min(1).optional(),
    user_id: z.string().min(1).optional(),
    page_num: z.number().int().positive().optional(),
    page_token: z.string().min(1).optional(),
    page_size: z.number().int().positive().max(200).optional(),
    requested_pages: z.number().int().positive().max(500).optional(),
    scan_max_pages: z.number().int().positive().max(500).optional(),
    mode: z
      .enum([
        "todo",
        "done",
        "mine_approved",
        "mine_rejected",
        "mine_draft",
        "mine_need_improve",
        "mine_processing",
        "all",
        "all_approved",
        "all_rejected",
        "all_processing",
        "cc"
      ])
      .optional(),
    type: z.number().int().min(1).max(12).optional(),
    keyword: z.string().optional(),
    query_logic: z.enum(["and", "or"]).optional(),
    apply_ids: z.array(z.union([z.string(), z.number()])).optional(),
    sort: z
      .array(
        z.object({
          que_id: z.union([z.string().min(1), z.number().int()]),
          ascend: z.boolean().optional()
        })
      )
      .optional(),
    filters: z
      .array(
        z.object({
          que_id: z.union([z.string().min(1), z.number().int()]).optional(),
          search_key: z.string().optional(),
          search_keys: z.array(z.string()).optional(),
          min_value: z.string().optional(),
          max_value: z.string().optional(),
          scope: z.number().int().optional(),
          search_options: z.array(z.union([z.string(), z.number()])).optional(),
          search_user_ids: z.array(z.string()).optional()
        })
      )
      .optional(),
    time_range: z
      .object({
        column: z.union([z.string().min(1), z.number().int()]),
        from: z.string().optional(),
        to: z.string().optional(),
        timezone: z.string().optional()
      })
      .optional(),
    max_rows: z.number().int().positive().max(200).optional(),
    max_items: z.number().int().positive().max(200).optional(),
    max_columns: z.number().int().positive().max(10).optional(),
    // Strict mode: callers must explicitly choose columns.
    select_columns: z
      .array(z.union([z.string().min(1), z.number().int()]))
      .min(1)
      .max(10)
      .optional(),
    include_answers: z.boolean().optional(),
    strict_full: z.boolean().optional()
    })
  )
  .refine((value) => value.include_answers !== false, {
    message: "include_answers=false is not allowed in strict column mode"
  })
  .refine((value) => !(value.page_num !== undefined && value.page_token !== undefined), {
    message: "page_num and page_token cannot be used together"
  })

const listSuccessOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    app_key: z.string(),
    pagination: z.object({
      page_num: z.number().int().positive(),
      page_size: z.number().int().positive(),
      page_amount: z.number().int().nonnegative().nullable(),
      result_amount: z.number().int().nonnegative()
    }),
    items: z.array(recordItemSchema),
    applied_limits: z
      .object({
        include_answers: z.boolean(),
        row_cap: z.number().int().nonnegative(),
        column_cap: z.number().int().positive().nullable(),
        selected_columns: z.array(z.string())
      })
      .optional(),
    completeness: completenessSchema,
    evidence: evidenceSchema
  }),
  ...queryContractFields,
  meta: apiMetaSchema
})
const listOutputSchema = listSuccessOutputSchema

const recordGetInputSchema = z.preprocess(
  normalizeRecordGetInput,
  z.object({
    apply_id: z.union([z.string().min(1), z.number().int()]),
    max_columns: z.number().int().positive().max(10).optional(),
    select_columns: z
      .array(z.union([z.string().min(1), z.number().int()]))
      .min(1)
      .max(10)
      .optional()
  })
)

const recordGetSuccessOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    apply_id: z.union([z.string(), z.number(), z.null()]),
    answer_count: z.number().int().nonnegative(),
    record: z.unknown(),
    applied_limits: z
      .object({
        column_cap: z.number().int().positive().nullable(),
        selected_columns: z.array(z.string()).nullable()
      })
      .optional(),
    completeness: completenessSchema,
    evidence: z.object({
      query_id: z.string(),
      apply_id: z.string(),
      selected_columns: z.array(z.string())
    })
  }),
  ...queryContractFields,
  meta: apiMetaSchema
})
const recordGetOutputSchema = recordGetSuccessOutputSchema

const createInputSchema = z
  .object({
    app_key: z.string().min(1),
    user_id: z.string().min(1).optional(),
    force_refresh_form: z.boolean().optional(),
    apply_user: z
      .object({
        email: z.string().optional(),
        areaCode: z.string().optional(),
        mobile: z.string().optional()
      })
      .passthrough()
      .optional(),
    answers: z.array(answerInputSchema).optional(),
    fields: z.record(fieldValueSchema).optional()
  })
  .refine((value) => hasWritePayload(value.answers, value.fields), {
    message: "Either answers or fields is required"
  })

const createSuccessOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    request_id: z.string().nullable(),
    apply_id: z.union([z.string(), z.number(), z.null()]),
    async_hint: z.string()
  }),
  meta: apiMetaSchema
})
const createOutputSchema = createSuccessOutputSchema

const updateInputSchema = z
  .object({
    apply_id: z.union([z.string().min(1), z.number().int()]),
    app_key: z.string().min(1).optional(),
    user_id: z.string().min(1).optional(),
    force_refresh_form: z.boolean().optional(),
    answers: z.array(answerInputSchema).optional(),
    fields: z.record(fieldValueSchema).optional()
  })
  .refine((value) => hasWritePayload(value.answers, value.fields), {
    message: "Either answers or fields is required"
  })

const updateSuccessOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    request_id: z.string().nullable(),
    async_hint: z.string()
  }),
  meta: apiMetaSchema
})
const updateOutputSchema = updateSuccessOutputSchema

const operationInputSchema = z.object({
  request_id: z.string().min(1)
})

const operationSuccessOutputSchema = z.object({
  ok: z.literal(true),
  data: operationResultSchema,
  meta: apiMetaSchema
})
const operationOutputSchema = operationSuccessOutputSchema

const queryInputSchema = z
  .preprocess(
    normalizeQueryInput,
    z.object({
      query_mode: z.enum(["auto", "list", "record", "summary"]).optional(),
      app_key: z.string().min(1).optional(),
      apply_id: z.union([z.string().min(1), z.number().int()]).optional(),
      user_id: z.string().min(1).optional(),
      page_num: z.number().int().positive().optional(),
      page_token: z.string().min(1).optional(),
      page_size: z.number().int().positive().max(200).optional(),
      requested_pages: z.number().int().positive().max(500).optional(),
      mode: z
        .enum([
          "todo",
          "done",
          "mine_approved",
          "mine_rejected",
          "mine_draft",
          "mine_need_improve",
          "mine_processing",
          "all",
          "all_approved",
          "all_rejected",
          "all_processing",
          "cc"
        ])
        .optional(),
      type: z.number().int().min(1).max(12).optional(),
      keyword: z.string().optional(),
      query_logic: z.enum(["and", "or"]).optional(),
      apply_ids: z.array(z.union([z.string(), z.number()])).optional(),
      sort: z
        .array(
          z.object({
            que_id: z.union([z.string().min(1), z.number().int()]),
            ascend: z.boolean().optional()
          })
        )
        .optional(),
      filters: z
        .array(
          z.object({
            que_id: z.union([z.string().min(1), z.number().int()]).optional(),
            search_key: z.string().optional(),
            search_keys: z.array(z.string()).optional(),
            min_value: z.string().optional(),
            max_value: z.string().optional(),
            scope: z.number().int().optional(),
            search_options: z.array(z.union([z.string(), z.number()])).optional(),
            search_user_ids: z.array(z.string()).optional()
          })
        )
        .optional(),
      max_rows: z.number().int().positive().max(200).optional(),
      max_items: z.number().int().positive().max(200).optional(),
      max_columns: z.number().int().positive().max(10).optional(),
      select_columns: z
        .array(z.union([z.string().min(1), z.number().int()]))
        .min(1)
        .max(10)
        .optional(),
      include_answers: z.boolean().optional(),
      amount_column: z.union([z.string().min(1), z.number().int()]).optional(),
      time_range: z
        .object({
          column: z.union([z.string().min(1), z.number().int()]),
          from: z.string().optional(),
          to: z.string().optional(),
          timezone: z.string().optional()
        })
        .optional(),
      stat_policy: z
        .object({
          include_negative: z.boolean().optional(),
          include_null: z.boolean().optional()
        })
        .optional(),
      scan_max_pages: z.number().int().positive().max(500).optional(),
      strict_full: z.boolean().optional()
    })
  )
  .refine((value) => !(value.page_num !== undefined && value.page_token !== undefined), {
    message: "page_num and page_token cannot be used together"
  })

const querySummaryOutputSchema = z.object({
  summary: z.object({
    total_count: z.number().int().nonnegative(),
    total_amount: z.number().nullable(),
    by_day: z.array(
      z.object({
        day: z.string(),
        count: z.number().int().nonnegative(),
        amount_total: z.number().nullable()
      })
    ),
    missing_count: z.number().int().nonnegative()
  }),
  rows: z.array(z.record(z.unknown())),
  completeness: completenessSchema,
  evidence: evidenceSchema,
  meta: z.object({
    field_mapping: z.array(
      z.object({
        role: z.enum(["row", "amount", "time"]),
        requested: z.string(),
        que_id: z.union([z.string(), z.number()]),
        que_title: z.string().nullable(),
        que_type: z.unknown()
      })
    ),
    filters: z.object({
      app_key: z.string(),
      time_range: z
        .object({
          column: z.string(),
          from: z.string().nullable(),
          to: z.string().nullable(),
          timezone: z.string()
        })
        .nullable()
    }),
    stat_policy: z.object({
      include_negative: z.boolean(),
      include_null: z.boolean()
    }),
    execution: z.object({
      scanned_records: z.number().int().nonnegative(),
      scanned_pages: z.number().int().nonnegative(),
      truncated: z.boolean(),
      row_cap: z.number().int().positive(),
      column_cap: z.number().int().positive().nullable(),
      scan_max_pages: z.number().int().positive()
    })
  })
})

const querySuccessOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    mode: z.enum(["list", "record", "summary"]),
    source_tool: z.enum(["qf_records_list", "qf_record_get", "qf_records_summary"]),
    list: listSuccessOutputSchema.shape.data.optional(),
    record: recordGetSuccessOutputSchema.shape.data.optional(),
    summary: querySummaryOutputSchema.optional()
  }),
  ...queryContractFields,
  meta: apiMetaSchema
})
const queryOutputSchema = querySuccessOutputSchema

const aggregateInputSchema = z
  .preprocess(
    normalizeAggregateInput,
    z.object({
      app_key: z.string().min(1),
    user_id: z.string().min(1).optional(),
    page_num: z.number().int().positive().optional(),
    page_token: z.string().min(1).optional(),
    page_size: z.number().int().positive().max(200).optional(),
    requested_pages: z.number().int().positive().max(500).optional(),
    scan_max_pages: z.number().int().positive().max(500).optional(),
    mode: z
      .enum([
        "todo",
        "done",
        "mine_approved",
        "mine_rejected",
        "mine_draft",
        "mine_need_improve",
        "mine_processing",
        "all",
        "all_approved",
        "all_rejected",
        "all_processing",
        "cc"
      ])
      .optional(),
    type: z.number().int().min(1).max(12).optional(),
    keyword: z.string().optional(),
    query_logic: z.enum(["and", "or"]).optional(),
    apply_ids: z.array(z.union([z.string(), z.number()])).optional(),
    sort: z
      .array(
        z.object({
          que_id: z.union([z.string().min(1), z.number().int()]),
          ascend: z.boolean().optional()
        })
      )
      .optional(),
    filters: z
      .array(
        z.object({
          que_id: z.union([z.string().min(1), z.number().int()]).optional(),
          search_key: z.string().optional(),
          search_keys: z.array(z.string()).optional(),
          min_value: z.string().optional(),
          max_value: z.string().optional(),
          scope: z.number().int().optional(),
          search_options: z.array(z.union([z.string(), z.number()])).optional(),
          search_user_ids: z.array(z.string()).optional()
        })
      )
      .optional(),
    time_range: z
      .object({
        column: z.union([z.string().min(1), z.number().int()]),
        from: z.string().optional(),
        to: z.string().optional(),
        timezone: z.string().optional()
      })
      .optional(),
    group_by: z.array(z.union([z.string().min(1), z.number().int()])).min(1).max(20),
    amount_column: z.union([z.string().min(1), z.number().int()]).optional(),
    stat_policy: z
      .object({
        include_negative: z.boolean().optional(),
        include_null: z.boolean().optional()
      })
      .optional(),
    max_groups: z.number().int().positive().max(2000).optional(),
    strict_full: z.boolean().optional()
    })
  )
  .refine((value) => !(value.page_num !== undefined && value.page_token !== undefined), {
    message: "page_num and page_token cannot be used together"
  })

const aggregateOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    app_key: z.string(),
    summary: z.object({
      total_count: z.number().int().nonnegative(),
      total_amount: z.number().nullable()
    }),
    groups: z.array(
      z.object({
        group: z.record(z.unknown()),
        count: z.number().int().nonnegative(),
        count_ratio: z.number().min(0).max(1),
        amount_total: z.number().nullable(),
        amount_ratio: z.number().nullable()
      })
    ),
    completeness: completenessSchema,
    evidence: evidenceSchema,
    meta: z.object({
      field_mapping: z.array(
        z.object({
          role: z.enum(["group_by", "amount", "time"]),
          requested: z.string(),
          que_id: z.union([z.string(), z.number()]),
          que_title: z.string().nullable(),
          que_type: z.unknown()
        })
      ),
      stat_policy: z.object({
        include_negative: z.boolean(),
        include_null: z.boolean()
      })
    })
  }),
  ...queryContractFields,
  meta: apiMetaSchema
})

server.registerTool(
  "qf_apps_list",
  {
    title: "Qingflow Apps List",
    description: "List Qingflow apps with optional filtering and client-side slicing.",
    inputSchema: appsInputSchema,
    outputSchema: appsOutputSchema,
    annotations: {
      readOnlyHint: true,
      idempotentHint: true
    }
  },
  async (args) => {
    try {
      const response = await client.listApps({
        userId: args.user_id,
        favourite: args.favourite
      })
      const appList = asArray(asObject(response.result)?.appList)
        .map((item) => asObject(item))
        .filter((item): item is Record<string, unknown> => Boolean(item))
        .map((item) => ({
          appKey: String(item.appKey ?? ""),
          appName: String(item.appName ?? "")
        }))
        .filter((item) => item.appKey.length > 0)

      const keyword = args.keyword?.trim().toLowerCase()
      const filtered = keyword
        ? appList.filter(
            (item) =>
              item.appKey.toLowerCase().includes(keyword) ||
              item.appName.toLowerCase().includes(keyword)
          )
        : appList

      const offset = args.offset ?? 0
      const limit = args.limit ?? 50
      const apps = filtered.slice(offset, offset + limit)

      return okResult(
        {
          ok: true,
          data: {
            total_apps: filtered.length,
            returned_apps: apps.length,
            limit,
            offset,
            apps
          },
          meta: buildMeta(response)
        },
        `Returned ${apps.length}/${filtered.length} apps`
      )
    } catch (error) {
      return errorResult(error)
    }
  }
)

server.registerTool(
  "qf_form_get",
  {
    title: "Qingflow Form Get",
    description: "Get form metadata and compact field summaries for one app.",
    inputSchema: formInputSchema,
    outputSchema: formOutputSchema,
    annotations: {
      readOnlyHint: true,
      idempotentHint: true
    }
  },
  async (args) => {
    try {
      const response = await getFormCached(args.app_key, args.user_id, Boolean(args.force_refresh))
      const form = asObject(response.result)
      const fieldSummaries = extractFieldSummaries(form)

      return okResult(
        {
          ok: true,
          data: {
            app_key: args.app_key,
            total_fields: fieldSummaries.length,
            field_summaries: fieldSummaries,
            ...(args.include_raw ? { form: response.result } : {})
          },
          meta: buildMeta(response)
        },
        `Fetched form for ${args.app_key}`
      )
    } catch (error) {
      return errorResult(error)
    }
  }
)

server.registerTool(
  "qf_records_list",
  {
    title: "Qingflow Records List",
    description: "List records with pagination, filters and sorting.",
    inputSchema: listInputSchema,
    outputSchema: listOutputSchema,
    annotations: {
      readOnlyHint: true,
      idempotentHint: true
    }
  },
  async (args) => {
    try {
      const executed = await executeRecordsList(args)
      return okResult(executed.payload, executed.message)
    } catch (error) {
      return errorResult(error)
    }
  }
)

server.registerTool(
  "qf_record_get",
  {
    title: "Qingflow Record Get",
    description: "Get one record by applyId.",
    inputSchema: recordGetInputSchema,
    outputSchema: recordGetOutputSchema,
    annotations: {
      readOnlyHint: true,
      idempotentHint: true
    }
  },
  async (args) => {
    try {
      const executed = await executeRecordGet(args)
      return okResult(executed.payload, executed.message)
    } catch (error) {
      return errorResult(error)
    }
  }
)

server.registerTool(
  "qf_query",
  {
    title: "Qingflow Unified Query",
    description:
      "Unified read entry for list/record/summary. Use query_mode=auto to route automatically.",
    inputSchema: queryInputSchema,
    outputSchema: queryOutputSchema,
    annotations: {
      readOnlyHint: true,
      idempotentHint: true
    }
  },
  async (args) => {
    try {
      const routedMode = resolveQueryMode(args)

      if (routedMode === "record") {
        const recordArgs = buildRecordGetArgsFromQuery(args)
        const executed = await executeRecordGet(recordArgs)
        const completeness = executed.payload.completeness
        const evidence = executed.payload.evidence
        return okResult(
          {
            ok: true,
            data: {
              mode: "record",
              source_tool: "qf_record_get",
              record: executed.payload.data
            },
            completeness,
            evidence,
            error_code: null,
            fix_hint: null,
            next_page_token: completeness.next_page_token,
            meta: executed.payload.meta
          },
          executed.message
        )
      }

      if (routedMode === "summary") {
        const executed = await executeRecordsSummary(args)
        const completeness = executed.data.completeness
        const evidence = executed.data.evidence
        return okResult(
          {
            ok: true,
            data: {
              mode: "summary",
              source_tool: "qf_records_summary",
              summary: executed.data
            },
            completeness,
            evidence,
            error_code: null,
            fix_hint: null,
            next_page_token: completeness.next_page_token,
            meta: executed.meta
          },
          executed.message
        )
      }

      const listArgs = buildListArgsFromQuery(args)
      const executed = await executeRecordsList(listArgs)
      const completeness = executed.payload.completeness
      const evidence = executed.payload.evidence
      return okResult(
        {
          ok: true,
          data: {
            mode: "list",
            source_tool: "qf_records_list",
            list: executed.payload.data
          },
          completeness,
          evidence,
          error_code: null,
          fix_hint: null,
          next_page_token: completeness.next_page_token,
          meta: executed.payload.meta
        },
        executed.message
      )
    } catch (error) {
      return errorResult(error)
    }
  }
)

server.registerTool(
  "qf_record_create",
  {
    title: "Qingflow Record Create",
    description:
      "Create one record. Supports explicit answers and ergonomic fields mapping (title or queId).",
    inputSchema: createInputSchema,
    outputSchema: createOutputSchema,
    annotations: {
      readOnlyHint: false,
      idempotentHint: false
    }
  },
  async (args) => {
    try {
      const form =
        needsFormResolution(args.fields) || Boolean(args.force_refresh_form)
          ? await getFormCached(args.app_key, args.user_id, Boolean(args.force_refresh_form))
          : null

      const normalizedAnswers = resolveAnswers({
        explicitAnswers: args.answers,
        fields: args.fields,
        form: form?.result
      })

      const payload: Record<string, unknown> = {
        answers: normalizedAnswers
      }
      if (args.apply_user) {
        payload.applyUser = args.apply_user
      }

      const response = await client.createRecord(args.app_key, payload, {
        userId: args.user_id
      })

      const result = asObject(response.result)
      return okResult(
        {
          ok: true,
          data: {
            request_id: asNullableString(result?.requestId),
            apply_id: (result?.applyId as string | number | null | undefined) ?? null,
            async_hint: "Use qf_operation_get with request_id when apply_id is null."
          },
          meta: buildMeta(response)
        },
        `Create request sent for app ${args.app_key}`
      )
    } catch (error) {
      return errorResult(error)
    }
  }
)

server.registerTool(
  "qf_record_update",
  {
    title: "Qingflow Record Update",
    description: "Patch one record by applyId with explicit answers or ergonomic fields mapping.",
    inputSchema: updateInputSchema,
    outputSchema: updateOutputSchema,
    annotations: {
      readOnlyHint: false,
      idempotentHint: false
    }
  },
  async (args) => {
    try {
      const requiresForm = needsFormResolution(args.fields)
      if (requiresForm && !args.app_key) {
        throw new Error("app_key is required when fields uses title-based keys")
      }

      const form =
        requiresForm && args.app_key
          ? await getFormCached(args.app_key, args.user_id, Boolean(args.force_refresh_form))
          : null

      const normalizedAnswers = resolveAnswers({
        explicitAnswers: args.answers,
        fields: args.fields,
        form: form?.result
      })

      const response = await client.updateRecord(
        String(args.apply_id),
        { answers: normalizedAnswers },
        { userId: args.user_id }
      )
      const result = asObject(response.result)

      return okResult(
        {
          ok: true,
          data: {
            request_id: asNullableString(result?.requestId),
            async_hint: "Use qf_operation_get with request_id to fetch update result when needed."
          },
          meta: buildMeta(response)
        },
        `Update request sent for apply ${String(args.apply_id)}`
      )
    } catch (error) {
      return errorResult(error)
    }
  }
)

server.registerTool(
  "qf_operation_get",
  {
    title: "Qingflow Operation Get",
    description: "Resolve async operation result by request_id.",
    inputSchema: operationInputSchema,
    outputSchema: operationOutputSchema,
    annotations: {
      readOnlyHint: true,
      idempotentHint: true
    }
  },
  async (args) => {
    try {
      const response = await client.getOperation(args.request_id)
      return okResult(
        {
          ok: true,
          data: {
            request_id: args.request_id,
            operation_result: response.result
          },
          meta: buildMeta(response)
        },
        `Resolved operation ${args.request_id}`
      )
    } catch (error) {
      return errorResult(error)
    }
  }
)

server.registerTool(
  "qf_records_aggregate",
  {
    title: "Qingflow Records Aggregate",
    description:
      "Aggregate records by group_by columns with optional amount metrics. Designed for deterministic, auditable statistics.",
    inputSchema: aggregateInputSchema,
    outputSchema: aggregateOutputSchema,
    annotations: {
      readOnlyHint: true,
      idempotentHint: true
    }
  },
  async (args) => {
    try {
      const executed = await executeRecordsAggregate(args)
      return okResult(executed.payload, executed.message)
    } catch (error) {
      return errorResult(error)
    }
  }
)

async function main(): Promise<void> {
  const cliExitCode = await runCli(process.argv.slice(2))
  if (cliExitCode !== null) {
    process.exitCode = cliExitCode
    return
  }

  const transport = new StdioServerTransport()
  await server.connect(transport)
}

void main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : "Unknown error"}\n`)
  process.exitCode = 1
})

async function runCli(argv: string[]): Promise<number | null> {
  if (argv.length === 0 || argv[0] === "--stdio-server") {
    return null
  }

  const [command, ...rest] = argv
  if (command === "--help" || command === "-h") {
    printCliHelp()
    return 0
  }

  if (command !== "cli") {
    printCliHelp()
    return 2
  }

  const [subcommand, ...subArgs] = rest
  if (!subcommand || subcommand === "--help" || subcommand === "-h") {
    printCliHelp()
    return 0
  }

  if (subcommand === "tools") {
    return runCliTools(subArgs)
  }

  if (subcommand === "call") {
    return runCliCall(subArgs)
  }

  process.stderr.write(`Unknown CLI subcommand: ${subcommand}\n`)
  printCliHelp()
  return 2
}

async function runCliTools(args: string[]): Promise<number> {
  let options: { argsText?: string; help: boolean; json: boolean }
  try {
    options = parseCliFlags(args)
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : "Invalid CLI options"}\n`)
    return 2
  }
  if (options.help) {
    process.stdout.write("Usage: qingflow-mcp cli tools [--json]\n")
    return 0
  }
  if (options.argsText !== undefined) {
    process.stderr.write("--args is not supported for 'cli tools'\n")
    return 2
  }

  const call = await callLocalMcp("tools")
  if (call.ok) {
    if (options.json) {
      process.stdout.write(`${JSON.stringify(call.tools, null, 2)}\n`)
      return 0
    }
    for (const tool of call.tools) {
      process.stdout.write(`${tool.name}\t${tool.description ?? ""}\n`)
    }
    return 0
  }

  process.stderr.write(`${JSON.stringify(call.error, null, 2)}\n`)
  return 1
}

async function runCliCall(args: string[]): Promise<number> {
  if (args.length === 0) {
    process.stderr.write("Usage: qingflow-mcp cli call <tool_name> [--args '{\"key\":\"value\"}']\n")
    return 2
  }

  const [toolName, ...flagArgs] = args
  let options: { argsText?: string; help: boolean; json: boolean }
  try {
    options = parseCliFlags(flagArgs)
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : "Invalid CLI options"}\n`)
    return 2
  }
  if (options.help) {
    process.stdout.write("Usage: qingflow-mcp cli call <tool_name> [--args '{\"key\":\"value\"}'] [--json]\n")
    return 0
  }

  const inputText = options.argsText ?? (process.stdin.isTTY ? "{}" : await readStdinText())
  let parsedInput: unknown
  try {
    parsedInput = inputText.trim() ? JSON.parse(inputText) : {}
  } catch {
    process.stderr.write("Invalid JSON for --args or stdin body\n")
    return 2
  }
  if (!parsedInput || typeof parsedInput !== "object" || Array.isArray(parsedInput)) {
    process.stderr.write("Tool arguments must be a JSON object\n")
    return 2
  }

  const call = await callLocalMcp("call", {
    toolName,
    args: parsedInput
  })

  if (call.ok) {
    if (options.json || typeof call.payload === "object") {
      process.stdout.write(`${JSON.stringify(call.payload, null, 2)}\n`)
      return 0
    }
    process.stdout.write(`${String(call.payload)}\n`)
    return 0
  }

  process.stderr.write(`${JSON.stringify(call.error, null, 2)}\n`)
  return 1
}

function parseCliFlags(args: string[]): { argsText?: string; help: boolean; json: boolean } {
  let argsText: string | undefined
  let help = false
  let json = false

  for (let i = 0; i < args.length; i += 1) {
    const token = args[i]
    if (token === "--help" || token === "-h") {
      help = true
      continue
    }
    if (token === "--json") {
      json = true
      continue
    }
    if (token === "--args") {
      const next = args[i + 1]
      if (next === undefined) {
        throw new Error("--args requires a JSON value")
      }
      argsText = next
      i += 1
      continue
    }
    if (token.startsWith("--args=")) {
      argsText = token.slice("--args=".length)
      continue
    }
    throw new Error(`Unknown CLI option: ${token}`)
  }

  return { argsText, help, json }
}

async function callLocalMcp(
  mode: "tools"
): Promise<{ ok: true; tools: Array<{ name: string; description?: string }> } | { ok: false; error: unknown }>
async function callLocalMcp(
  mode: "call",
  params: { toolName: string; args: unknown }
): Promise<{ ok: true; payload: unknown } | { ok: false; error: unknown }>
async function callLocalMcp(
  mode: "tools" | "call",
  params?: { toolName: string; args: unknown }
): Promise<
  | { ok: true; tools: Array<{ name: string; description?: string }> }
  | { ok: true; payload: unknown }
  | { ok: false; error: unknown }
> {
  const entrypoint = process.argv[1]
  if (!entrypoint) {
    return { ok: false, error: { message: "Cannot locate current executable entrypoint" } }
  }
  const childEnv: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      childEnv[key] = value
    }
  }

  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [entrypoint, "--stdio-server"],
    cwd: process.cwd(),
    env: childEnv,
    stderr: "pipe"
  })

  if (transport.stderr) {
    transport.stderr.on("data", () => {
      // Keep stderr drained to avoid child process backpressure.
    })
  }

  const localClient = new Client({
    name: "qingflow-mcp-cli",
    version: "0.3.0"
  })

  try {
    await localClient.connect(transport)

    if (mode === "tools") {
      const listed = await localClient.listTools()
      const tools = listed.tools.map((tool) => ({
        name: tool.name,
        description: tool.description
      }))
      return { ok: true, tools }
    }

    if (!params) {
      throw new Error("Missing tool call params")
    }

    const result = await localClient.callTool({
      name: params.toolName,
      arguments: params.args as Record<string, unknown>
    })

    if (result.isError) {
      const payload = tryParseToolPayload(result)
      return { ok: false, error: payload ?? { message: `Tool ${params.toolName} failed`, result } }
    }

    const payload = tryParseToolPayload(result)
    return { ok: true, payload: payload ?? result }
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? { message: error.message } : error
    }
  } finally {
    await localClient.close().catch(() => {})
  }
}

function tryParseToolPayload(result: unknown): unknown | null {
  const obj = asObject(result)
  if (!obj) {
    return null
  }

  if (obj.structuredContent !== undefined) {
    return obj.structuredContent
  }

  const textItems = Array.isArray(obj.content)
    ? obj.content.filter(
        (item) =>
          Boolean(item) &&
          typeof item === "object" &&
          (item as { type?: unknown }).type === "text" &&
          typeof (item as { text?: unknown }).text === "string"
      )
    : []

  for (const item of textItems) {
    const text = (item as { text: string }).text
    try {
      return JSON.parse(text)
    } catch {
      // keep scanning and fallback
    }
  }
  return null
}

async function readStdinText(): Promise<string> {
  const chunks: string[] = []
  for await (const chunk of process.stdin) {
    chunks.push(String(chunk))
  }
  return chunks.join("")
}

function printCliHelp(): void {
  process.stdout.write(`qingflow-mcp usage

Default (MCP stdio server):
  qingflow-mcp

CLI mode:
  qingflow-mcp cli tools [--json]
  qingflow-mcp cli call <tool_name> [--args '{"key":"value"}'] [--json]
  echo '{"app_key":"xxx","mode":"all","select_columns":[1001]}' | qingflow-mcp cli call qf_query
`)
}

function hasWritePayload(
  answers?: z.infer<typeof answerInputSchema>[],
  fields?: Record<string, unknown>
): boolean {
  return Boolean((answers && answers.length > 0) || (fields && Object.keys(fields).length > 0))
}

function buildMeta(response: QingflowResponse<unknown>) {
  return {
    provider_err_code: response.errCode,
    provider_err_msg: response.errMsg || null,
    base_url: baseUrl as string
  }
}

function missingRequiredFieldError(params: {
  field: string
  tool: string
  fixHint: string
}): InputValidationError {
  return new InputValidationError({
    message: `Missing required field "${params.field}" for ${params.tool}`,
    errorCode: "MISSING_REQUIRED_FIELD",
    fixHint: params.fixHint,
    details: {
      field: params.field,
      tool: params.tool
    }
  })
}

function normalizeListInput(raw: unknown): unknown {
  const obj = asObject(raw)
  if (!obj) {
    return raw
  }
  return {
    ...obj,
    page_num: coerceNumberLike(obj.page_num),
    page_size: coerceNumberLike(obj.page_size),
    requested_pages: coerceNumberLike(obj.requested_pages),
    scan_max_pages: coerceNumberLike(obj.scan_max_pages),
    type: coerceNumberLike(obj.type),
    max_rows: coerceNumberLike(obj.max_rows),
    max_items: coerceNumberLike(obj.max_items),
    max_columns: coerceNumberLike(obj.max_columns),
    strict_full: coerceBooleanLike(obj.strict_full),
    include_answers: coerceBooleanLike(obj.include_answers),
    apply_ids: normalizeIdArrayInput(obj.apply_ids),
    sort: normalizeSortInput(obj.sort),
    filters: normalizeFiltersInput(obj.filters),
    select_columns: normalizeSelectorListInput(obj.select_columns),
    time_range: normalizeTimeRangeInput(obj.time_range)
  }
}

function normalizeRecordGetInput(raw: unknown): unknown {
  const obj = asObject(raw)
  if (!obj) {
    return raw
  }
  return {
    ...obj,
    apply_id: coerceNumberLike(obj.apply_id),
    max_columns: coerceNumberLike(obj.max_columns),
    select_columns: normalizeSelectorListInput(obj.select_columns)
  }
}

function normalizeQueryInput(raw: unknown): unknown {
  const obj = asObject(raw)
  if (!obj) {
    return raw
  }
  return {
    ...obj,
    page_num: coerceNumberLike(obj.page_num),
    page_size: coerceNumberLike(obj.page_size),
    requested_pages: coerceNumberLike(obj.requested_pages),
    scan_max_pages: coerceNumberLike(obj.scan_max_pages),
    type: coerceNumberLike(obj.type),
    max_rows: coerceNumberLike(obj.max_rows),
    max_items: coerceNumberLike(obj.max_items),
    max_columns: coerceNumberLike(obj.max_columns),
    apply_id: coerceNumberLike(obj.apply_id),
    strict_full: coerceBooleanLike(obj.strict_full),
    include_answers: coerceBooleanLike(obj.include_answers),
    apply_ids: normalizeIdArrayInput(obj.apply_ids),
    sort: normalizeSortInput(obj.sort),
    filters: normalizeFiltersInput(obj.filters),
    select_columns: normalizeSelectorListInput(obj.select_columns),
    time_range: normalizeTimeRangeInput(obj.time_range)
  }
}

function normalizeAggregateInput(raw: unknown): unknown {
  const obj = asObject(raw)
  if (!obj) {
    return raw
  }
  return {
    ...obj,
    page_num: coerceNumberLike(obj.page_num),
    page_size: coerceNumberLike(obj.page_size),
    requested_pages: coerceNumberLike(obj.requested_pages),
    scan_max_pages: coerceNumberLike(obj.scan_max_pages),
    type: coerceNumberLike(obj.type),
    max_groups: coerceNumberLike(obj.max_groups),
    strict_full: coerceBooleanLike(obj.strict_full),
    group_by: normalizeSelectorListInput(obj.group_by),
    amount_column: coerceNumberLike(obj.amount_column),
    apply_ids: normalizeIdArrayInput(obj.apply_ids),
    sort: normalizeSortInput(obj.sort),
    filters: normalizeFiltersInput(obj.filters),
    time_range: normalizeTimeRangeInput(obj.time_range)
  }
}

function coerceNumberLike(value: unknown): unknown {
  if (typeof value === "string") {
    const trimmed = value.trim()
    if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
      const parsed = Number(trimmed)
      if (Number.isFinite(parsed)) {
        return parsed
      }
    }
  }
  return value
}

function coerceBooleanLike(value: unknown): unknown {
  if (typeof value === "string") {
    const trimmed = value.trim().toLowerCase()
    if (trimmed === "true") {
      return true
    }
    if (trimmed === "false") {
      return false
    }
  }
  return value
}

function parseJsonLike(value: unknown): unknown {
  if (typeof value !== "string") {
    return value
  }
  const trimmed = value.trim()
  if (!trimmed) {
    return value
  }
  if (!trimmed.startsWith("[") && !trimmed.startsWith("{")) {
    return value
  }
  try {
    return JSON.parse(trimmed)
  } catch {
    return value
  }
}

function normalizeSelectorListInput(value: unknown): unknown {
  const parsed = parseJsonLike(value)
  if (Array.isArray(parsed)) {
    return parsed.map((item) => coerceNumberLike(item))
  }
  if (typeof parsed === "string") {
    const trimmed = parsed.trim()
    if (!trimmed) {
      return parsed
    }
    if (trimmed.includes(",")) {
      return trimmed
        .split(",")
        .map((item) => item.trim())
        .filter((item) => item.length > 0)
        .map((item) => coerceNumberLike(item))
    }
    return [coerceNumberLike(trimmed)]
  }
  if (parsed !== undefined && parsed !== null) {
    return [coerceNumberLike(parsed)]
  }
  return parsed
}

function normalizeIdArrayInput(value: unknown): unknown {
  const parsed = parseJsonLike(value)
  if (Array.isArray(parsed)) {
    return parsed.map((item) => coerceNumberLike(item))
  }
  if (typeof parsed === "string" && parsed.includes(",")) {
    return parsed
      .split(",")
      .map((item) => item.trim())
      .filter((item) => item.length > 0)
      .map((item) => coerceNumberLike(item))
  }
  return parsed
}

function normalizeSortInput(value: unknown): unknown {
  const parsed = parseJsonLike(value)
  if (!Array.isArray(parsed)) {
    return parsed
  }
  return parsed.map((item) => {
    const obj = asObject(item)
    if (!obj) {
      return item
    }
    return {
      ...obj,
      que_id: coerceNumberLike(obj.que_id),
      ascend: coerceBooleanLike(obj.ascend)
    }
  })
}

function normalizeFiltersInput(value: unknown): unknown {
  const parsed = parseJsonLike(value)
  if (parsed === undefined || parsed === null) {
    return parsed
  }
  const list = Array.isArray(parsed) ? parsed : [parsed]
  return list.map((item) => {
    const obj = asObject(item)
    if (!obj) {
      return item
    }
    return {
      ...obj,
      que_id: coerceNumberLike(obj.que_id),
      scope: coerceNumberLike(obj.scope),
      search_options: normalizeIdArrayInput(obj.search_options)
    }
  })
}

function normalizeTimeRangeInput(value: unknown): unknown {
  const parsed = parseJsonLike(value)
  const obj = asObject(parsed)
  if (!obj) {
    return parsed
  }
  return {
    ...obj,
    column: coerceNumberLike(obj.column)
  }
}

function resolveStartPage(pageNum: number | undefined, pageToken: string | undefined, appKey: string): number {
  if (!pageToken) {
    return pageNum ?? 1
  }

  const payload = decodeContinuationToken(pageToken)
  if (payload.app_key !== appKey) {
    throw new Error(
      `page_token app_key mismatch: token for ${payload.app_key}, request for ${appKey}`
    )
  }
  return payload.next_page_num
}

function encodeContinuationToken(payload: ContinuationTokenPayload): string {
  return Buffer.from(JSON.stringify(payload), "utf8").toString("base64url")
}

function decodeContinuationToken(token: string): ContinuationTokenPayload {
  let parsed: unknown
  try {
    const decoded = Buffer.from(token, "base64url").toString("utf8")
    parsed = JSON.parse(decoded)
  } catch {
    throw new Error("Invalid page_token")
  }

  const obj = asObject(parsed)
  const appKey = asNullableString(obj?.app_key)
  const nextPageNum = toPositiveInt(obj?.next_page_num)
  const pageSize = toPositiveInt(obj?.page_size)
  if (!appKey || !nextPageNum || !pageSize) {
    throw new Error("Invalid page_token payload")
  }
  return {
    app_key: appKey,
    next_page_num: nextPageNum,
    page_size: pageSize
  }
}

function buildEvidencePayload(
  state: ListQueryState,
  sourcePages: number[]
): z.infer<typeof evidenceSchema> {
  return {
    query_id: state.query_id,
    app_key: state.app_key,
    filters: state.filters,
    selected_columns: state.selected_columns,
    time_range: state.time_range,
    source_pages: sourcePages
  }
}

function echoFilters(
  filters?: Array<{
    que_id?: string | number
    search_key?: string
    search_keys?: string[]
    min_value?: string
    max_value?: string
    scope?: number
    search_options?: Array<string | number>
    search_user_ids?: string[]
  }>
): Array<Record<string, unknown>> {
  return (filters ?? []).map((item) => ({
    ...(item.que_id !== undefined ? { que_id: String(item.que_id) } : {}),
    ...(item.search_key !== undefined ? { search_key: item.search_key } : {}),
    ...(item.search_keys !== undefined ? { search_keys: item.search_keys } : {}),
    ...(item.min_value !== undefined ? { min_value: item.min_value } : {}),
    ...(item.max_value !== undefined ? { max_value: item.max_value } : {}),
    ...(item.scope !== undefined ? { scope: item.scope } : {}),
    ...(item.search_options !== undefined ? { search_options: item.search_options } : {}),
    ...(item.search_user_ids !== undefined ? { search_user_ids: item.search_user_ids } : {})
  }))
}

function resolveQueryMode(args: z.infer<typeof queryInputSchema>): "list" | "record" | "summary" {
  const requested = args.query_mode ?? "auto"
  if (requested !== "auto") {
    return requested
  }

  if (args.apply_id !== undefined) {
    return "record"
  }

  if (
    args.amount_column !== undefined ||
    args.time_range !== undefined ||
    args.stat_policy !== undefined ||
    args.scan_max_pages !== undefined
  ) {
    return "summary"
  }

  return "list"
}

function buildListArgsFromQuery(args: z.infer<typeof queryInputSchema>): z.infer<typeof listInputSchema> {
  if (!args.app_key) {
    throw missingRequiredFieldError({
      field: "app_key",
      tool: "qf_query(list)",
      fixHint: "Provide app_key, for example: {\"query_mode\":\"list\",\"app_key\":\"21b3d559\",...}"
    })
  }
  if (!args.select_columns?.length) {
    throw missingRequiredFieldError({
      field: "select_columns",
      tool: "qf_query(list)",
      fixHint:
        "Provide select_columns as an array (<=10), for example: {\"select_columns\":[0,\"客户全称\",\"报价总金额\"]}"
    })
  }

  const filters = buildListFiltersFromQuery(args)

  return listInputSchema.parse({
    app_key: args.app_key,
    user_id: args.user_id,
    page_num: args.page_num,
    page_token: args.page_token,
    page_size: args.page_size,
    requested_pages: args.requested_pages,
    scan_max_pages: args.scan_max_pages,
    mode: args.mode,
    type: args.type,
    keyword: args.keyword,
    query_logic: args.query_logic,
    apply_ids: args.apply_ids,
    sort: args.sort,
    filters,
    time_range: args.time_range,
    max_rows: args.max_rows,
    max_items: args.max_items,
    max_columns: args.max_columns,
    select_columns: args.select_columns,
    include_answers: args.include_answers,
    strict_full: args.strict_full
  })
}

function buildListFiltersFromQuery(
  args: z.infer<typeof queryInputSchema>
): z.infer<typeof listInputSchema>["filters"] {
  return appendTimeRangeFilter(args.filters, args.time_range)
}

function appendTimeRangeFilter(
  inputFilters:
    | Array<{
        que_id?: string | number
        search_key?: string
        search_keys?: string[]
        min_value?: string
        max_value?: string
        scope?: number
        search_options?: Array<string | number>
        search_user_ids?: string[]
      }>
    | undefined,
  timeRange:
    | {
        column: string | number
        from?: string
        to?: string
        timezone?: string
      }
    | undefined
): z.infer<typeof listInputSchema>["filters"] {
  const filters = [...(inputFilters ?? [])]
  if (!timeRange) {
    return filters.length > 0 ? filters : undefined
  }
  if (timeRange.from === undefined && timeRange.to === undefined) {
    return filters.length > 0 ? filters : undefined
  }

  const timeSelector = normalizeColumnSelector(timeRange.column)
  const alreadyHasTimeFilter = filters.some((item) => {
    if (item.que_id === undefined) {
      return false
    }
    return normalizeColumnSelector(item.que_id) === timeSelector
  })
  if (!alreadyHasTimeFilter) {
    filters.push({
      que_id: timeRange.column,
      ...(timeRange.from !== undefined ? { min_value: timeRange.from } : {}),
      ...(timeRange.to !== undefined ? { max_value: timeRange.to } : {})
    })
  }

  return filters.length > 0 ? filters : undefined
}

function buildRecordGetArgsFromQuery(
  args: z.infer<typeof queryInputSchema>
): z.infer<typeof recordGetInputSchema> {
  if (args.apply_id === undefined) {
    throw missingRequiredFieldError({
      field: "apply_id",
      tool: "qf_query(record)",
      fixHint: "Provide apply_id, for example: {\"query_mode\":\"record\",\"apply_id\":\"497600278750478338\",...}"
    })
  }
  if (!args.select_columns?.length) {
    throw missingRequiredFieldError({
      field: "select_columns",
      tool: "qf_query(record)",
      fixHint: "Provide select_columns as an array (<=10), for example: {\"select_columns\":[0,\"客户全称\"]}"
    })
  }

  return recordGetInputSchema.parse({
    apply_id: args.apply_id,
    max_columns: args.max_columns,
    select_columns: args.select_columns
  })
}

async function executeRecordsList(
  args: z.infer<typeof listInputSchema>
): Promise<{ payload: z.infer<typeof listSuccessOutputSchema>; message: string }> {
  if (!args.app_key) {
    throw missingRequiredFieldError({
      field: "app_key",
      tool: "qf_records_list",
      fixHint: "Provide app_key, for example: {\"app_key\":\"21b3d559\",...}"
    })
  }
  if (!args.select_columns?.length) {
    throw missingRequiredFieldError({
      field: "select_columns",
      tool: "qf_records_list",
      fixHint:
        "Provide select_columns as an array (<=10), for example: {\"select_columns\":[0,\"客户全称\",\"报价总金额\"]}"
    })
  }

  const queryId = randomUUID()
  const pageNum = resolveStartPage(args.page_num, args.page_token, args.app_key)
  const pageSize = args.page_size ?? DEFAULT_PAGE_SIZE
  const requestedPages = args.requested_pages ?? 1
  const scanMaxPages = args.scan_max_pages ?? requestedPages
  const effectiveFilters = appendTimeRangeFilter(args.filters, args.time_range)
  const normalizedSort = await normalizeListSort(args.sort, args.app_key, args.user_id)
  const includeAnswers = true
  let currentPage = pageNum
  let fetchedPages = 0
  let hasMore = false
  let nextPageNum: number | null = null
  let resultAmount: number | null = null
  let pageAmount: number | null = null
  let responseMeta: ReturnType<typeof buildMeta> | null = null
  const sourcePages: number[] = []
  const collectedRawItems: unknown[] = []

  while (fetchedPages < requestedPages && fetchedPages < scanMaxPages) {
    const payload = buildListPayload({
      pageNum: currentPage,
      pageSize,
      mode: args.mode,
      type: args.type,
      keyword: args.keyword,
      queryLogic: args.query_logic,
      applyIds: args.apply_ids,
      sort: normalizedSort,
      filters: effectiveFilters
    })
    const response = await client.listRecords(args.app_key, payload, { userId: args.user_id })
    responseMeta = responseMeta ?? buildMeta(response)

    const result = asObject(response.result)
    const rawItems = asArray(result?.result)
    collectedRawItems.push(...rawItems)
    sourcePages.push(currentPage)
    fetchedPages += 1

    resultAmount = resultAmount ?? toNonNegativeInt(result?.resultAmount)
    pageAmount = pageAmount ?? toPositiveInt(result?.pageAmount)
    hasMore = pageAmount !== null ? currentPage < pageAmount : rawItems.length === pageSize
    nextPageNum = hasMore ? currentPage + 1 : null
    if (!hasMore) {
      break
    }
    currentPage = currentPage + 1
  }

  if (!responseMeta) {
    throw new Error("Failed to fetch list pages")
  }

  const knownResultAmount = resultAmount ?? collectedRawItems.length
  const listLimit = resolveListItemLimit({
    total: collectedRawItems.length,
    requestedMaxRows: args.max_rows,
    requestedMaxItems: args.max_items
  })

  const items = collectedRawItems
    .slice(0, listLimit.limit)
    .map((raw) => normalizeRecordItem(raw, includeAnswers))
  const columnProjection = projectRecordItemsColumns({
    items,
    includeAnswers,
    maxColumns: args.max_columns,
    selectColumns: args.select_columns
  })
  if (items.length > 0 && columnProjection.matchedAnswersCount === 0) {
    throw new Error(
      `No answers matched select_columns (${args.select_columns
        .map((item) => String(item))
        .join(", ")}). Check que_id/title from qf_form_get.`
    )
  }
  const fitted = fitListItemsWithinSize({
    items: columnProjection.items,
    limitBytes: MAX_LIST_ITEMS_BYTES
  })
  const truncationReason = mergeTruncationReasons(listLimit.reason, columnProjection.reason, fitted.reason)
  const omittedItems = Math.max(0, knownResultAmount - fitted.items.length)
  const isComplete =
    !hasMore &&
    omittedItems === 0 &&
    fitted.omittedItems === 0 &&
    fitted.omittedChars === 0
  const nextPageToken =
    hasMore && nextPageNum
      ? encodeContinuationToken({
          app_key: args.app_key,
          next_page_num: nextPageNum,
          page_size: pageSize
        })
      : null

  const completeness: z.infer<typeof completenessSchema> = {
    result_amount: knownResultAmount,
    returned_items: fitted.items.length,
    fetched_pages: fetchedPages,
    requested_pages: requestedPages,
    actual_scanned_pages: fetchedPages,
    has_more: hasMore,
    next_page_token: nextPageToken,
    is_complete: isComplete,
    partial: !isComplete,
    omitted_items: omittedItems,
    omitted_chars: fitted.omittedChars
  }
  const listState: ListQueryState = {
    query_id: queryId,
    app_key: args.app_key,
    selected_columns: columnProjection.selectedColumns,
    filters: echoFilters(effectiveFilters),
    time_range: args.time_range
      ? {
          column: String(args.time_range.column),
          from: args.time_range.from ?? null,
          to: args.time_range.to ?? null,
          timezone: args.time_range.timezone ?? null
        }
      : null
  }
  const evidence = buildEvidencePayload(listState, sourcePages)

  if (args.strict_full && !isComplete) {
    throw new NeedMoreDataError(
      "List result is incomplete. Increase requested_pages/max_rows or continue with next_page_token.",
      {
        code: "NEED_MORE_DATA",
        completeness,
        evidence
      }
    )
  }

  const responsePayload: z.infer<typeof listSuccessOutputSchema> = {
    ok: true,
    data: {
      app_key: args.app_key,
      pagination: {
        page_num: pageNum,
        page_size: pageSize,
        page_amount: pageAmount,
        result_amount: knownResultAmount
      },
      items: fitted.items as z.infer<typeof recordItemSchema>[],
      applied_limits: {
        include_answers: includeAnswers,
        row_cap: listLimit.limit,
        column_cap: args.max_columns ?? null,
        selected_columns: columnProjection.selectedColumns
      },
      completeness,
      evidence
    },
    completeness,
    evidence,
    error_code: null,
    fix_hint: null,
    next_page_token: completeness.next_page_token,
    meta: responseMeta
  }

  return {
    payload: responsePayload,
    message: buildRecordsListMessage({
      returned: fitted.items.length,
      total: knownResultAmount,
      truncationReason
    })
  }
}

async function executeRecordGet(
  args: z.infer<typeof recordGetInputSchema>
): Promise<{ payload: z.infer<typeof recordGetSuccessOutputSchema>; message: string }> {
  if (!args.select_columns?.length) {
    throw missingRequiredFieldError({
      field: "select_columns",
      tool: "qf_record_get",
      fixHint: "Provide select_columns as an array (<=10), for example: {\"apply_id\":\"...\",\"select_columns\":[0]}"
    })
  }

  const queryId = randomUUID()
  const response = await client.getRecord(String(args.apply_id))
  const record = asObject(response.result) ?? {}
  const projection = projectAnswersForOutput({
    answers: asArray(record.answers),
    maxColumns: args.max_columns,
    selectColumns: args.select_columns
  })
  const projectedRecord: Record<string, unknown> = {
    ...record,
    answers: projection.answers
  }
  const answerCount = projection.answers.length

  return {
    payload: {
      ok: true,
      data: {
        apply_id: (record.applyId as string | number | null | undefined) ?? null,
        answer_count: answerCount,
        record: projectedRecord,
        applied_limits: {
          column_cap: args.max_columns ?? null,
          selected_columns: projection.selectedColumns
        },
        completeness: {
          result_amount: 1,
          returned_items: 1,
          fetched_pages: 1,
          requested_pages: 1,
          actual_scanned_pages: 1,
          has_more: false,
          next_page_token: null,
          is_complete: true,
          partial: false,
          omitted_items: 0,
          omitted_chars: 0
        },
        evidence: {
          query_id: queryId,
          apply_id: String(args.apply_id),
          selected_columns: projection.selectedColumns ?? []
        }
      },
      completeness: {
        result_amount: 1,
        returned_items: 1,
        fetched_pages: 1,
        requested_pages: 1,
        actual_scanned_pages: 1,
        has_more: false,
        next_page_token: null,
        is_complete: true,
        partial: false,
        omitted_items: 0,
        omitted_chars: 0
      },
      evidence: {
        query_id: queryId,
        apply_id: String(args.apply_id),
        selected_columns: projection.selectedColumns ?? []
      },
      error_code: null,
      fix_hint: null,
      next_page_token: null,
      meta: buildMeta(response)
    },
    message: `Fetched record ${String(args.apply_id)}`
  }
}

interface SummaryColumn {
  requested: string
  que_id: string | number
  que_title: string | null
  que_type: unknown
}

async function executeRecordsSummary(args: z.infer<typeof queryInputSchema>): Promise<{
  data: z.infer<typeof querySummaryOutputSchema>
  meta: ReturnType<typeof buildMeta>
  message: string
}> {
  if (!args.app_key) {
    throw missingRequiredFieldError({
      field: "app_key",
      tool: "qf_query(summary)",
      fixHint: "Provide app_key, for example: {\"query_mode\":\"summary\",\"app_key\":\"21b3d559\",...}"
    })
  }
  if (!args.select_columns?.length) {
    throw missingRequiredFieldError({
      field: "select_columns",
      tool: "qf_query(summary)",
      fixHint: "Provide select_columns as an array (<=10), for example: {\"select_columns\":[\"客户全称\"]}"
    })
  }

  const queryId = randomUUID()
  const strictFull = args.strict_full ?? true
  const includeNegative = args.stat_policy?.include_negative ?? true
  const includeNull = args.stat_policy?.include_null ?? false
  const scanMaxPages = args.scan_max_pages ?? DEFAULT_SCAN_MAX_PAGES
  const requestedPages = args.requested_pages ?? scanMaxPages
  const startPage = resolveStartPage(args.page_num, args.page_token, args.app_key)
  const pageSize = args.page_size ?? DEFAULT_PAGE_SIZE
  const rowCap = Math.min(args.max_rows ?? DEFAULT_ROW_LIMIT, DEFAULT_ROW_LIMIT)
  const timezone = args.time_range?.timezone ?? "Asia/Shanghai"

  const form = await getFormCached(args.app_key, args.user_id, false)
  const index = buildFieldIndex(form.result)
  const selectedColumns = resolveSummaryColumns(args.select_columns, index, "select_columns")
  const effectiveColumns =
    args.max_columns !== undefined ? selectedColumns.slice(0, args.max_columns) : selectedColumns

  if (effectiveColumns.length === 0) {
    throw new Error("No output columns remain after max_columns cap")
  }

  const amountColumn =
    args.amount_column !== undefined
      ? resolveSummaryColumn(args.amount_column, index, "amount_column")
      : null
  const timeColumn = args.time_range ? resolveSummaryColumn(args.time_range.column, index, "time_range.column") : null

  const normalizedSort = await normalizeListSort(args.sort, args.app_key, args.user_id)
  const summaryFilters = [...(args.filters ?? [])]
  if (timeColumn && (args.time_range?.from || args.time_range?.to)) {
    summaryFilters.push({
      que_id: timeColumn.que_id,
      ...(args.time_range.from ? { min_value: args.time_range.from } : {}),
      ...(args.time_range.to ? { max_value: args.time_range.to } : {})
    })
  }

  const listState: ListQueryState = {
    query_id: queryId,
    app_key: args.app_key,
    selected_columns: effectiveColumns.map((item) => item.requested),
    filters: echoFilters(summaryFilters),
    time_range: timeColumn
      ? {
          column: timeColumn.requested,
          from: args.time_range?.from ?? null,
          to: args.time_range?.to ?? null,
          timezone
        }
      : null
  }

  let currentPage = startPage
  let scannedPages = 0
  let scannedRecords = 0
  let hasMore = false
  let nextPageNum: number | null = null
  let resultAmount: number | null = null
  let summaryMeta: ReturnType<typeof buildMeta> | null = null
  let totalAmount = 0
  let missingCount = 0
  const sourcePages: number[] = []

  const rows: Array<Record<string, unknown>> = []
  const byDay = new Map<string, { count: number; amount: number }>()

  while (scannedPages < requestedPages && scannedPages < scanMaxPages) {
    const payload = buildListPayload({
      pageNum: currentPage,
      pageSize,
      mode: args.mode,
      type: args.type,
      keyword: args.keyword,
      queryLogic: args.query_logic,
      applyIds: args.apply_ids,
      sort: normalizedSort,
      filters: summaryFilters
    })
    const response = await client.listRecords(args.app_key, payload, { userId: args.user_id })
    summaryMeta = summaryMeta ?? buildMeta(response)
    scannedPages += 1
    sourcePages.push(currentPage)

    const result = asObject(response.result)
    const rawItems = asArray(result?.result)
    const pageAmount = toPositiveInt(result?.pageAmount)
    resultAmount = resultAmount ?? toNonNegativeInt(result?.resultAmount)
    hasMore = pageAmount !== null ? currentPage < pageAmount : rawItems.length === pageSize
    nextPageNum = hasMore ? currentPage + 1 : null

    for (const rawItem of rawItems) {
      const record = asObject(rawItem) ?? {}
      const answers = asArray(record.answers)
      scannedRecords += 1

      if (rows.length < rowCap) {
        rows.push(buildSummaryRow(answers, effectiveColumns))
      }

      let amountContribution = 0
      let hasAmountContribution = false
      if (amountColumn) {
        const amountValue = extractSummaryColumnValue(answers, amountColumn)
        const numericAmount = toFiniteAmount(amountValue)

        if (numericAmount === null) {
          if (!includeNull) {
            missingCount += 1
          } else {
            hasAmountContribution = true
          }
        } else if (includeNegative || numericAmount >= 0) {
          amountContribution = numericAmount
          hasAmountContribution = true
        }
      }

      if (hasAmountContribution) {
        totalAmount += amountContribution
      }

      const dayKey = timeColumn
        ? toDayBucket(extractSummaryColumnValue(answers, timeColumn), timezone)
        : "all"
      const bucket = byDay.get(dayKey) ?? { count: 0, amount: 0 }
      bucket.count += 1
      if (amountColumn && hasAmountContribution) {
        bucket.amount += amountContribution
      }
      byDay.set(dayKey, bucket)
    }

    if (!hasMore) {
      break
    }

    currentPage = currentPage + 1
  }

  const byDayStats = Array.from(byDay.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([day, bucket]) => ({
      day,
      count: bucket.count,
      amount_total: amountColumn ? bucket.amount : null
    }))

  const fieldMapping = [
    ...effectiveColumns.map((item) => ({
      role: "row" as const,
      requested: item.requested,
      que_id: item.que_id,
      que_title: item.que_title,
      que_type: item.que_type
    })),
    ...(amountColumn
      ? [
          {
            role: "amount" as const,
            requested: amountColumn.requested,
            que_id: amountColumn.que_id,
            que_title: amountColumn.que_title,
            que_type: amountColumn.que_type
          }
        ]
      : []),
    ...(timeColumn
      ? [
          {
            role: "time" as const,
            requested: timeColumn.requested,
            que_id: timeColumn.que_id,
            que_title: timeColumn.que_title,
            que_type: timeColumn.que_type
          }
        ]
      : [])
  ]

  if (!summaryMeta) {
    throw new Error("Failed to build summary metadata")
  }

  const knownResultAmount = resultAmount ?? scannedRecords
  const omittedItems = Math.max(0, knownResultAmount - scannedRecords)
  const isComplete = !hasMore && omittedItems === 0
  const nextPageToken =
    hasMore && nextPageNum
      ? encodeContinuationToken({
          app_key: args.app_key,
          next_page_num: nextPageNum,
          page_size: pageSize
        })
      : null
  const completeness: z.infer<typeof completenessSchema> = {
    result_amount: knownResultAmount,
    returned_items: scannedRecords,
    fetched_pages: scannedPages,
    requested_pages: requestedPages,
    actual_scanned_pages: scannedPages,
    has_more: hasMore,
    next_page_token: nextPageToken,
    is_complete: isComplete,
    partial: !isComplete,
    omitted_items: omittedItems,
    omitted_chars: 0
  }
  const evidence = buildEvidencePayload(listState, sourcePages)

  if (strictFull && !isComplete) {
    throw new NeedMoreDataError(
      "Summary is incomplete. Continue with next_page_token or increase requested_pages/scan_max_pages.",
      {
        code: "NEED_MORE_DATA",
        completeness,
        evidence
      }
    )
  }

  return {
    data: {
      summary: {
        total_count: scannedRecords,
        total_amount: amountColumn ? totalAmount : null,
        by_day: byDayStats,
        missing_count: missingCount
      },
      rows,
      completeness,
      evidence,
      meta: {
        field_mapping: fieldMapping,
        filters: {
          app_key: args.app_key,
          time_range: timeColumn
            ? {
                column: timeColumn.requested,
                from: args.time_range?.from ?? null,
                to: args.time_range?.to ?? null,
                timezone
              }
            : null
        },
        stat_policy: {
          include_negative: includeNegative,
          include_null: includeNull
        },
        execution: {
          scanned_records: scannedRecords,
          scanned_pages: scannedPages,
          truncated: !isComplete,
          row_cap: rowCap,
          column_cap: args.max_columns ?? null,
          scan_max_pages: scanMaxPages
        }
      }
    },
    meta: summaryMeta,
    message: isComplete
      ? `Summarized ${scannedRecords} records`
      : `Summarized ${scannedRecords}/${knownResultAmount} records (partial)`
  }
}

async function executeRecordsAggregate(args: z.infer<typeof aggregateInputSchema>): Promise<{
  payload: z.infer<typeof aggregateOutputSchema>
  message: string
}> {
  const queryId = randomUUID()
  const strictFull = args.strict_full ?? true
  const includeNegative = args.stat_policy?.include_negative ?? true
  const includeNull = args.stat_policy?.include_null ?? false
  const pageSize = args.page_size ?? DEFAULT_PAGE_SIZE
  const scanMaxPages = args.scan_max_pages ?? DEFAULT_SCAN_MAX_PAGES
  const requestedPages = args.requested_pages ?? scanMaxPages
  const startPage = resolveStartPage(args.page_num, args.page_token, args.app_key)
  const maxGroups = args.max_groups ?? 200
  const timezone = args.time_range?.timezone ?? "Asia/Shanghai"

  const form = await getFormCached(args.app_key, args.user_id, false)
  const index = buildFieldIndex(form.result)
  const groupColumns = resolveSummaryColumns(args.group_by, index, "group_by")
  const amountColumn =
    args.amount_column !== undefined
      ? resolveSummaryColumn(args.amount_column, index, "amount_column")
      : null
  const timeColumn = args.time_range ? resolveSummaryColumn(args.time_range.column, index, "time_range.column") : null

  const normalizedSort = await normalizeListSort(args.sort, args.app_key, args.user_id)
  const aggregateFilters = [...(args.filters ?? [])]
  if (timeColumn && (args.time_range?.from || args.time_range?.to)) {
    aggregateFilters.push({
      que_id: timeColumn.que_id,
      ...(args.time_range.from ? { min_value: args.time_range.from } : {}),
      ...(args.time_range.to ? { max_value: args.time_range.to } : {})
    })
  }

  const listState: ListQueryState = {
    query_id: queryId,
    app_key: args.app_key,
    selected_columns: groupColumns.map((item) => item.requested),
    filters: echoFilters(aggregateFilters),
    time_range: timeColumn
      ? {
          column: timeColumn.requested,
          from: args.time_range?.from ?? null,
          to: args.time_range?.to ?? null,
          timezone
        }
      : null
  }

  let currentPage = startPage
  let scannedPages = 0
  let scannedRecords = 0
  let hasMore = false
  let nextPageNum: number | null = null
  let resultAmount: number | null = null
  let responseMeta: ReturnType<typeof buildMeta> | null = null
  let totalAmount = 0
  const sourcePages: number[] = []
  const groupStats = new Map<string, { group: Record<string, unknown>; count: number; amount: number }>()

  while (scannedPages < requestedPages && scannedPages < scanMaxPages) {
    const payload = buildListPayload({
      pageNum: currentPage,
      pageSize,
      mode: args.mode,
      type: args.type,
      keyword: args.keyword,
      queryLogic: args.query_logic,
      applyIds: args.apply_ids,
      sort: normalizedSort,
      filters: aggregateFilters
    })
    const response = await client.listRecords(args.app_key, payload, { userId: args.user_id })
    responseMeta = responseMeta ?? buildMeta(response)
    scannedPages += 1
    sourcePages.push(currentPage)

    const result = asObject(response.result)
    const rawItems = asArray(result?.result)
    const pageAmount = toPositiveInt(result?.pageAmount)
    resultAmount = resultAmount ?? toNonNegativeInt(result?.resultAmount)
    hasMore = pageAmount !== null ? currentPage < pageAmount : rawItems.length === pageSize
    nextPageNum = hasMore ? currentPage + 1 : null

    for (const rawItem of rawItems) {
      const record = asObject(rawItem) ?? {}
      const answers = asArray(record.answers)
      scannedRecords += 1

      const group: Record<string, unknown> = {}
      for (const column of groupColumns) {
        group[column.requested] = extractSummaryColumnValue(answers, column)
      }
      const groupKey = stableJson(group)
      const bucket = groupStats.get(groupKey) ?? { group, count: 0, amount: 0 }
      bucket.count += 1

      if (amountColumn) {
        const amountValue = extractSummaryColumnValue(answers, amountColumn)
        const numericAmount = toFiniteAmount(amountValue)
        if (numericAmount === null) {
          if (includeNull) {
            // Keep group count while amount contributes 0.
          }
        } else if (includeNegative || numericAmount >= 0) {
          bucket.amount += numericAmount
          totalAmount += numericAmount
        }
      }

      groupStats.set(groupKey, bucket)
    }

    if (!hasMore) {
      break
    }
    currentPage = currentPage + 1
  }

  if (!responseMeta) {
    throw new Error("Failed to fetch aggregate pages")
  }

  const knownResultAmount = resultAmount ?? scannedRecords
  const omittedItems = Math.max(0, knownResultAmount - scannedRecords)
  const isComplete = !hasMore && omittedItems === 0
  const nextPageToken =
    hasMore && nextPageNum
      ? encodeContinuationToken({
          app_key: args.app_key,
          next_page_num: nextPageNum,
          page_size: pageSize
        })
      : null
  const completeness: z.infer<typeof completenessSchema> = {
    result_amount: knownResultAmount,
    returned_items: scannedRecords,
    fetched_pages: scannedPages,
    requested_pages: requestedPages,
    actual_scanned_pages: scannedPages,
    has_more: hasMore,
    next_page_token: nextPageToken,
    is_complete: isComplete,
    partial: !isComplete,
    omitted_items: omittedItems,
    omitted_chars: 0
  }
  const evidence = buildEvidencePayload(listState, sourcePages)

  if (strictFull && !isComplete) {
    throw new NeedMoreDataError(
      "Aggregate result is incomplete. Continue with next_page_token or increase requested_pages/scan_max_pages.",
      {
        code: "NEED_MORE_DATA",
        completeness,
        evidence
      }
    )
  }

  const groups = Array.from(groupStats.values())
    .sort((a, b) => b.count - a.count)
    .slice(0, maxGroups)
    .map((bucket) => ({
      group: bucket.group,
      count: bucket.count,
      count_ratio: scannedRecords > 0 ? bucket.count / scannedRecords : 0,
      amount_total: amountColumn ? bucket.amount : null,
      amount_ratio:
        amountColumn && totalAmount !== 0
          ? bucket.amount / totalAmount
          : amountColumn
            ? 0
            : null
    }))

  const fieldMapping = [
    ...groupColumns.map((item) => ({
      role: "group_by" as const,
      requested: item.requested,
      que_id: item.que_id,
      que_title: item.que_title,
      que_type: item.que_type
    })),
    ...(amountColumn
      ? [
          {
            role: "amount" as const,
            requested: amountColumn.requested,
            que_id: amountColumn.que_id,
            que_title: amountColumn.que_title,
            que_type: amountColumn.que_type
          }
        ]
      : []),
    ...(timeColumn
      ? [
          {
            role: "time" as const,
            requested: timeColumn.requested,
            que_id: timeColumn.que_id,
            que_title: timeColumn.que_title,
            que_type: timeColumn.que_type
          }
        ]
      : [])
  ]

  return {
    payload: {
      ok: true,
      data: {
        app_key: args.app_key,
        summary: {
          total_count: scannedRecords,
          total_amount: amountColumn ? totalAmount : null
        },
        groups,
        completeness,
        evidence,
        meta: {
          field_mapping: fieldMapping,
          stat_policy: {
            include_negative: includeNegative,
            include_null: includeNull
          }
        }
      },
      completeness,
      evidence,
      error_code: null,
      fix_hint: null,
      next_page_token: completeness.next_page_token,
      meta: responseMeta
    },
    message: isComplete
      ? `Aggregated ${scannedRecords} records`
      : `Aggregated ${scannedRecords}/${knownResultAmount} records (partial)`
  }
}

function resolveSummaryColumns(
  columns: Array<string | number>,
  index: FieldIndex,
  label: string
): SummaryColumn[] {
  return normalizeColumnSelectors(columns).map((requested) =>
    resolveSummaryColumn(requested, index, label)
  )
}

function resolveSummaryColumn(
  column: string | number,
  index: FieldIndex,
  label: string
): SummaryColumn {
  const requested = String(column).trim()
  if (!requested) {
    throw new Error(`${label} contains an empty column selector`)
  }

  if (isNumericKey(requested)) {
    const hit = index.byId.get(String(Number(requested)))
    if (!hit) {
      throw new Error(`${label} references unknown que_id "${requested}"`)
    }
    return {
      requested,
      que_id: normalizeQueId(hit.queId),
      que_title: asNullableString(hit.queTitle),
      que_type: hit.queType
    }
  }

  const hit = resolveFieldByKey(requested, index)
  if (!hit || hit.queId === undefined || hit.queId === null) {
    throw new Error(`${label} cannot resolve field "${requested}"`)
  }

  return {
    requested,
    que_id: normalizeQueId(hit.queId),
    que_title: asNullableString(hit.queTitle),
    que_type: hit.queType
  }
}

function buildSummaryRow(answers: unknown[], columns: SummaryColumn[]): Record<string, unknown> {
  const row: Record<string, unknown> = {}
  for (const column of columns) {
    row[column.requested] = extractSummaryColumnValue(answers, column)
  }
  return row
}

function extractSummaryColumnValue(answers: unknown[], column: SummaryColumn): unknown {
  const targetId = normalizeColumnSelector(String(column.que_id))
  const targetTitle = column.que_title ? normalizeColumnSelector(column.que_title) : null

  for (const answerRaw of answers) {
    const answer = asObject(answerRaw)
    if (!answer) {
      continue
    }

    const answerQueId = asNullableString(answer.queId)
    if (answerQueId && normalizeColumnSelector(answerQueId) === targetId) {
      return extractAnswerDisplayValue(answer)
    }

    if (targetTitle) {
      const answerQueTitle = asNullableString(answer.queTitle)
      if (answerQueTitle && normalizeColumnSelector(answerQueTitle) === targetTitle) {
        return extractAnswerDisplayValue(answer)
      }
    }
  }

  return null
}

function extractAnswerDisplayValue(answer: Record<string, unknown>): unknown {
  const tableValues = answer.tableValues ?? answer.table_values
  if (tableValues !== undefined) {
    return tableValues
  }

  const values = asArray(answer.values)
  if (values.length === 0) {
    return null
  }

  const normalized = values.map((item) => extractAnswerValueCell(item))
  return normalized.length === 1 ? normalized[0] : normalized
}

function extractAnswerValueCell(value: unknown): unknown {
  const obj = asObject(value)
  if (!obj) {
    return value
  }

  if (obj.dataValue !== undefined) {
    return obj.dataValue
  }
  if (obj.value !== undefined) {
    return obj.value
  }
  if (obj.valueStr !== undefined) {
    return obj.valueStr
  }
  return obj
}

function toFiniteAmount(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null
  }
  if (Array.isArray(value)) {
    if (value.length !== 1) {
      return null
    }
    return toFiniteAmount(value[0])
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value
  }
  if (typeof value === "string") {
    const normalized = value.replace(/,/g, "").trim()
    if (!normalized) {
      return null
    }
    const parsed = Number(normalized)
    if (Number.isFinite(parsed)) {
      return parsed
    }
  }
  return null
}

function toDayBucket(value: unknown, timezone: string): string {
  const first = Array.isArray(value) ? value[0] : value
  if (first === null || first === undefined) {
    return "unknown"
  }

  if (typeof first === "string") {
    const trimmed = first.trim()
    const direct = trimmed.match(/^(\d{4}-\d{2}-\d{2})/)
    if (direct) {
      return direct[1]
    }
    const parsed = new Date(trimmed)
    if (!Number.isNaN(parsed.getTime())) {
      return formatDateBucket(parsed, timezone)
    }
    return "unknown"
  }

  if (typeof first === "number") {
    const parsed = new Date(first)
    if (!Number.isNaN(parsed.getTime())) {
      return formatDateBucket(parsed, timezone)
    }
  }

  return "unknown"
}

function formatDateBucket(value: Date, timezone: string): string {
  try {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: timezone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit"
    }).format(value)
  } catch {
    return value.toISOString().slice(0, 10)
  }
}

function buildListPayload(params: {
  pageNum: number
  pageSize: number
  mode?: ModeKey
  type?: number
  keyword?: string
  queryLogic?: "and" | "or"
  applyIds?: Array<string | number>
  sort?: Array<{ que_id: string | number; ascend?: boolean }>
  filters?: Array<{
    que_id?: string | number
    search_key?: string
    search_keys?: string[]
    min_value?: string
    max_value?: string
    scope?: number
    search_options?: Array<string | number>
    search_user_ids?: string[]
  }>
}): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    pageNum: params.pageNum,
    pageSize: params.pageSize
  }

  if (params.mode) {
    payload.type = MODE_TO_TYPE[params.mode]
  } else if (params.type !== undefined) {
    payload.type = params.type
  }

  if (params.keyword) {
    payload.queryKey = params.keyword
  }

  if (params.queryLogic) {
    payload.queriesRel = params.queryLogic
  }

  if (params.applyIds?.length) {
    payload.applyIds = params.applyIds.map((id) => String(id))
  }

  if (params.sort?.length) {
    payload.sorts = params.sort.map((item) => ({
      queId: item.que_id,
      ...(item.ascend !== undefined ? { isAscend: item.ascend } : {})
    }))
  }

  if (params.filters?.length) {
    payload.queries = params.filters.map((item) => ({
      ...(item.que_id !== undefined ? { queId: item.que_id } : {}),
      ...(item.search_key !== undefined ? { searchKey: item.search_key } : {}),
      ...(item.search_keys !== undefined ? { searchKeys: item.search_keys } : {}),
      ...(item.min_value !== undefined ? { minValue: item.min_value } : {}),
      ...(item.max_value !== undefined ? { maxValue: item.max_value } : {}),
      ...(item.scope !== undefined ? { scope: item.scope } : {}),
      ...(item.search_options !== undefined ? { searchOptions: item.search_options } : {}),
      ...(item.search_user_ids !== undefined ? { searchUserIds: item.search_user_ids } : {})
    }))
  }

  return payload
}

function normalizeRecordItem(raw: unknown, includeAnswers: boolean) {
  const item = asObject(raw) ?? {}
  const normalized = {
    apply_id: (item.applyId as string | number | null | undefined) ?? null,
    app_key: asNullableString(item.appKey),
    apply_num: (item.applyNum as string | number | null | undefined) ?? null,
    apply_time: asNullableString(item.applyTime),
    last_update_time: asNullableString(item.lastUpdateTime),
    ...(includeAnswers ? { answers: asArray(item.answers) } : {})
  }
  return normalized
}

function resolveAnswers(params: {
  explicitAnswers?: z.infer<typeof answerInputSchema>[]
  fields?: Record<string, unknown>
  form?: unknown
}): Record<string, unknown>[] {
  const normalizedFromFields = resolveFieldAnswers(params.fields, params.form)
  const normalizedExplicit = normalizeExplicitAnswers(params.explicitAnswers)

  const merged = new Map<string, Record<string, unknown>>()
  for (const answer of normalizedFromFields) {
    merged.set(String(answer.queId), answer)
  }
  for (const answer of normalizedExplicit) {
    merged.set(String(answer.queId), answer)
  }

  if (merged.size === 0) {
    throw new Error("answers or fields must contain at least one field")
  }

  return Array.from(merged.values())
}

function normalizeExplicitAnswers(
  answers?: z.infer<typeof answerInputSchema>[]
): Record<string, unknown>[] {
  if (!answers?.length) {
    return []
  }

  const output: Record<string, unknown>[] = []
  for (const item of answers) {
    const queId = item.que_id ?? item.queId
    if (queId === undefined || queId === null || String(queId).trim() === "") {
      throw new Error("answer item requires que_id or queId")
    }

    const normalized: Record<string, unknown> = {
      queId: isNumericKey(String(queId)) ? Number(queId) : String(queId)
    }

    const queTitle = item.que_title ?? item.queTitle
    if (typeof queTitle === "string" && queTitle.trim()) {
      normalized.queTitle = queTitle
    }

    const queType = item.que_type ?? item.queType
    if (queType !== undefined) {
      normalized.queType = queType
    }

    const tableValues = item.table_values ?? item.tableValues
    if (tableValues !== undefined) {
      normalized.tableValues = tableValues
      output.push(normalized)
      continue
    }

    const values = item.values ?? (item.value !== undefined ? [item.value] : undefined)
    if (values === undefined) {
      throw new Error(`answer item ${String(queId)} requires values or table_values`)
    }
    normalized.values = values.map((value) => normalizeAnswerValue(value))
    output.push(normalized)
  }

  return output
}

function resolveFieldAnswers(
  fields: Record<string, unknown> | undefined,
  form: unknown
): Record<string, unknown>[] {
  const entries = Object.entries(fields ?? {})
  if (entries.length === 0) {
    return []
  }

  const index = buildFieldIndex(form)
  const answers: Record<string, unknown>[] = []

  for (const [fieldKey, fieldValue] of entries) {
    const field = resolveFieldByKey(fieldKey, index)
    if (!field) {
      throw new Error(`Cannot resolve field key "${fieldKey}" from form metadata`)
    }
    answers.push(makeAnswerFromField(field, fieldValue))
  }

  return answers
}

function makeAnswerFromField(field: FormField, value: unknown): Record<string, unknown> {
  const base: Record<string, unknown> = {
    queId: field.queId
  }
  if (field.queTitle !== undefined) {
    base.queTitle = field.queTitle
  }
  if (field.queType !== undefined) {
    base.queType = field.queType
  }

  if (value && typeof value === "object" && !Array.isArray(value)) {
    const objectValue = value as Record<string, unknown>
    if ("tableValues" in objectValue || "table_values" in objectValue) {
      return {
        ...base,
        tableValues: objectValue.tableValues ?? objectValue.table_values
      }
    }
    if ("values" in objectValue) {
      return {
        ...base,
        values: asArray(objectValue.values).map((item) => normalizeAnswerValue(item))
      }
    }
  }

  if (Array.isArray(value) && value.length > 0 && Array.isArray(value[0])) {
    return {
      ...base,
      tableValues: value
    }
  }

  const valueArray = Array.isArray(value) ? value : [value]
  return {
    ...base,
    values: valueArray.map((item) => normalizeAnswerValue(item))
  }
}

function normalizeAnswerValue(value: unknown): unknown {
  if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return {
      value,
      dataValue: value
    }
  }
  return value
}

function needsFormResolution(fields?: Record<string, unknown>): boolean {
  const keys = Object.keys(fields ?? {})
  if (!keys.length) {
    return false
  }
  return keys.some((key) => !isNumericKey(key))
}

async function getFormCached(
  appKey: string,
  userId?: string,
  forceRefresh = false
): Promise<QingflowResponse<unknown>> {
  const cacheKey = `${appKey}::${userId ?? ""}`
  if (!forceRefresh) {
    const cached = formCache.get(cacheKey)
    if (cached && cached.expiresAt > Date.now()) {
      return cached.data
    }
  }

  const response = await client.getForm(appKey, { userId })
  formCache.set(cacheKey, {
    expiresAt: Date.now() + FORM_CACHE_TTL_MS,
    data: response
  })

  return response
}

function extractFieldSummaries(form: Record<string, unknown> | null) {
  const root = asArray(form?.questionBaseInfos)
  return root.map((raw) => {
    const field = asObject(raw) ?? {}
    const sub = asArray(field.subQuestionBaseInfos)
    return {
      que_id: (field.queId as string | number | null | undefined) ?? null,
      que_title: asNullableString(field.queTitle),
      que_type: field.queType,
      has_sub_fields: sub.length > 0,
      sub_field_count: sub.length
    }
  })
}

function buildFieldIndex(form: unknown): FieldIndex {
  const byId = new Map<string, FormField>()
  const byTitle = new Map<string, FormField[]>()
  const root = asArray(asObject(form)?.questionBaseInfos) as FormField[]
  const queue = [...root]

  while (queue.length > 0) {
    const current = queue.shift()
    if (!current) {
      continue
    }
    if (current.queId !== undefined && current.queId !== null) {
      byId.set(String(current.queId), current)
    }
    if (typeof current.queTitle === "string" && current.queTitle.trim()) {
      const titleKey = current.queTitle.trim().toLowerCase()
      const list = byTitle.get(titleKey) ?? []
      list.push(current)
      byTitle.set(titleKey, list)
    }
    const sub = asArray(current.subQuestionBaseInfos)
    for (const child of sub) {
      queue.push(child as FormField)
    }
  }

  return { byId, byTitle }
}

function resolveFieldByKey(fieldKey: string, index: FieldIndex): FormField | null {
  if (isNumericKey(fieldKey)) {
    const normalized = String(Number(fieldKey))
    const hit = index.byId.get(normalized)
    if (hit) {
      return hit
    }
    return { queId: Number(fieldKey) }
  }

  const titleKey = fieldKey.trim().toLowerCase()
  const matches = index.byTitle.get(titleKey) ?? []
  if (matches.length === 1) {
    return matches[0]
  }
  if (matches.length > 1) {
    const candidateIds = matches.map((item) => String(item.queId)).join(", ")
    throw new Error(`Field title "${fieldKey}" is ambiguous. Candidate queId: ${candidateIds}`)
  }
  return null
}

async function normalizeListSort(
  sort: Array<{ que_id: string | number; ascend?: boolean }> | undefined,
  appKey: string,
  userId?: string
): Promise<Array<{ que_id: string | number; ascend?: boolean }> | undefined> {
  if (!sort?.length) {
    return sort
  }

  // Fast path for numeric que_id, which can be passed through directly.
  if (sort.every((item) => isNumericKey(String(item.que_id)))) {
    return sort.map((item) => ({
      que_id: Number(item.que_id),
      ...(item.ascend !== undefined ? { ascend: item.ascend } : {})
    }))
  }

  const form = await getFormCached(appKey, userId, false)
  const index = buildFieldIndex(form.result)

  return sort.map((item) => {
    const rawKey = String(item.que_id).trim()
    const resolved = resolveFieldByKey(rawKey, index)
    if (!resolved || resolved.queId === undefined || resolved.queId === null) {
      throw new Error(
        `Cannot resolve sort.que_id "${rawKey}". Use numeric que_id or exact field title from qf_form_get.`
      )
    }
    return {
      que_id: normalizeQueId(resolved.queId),
      ...(item.ascend !== undefined ? { ascend: item.ascend } : {})
    }
  })
}

function resolveListItemLimit(params: {
  total: number
  requestedMaxRows?: number
  requestedMaxItems?: number
}): { limit: number; reason: string | null } {
  if (params.total <= 0) {
    return { limit: 0, reason: null }
  }

  const explicitLimits: Array<{ name: string; value: number }> = []
  if (params.requestedMaxRows !== undefined) {
    explicitLimits.push({ name: "max_rows", value: params.requestedMaxRows })
  }
  if (params.requestedMaxItems !== undefined) {
    explicitLimits.push({ name: "max_items", value: params.requestedMaxItems })
  }

  if (explicitLimits.length > 0) {
    const limit = Math.min(params.total, ...explicitLimits.map((item) => item.value))
    if (limit < params.total) {
      return {
        limit,
        reason: `limited by ${explicitLimits
          .map((item) => `${item.name}=${item.value}`)
          .join(", ")} (effective=${limit})`
      }
    }
    return { limit, reason: null }
  }

  if (params.total > DEFAULT_ROW_LIMIT) {
    return {
      limit: DEFAULT_ROW_LIMIT,
      reason: `default-limited to ${DEFAULT_ROW_LIMIT} items`
    }
  }

  return { limit: params.total, reason: null }
}

function projectRecordItemsColumns(params: {
  items: Array<Record<string, unknown>>
  includeAnswers: boolean
  maxColumns?: number
  selectColumns: Array<string | number>
}): {
  items: Array<Record<string, unknown>>
  reason: string | null
  selectedColumns: string[]
  matchedAnswersCount: number
} {
  if (!params.includeAnswers) {
    return {
      items: params.items,
      reason: null,
      selectedColumns: [],
      matchedAnswersCount: 0
    }
  }

  const normalizedSelectors = normalizeColumnSelectors(params.selectColumns)
  if (normalizedSelectors.length === 0) {
    throw new Error("select_columns must contain at least one non-empty column identifier")
  }
  const selectorSet = new Set(normalizedSelectors.map((item) => normalizeColumnSelector(item)))
  let columnCapped = false
  let matchedAnswersCount = 0

  const projectedItems = params.items.map((item) => {
    const answers = asArray(item.answers)
    let projected = answers

    if (selectorSet.size > 0) {
      projected = answers.filter((answer) => answerMatchesAnySelector(answer, selectorSet))
    }

    if (params.maxColumns !== undefined && projected.length > params.maxColumns) {
      projected = projected.slice(0, params.maxColumns)
      columnCapped = true
    }
    matchedAnswersCount += projected.length

    return {
      ...item,
      answers: projected
    }
  })

  const reason = mergeTruncationReasons(
    selectorSet.size > 0 ? `selected columns=${normalizedSelectors.length}` : null,
    columnCapped && params.maxColumns !== undefined
      ? `limited to max_columns=${params.maxColumns}`
      : null
  )

  return {
    items: projectedItems,
    reason,
    selectedColumns: normalizedSelectors,
    matchedAnswersCount
  }
}

function projectAnswersForOutput(params: {
  answers: unknown[]
  maxColumns?: number
  selectColumns?: Array<string | number>
}): { answers: unknown[]; selectedColumns: string[] | null } {
  const normalizedSelectors = normalizeColumnSelectors(params.selectColumns)
  const selectorSet = new Set(normalizedSelectors.map((item) => normalizeColumnSelector(item)))
  let projected = params.answers

  if (selectorSet.size > 0) {
    projected = projected.filter((answer) => answerMatchesAnySelector(answer, selectorSet))
  }

  if (params.maxColumns !== undefined && projected.length > params.maxColumns) {
    projected = projected.slice(0, params.maxColumns)
  }

  return {
    answers: projected,
    selectedColumns: normalizedSelectors.length > 0 ? normalizedSelectors : null
  }
}

function fitListItemsWithinSize(params: {
  items: Array<Record<string, unknown>>
  limitBytes: number
}): {
  items: Array<Record<string, unknown>>
  reason: string | null
  omittedItems: number
  omittedChars: number
} {
  let candidate = params.items
  const originalSize = jsonSizeBytes(candidate)
  let size = originalSize
  if (size <= params.limitBytes) {
    return { items: candidate, reason: null, omittedItems: 0, omittedChars: 0 }
  }

  const originalCount = candidate.length
  while (candidate.length > 1) {
    candidate = candidate.slice(0, candidate.length - 1)
    size = jsonSizeBytes(candidate)
    if (size <= params.limitBytes) {
      return {
        items: candidate,
        reason: `auto-limited to ${candidate.length} items to keep response <= ${params.limitBytes} bytes`,
        omittedItems: Math.max(0, originalCount - candidate.length),
        omittedChars: Math.max(0, originalSize - size)
      }
    }
  }

  throw new Error(
    `qf_records_list response is too large (${size} bytes > ${params.limitBytes}) even with 1 item. Use qf_record_get(apply_id).`
  )
}

function mergeTruncationReasons(...reasons: Array<string | null>): string | null {
  const list = reasons.filter((item): item is string => Boolean(item))
  return list.length > 0 ? list.join("; ") : null
}

function buildRecordsListMessage(params: {
  returned: number
  total: number
  truncationReason: string | null
}): string {
  if (!params.truncationReason) {
    return `Fetched ${params.returned} records`
  }
  return `Fetched ${params.returned}/${params.total} records (${params.truncationReason})`
}

function normalizeColumnSelectors(selectColumns?: Array<string | number>): string[] {
  if (!selectColumns?.length) {
    return []
  }

  const deduped = new Set<string>()
  for (const value of selectColumns) {
    const normalized = String(value).trim()
    if (!normalized) {
      continue
    }
    deduped.add(normalized)
  }
  return Array.from(deduped)
}

function normalizeColumnSelector(value: string | number): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return `id:${Math.trunc(value)}`
  }

  const normalized = String(value).trim()
  if (!normalized) {
    return "title:"
  }
  if (isNumericKey(normalized)) {
    return `id:${Number(normalized)}`
  }
  return `title:${normalized.toLowerCase()}`
}

function answerMatchesAnySelector(answer: unknown, selectorSet: Set<string>): boolean {
  const obj = asObject(answer)
  if (!obj) {
    return false
  }

  const candidates = [
    normalizeColumnSelector(asNullableString(obj.queId) ?? ""),
    normalizeColumnSelector(asNullableString(obj.queTitle) ?? "")
  ]

  return candidates.some((candidate) => selectorSet.has(candidate))
}

function normalizeQueId(queId: unknown): string | number {
  if (typeof queId === "number" && Number.isInteger(queId)) {
    return queId
  }
  if (typeof queId === "string") {
    const normalized = queId.trim()
    if (!normalized) {
      throw new Error("Resolved que_id is empty")
    }
    return isNumericKey(normalized) ? Number(normalized) : normalized
  }
  throw new Error(`Resolved que_id has unsupported type: ${typeof queId}`)
}

function stableJson(value: unknown): string {
  if (value === null || value === undefined) {
    return "null"
  }
  if (typeof value !== "object") {
    return JSON.stringify(value)
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableJson(item)).join(",")}]`
  }
  const obj = value as Record<string, unknown>
  const keys = Object.keys(obj).sort()
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stableJson(obj[key])}`).join(",")}}`
}

function jsonSizeBytes(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value), "utf8")
}

function isNumericKey(value: string): boolean {
  return /^\d+$/.test(value.trim())
}

function okResult<T extends Record<string, unknown>>(payload: T, message: string) {
  return {
    structuredContent: payload,
    content: [
      {
        type: "text" as const,
        text: message
      }
    ]
  }
}

function errorResult(error: unknown) {
  const payload = toErrorPayload(error)
  return {
    isError: true,
    // Keep error payload in text to avoid outputSchema(success) validation conflicts across MCP clients.
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(payload, null, 2)
      }
    ]
  }
}

function toErrorPayload(error: unknown): Record<string, unknown> {
  if (error instanceof NeedMoreDataError) {
    const details = asObject(error.details)
    const completeness = asObject(details?.completeness)
    return {
      ok: false,
      code: error.code,
      error_code: error.code,
      status: "need_more_data",
      message: error.message,
      fix_hint: "Continue with next_page_token or increase requested_pages/scan_max_pages.",
      next_page_token: asNullableString(completeness?.next_page_token),
      details: error.details
    }
  }
  if (error instanceof InputValidationError) {
    return {
      ok: false,
      error_code: error.errorCode,
      message: error.message,
      fix_hint: error.fixHint,
      next_page_token: null,
      details: error.details
    }
  }
  if (error instanceof QingflowApiError) {
    return {
      ok: false,
      error_code: "QINGFLOW_API_ERROR",
      message: error.message,
      err_code: error.errCode,
      err_msg: error.errMsg || null,
      http_status: error.httpStatus,
      fix_hint: "Check app_key/accessToken and request body against qf_form_get field definitions.",
      next_page_token: null,
      details: error.details ?? null
    }
  }
  if (error instanceof z.ZodError) {
    const firstIssue = error.issues[0]
    const firstPath = firstIssue?.path?.join(".") || "arguments"
    return {
      ok: false,
      error_code: "INVALID_ARGUMENTS",
      message: "Invalid arguments",
      fix_hint: `Fix field "${firstPath}" and retry with schema-compliant values.`,
      next_page_token: null,
      issues: error.issues
    }
  }
  if (error instanceof Error) {
    return {
      ok: false,
      error_code: "INTERNAL_ERROR",
      message: error.message,
      fix_hint: "Retry the request. If it persists, report query_id and input payload.",
      next_page_token: null
    }
  }
  return {
    ok: false,
    error_code: "UNKNOWN_ERROR",
    message: "Unknown error",
    fix_hint: "Retry the request with explicit app_key/select_columns and deterministic page parameters.",
    next_page_token: null,
    details: error
  }
}

function asObject(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : []
}

function toPositiveInt(value: unknown): number | null {
  if (typeof value === "number" && Number.isInteger(value) && value > 0) {
    return value
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value)
    if (Number.isInteger(parsed) && parsed > 0) {
      return parsed
    }
  }
  return null
}

function toNonNegativeInt(value: unknown): number | null {
  if (typeof value === "number" && Number.isInteger(value) && value >= 0) {
    return value
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value)
    if (Number.isInteger(parsed) && parsed >= 0) {
      return parsed
    }
  }
  return null
}

function asNullableString(value: unknown): string | null {
  if (typeof value === "string") {
    return value
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value)
  }
  return null
}

#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js"
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js"
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

const FORM_CACHE_TTL_MS = Number(process.env.QINGFLOW_FORM_CACHE_TTL_MS ?? "300000")
const formCache = new Map<string, FormCacheEntry>()
const DEFAULT_PAGE_SIZE = 50
const DEFAULT_MAX_ITEMS_WITH_ANSWERS =
  toPositiveInt(process.env.QINGFLOW_LIST_MAX_ITEMS_WITH_ANSWERS) ?? 5
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
  version: "0.2.0"
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

const appsOutputSchema = z.object({
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

const formInputSchema = z.object({
  app_key: z.string().min(1),
  user_id: z.string().min(1).optional(),
  force_refresh: z.boolean().optional(),
  include_raw: z.boolean().optional()
})

const formOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    app_key: z.string(),
    total_fields: z.number().int().nonnegative(),
    field_summaries: z.array(fieldSummarySchema),
    form: z.unknown().optional()
  }),
  meta: apiMetaSchema
})

const listInputSchema = z
  .object({
    app_key: z.string().min(1),
    user_id: z.string().min(1).optional(),
    page_num: z.number().int().positive().optional(),
    page_size: z.number().int().positive().max(200).optional(),
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
    max_columns: z.number().int().positive().max(200).optional(),
    // Strict mode: callers must explicitly choose columns.
    select_columns: z.array(z.union([z.string().min(1), z.number().int()])).min(1).max(200),
    include_answers: z.boolean().optional()
  })
  .refine((value) => value.include_answers !== false, {
    message: "include_answers=false is not allowed in strict column mode"
  })

const listOutputSchema = z.object({
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
      .optional()
  }),
  meta: apiMetaSchema
})

const recordGetInputSchema = z.object({
  apply_id: z.union([z.string().min(1), z.number().int()]),
  max_columns: z.number().int().positive().max(200).optional(),
  select_columns: z
    .array(z.union([z.string().min(1), z.number().int()]))
    .min(1)
    .max(200)
    .optional()
})

const recordGetOutputSchema = z.object({
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
      .optional()
  }),
  meta: apiMetaSchema
})

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

const createOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    request_id: z.string().nullable(),
    apply_id: z.union([z.string(), z.number(), z.null()]),
    async_hint: z.string()
  }),
  meta: apiMetaSchema
})

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

const updateOutputSchema = z.object({
  ok: z.literal(true),
  data: z.object({
    request_id: z.string().nullable(),
    async_hint: z.string()
  }),
  meta: apiMetaSchema
})

const operationInputSchema = z.object({
  request_id: z.string().min(1)
})

const operationOutputSchema = z.object({
  ok: z.literal(true),
  data: operationResultSchema,
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
      const pageNum = args.page_num ?? 1
      const pageSize = args.page_size ?? DEFAULT_PAGE_SIZE
      const normalizedSort = await normalizeListSort(args.sort, args.app_key, args.user_id)
      const includeAnswers = true
      const payload = buildListPayload({
        pageNum,
        pageSize,
        mode: args.mode,
        type: args.type,
        keyword: args.keyword,
        queryLogic: args.query_logic,
        applyIds: args.apply_ids,
        sort: normalizedSort,
        filters: args.filters
      })

      const response = await client.listRecords(args.app_key, payload, { userId: args.user_id })
      const result = asObject(response.result)
      const rawItems = asArray(result?.result)
      const listLimit = resolveListItemLimit({
        total: rawItems.length,
        requestedMaxRows: args.max_rows,
        requestedMaxItems: args.max_items,
        includeAnswers
      })

      const items = rawItems
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
      const truncationReason = mergeTruncationReasons(
        listLimit.reason,
        columnProjection.reason,
        fitted.reason
      )
      return okResult(
        {
          ok: true,
          data: {
            app_key: args.app_key,
            pagination: {
              page_num: toPositiveInt(result?.pageNum) ?? pageNum,
              page_size: toPositiveInt(result?.pageSize) ?? pageSize,
              page_amount: toNonNegativeInt(result?.pageAmount),
              result_amount: toNonNegativeInt(result?.resultAmount) ?? fitted.items.length
            },
            items: fitted.items,
            applied_limits: {
              include_answers: includeAnswers,
              row_cap: listLimit.limit,
              column_cap: args.max_columns ?? null,
              selected_columns: columnProjection.selectedColumns
            }
          },
          meta: buildMeta(response)
        },
        buildRecordsListMessage({
          returned: fitted.items.length,
          total: rawItems.length,
          truncationReason
        })
      )
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

      return okResult(
        {
          ok: true,
          data: {
            apply_id: (record.applyId as string | number | null | undefined) ?? null,
            answer_count: answerCount,
            record: projectedRecord,
            applied_limits: {
              column_cap: args.max_columns ?? null,
              selected_columns: projection.selectedColumns
            }
          },
          meta: buildMeta(response)
        },
        `Fetched record ${String(args.apply_id)}`
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

async function main(): Promise<void> {
  const transport = new StdioServerTransport()
  await server.connect(transport)
}

void main()

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
  includeAnswers: boolean
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

  if (params.includeAnswers && params.total > DEFAULT_MAX_ITEMS_WITH_ANSWERS) {
    return {
      limit: DEFAULT_MAX_ITEMS_WITH_ANSWERS,
      reason: `auto-limited to ${DEFAULT_MAX_ITEMS_WITH_ANSWERS} items because include_answers=true`
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
}): { items: Array<Record<string, unknown>>; reason: string | null } {
  let candidate = params.items
  let size = jsonSizeBytes(candidate)
  if (size <= params.limitBytes) {
    return { items: candidate, reason: null }
  }

  while (candidate.length > 1) {
    candidate = candidate.slice(0, candidate.length - 1)
    size = jsonSizeBytes(candidate)
    if (size <= params.limitBytes) {
      return {
        items: candidate,
        reason: `auto-limited to ${candidate.length} items to keep response <= ${params.limitBytes} bytes`
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
    structuredContent: payload,
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(payload, null, 2)
      }
    ]
  }
}

function toErrorPayload(error: unknown): Record<string, unknown> {
  if (error instanceof QingflowApiError) {
    return {
      ok: false,
      message: error.message,
      err_code: error.errCode,
      err_msg: error.errMsg || null,
      http_status: error.httpStatus,
      details: error.details ?? null
    }
  }
  if (error instanceof z.ZodError) {
    return {
      ok: false,
      message: "Invalid arguments",
      issues: error.issues
    }
  }
  if (error instanceof Error) {
    return {
      ok: false,
      message: error.message
    }
  }
  return {
    ok: false,
    message: "Unknown error",
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

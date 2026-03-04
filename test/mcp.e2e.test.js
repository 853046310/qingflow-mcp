import assert from "node:assert/strict"
import http from "node:http"
import { once } from "node:events"
import path from "node:path"
import { randomUUID } from "node:crypto"
import test from "node:test"
import { fileURLToPath } from "node:url"

import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js"

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const ROOT_DIR = path.resolve(__dirname, "..")

const ACCESS_TOKEN = "test-token"
const APP_KEY = "app_demo"

function buildForm() {
  return {
    questionBaseInfos: [
      { queId: 1001, queTitle: "客户名称", queType: 2, subQuestionBaseInfos: [] },
      { queId: 1002, queTitle: "金额", queType: 6, subQuestionBaseInfos: [] },
      { queId: 1003, queTitle: "下单日期", queType: 3, subQuestionBaseInfos: [] }
    ],
    questionRelations: []
  }
}

function makeAnswer(queId, queTitle, value) {
  if (value === null || value === undefined || value === "") {
    return {
      queId,
      queTitle,
      values: []
    }
  }

  return {
    queId,
    queTitle,
    values: [
      {
        value,
        dataValue: value
      }
    ]
  }
}

function makeRecord({ applyId, customer, amount, day }) {
  return {
    applyId: String(applyId),
    appKey: APP_KEY,
    applyNum: Number(applyId),
    applyTime: `${day} 09:00:00`,
    lastUpdateTime: `${day} 10:00:00`,
    answers: [
      makeAnswer(1001, "客户名称", customer),
      makeAnswer(1002, "金额", amount),
      makeAnswer(1003, "下单日期", day)
    ]
  }
}

function buildInitialRecords() {
  return [
    makeRecord({ applyId: 5001, customer: "客户A", amount: 100, day: "2026-01-01" }),
    makeRecord({ applyId: 5002, customer: "客户B", amount: 200, day: "2026-01-02" }),
    makeRecord({ applyId: 5003, customer: "客户C", amount: -50, day: "2026-01-02" }),
    makeRecord({ applyId: 5004, customer: "客户D", amount: null, day: "2026-01-03" }),
    makeRecord({ applyId: 5005, customer: "客户E", amount: 70, day: "2026-02-01" }),
    makeRecord({ applyId: 5006, customer: "客户F", amount: 30, day: "2026-01-03" })
  ]
}

function clone(value) {
  return JSON.parse(JSON.stringify(value))
}

function normalizeQueId(value) {
  if (typeof value === "number" && Number.isInteger(value)) {
    return value
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value)
    if (Number.isInteger(parsed)) {
      return parsed
    }
  }
  return null
}

function extractAnswerValue(answers, queId) {
  for (const answerRaw of answers) {
    if (!answerRaw || typeof answerRaw !== "object") {
      continue
    }
    const answer = answerRaw
    const answerQueId = normalizeQueId(answer.queId)
    if (answerQueId !== queId) {
      continue
    }

    const values = Array.isArray(answer.values) ? answer.values : []
    if (values.length === 0) {
      return null
    }

    const first = values[0]
    if (first && typeof first === "object") {
      if (first.dataValue !== undefined) {
        return first.dataValue
      }
      if (first.value !== undefined) {
        return first.value
      }
    }
    return first ?? null
  }
  return null
}

function updateAnswer(answers, patch) {
  const queId = normalizeQueId(patch.queId)
  if (queId === null) {
    return
  }
  const index = answers.findIndex((item) => normalizeQueId(item?.queId) === queId)
  const next = clone(patch)
  if (index >= 0) {
    answers[index] = next
  } else {
    answers.push(next)
  }
}

async function readBody(req) {
  const chunks = []
  for await (const chunk of req) {
    chunks.push(Buffer.from(chunk))
  }
  const raw = Buffer.concat(chunks).toString("utf8")
  if (!raw.trim()) {
    return {}
  }
  try {
    return JSON.parse(raw)
  } catch {
    return null
  }
}

function sendJson(res, payload, status = 200) {
  res.statusCode = status
  res.setHeader("content-type", "application/json; charset=utf-8")
  res.end(JSON.stringify(payload))
}

function handleFilter(state, body) {
  if (!body || typeof body !== "object") {
    return {
      error: {
        errCode: 400,
        errMsg: "请求体解析错误",
        result: {
          statusCode: 400,
          message: "请求体解析错误",
          details: null
        }
      }
    }
  }

  const knownQueIds = new Set([1001, 1002, 1003])
  const sorts = Array.isArray(body.sorts) ? body.sorts : []
  for (const sort of sorts) {
    const queId = normalizeQueId(sort?.queId)
    if (queId === null || !knownQueIds.has(queId)) {
      return {
        error: {
          errCode: 400,
          errMsg: "请求体解析错误",
          result: {
            statusCode: 400,
            message: "请求体解析错误",
            details: null
          }
        }
      }
    }
  }

  let items = state.records.map((record) => clone(record))

  const applyIds = Array.isArray(body.applyIds) ? new Set(body.applyIds.map((item) => String(item))) : null
  if (applyIds && applyIds.size > 0) {
    items = items.filter((record) => applyIds.has(String(record.applyId)))
  }

  if (typeof body.queryKey === "string" && body.queryKey.trim()) {
    const keyword = body.queryKey.trim().toLowerCase()
    items = items.filter((record) =>
      record.answers.some((answer) => {
        const value = extractAnswerValue(record.answers, normalizeQueId(answer.queId) ?? -1)
        return value !== null && String(value).toLowerCase().includes(keyword)
      })
    )
  }

  const queries = Array.isArray(body.queries) ? body.queries : []
  for (const query of queries) {
    if (!query || typeof query !== "object") {
      continue
    }
    const queId = normalizeQueId(query.queId)
    if (queId === null || !knownQueIds.has(queId)) {
      continue
    }
    const minValue = typeof query.minValue === "string" ? query.minValue : undefined
    const maxValue = typeof query.maxValue === "string" ? query.maxValue : undefined

    if (minValue === undefined && maxValue === undefined) {
      continue
    }

    items = items.filter((record) => {
      const rawValue = extractAnswerValue(record.answers, queId)
      if (rawValue === null || rawValue === undefined) {
        return false
      }

      if (typeof rawValue === "number") {
        const min = minValue !== undefined ? Number(minValue) : null
        const max = maxValue !== undefined ? Number(maxValue) : null
        if (min !== null && Number.isFinite(min) && rawValue < min) {
          return false
        }
        if (max !== null && Number.isFinite(max) && rawValue > max) {
          return false
        }
        return true
      }

      const text = String(rawValue)
      if (minValue !== undefined && text < minValue) {
        return false
      }
      if (maxValue !== undefined && text > maxValue) {
        return false
      }
      return true
    })
  }

  if (sorts.length > 0) {
    const sort = sorts[0]
    const sortQueId = normalizeQueId(sort.queId)
    const isAscend = sort.isAscend !== false
    if (sortQueId !== null) {
      items.sort((a, b) => {
        const left = extractAnswerValue(a.answers, sortQueId)
        const right = extractAnswerValue(b.answers, sortQueId)

        if (left === right) {
          return 0
        }
        if (left === null || left === undefined) {
          return isAscend ? 1 : -1
        }
        if (right === null || right === undefined) {
          return isAscend ? -1 : 1
        }

        if (typeof left === "number" && typeof right === "number") {
          return isAscend ? left - right : right - left
        }

        const compared = String(left).localeCompare(String(right))
        return isAscend ? compared : -compared
      })
    }
  }

  const pageNum = Number.isInteger(body.pageNum) && body.pageNum > 0 ? body.pageNum : 1
  const pageSize = Number.isInteger(body.pageSize) && body.pageSize > 0 ? body.pageSize : 50
  const pageAmount = items.length > 0 ? Math.ceil(items.length / pageSize) : 0
  const start = (pageNum - 1) * pageSize
  const pageItems = items.slice(start, start + pageSize)

  return {
    payload: {
      errCode: 0,
      errMsg: "ok",
      result: {
        pageAmount,
        pageNum,
        pageSize,
        resultAmount: items.length,
        result: pageItems
      }
    }
  }
}

async function startMockQingflowServer() {
  const state = {
    form: buildForm(),
    records: buildInitialRecords(),
    operations: new Map(),
    nextApplyId: 7000
  }

  const server = http.createServer(async (req, res) => {
    const method = req.method ?? "GET"
    const host = req.headers.host ?? "127.0.0.1"
    const url = new URL(req.url ?? "/", `http://${host}`)
    const pathname = url.pathname

    if (req.headers.accesstoken !== ACCESS_TOKEN) {
      sendJson(res, {
        errCode: 401,
        errMsg: "invalid access token",
        result: null
      })
      return
    }

    if (method === "GET" && pathname === "/app") {
      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: {
          appList: [
            { appKey: APP_KEY, appName: "Demo Sales" },
            { appKey: "other_app", appName: "Other" }
          ]
        }
      })
      return
    }

    const formMatch = pathname.match(/^\/app\/([^/]+)\/form$/)
    if (method === "GET" && formMatch) {
      const appKey = decodeURIComponent(formMatch[1])
      if (appKey !== APP_KEY) {
        sendJson(res, { errCode: 404, errMsg: "app not found", result: null })
        return
      }
      sendJson(res, { errCode: 0, errMsg: "ok", result: state.form })
      return
    }

    const listMatch = pathname.match(/^\/app\/([^/]+)\/apply\/filter$/)
    if (method === "POST" && listMatch) {
      const appKey = decodeURIComponent(listMatch[1])
      if (appKey !== APP_KEY) {
        sendJson(res, { errCode: 404, errMsg: "app not found", result: null })
        return
      }

      const body = await readBody(req)
      const filtered = handleFilter(state, body)
      if (filtered.error) {
        sendJson(res, filtered.error)
        return
      }
      sendJson(res, filtered.payload)
      return
    }

    const createMatch = pathname.match(/^\/app\/([^/]+)\/apply$/)
    if (method === "POST" && createMatch) {
      const appKey = decodeURIComponent(createMatch[1])
      if (appKey !== APP_KEY) {
        sendJson(res, { errCode: 404, errMsg: "app not found", result: null })
        return
      }

      const body = await readBody(req)
      const answers = Array.isArray(body?.answers) ? body.answers : []
      const customer = extractAnswerValue(answers, 1001) ?? `客户-${state.nextApplyId}`
      const amount = extractAnswerValue(answers, 1002)
      const day = String(extractAnswerValue(answers, 1003) ?? "2026-01-15")
      const applyId = String(state.nextApplyId++)

      state.records.push(
        makeRecord({
          applyId,
          customer: String(customer),
          amount: amount === null ? null : Number(amount),
          day
        })
      )

      const requestId = `req-${randomUUID()}`
      state.operations.set(requestId, {
        status: "SUCCESS",
        applyId,
        action: "create"
      })

      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: {
          requestId,
          applyId: Number(applyId)
        }
      })
      return
    }

    const applyMatch = pathname.match(/^\/apply\/([^/]+)$/)
    if (applyMatch && method === "GET") {
      const applyId = decodeURIComponent(applyMatch[1])
      const record = state.records.find((item) => String(item.applyId) === String(applyId))
      if (!record) {
        sendJson(res, { errCode: 404, errMsg: "record not found", result: null })
        return
      }
      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: clone(record)
      })
      return
    }

    if (applyMatch && method === "POST") {
      const applyId = decodeURIComponent(applyMatch[1])
      const record = state.records.find((item) => String(item.applyId) === String(applyId))
      if (!record) {
        sendJson(res, { errCode: 404, errMsg: "record not found", result: null })
        return
      }

      const body = await readBody(req)
      const answers = Array.isArray(body?.answers) ? body.answers : []
      for (const patch of answers) {
        if (!patch || typeof patch !== "object") {
          continue
        }
        updateAnswer(record.answers, patch)
      }
      record.lastUpdateTime = "2026-03-01 10:00:00"

      const requestId = `req-${randomUUID()}`
      state.operations.set(requestId, {
        status: "SUCCESS",
        applyId,
        action: "update"
      })

      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: {
          requestId
        }
      })
      return
    }

    const operationMatch = pathname.match(/^\/operation\/([^/]+)$/)
    if (method === "GET" && operationMatch) {
      const requestId = decodeURIComponent(operationMatch[1])
      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result:
          state.operations.get(requestId) ?? {
            status: "UNKNOWN",
            requestId
          }
      })
      return
    }

    sendJson(res, { errCode: 404, errMsg: "not found", result: null })
  })

  server.listen(0, "127.0.0.1")
  await once(server, "listening")

  const address = server.address()
  if (!address || typeof address === "string") {
    throw new Error("failed to start mock server")
  }

  const baseUrl = `http://127.0.0.1:${address.port}`

  return {
    baseUrl,
    state,
    close: async () => {
      await new Promise((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error)
            return
          }
          resolve(undefined)
        })
      })
    }
  }
}

async function startMcpClient(baseUrl) {
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [path.join(ROOT_DIR, "dist/server.js")],
    cwd: ROOT_DIR,
    env: {
      ...process.env,
      QINGFLOW_BASE_URL: baseUrl,
      QINGFLOW_ACCESS_TOKEN: ACCESS_TOKEN,
      QINGFLOW_FORM_CACHE_TTL_MS: "1",
      QINGFLOW_LIST_MAX_ITEMS_WITH_ANSWERS: "50",
      QINGFLOW_LIST_MAX_ITEMS_BYTES: "1000000"
    },
    stderr: "pipe"
  })

  if (transport.stderr) {
    transport.stderr.on("data", () => {
      // Keep stderr drained to avoid child process backpressure in tests.
    })
  }

  const client = new Client({
    name: "qingflow-mcp-e2e-test",
    version: "1.0.0"
  })

  await client.connect(transport)

  return {
    client,
    transport,
    close: async () => {
      await client.close()
    }
  }
}

async function callTool(client, name, args) {
  const result = await client.callTool({
    name,
    arguments: args
  })

  if (result.structuredContent && typeof result.structuredContent === "object") {
    return result.structuredContent
  }

  // Compatibility fallback for clients that return only text content.
  const textItems = Array.isArray(result.content)
    ? result.content.filter((item) => item?.type === "text" && typeof item?.text === "string")
    : []
  for (const item of textItems) {
    try {
      const parsed = JSON.parse(item.text)
      if (parsed && typeof parsed === "object") {
        return parsed
      }
    } catch {
      // ignore non-JSON text
    }
  }

  const debug = JSON.stringify(
    {
      keys: Object.keys(result ?? {}),
      hasContent: Array.isArray(result?.content),
      contentPreview: Array.isArray(result?.content) ? result.content.slice(0, 1) : null
    },
    null,
    2
  )
  throw new Error(`Tool ${name} returned no structured JSON payload: ${debug}`)
}

function firstAnswerValue(recordItem) {
  const answers = Array.isArray(recordItem?.answers) ? recordItem.answers : []
  if (answers.length === 0) {
    return null
  }
  return extractAnswerValue(answers, normalizeQueId(answers[0].queId) ?? -1)
}

test("MCP E2E: unified query + strict column controls + CRUD", async (t) => {
  const mock = await startMockQingflowServer()
  t.after(async () => {
    await mock.close()
  })

  const mcp = await startMcpClient(mock.baseUrl)
  t.after(async () => {
    await mcp.close()
  })

  await t.test("tools are exposed", async () => {
    const tools = await mcp.client.listTools()
    const names = tools.tools.map((item) => item.name)
    assert.ok(names.includes("qf_query"))
    assert.ok(names.includes("qf_records_list"))
    assert.ok(names.includes("qf_record_create"))
  })

  await t.test("apps and form basics", async () => {
    const apps = await callTool(mcp.client, "qf_apps_list", { keyword: "demo", limit: 5 })
    assert.equal(apps.ok, true)
    assert.equal(apps.data.returned_apps, 1)
    assert.equal(apps.data.apps[0].appKey, APP_KEY)

    const form = await callTool(mcp.client, "qf_form_get", { app_key: APP_KEY })
    assert.equal(form.ok, true)
    assert.equal(form.data.total_fields, 3)
  })

  await t.test("qf_records_list applies strict row/column projection", async () => {
    const listed = await callTool(mcp.client, "qf_records_list", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 20,
      max_rows: 3,
      max_columns: 1,
      select_columns: [1001, "金额"]
    })

    assert.equal(listed.ok, true)
    assert.equal(listed.data.items.length, 3)
    assert.deepEqual(listed.data.applied_limits.selected_columns, ["1001", "金额"])
    assert.equal(listed.data.applied_limits.row_cap, 3)

    for (const item of listed.data.items) {
      const answers = Array.isArray(item.answers) ? item.answers : []
      assert.ok(answers.length <= 1)
      if (answers[0]) {
        const queId = normalizeQueId(answers[0].queId)
        assert.ok(queId === 1001 || queId === 1002)
      }
    }
  })

  await t.test("qf_records_list resolves sort by field title", async () => {
    const listed = await callTool(mcp.client, "qf_records_list", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 3,
      sort: [{ que_id: "下单日期", ascend: false }],
      select_columns: [1003]
    })

    assert.equal(listed.ok, true)
    assert.equal(listed.data.items.length, 3)
    const firstDay = firstAnswerValue(listed.data.items[0])
    assert.equal(firstDay, "2026-02-01")
  })

  await t.test("qf_record_get supports select_columns/max_columns", async () => {
    const record = await callTool(mcp.client, "qf_record_get", {
      apply_id: "5001",
      select_columns: [1001, 1002],
      max_columns: 1
    })

    assert.equal(record.ok, true)
    assert.equal(record.data.answer_count, 1)
    assert.deepEqual(record.data.applied_limits.selected_columns, ["1001", "1002"])
    const answer = record.data.record.answers[0]
    assert.equal(normalizeQueId(answer.queId), 1001)
  })

  await t.test("qf_query auto routes to list and record", async () => {
    const queryList = await callTool(mcp.client, "qf_query", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 2,
      select_columns: [1001]
    })

    assert.equal(queryList.ok, true)
    assert.equal(queryList.data.mode, "list")
    assert.equal(queryList.data.source_tool, "qf_records_list")
    assert.equal(queryList.data.list.items.length, 2)

    const queryRecord = await callTool(mcp.client, "qf_query", {
      apply_id: "5001",
      select_columns: [1002]
    })

    assert.equal(queryRecord.ok, true)
    assert.equal(queryRecord.data.mode, "record")
    assert.equal(queryRecord.data.source_tool, "qf_record_get")
    assert.equal(queryRecord.data.record.apply_id, "5001")
  })

  await t.test("qf_query list mode applies time_range as filter", async () => {
    const queryList = await callTool(mcp.client, "qf_query", {
      query_mode: "list",
      app_key: APP_KEY,
      mode: "all",
      page_size: 20,
      select_columns: [1001],
      time_range: {
        column: 1003,
        from: "2026-01-02",
        to: "2026-01-02"
      }
    })

    assert.equal(queryList.ok, true)
    assert.equal(queryList.data.mode, "list")
    assert.equal(queryList.data.list.pagination.result_amount, 2)
    assert.equal(queryList.data.list.items.length, 2)
  })

  await t.test("qf_query summary returns computed aggregates + strict rows", async () => {
    const summary = await callTool(mcp.client, "qf_query", {
      query_mode: "summary",
      app_key: APP_KEY,
      mode: "all",
      select_columns: [1001],
      amount_column: 1002,
      time_range: {
        column: 1003,
        from: "2026-01-01",
        to: "2026-01-31",
        timezone: "UTC"
      },
      stat_policy: {
        include_negative: false,
        include_null: false
      },
      page_size: 2,
      scan_max_pages: 10,
      max_rows: 3
    })

    assert.equal(summary.ok, true)
    assert.equal(summary.data.mode, "summary")
    assert.equal(summary.data.summary.summary.total_count, 5)
    assert.equal(summary.data.summary.summary.total_amount, 330)
    assert.equal(summary.data.summary.summary.missing_count, 1)
    assert.equal(summary.data.summary.rows.length, 3)

    for (const row of summary.data.summary.rows) {
      assert.deepEqual(Object.keys(row), ["1001"])
    }

    const roles = summary.data.summary.meta.field_mapping.map((item) => item.role).sort()
    assert.deepEqual(roles, ["amount", "row", "time"])
    assert.equal(summary.data.summary.meta.execution.scanned_pages, 3)
    assert.equal(summary.data.summary.meta.execution.truncated, false)
  })

  await t.test("qf_query list mode missing select_columns returns structured error", async () => {
    const failed = await callTool(mcp.client, "qf_query", {
      query_mode: "list",
      app_key: APP_KEY,
      mode: "all",
      page_size: 2
    })

    assert.equal(failed.ok, false)
    assert.match(failed.message, /select_columns is required for list query/)
  })

  await t.test("create + update + operation are still working", async () => {
    const created = await callTool(mcp.client, "qf_record_create", {
      app_key: APP_KEY,
      fields: {
        客户名称: "测试客户",
        金额: 123,
        下单日期: "2026-01-20"
      }
    })

    assert.equal(created.ok, true)
    const applyId = String(created.data.apply_id)

    const beforeUpdate = await callTool(mcp.client, "qf_record_get", {
      apply_id: applyId,
      select_columns: [1002]
    })
    assert.equal(beforeUpdate.ok, true)
    assert.equal(firstAnswerValue(beforeUpdate.data.record), 123)

    const updated = await callTool(mcp.client, "qf_record_update", {
      apply_id: applyId,
      app_key: APP_KEY,
      fields: {
        金额: 456
      }
    })

    assert.equal(updated.ok, true)
    assert.equal(typeof updated.data.request_id, "string")

    const operation = await callTool(mcp.client, "qf_operation_get", {
      request_id: updated.data.request_id
    })
    assert.equal(operation.ok, true)
    assert.equal(operation.data.operation_result.status, "SUCCESS")

    const afterUpdate = await callTool(mcp.client, "qf_record_get", {
      apply_id: applyId,
      select_columns: [1002]
    })
    assert.equal(afterUpdate.ok, true)
    assert.equal(firstAnswerValue(afterUpdate.data.record), 456)
  })
})

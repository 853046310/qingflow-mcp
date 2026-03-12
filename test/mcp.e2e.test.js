import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { promises as fs } from "node:fs"
import http from "node:http"
import { once } from "node:events"
import path from "node:path"
import { randomUUID } from "node:crypto"
import test from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js"

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const ROOT_DIR = path.resolve(__dirname, "..")
const execFileAsync = promisify(execFile)

const ACCESS_TOKEN = "test-token"
const APP_KEY = "app_demo"
const BIG_APPLY_ID = "499569877794004993"
const BIG_RECORD = makeRecord({ applyId: BIG_APPLY_ID, customer: "大ID客户", amount: 888, day: "2026-01-04" })

const DEPARTMENTS = [
  {
    deptId: 1,
    name: "总部",
    parentId: null,
    ordinal: 1,
    deptLeader: ["u_admin"]
  },
  {
    deptId: 111,
    name: "销售部",
    parentId: 1,
    ordinal: 2,
    deptLeader: ["u_1", "u_2"]
  },
  {
    deptId: 112,
    name: "销售支持",
    parentId: 111,
    ordinal: 1,
    deptLeader: ["u_2"]
  },
  {
    deptId: 210,
    name: "研发部",
    parentId: 1,
    ordinal: 3,
    deptLeader: ["u_9"]
  }
]

const DIRECTORY_USERS = [
  {
    userId: "u_admin",
    name: "管理员",
    areaCode: "86",
    mobileNum: "13800000001",
    email: "admin@example.com",
    headImg: "https://img.example.com/admin.png",
    department: ["1"],
    role: ["r_admin"],
    customRole: [],
    customDepartment: [],
    beingDisabled: false,
    beingActive: true,
    superiorId: null
  },
  {
    userId: "u_1",
    name: "张三",
    areaCode: "86",
    mobileNum: "13800000002",
    email: "zhangsan@example.com",
    headImg: "https://img.example.com/u1.png",
    department: ["111"],
    role: ["sales"],
    customRole: ["custom_sales"],
    customDepartment: ["111"],
    beingDisabled: false,
    beingActive: true,
    superiorId: "u_admin"
  },
  {
    userId: "u_2",
    name: "李四",
    areaCode: "86",
    mobileNum: "13800000003",
    email: "lisi@example.com",
    headImg: "https://img.example.com/u2.png",
    department: ["111", "112"],
    role: ["sales_manager"],
    customRole: [],
    customDepartment: ["112"],
    beingDisabled: false,
    beingActive: true,
    superiorId: "u_admin"
  },
  {
    userId: "u_3",
    name: "赵六",
    areaCode: "86",
    mobileNum: "13800000004",
    email: "zhaoliu@example.com",
    headImg: "https://img.example.com/u3.png",
    department: ["112"],
    role: ["sales_support"],
    customRole: [],
    customDepartment: ["112"],
    beingDisabled: false,
    beingActive: true,
    superiorId: "u_2"
  },
  {
    userId: "u_9",
    name: "王五",
    areaCode: "86",
    mobileNum: "13800000009",
    email: "wangwu@example.com",
    headImg: "https://img.example.com/u9.png",
    department: ["210"],
    role: ["rd"],
    customRole: [],
    customDepartment: [],
    beingDisabled: false,
    beingActive: true,
    superiorId: "u_admin"
  }
]

const APP_INFO_ITEMS = [
  {
    appKey: APP_KEY,
    appName: "Demo Sales",
    appAuth: 1,
    appIcon: "icon-sales",
    authmembers: {
      users: [{ userId: "u_1", userName: "张三" }],
      depts: [{ deptId: 111, deptName: "销售部" }],
      roles: [{ roleId: 10, roleName: "销售经理" }]
    },
    creator: {
      userId: "u_admin",
      nickName: "管理员",
      headImg: "https://img.example.com/admin.png"
    },
    createTime: "2024-01-01 10:00:00",
    tags: [{ tagId: 1001, tagName: "销售应用包" }],
    appPublishStatus: 1
  },
  {
    appKey: "app_rd",
    appName: "研发日报",
    appAuth: 2,
    appIcon: "icon-rd",
    authmembers: {
      users: [{ userId: "u_9", userName: "王五" }],
      depts: [{ deptId: 210, deptName: "研发部" }],
      roles: [{ roleId: 20, roleName: "研发经理" }]
    },
    creator: {
      userId: "u_9",
      nickName: "王五",
      headImg: "https://img.example.com/u9.png"
    },
    createTime: "2024-01-02 10:00:00",
    tags: [{ tagId: 1002, tagName: "研发应用包" }],
    appPublishStatus: 2
  }
]

const APP_PACKAGE_LIST = [
  {
    tagId: 1001,
    tagName: "销售应用包",
    tagIcon: "icon-tag-sales",
    appList: [
      { appKey: APP_KEY, appName: "Demo Sales" },
      { appKey: "app_quote", appName: "报价系统" }
    ],
    dashList: [{ dashKey: "dash_sales", dashName: "销售总览" }]
  },
  {
    tagId: 1002,
    tagName: "研发应用包",
    tagIcon: "icon-tag-rd",
    appList: [{ appKey: "app_rd", appName: "研发日报" }],
    dashList: [{ dashKey: "dash_rd", dashName: "研发看板" }]
  }
]

const APPLY_AUDIT_RECORDS = {
  "5001": {
    applyStatus: { value: 1, label: "processing" },
    auditRecords: [
      {
        auditRcdId: 1111,
        auditNodeId: 1,
        auditNodeName: "直属主管审批",
        auditTime: 1534482649832,
        auditResult: { value: "pass", label: "通过" },
        auditFeedback: "同意",
        auditUser: {
          userId: "u_1",
          userName: "张三",
          nickName: "张三",
          headImg: "https://img.example.com/u1.png"
        },
        waitAuditUserList: [
          {
            userId: "u_2",
            userName: "李四",
            nickName: "李四",
            headImg: "https://img.example.com/u2.png"
          }
        ]
      }
    ],
    currentNodes: [
      {
        auditRcdId: null,
        auditNodeId: 2,
        auditNodeName: "财务审批",
        auditTime: null,
        auditResult: null,
        auditFeedback: null,
        auditUser: null,
        waitAuditUserList: [
          {
            userId: "u_3",
            userName: "赵六",
            nickName: "赵六",
            headImg: "https://img.example.com/u3.png"
          }
        ]
      }
    ]
  }
}

const APPLY_AUDIT_RECORD_DETAILS = {
  "5001:1111": {
    auditRcdId: 1111,
    auditModifies: {
      first: {
        queId: 876,
        queTitle: "单行文字",
        queType: { code: 2, name: "text" },
        beforeAnswer: ["旧值"],
        afterAnswer: ["新值"]
      }
    }
  }
}

function buildForm() {
  return {
    questionBaseInfos: [
      { queId: 1001, queTitle: "客户名称", queType: 2, subQuestionBaseInfos: [] },
      { queId: 1002, queTitle: "金额", queType: 6, subQuestionBaseInfos: [] },
      { queId: 1003, queTitle: "下单日期", queType: 4, subQuestionBaseInfos: [] },
      { queId: 1004, queTitle: "归属销售", queType: { code: 18, name: "member" }, subQuestionBaseInfos: [] },
      { queId: 1005, queTitle: "归属部门", queType: { code: 19, name: "department" }, subQuestionBaseInfos: [] },
      {
        queId: 1006,
        queTitle: "报销类型",
        queType: { code: 3, name: "single_select" },
        options: [
          { optId: 1, optValue: "出差", linkQueIds: ["1007"] },
          { optId: 2, optValue: "日常", linkQueIds: [] }
        ],
        subQuestionBaseInfos: []
      },
      {
        queId: 1007,
        queTitle: "出差城市",
        queType: 2,
        required: true,
        subQuestionBaseInfos: []
      },
      {
        queId: 1008,
        queTitle: "系统创建时间",
        queType: 4,
        readonly: true,
        system: true,
        subQuestionBaseInfos: []
      }
    ],
    questionRelations: [
      {
        sourceQueId: 1006,
        targetQueId: 1007,
        relationType: "show_when_option_selected"
      }
    ]
  }
}

function makeAnswer(queId, queTitle, value) {
  if (value === null || value === undefined || value === "") {
    return {
      queId,
      queTitle,
      values: [],
      tableValues: []
    }
  }

  return {
    queId,
    queTitle,
    tableValues: [],
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
    if (applyIds.has(BIG_APPLY_ID)) {
      items.push(clone(BIG_RECORD))
    }
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

    if (method === "GET" && pathname === "/apps") {
      const appKey = url.searchParams.get("appKey")
      const pageNum = normalizeQueId(url.searchParams.get("pageNum")) ?? 1
      const pageSize = normalizeQueId(url.searchParams.get("pageSize")) ?? 50
      let apps = APP_INFO_ITEMS.map((item) => clone(item))
      if (appKey) {
        apps = apps.filter((item) => item.appKey === appKey)
      }
      const pageAmount = apps.length > 0 ? Math.ceil(apps.length / pageSize) : 0
      const start = (pageNum - 1) * pageSize
      const pageItems = apps.slice(start, start + pageSize)
      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: {
          pageNum,
          pageSize,
          pageAmount,
          resultAmount: apps.length,
          apps: pageItems
        }
      })
      return
    }

    if (method === "GET" && pathname === "/tags") {
      const userId = url.searchParams.get("userId")
      if (!userId) {
        sendJson(res, { errCode: 400, errMsg: "missing userId", result: null })
        return
      }
      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: {
          tagList: APP_PACKAGE_LIST.map((item) => clone(item))
        }
      })
      return
    }

    const auditRecordDetailMatch = pathname.match(/^\/apply\/([^/]+)\/auditRecord\/([^/]+)$/)
    if (method === "GET" && auditRecordDetailMatch) {
      const applyId = decodeURIComponent(auditRecordDetailMatch[1])
      const auditRcdId = decodeURIComponent(auditRecordDetailMatch[2])
      const detail = APPLY_AUDIT_RECORD_DETAILS[`${applyId}:${auditRcdId}`]
      if (!detail) {
        sendJson(res, { errCode: 404, errMsg: "audit record not found", result: null })
        return
      }
      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: clone(detail)
      })
      return
    }

    const auditRecordsMatch = pathname.match(/^\/apply\/([^/]+)\/auditRecord$/)
    if (method === "GET" && auditRecordsMatch) {
      const applyId = decodeURIComponent(auditRecordsMatch[1])
      const detail = APPLY_AUDIT_RECORDS[applyId]
      if (!detail) {
        sendJson(res, { errCode: 404, errMsg: "apply not found", result: null })
        return
      }
      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: clone(detail)
      })
      return
    }

    if (method === "GET" && pathname === "/department") {
      const deptId = normalizeQueId(url.searchParams.get("deptId"))
      if (url.searchParams.has("deptId") && deptId === null) {
        sendJson(res, { errCode: 404, errMsg: "department not found", result: null })
        return
      }

      let departments = DEPARTMENTS.map((item) => clone(item))
      if (deptId !== null) {
        const root = DEPARTMENTS.find((item) => item.deptId === deptId)
        if (!root) {
          sendJson(res, { errCode: 404, errMsg: "department not found", result: null })
          return
        }

        const included = new Set([deptId])
        let changed = true
        while (changed) {
          changed = false
          for (const item of DEPARTMENTS) {
            if (item.parentId !== null && included.has(item.parentId) && !included.has(item.deptId)) {
              included.add(item.deptId)
              changed = true
            }
          }
        }
        departments = departments.filter((item) => included.has(item.deptId))
      }

      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: {
          department: departments
        }
      })
      return
    }

    const departmentUsersMatch = pathname.match(/^\/department\/([^/]+)\/user$/)
    if (method === "GET" && departmentUsersMatch) {
      const deptId = decodeURIComponent(departmentUsersMatch[1])
      const numericDeptId = normalizeQueId(deptId)
      const fetchChild = url.searchParams.get("fetchChild") === "true"
      const root = DEPARTMENTS.find((item) => String(item.deptId) === String(numericDeptId ?? deptId))
      if (!root) {
        sendJson(res, { errCode: 404, errMsg: "department not found", result: null })
        return
      }

      const included = new Set([String(root.deptId)])
      if (fetchChild) {
        let changed = true
        while (changed) {
          changed = false
          for (const item of DEPARTMENTS) {
            if (item.parentId !== null && included.has(String(item.parentId)) && !included.has(String(item.deptId))) {
              included.add(String(item.deptId))
              changed = true
            }
          }
        }
      }

      const userList = DIRECTORY_USERS.filter((user) =>
        (Array.isArray(user.department) ? user.department : []).some((id) => included.has(String(id)))
      )

      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: {
          leaderIds: Array.isArray(root.deptLeader) ? root.deptLeader : [],
          userList: userList.map((item) => clone(item))
        }
      })
      return
    }

    if (method === "GET" && pathname === "/user") {
      const pageNum = normalizeQueId(url.searchParams.get("pageNum")) ?? 1
      const pageSize = normalizeQueId(url.searchParams.get("pageSize")) ?? 50
      const pageAmount = DIRECTORY_USERS.length > 0 ? Math.ceil(DIRECTORY_USERS.length / pageSize) : 0
      const start = (pageNum - 1) * pageSize
      const pageItems = DIRECTORY_USERS.slice(start, start + pageSize).map((item) => clone(item))
      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: {
          pageSize,
          pageNum,
          resultAmount: DIRECTORY_USERS.length,
          pageAmount,
          result: pageItems
        }
      })
      return
    }

    const userMatch = pathname.match(/^\/user\/([^/]+)$/)
    if (method === "GET" && userMatch) {
      const userId = decodeURIComponent(userMatch[1])
      const user = DIRECTORY_USERS.find((item) => item.userId === userId)
      if (!user) {
        sendJson(res, { errCode: 404, errMsg: "user not found", result: null })
        return
      }
      sendJson(res, {
        errCode: 0,
        errMsg: "ok",
        result: clone(user)
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
      if (applyId === "5002" || applyId === BIG_APPLY_ID) {
        sendJson(res, { errCode: 49304, errMsg: "record get unsupported", result: null })
        return
      }
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

async function runCliCommand(baseUrl, args) {
  const entry = path.join(ROOT_DIR, "dist/server.js")
  const result = await execFileAsync(process.execPath, [entry, "cli", ...args], {
    cwd: ROOT_DIR,
    env: {
      ...process.env,
      QINGFLOW_BASE_URL: baseUrl,
      QINGFLOW_ACCESS_TOKEN: ACCESS_TOKEN,
      QINGFLOW_FORM_CACHE_TTL_MS: "1",
      QINGFLOW_LIST_MAX_ITEMS_BYTES: "1000000"
    }
  })

  return {
    stdout: result.stdout.trim(),
    stderr: result.stderr.trim()
  }
}

async function callTool(client, name, args) {
  const readTools = new Set([
    "qf_records_list",
    "qf_record_get",
    "qf_query",
    "qf_records_aggregate",
    "qf_records_batch_get",
    "qf_export_csv",
    "qf_export_json"
  ])
  const normalizedArgs =
    readTools.has(name) && args && typeof args === "object" && args.output_profile === undefined
      ? { ...args, output_profile: "verbose" }
      : args
  const result = await client.callTool({
    name,
    arguments: normalizedArgs
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

async function callToolRaw(client, name, args) {
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

async function callToolProtocolError(client, name, args) {
  const result = await client.callTool({
    name,
    arguments: args
  })

  assert.equal(result?.isError, true, `${name} should fail at MCP validation boundary`)
  const textItems = Array.isArray(result?.content)
    ? result.content.filter((item) => item?.type === "text" && typeof item?.text === "string")
    : []
  assert.ok(textItems.length > 0, `${name} should return text error content`)
  return textItems.map((item) => item.text).join("\n")
}

function firstRowValue(row) {
  if (!row || typeof row !== "object") {
    return null
  }
  for (const [key, value] of Object.entries(row)) {
    if (key === "apply_id") {
      continue
    }
    return value
  }
  return null
}

function rowValueByCandidates(row, candidates) {
  if (!row || typeof row !== "object") {
    return null
  }
  for (const key of candidates) {
    if (row[key] !== undefined) {
      return row[key]
    }
  }
  return null
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
    assert.ok(names.includes("qf_tool_spec_get"))
    assert.ok(names.includes("qf_departments_list"))
    assert.ok(names.includes("qf_department_users_list"))
    assert.ok(names.includes("qf_users_list"))
    assert.ok(names.includes("qf_user_get"))
    assert.ok(names.includes("qf_apps_info_list"))
    assert.ok(names.includes("qf_app_info_get"))
    assert.ok(names.includes("qf_app_packages_list"))
    assert.ok(names.includes("qf_apply_audit_records_list"))
    assert.ok(names.includes("qf_apply_audit_record_get"))
    assert.ok(names.includes("qf_field_resolve"))
    assert.ok(names.includes("qf_query_plan"))
    assert.ok(names.includes("qf_write_plan"))
    assert.ok(names.includes("qf_query"))
    assert.ok(names.includes("qf_records_list"))
    assert.ok(names.includes("qf_records_batch_get"))
    assert.ok(names.includes("qf_export_csv"))
    assert.ok(names.includes("qf_export_json"))
    assert.ok(names.includes("qf_records_aggregate"))
    assert.ok(names.includes("qf_record_create"))

    const byName = new Map(tools.tools.map((item) => [item.name, item]))
    const expectSchemaProps = (toolName, props) => {
      const tool = byName.get(toolName)
      assert.ok(tool, `missing tool ${toolName}`)
      const schemaProps = tool.inputSchema?.properties ?? {}
      assert.ok(
        Object.keys(schemaProps).length > 0,
        `${toolName} should expose non-empty inputSchema.properties`
      )
      for (const prop of props) {
        assert.ok(prop in schemaProps, `${toolName} should expose inputSchema property ${prop}`)
      }
    }

    expectSchemaProps("qf_tool_spec_get", ["tool_name", "include_all"])
    expectSchemaProps("qf_apps_info_list", ["page_num", "pageNum", "page_size", "pageSize", "app_key", "appKey"])
    expectSchemaProps("qf_app_info_get", ["app_key", "appKey"])
    expectSchemaProps("qf_app_packages_list", ["user_id", "userId", "tag_id", "tagId"])
    expectSchemaProps("qf_apply_audit_records_list", ["apply_id", "applyId"])
    expectSchemaProps("qf_apply_audit_record_get", ["apply_id", "applyId", "audit_rcd_id", "auditRcdId"])
    expectSchemaProps("qf_departments_list", ["dept_id", "deptId", "department_id", "departmentId"])
    expectSchemaProps("qf_department_users_list", ["dept_id", "fetch_child", "fetchChild"])
    expectSchemaProps("qf_users_list", ["page_num", "pageNum", "page_size", "pageSize"])
    expectSchemaProps("qf_user_get", ["user_id", "userId"])
    expectSchemaProps("qf_query_plan", ["tool", "arguments", "resolve_fields", "probe"])
    expectSchemaProps("qf_write_plan", ["operation", "app_key", "apply_id", "fields", "answers"])
    expectSchemaProps("qf_records_list", ["app_key", "page_size", "select_columns", "filters"])
    expectSchemaProps("qf_record_get", ["apply_id", "select_columns"])
    expectSchemaProps("qf_query", ["query_mode", "app_key", "select_columns", "amount_column"])
    expectSchemaProps("qf_records_aggregate", ["app_key", "group_by", "metrics", "time_range"])
    expectSchemaProps("qf_record_create", ["app_key", "fields", "answers"])

    const listSchema = byName.get("qf_records_list").inputSchema
    assert.equal(listSchema.additionalProperties, false)
    assert.equal(listSchema.properties.page_size.type, "integer")
    assert.equal(listSchema.properties.page_size.maximum, 200)
    assert.ok(!("anyOf" in listSchema.properties.page_size))
    assert.equal(listSchema.properties.select_columns.type, "array")
    assert.equal(listSchema.properties.filters.type, "array")
    assert.ok(Array.isArray(listSchema.required))
    assert.ok(listSchema.required.includes("app_key"))
    assert.ok(listSchema.required.includes("select_columns"))

    const aggregateSchema = byName.get("qf_records_aggregate").inputSchema
    assert.equal(aggregateSchema.additionalProperties, false)
    assert.equal(aggregateSchema.properties.page_size.type, "integer")
    assert.equal(aggregateSchema.properties.group_by.type, "array")
    assert.equal(aggregateSchema.properties.time_range.type, "object")
    assert.ok(Array.isArray(aggregateSchema.required))
    assert.ok(aggregateSchema.required.includes("app_key"))

    const departmentUsersSchema = byName.get("qf_department_users_list").inputSchema
    assert.equal(departmentUsersSchema.additionalProperties, false)
    assert.equal(departmentUsersSchema.properties.fetch_child.type, "boolean")
    assert.equal(departmentUsersSchema.properties.fetchChild.type, "boolean")
  })

  await t.test("cli mode can list tools and call one tool", async () => {
    const listed = await runCliCommand(mock.baseUrl, ["tools", "--json"])
    assert.equal(listed.stderr, "")
    const tools = JSON.parse(listed.stdout)
    const names = tools.map((item) => item.name)
    assert.ok(names.includes("qf_tool_spec_get"))
    assert.ok(names.includes("qf_apps_info_list"))
    assert.ok(names.includes("qf_apply_audit_record_get"))
    assert.ok(names.includes("qf_departments_list"))
    assert.ok(names.includes("qf_users_list"))
    assert.ok(names.includes("qf_query"))
    assert.ok(names.includes("qf_records_aggregate"))

    const called = await runCliCommand(mock.baseUrl, [
      "call",
      "qf_apps_list",
      "--args",
      '{"keyword":"demo","limit":1}'
    ])
    assert.equal(called.stderr, "")
    const payload = JSON.parse(called.stdout)
    assert.equal(payload.ok, true)
    assert.equal(payload.data.returned_apps, 1)
    assert.equal(payload.data.apps[0].appKey, APP_KEY)
  })

  await t.test("apps and form basics", async () => {
    const apps = await callTool(mcp.client, "qf_apps_list", { keyword: "demo", limit: 5 })
    assert.equal(apps.ok, true)
    assert.equal(apps.data.returned_apps, 1)
    assert.equal(apps.data.apps[0].appKey, APP_KEY)

    const form = await callTool(mcp.client, "qf_form_get", { app_key: APP_KEY })
    assert.equal(form.ok, true)
    assert.equal(form.data.total_fields, 8)
    const memberField = form.data.field_summaries.find((item) => item.que_id === 1004)
    const departmentField = form.data.field_summaries.find((item) => item.que_id === 1005)
    assert.equal(memberField.write_format.kind, "member_list")
    assert.equal(memberField.write_format.example[0].userId, "u_123")
    assert.equal(departmentField.write_format.kind, "department_list")
    assert.equal(departmentField.write_format.example[0].deptId, 111)
  })

  await t.test("app info and audit tools spec + behaviors", async () => {
    const appInfoSpec = await callTool(mcp.client, "qf_tool_spec_get", {
      tool_name: "qf_apps_info_list"
    })
    assert.equal(appInfoSpec.ok, true)
    assert.equal(appInfoSpec.data.tools[0].tool, "qf_apps_info_list")
    assert.deepEqual(appInfoSpec.data.tools[0].aliases.page_num, ["pageNum"])

    const appsInfo = await callTool(mcp.client, "qf_apps_info_list", {
      pageNum: 1,
      pageSize: 1,
      appKey: APP_KEY
    })
    assert.equal(appsInfo.ok, true)
    assert.equal(appsInfo.data.pagination.page_num, 1)
    assert.equal(appsInfo.data.pagination.page_size, 1)
    assert.equal(appsInfo.data.pagination.result_amount, 1)
    assert.equal(appsInfo.data.apps[0].app_key, APP_KEY)
    assert.equal(appsInfo.data.apps[0].auth_members.users[0].user_id, "u_1")
    assert.equal(appsInfo.data.apps[0].tags[0].tag_id, 1001)

    const appInfo = await callTool(mcp.client, "qf_app_info_get", {
      appKey: APP_KEY
    })
    assert.equal(appInfo.ok, true)
    assert.equal(appInfo.data.app.app_key, APP_KEY)
    assert.equal(appInfo.data.app.creator.user_id, "u_admin")

    const appPackages = await callTool(mcp.client, "qf_app_packages_list", {
      userId: "u_1",
      tagId: 1001,
      keyword: "销售",
      limit: 10,
      offset: 0
    })
    assert.equal(appPackages.ok, true)
    assert.equal(appPackages.data.user_id, "u_1")
    assert.equal(appPackages.data.tag_id_filter, 1001)
    assert.equal(appPackages.data.total_packages, 1)
    assert.equal(appPackages.data.packages[0].apps[0].app_key, APP_KEY)
    assert.equal(appPackages.data.packages[0].dashboards[0].dash_key, "dash_sales")

    const auditRecords = await callTool(mcp.client, "qf_apply_audit_records_list", {
      applyId: "5001"
    })
    assert.equal(auditRecords.ok, true)
    assert.equal(auditRecords.data.apply_id, "5001")
    assert.equal(auditRecords.data.audit_records[0].audit_rcd_id, 1111)
    assert.equal(auditRecords.data.current_nodes[0].audit_node_id, 2)
    assert.equal(auditRecords.data.audit_records[0].audit_user.user_id, "u_1")

    const auditRecord = await callTool(mcp.client, "qf_apply_audit_record_get", {
      applyId: "5001",
      auditRcdId: "1111"
    })
    assert.equal(auditRecord.ok, true)
    assert.equal(auditRecord.data.apply_id, "5001")
    assert.equal(auditRecord.data.audit_rcd_id, 1111)
    assert.equal(Array.isArray(auditRecord.data.modifies), true)
    assert.equal(auditRecord.data.modifies[0].que_id, 876)
  })

  await t.test("app info and audit tools return local validation/not-found errors", async () => {
    const missingAppListPage = await callTool(mcp.client, "qf_apps_info_list", {
      page_size: 10
    })
    assert.equal(missingAppListPage.ok, false)
    assert.equal(missingAppListPage.error_code, "MISSING_REQUIRED_FIELD")

    const missingUserId = await callTool(mcp.client, "qf_app_packages_list", {})
    assert.equal(missingUserId.ok, false)
    assert.equal(missingUserId.error_code, "MISSING_REQUIRED_FIELD")

    const missingApplyId = await callTool(mcp.client, "qf_apply_audit_record_get", {
      audit_rcd_id: "1111"
    })
    assert.equal(missingApplyId.ok, false)
    assert.equal(missingApplyId.error_code, "MISSING_REQUIRED_FIELD")

    const unknownApp = await callTool(mcp.client, "qf_app_info_get", {
      app_key: "missing_app"
    })
    assert.equal(unknownApp.ok, false)
    assert.equal(unknownApp.error_code, "APP_NOT_FOUND")

    const unknownPackage = await callTool(mcp.client, "qf_app_packages_list", {
      user_id: "u_1",
      tag_id: 9999
    })
    assert.equal(unknownPackage.ok, false)
    assert.equal(unknownPackage.error_code, "APP_PACKAGE_NOT_FOUND")

    const unknownApply = await callTool(mcp.client, "qf_apply_audit_records_list", {
      apply_id: "9999"
    })
    assert.equal(unknownApply.ok, false)
    assert.equal(unknownApply.error_code, "APPLY_NOT_FOUND")

    const unknownAudit = await callTool(mcp.client, "qf_apply_audit_record_get", {
      apply_id: "5001",
      audit_rcd_id: "9999"
    })
    assert.equal(unknownAudit.ok, false)
    assert.equal(unknownAudit.error_code, "AUDIT_RECORD_NOT_FOUND")
  })

  await t.test("directory tools spec and list basics", async () => {
    const departmentSpec = await callTool(mcp.client, "qf_tool_spec_get", {
      tool_name: "qf_departments_list"
    })
    assert.equal(departmentSpec.ok, true)
    assert.equal(departmentSpec.data.tools[0].tool, "qf_departments_list")
    assert.deepEqual(departmentSpec.data.tools[0].aliases.dept_id, [
      "deptId",
      "department_id",
      "departmentId"
    ])

    const departments = await callTool(mcp.client, "qf_departments_list", {})
    assert.equal(departments.ok, true)
    assert.equal(departments.data.total_departments, 4)
    assert.equal(departments.data.returned_departments, 4)
    assert.equal(departments.data.departments[1].dept_id, 111)
    assert.equal(departments.data.departments[1].parent_id, 1)
    assert.deepEqual(departments.data.departments[1].dept_leader_ids, ["u_1", "u_2"])

    const filteredDepartments = await callTool(mcp.client, "qf_departments_list", {
      deptId: 111,
      keyword: "销售",
      limit: 1,
      offset: 1
    })
    assert.equal(filteredDepartments.ok, true)
    assert.equal(filteredDepartments.data.total_departments, 2)
    assert.equal(filteredDepartments.data.returned_departments, 1)
    assert.equal(filteredDepartments.data.dept_id_filter, 111)
    assert.equal(filteredDepartments.data.departments[0].dept_id, 112)
  })

  await t.test("directory department users list supports fetch_child alias and local slicing", async () => {
    const directOnly = await callTool(mcp.client, "qf_department_users_list", {
      dept_id: 111,
      fetch_child: false
    })
    assert.equal(directOnly.ok, true)
    assert.equal(directOnly.data.total_users, 2)
    assert.equal(directOnly.data.returned_users, 2)
    assert.deepEqual(directOnly.data.leader_ids, ["u_1", "u_2"])
    assert.equal(directOnly.data.users[0].user_id, "u_1")

    const withChildren = await callTool(mcp.client, "qf_department_users_list", {
      deptId: 111,
      fetchChild: true,
      keyword: "example.com",
      limit: 2,
      offset: 1
    })
    assert.equal(withChildren.ok, true)
    assert.equal(withChildren.data.total_users, 3)
    assert.equal(withChildren.data.returned_users, 2)
    assert.equal(withChildren.data.dept_id, "111")
    assert.equal(withChildren.data.fetch_child, true)
    assert.equal(withChildren.data.users[0].user_id, "u_2")
    assert.equal(withChildren.data.users[1].user_id, "u_3")
  })

  await t.test("directory users list and user get support canonical and alias inputs", async () => {
    const users = await callTool(mcp.client, "qf_users_list", {
      pageNum: 1,
      pageSize: 2
    })
    assert.equal(users.ok, true)
    assert.equal(users.data.pagination.page_num, 1)
    assert.equal(users.data.pagination.page_size, 2)
    assert.equal(users.data.pagination.result_amount, 5)
    assert.equal(users.data.pagination.page_amount, 3)
    assert.equal(users.data.users.length, 2)
    assert.equal(users.data.users[0].user_id, "u_admin")

    const user = await callTool(mcp.client, "qf_user_get", {
      userId: "u_2"
    })
    assert.equal(user.ok, true)
    assert.equal(user.data.user.user_id, "u_2")
    assert.deepEqual(user.data.user.department_ids, ["111", "112"])
    assert.equal(user.data.user.being_active, true)
  })

  await t.test("directory tools return local not-found and missing-field errors", async () => {
    const missingFetchChild = await callTool(mcp.client, "qf_department_users_list", {
      dept_id: 111
    })
    assert.equal(missingFetchChild.ok, false)
    assert.equal(missingFetchChild.error_code, "MISSING_REQUIRED_FIELD")

    const missingPageSize = await callTool(mcp.client, "qf_users_list", {
      page_num: 1
    })
    assert.equal(missingPageSize.ok, false)
    assert.equal(missingPageSize.error_code, "MISSING_REQUIRED_FIELD")

    const unknownDepartment = await callTool(mcp.client, "qf_department_users_list", {
      dept_id: 999,
      fetch_child: false
    })
    assert.equal(unknownDepartment.ok, false)
    assert.equal(unknownDepartment.error_code, "DEPARTMENT_NOT_FOUND")

    const unknownUser = await callTool(mcp.client, "qf_user_get", {
      user_id: "missing_user"
    })
    assert.equal(unknownUser.ok, false)
    assert.equal(unknownUser.error_code, "USER_NOT_FOUND")
  })

  await t.test("qf_tool_spec_get returns constraints and examples", async () => {
    const spec = await callTool(mcp.client, "qf_tool_spec_get", {
      tool_name: "qf_records_list"
    })

    assert.equal(spec.ok, true)
    assert.equal(spec.data.requested_tool, "qf_records_list")
    assert.equal(spec.data.tool_count, 1)

    const item = spec.data.tools[0]
    assert.equal(item.tool, "qf_records_list")
    assert.ok(item.required.includes("app_key"))
    assert.ok(item.required.includes("select_columns"))
    assert.equal(item.limits.page_size_max, 200)
    assert.equal(item.limits.select_columns_max, 2)
    assert.deepEqual(item.aliases, {})
    assert.equal(
      item.limits.input_contract,
      "strict JSON only; numbers/arrays/objects/booleans must use native JSON types"
    )
    assert.equal(item.limits.output_profile, "compact|verbose (default compact)")
    assert.equal(item.minimal_example.app_key, "21b3d559")
    assert.ok(Array.isArray(item.minimal_example.select_columns))

    const createSpec = await callTool(mcp.client, "qf_tool_spec_get", {
      tool_name: "qf_record_create"
    })
    assert.equal(createSpec.ok, true)
    assert.deepEqual(createSpec.data.tools[0].limits.special_field_write_formats.member_list[0], {
      userId: "u_123",
      userName: "张三"
    })
    assert.deepEqual(createSpec.data.tools[0].limits.special_field_write_formats.department_list[0], {
      deptId: 111,
      deptName: "销售部"
    })
  })

  await t.test("qf_field_resolve maps title/id queries to que_id", async () => {
    const resolved = await callTool(mcp.client, "qf_field_resolve", {
      app_key: APP_KEY,
      queries: ["客户名称", "1002"],
      top_k: 2
    })

    assert.equal(resolved.ok, true)
    assert.equal(resolved.data.query_count, 2)
    assert.equal(resolved.data.results[0].requested, "客户名称")
    assert.equal(resolved.data.results[0].matches[0].que_id, 1001)
    assert.equal(resolved.data.results[1].requested, "1002")
    assert.equal(resolved.data.results[1].matches[0].que_id, 1002)
  })

  await t.test("qf_query_plan normalizes stringified args and estimates pages", async () => {
    const planned = await callTool(mcp.client, "qf_query_plan", {
      tool: "qf_records_list",
      arguments: {
        app_key: APP_KEY,
        page_size: "20",
        requested_pages: "2",
        max_rows: "10",
        select_columns: "[1001,1002]"
      },
      resolve_fields: true,
      probe: true
    })

    assert.equal(planned.ok, true)
    assert.equal(planned.data.tool, "qf_records_list")
    assert.equal(planned.data.validation.valid, true)
    assert.deepEqual(planned.data.validation.missing_required, [])
    assert.ok(Array.isArray(planned.data.normalized_arguments.select_columns))
    assert.equal(planned.data.normalized_arguments.page_size, 20)
    assert.equal(planned.data.estimate.page_size, 20)
    assert.equal(planned.data.estimate.requested_pages, 2)
    assert.equal(typeof planned.data.estimate.estimated_items_upper_bound, "number")
    assert.ok(Array.isArray(planned.data.field_mapping))
    assert.ok(planned.data.field_mapping.some((item) => item.resolved === true))
    assert.equal(planned.data.ready_for_final_conclusion, false)
    assert.ok(
      planned.data.final_conclusion_blockers.includes(
        "list mode is not a safe final-analysis endpoint"
      )
    )
  })

  await t.test("qf_query_plan reports missing required fields", async () => {
    const planned = await callTool(mcp.client, "qf_query_plan", {
      tool: "qf_records_list",
      arguments: {
        app_key: APP_KEY
      },
      resolve_fields: false,
      probe: false
    })

    assert.equal(planned.ok, true)
    assert.equal(planned.data.validation.valid, false)
    assert.ok(planned.data.validation.missing_required.includes("select_columns"))
  })

  await t.test("qf_query_plan blocks final conclusion when summary scan budget is insufficient", async () => {
    const planned = await callTool(mcp.client, "qf_query_plan", {
      tool: "qf_query",
      arguments: {
        query_mode: "summary",
        app_key: APP_KEY,
        mode: "all",
        select_columns: [1001],
        amount_column: 1002,
        page_size: 2,
        requested_pages: 1,
        scan_max_pages: 1,
        strict_full: false
      },
      resolve_fields: true,
      probe: true
    })

    assert.equal(planned.ok, true)
    assert.equal(planned.data.ready_for_final_conclusion, false)
    assert.ok(
      planned.data.final_conclusion_blockers.includes(
        "strict_full must be true for final conclusions"
      )
    )
    assert.ok(
      planned.data.final_conclusion_blockers.some((item) =>
        item.includes("scan budget is smaller than estimated page count")
      )
    )
  })

  await t.test("qf_query_plan marks summary ready when strict_full and scan budget cover probe", async () => {
    const planned = await callTool(mcp.client, "qf_query_plan", {
      tool: "qf_query",
      arguments: {
        query_mode: "summary",
        app_key: APP_KEY,
        mode: "all",
        select_columns: [1001],
        amount_column: 1002,
        page_size: 2,
        requested_pages: 3,
        scan_max_pages: 3,
        strict_full: true
      },
      resolve_fields: true,
      probe: true
    })

    assert.equal(planned.ok, true)
    assert.equal(planned.data.ready_for_final_conclusion, true)
    assert.deepEqual(planned.data.final_conclusion_blockers, [])
  })

  await t.test("qf_write_plan resolves fields and flags linked required risks", async () => {
    const planned = await callTool(mcp.client, "qf_write_plan", {
      app_key: APP_KEY,
      operation: "create",
      fields: {
        客户名称: "测试客户",
        报销类型: [{ optionId: 1, value: "出差" }]
      }
    })

    assert.equal(planned.ok, true)
    assert.equal(planned.data.operation, "create")
    assert.equal(planned.data.ready_to_submit, false)
    assert.equal(planned.data.dependencies.question_relations_present, true)
    assert.equal(planned.data.dependencies.relation_count, 1)
    assert.ok(planned.data.dependencies.option_links.length > 0)
    assert.equal(planned.data.dependencies.option_links[0].missing_linked_fields[0].que_id, 1007)
    assert.equal(planned.data.validation.likely_hidden_required_fields[0].que_id, 1007)
    assert.ok(
      planned.data.recommended_next_actions.some((item) =>
        item.includes("Provide fields linked by the currently selected options")
      )
    )
  })

  await t.test("qf_write_plan flags readonly/system fields and invalid member shape", async () => {
    const planned = await callTool(mcp.client, "qf_write_plan", {
      app_key: APP_KEY,
      operation: "update",
      apply_id: "5001",
      fields: {
        1004: "张三",
        1008: "2026-01-01"
      }
    })

    assert.equal(planned.ok, true)
    assert.equal(planned.data.validation.valid, false)
    assert.equal(planned.data.validation.invalid_fields[0].error_code, "FIELD_VALUE_FORMAT_ERROR")
    assert.equal(planned.data.validation.invalid_fields[0].expected_format.kind, "member_list")
    assert.equal(planned.data.validation.readonly_or_system_fields[0].que_id, 1008)
    assert.equal(planned.data.ready_to_submit, false)
  })

  await t.test("qf_records_list defaults to compact output profile", async () => {
    const listed = await callToolRaw(mcp.client, "qf_records_list", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 20,
      max_rows: 3,
      select_columns: [1001, 1002]
    })

    assert.equal(listed.ok, true)
    assert.equal(listed.output_profile, "compact")
    assert.equal(listed.data.rows.length, 3)
    assert.equal(listed.next_page_token, null)
    assert.equal(listed.data.completeness, undefined)
    assert.equal(listed.data.evidence, undefined)
    assert.equal(listed.completeness, undefined)
    assert.equal(listed.evidence, undefined)
    assert.equal(listed.meta, undefined)
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
    assert.equal(listed.data.rows.length, 3)
    assert.deepEqual(listed.data.applied_limits.selected_columns, ["1001"])
    assert.equal(listed.data.applied_limits.row_cap, 3)
    assert.equal(listed.data.completeness.result_amount, 6)
    assert.equal(listed.data.completeness.returned_items, 3)
    assert.equal(listed.data.completeness.is_complete, false)
    assert.equal(listed.data.completeness.partial, true)
    assert.equal(listed.data.evidence.app_key, APP_KEY)
    assert.equal(listed.data.evidence.source_pages.length, 1)
    assert.ok(Array.isArray(listed.data.rows))
    assert.equal(listed.data.rows.length, 3)
    assert.ok("apply_id" in listed.data.rows[0])
    assert.equal(firstRowValue(listed.data.rows[0]), "客户A")
  })

  await t.test("qf_records_list resolves sort by field title", async () => {
    const listed = await callTool(mcp.client, "qf_records_list", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 3,
      max_rows: 3,
      sort: [{ que_id: "下单日期", ascend: false }],
      select_columns: [1003]
    })

    assert.equal(listed.ok, true)
    assert.equal(listed.data.rows.length, 3)
    const firstDay = rowValueByCandidates(listed.data.rows[0], ["下单日期", "1003"])
    assert.equal(firstDay, "2026-02-01")
  })

  await t.test("qf_records_list rejects stringified select_columns/filters/max_rows at MCP boundary", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_records_list", {
      app_key: APP_KEY,
      mode: "all",
      page_size: "20",
      max_rows: "2",
      select_columns: "[1001,1003]",
      filters: "[{\"que_id\":1003,\"min_value\":\"2026-01-02\",\"max_value\":\"2026-01-02\"}]"
    })

    assert.match(errorText, /Input validation error/)
    assert.match(errorText, /qf_records_list/)
  })

  await t.test("qf_query list mode rejects double-stringified params at MCP boundary", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_query", {
      query_mode: "list",
      app_key: APP_KEY,
      mode: "all",
      page_size: "\"20\"",
      max_rows: "\"2\"",
      select_columns: "\"[1001]\"",
      filters:
        "\"[{\\\"que_id\\\":1003,\\\"min_value\\\":\\\"2026-01-02\\\",\\\"max_value\\\":\\\"2026-01-02\\\"}]\""
    })

    assert.match(errorText, /Input validation error/)
    assert.match(errorText, /qf_query/)
  })

  await t.test("qf_query list mode returns structured missing-field errors for camelCase aliases", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_query", {
      queryMode: "list",
      appKey: APP_KEY,
      mode: "all",
      pageSize: 2,
      maxRows: 2,
      selectColumns: [1001]
    })

    assert.match(errorText, /MISSING_REQUIRED_FIELD/)
    assert.match(errorText, /select_columns|app_key|qf_query/)
  })

  await t.test("qf_query list mode rejects model-style date_range filter with actionable error", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_query", {
      query_mode: "list",
      app_key: APP_KEY,
      mode: "all",
      page_size: 20,
      max_rows: 20,
      selected_columns: [1001, 1003],
      filters: [
        {
          que_id: 1003,
          compare_type: "date_range",
          value: {
            start: "2026-01-02",
            end: "2026-01-02"
          }
        }
      ]
    })

    assert.match(errorText, /MISSING_REQUIRED_FIELD|filters|qf_query/)
    assert.match(errorText, /select_columns|filters|qf_query/)
  })

  await t.test("qf_records_list returns deterministic pagination token", async () => {
    const page1 = await callTool(mcp.client, "qf_records_list", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 2,
      requested_pages: 1,
      max_rows: 50,
      select_columns: [1001]
    })

    assert.equal(page1.ok, true)
    assert.equal(page1.data.completeness.has_more, true)
    assert.equal(typeof page1.data.completeness.next_page_token, "string")
    assert.equal(page1.data.completeness.actual_scanned_pages, 1)

    const page2 = await callTool(mcp.client, "qf_records_list", {
      app_key: APP_KEY,
      page_token: page1.data.completeness.next_page_token,
      page_size: 2,
      requested_pages: 1,
      max_rows: 50,
      select_columns: [1001]
    })

    assert.equal(page2.ok, true)
    assert.equal(page2.data.evidence.source_pages[0], 2)
  })

  await t.test("qf_record_get supports select_columns/max_columns", async () => {
    const record = await callTool(mcp.client, "qf_record_get", {
      apply_id: "5001",
      select_columns: [1001, 1002],
      max_columns: 1
    })

    assert.equal(record.ok, true)
    assert.deepEqual(record.data.applied_limits.selected_columns, ["1001"])
    assert.ok(record.data.row)
    assert.ok("apply_id" in record.data.row)
    assert.equal(rowValueByCandidates(record.data.row, ["客户名称", "1001"]), "客户A")
  })

  await t.test("qf_record_get falls back to list lookup when direct record endpoint returns 49304", async () => {
    const record = await callTool(mcp.client, "qf_record_get", {
      apply_id: "5002",
      app_key: APP_KEY,
      select_columns: [1001, 1002]
    })

    assert.equal(record.ok, true)
    assert.equal(record.data.apply_id, "5002")
    assert.equal(rowValueByCandidates(record.data.row, ["客户名称", "1001"]), "客户B")
    assert.equal(rowValueByCandidates(record.data.row, ["金额", "1002"]), 200)
  })

  await t.test("qf_records_batch_get returns rows and missing ids", async () => {
    const batch = await callTool(mcp.client, "qf_records_batch_get", {
      app_key: APP_KEY,
      apply_ids: ["5001", "999999999"],
      select_columns: [1001]
    })

    assert.equal(batch.ok, true)
    assert.equal(batch.data.found_count, 1)
    assert.equal(batch.data.missing_apply_ids.length, 1)
    assert.equal(batch.data.missing_apply_ids[0], "999999999")
    assert.equal(batch.data.rows.length, 1)
    assert.equal(rowValueByCandidates(batch.data.rows[0], ["客户名称", "1001"]), "客户A")
  })

  await t.test("qf_records_batch_get no longer depends on direct record endpoint", async () => {
    const batch = await callTool(mcp.client, "qf_records_batch_get", {
      app_key: APP_KEY,
      apply_ids: ["5002"],
      select_columns: [1001, 1002]
    })

    assert.equal(batch.ok, true)
    assert.equal(batch.data.found_count, 1)
    assert.equal(batch.data.rows.length, 1)
    assert.equal(rowValueByCandidates(batch.data.rows[0], ["客户名称", "1001"]), "客户B")
    assert.equal(rowValueByCandidates(batch.data.rows[0], ["金额", "1002"]), 200)
  })

  await t.test("qf_record_get can infer app_key from cache after batch/list/update flows", async () => {
    const record = await callTool(mcp.client, "qf_record_get", {
      apply_id: "5002",
      select_columns: [1001, 1002]
    })

    assert.equal(record.ok, true)
    assert.equal(record.data.apply_id, "5002")
    assert.equal(rowValueByCandidates(record.data.row, ["客户名称", "1001"]), "客户B")
    assert.equal(rowValueByCandidates(record.data.row, ["金额", "1002"]), 200)
  })

  await t.test("large apply_id stays exact across record_get, batch_get and qf_query(record)", async () => {
    const record = await callTool(mcp.client, "qf_record_get", {
      apply_id: BIG_APPLY_ID,
      app_key: APP_KEY,
      select_columns: [1001, 1002]
    })
    assert.equal(record.ok, true)
    assert.equal(record.data.apply_id, BIG_APPLY_ID)
    assert.equal(rowValueByCandidates(record.data.row, ["客户名称", "1001"]), "大ID客户")

    const batch = await callTool(mcp.client, "qf_records_batch_get", {
      app_key: APP_KEY,
      apply_ids: [BIG_APPLY_ID],
      select_columns: [1001, 1002],
      output_profile: "verbose"
    })
    assert.equal(batch.ok, true)
    assert.deepEqual(batch.data.requested_apply_ids, [BIG_APPLY_ID])
    assert.equal(rowValueByCandidates(batch.data.rows[0], ["客户名称", "1001"]), "大ID客户")

    const queryRecord = await callTool(mcp.client, "qf_query", {
      query_mode: "record",
      app_key: APP_KEY,
      apply_id: BIG_APPLY_ID,
      select_columns: [1001, 1002]
    })
    assert.equal(queryRecord.ok, true)
    assert.equal(queryRecord.data.record.apply_id, BIG_APPLY_ID)
    assert.equal(rowValueByCandidates(queryRecord.data.record.row, ["客户名称", "1001"]), "大ID客户")
  })

  await t.test("qf_export_json writes export file with preview", async () => {
    const exported = await callTool(mcp.client, "qf_export_json", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 2,
      requested_pages: 2,
      max_rows: 3,
      select_columns: [1001, 1002]
    })

    assert.equal(exported.ok, true)
    assert.equal(exported.data.format, "json")
    assert.equal(typeof exported.data.file_path, "string")
    assert.equal(exported.data.row_count, 3)
    assert.ok(Array.isArray(exported.data.preview))
    assert.ok(exported.data.preview.length > 0)
    assert.ok(Array.isArray(exported.data.columns))
    assert.ok(exported.data.columns.includes("apply_id"))

    const raw = await fs.readFile(exported.data.file_path, "utf8")
    const parsed = JSON.parse(raw)
    assert.ok(Array.isArray(parsed))
    assert.equal(parsed.length, 3)

    await fs.unlink(exported.data.file_path)
  })

  await t.test("qf_export_csv writes export file", async () => {
    const exported = await callTool(mcp.client, "qf_export_csv", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 2,
      requested_pages: 1,
      max_rows: 2,
      select_columns: [1001]
    })

    assert.equal(exported.ok, true)
    assert.equal(exported.data.format, "csv")
    assert.equal(exported.data.row_count, 2)
    const csvText = await fs.readFile(exported.data.file_path, "utf8")
    assert.match(csvText, /apply_id/)
    assert.match(csvText, /客户A|客户B/)
    await fs.unlink(exported.data.file_path)
  })

  await t.test("qf_query auto routes to list and record", async () => {
    const queryList = await callTool(mcp.client, "qf_query", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 2,
      max_rows: 2,
      select_columns: [1001]
    })

    assert.equal(queryList.ok, true)
    assert.equal(queryList.data.mode, "list")
    assert.equal(queryList.data.source_tool, "qf_records_list")
    assert.equal(queryList.data.list.rows.length, 2)
    assert.equal(queryList.data.list.completeness.is_complete, false)
    assert.equal(queryList.data.list.completeness.partial, true)

    const queryRecord = await callTool(mcp.client, "qf_query", {
      apply_id: "5001",
      select_columns: [1002]
    })

    assert.equal(queryRecord.ok, true)
    assert.equal(queryRecord.data.mode, "record")
    assert.equal(queryRecord.data.source_tool, "qf_record_get")
    assert.equal(queryRecord.data.record.apply_id, "5001")
    assert.equal(queryRecord.data.record.completeness.is_complete, true)
    assert.equal(rowValueByCandidates(queryRecord.data.record.row, ["金额", "1002"]), 100)
  })

  await t.test("qf_query defaults to compact output profile", async () => {
    const queryList = await callToolRaw(mcp.client, "qf_query", {
      query_mode: "list",
      app_key: APP_KEY,
      mode: "all",
      page_size: 20,
      max_rows: 2,
      select_columns: [1001]
    })

    assert.equal(queryList.ok, true)
    assert.equal(queryList.output_profile, "compact")
    assert.equal(queryList.data.mode, "list")
    assert.equal(queryList.data.list.rows.length, 2)
    assert.equal(queryList.data.list.completeness, undefined)
    assert.equal(queryList.data.list.evidence, undefined)
    assert.equal(queryList.completeness, undefined)
    assert.equal(queryList.evidence, undefined)
    assert.equal(queryList.meta, undefined)
  })

  await t.test("qf_query summary compact still exposes scan completeness", async () => {
    const summary = await callToolRaw(mcp.client, "qf_query", {
      query_mode: "summary",
      app_key: APP_KEY,
      mode: "all",
      select_columns: [1001],
      amount_column: 1002,
      page_size: 2,
      requested_pages: 1,
      scan_max_pages: 1,
      max_rows: 1,
      strict_full: false
    })

    assert.equal(summary.ok, true)
    assert.equal(summary.output_profile, "compact")
    assert.equal(summary.data.mode, "summary")
    assert.equal(summary.data.summary.completeness.is_complete, false)
    assert.equal(summary.data.summary.completeness.raw_scan_complete, false)
    assert.equal(summary.data.summary.completeness.scan_limit_hit, true)
    assert.equal(summary.data.summary.completeness.output_page_complete, false)
    assert.equal(
      summary.data.summary.completeness.raw_next_page_token,
      summary.next_page_token
    )
    assert.equal(summary.data.summary.completeness.output_next_page_token, null)
  })

  await t.test("qf_query summary raw_next_page_token resumes cumulative scan", async () => {
    const first = await callTool(mcp.client, "qf_query", {
      query_mode: "summary",
      app_key: APP_KEY,
      mode: "all",
      select_columns: [1001],
      amount_column: 1002,
      page_size: 2,
      requested_pages: 1,
      scan_max_pages: 1,
      max_rows: 1,
      strict_full: false
    })

    assert.equal(first.ok, true)
    assert.equal(first.data.summary.summary.total_count, 2)
    assert.equal(typeof first.next_page_token, "string")

    const resumed = await callTool(mcp.client, "qf_query", {
      query_mode: "summary",
      app_key: APP_KEY,
      mode: "all",
      select_columns: [1001],
      amount_column: 1002,
      page_size: 2,
      requested_pages: 2,
      scan_max_pages: 2,
      max_rows: 1,
      strict_full: false,
      raw_next_page_token: first.next_page_token
    })

    assert.equal(resumed.ok, true)
    assert.equal(resumed.data.summary.summary.total_count, 6)
    assert.equal(resumed.data.summary.summary.total_amount, 350)
    assert.equal(resumed.data.summary.completeness.raw_scan_complete, true)
    assert.equal(resumed.data.summary.completeness.raw_next_page_token, null)
    assert.equal(resumed.data.summary.completeness.actual_scanned_pages, 3)
    assert.deepEqual(resumed.data.summary.evidence.source_pages, [1, 2, 3])
    assert.equal(resumed.data.summary.rows.length, 1)
  })

  await t.test("qf_query summary continuation rejects changed query arguments", async () => {
    const first = await callTool(mcp.client, "qf_query", {
      query_mode: "summary",
      app_key: APP_KEY,
      mode: "all",
      select_columns: [1001],
      amount_column: 1002,
      page_size: 2,
      requested_pages: 1,
      scan_max_pages: 1,
      max_rows: 1,
      strict_full: false
    })

    assert.equal(first.ok, true)
    assert.equal(typeof first.next_page_token, "string")

    const failed = await callTool(mcp.client, "qf_query", {
      query_mode: "summary",
      app_key: APP_KEY,
      mode: "all",
      select_columns: [1002],
      amount_column: 1002,
      page_size: 2,
      requested_pages: 2,
      scan_max_pages: 2,
      max_rows: 1,
      strict_full: false,
      raw_next_page_token: first.next_page_token
    })

    assert.equal(failed.ok, false)
    assert.equal(failed.error_code, "CONTINUATION_MISMATCH")
  })

  await t.test("qf_query list mode applies time_range as filter", async () => {
    const queryList = await callTool(mcp.client, "qf_query", {
      query_mode: "list",
      app_key: APP_KEY,
      mode: "all",
      page_size: 20,
      max_rows: 20,
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
    assert.equal(queryList.data.list.rows.length, 2)
  })

  await t.test("qf_records_list rejects date-like range on non-date filter field", async () => {
    const failed = await callTool(mcp.client, "qf_records_list", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 20,
      max_rows: 20,
      select_columns: [1001],
      filters: [
        {
          que_id: 1002,
          min_value: "2026-01-01",
          max_value: "2026-01-31"
        }
      ]
    })

    assert.equal(failed.ok, false)
    assert.equal(failed.error_code, "FILTER_FIELD_TYPE_MISMATCH")
    assert.equal(typeof failed.fix_hint, "string")
    assert.ok(Array.isArray(failed.example_calls))
    assert.equal(failed.example_calls[0].tool, "qf_form_get")
    assert.equal(failed.example_calls[1].tool, "qf_query")
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
    assert.equal(summary.data.summary.completeness.raw_scan_complete, true)
    assert.equal(summary.data.summary.completeness.output_page_complete, false)
    assert.equal(summary.data.summary.completeness.is_complete, false)
    assert.equal(summary.data.summary.completeness.actual_scanned_pages, 3)
    assert.equal(summary.data.summary.evidence.source_pages.length, 3)

    for (const row of summary.data.summary.rows) {
      assert.deepEqual(Object.keys(row), ["1001"])
    }

    const roles = summary.data.summary.meta.field_mapping.map((item) => item.role).sort()
    assert.deepEqual(roles, ["amount", "row", "time"])
    assert.equal(summary.data.summary.meta.execution.scanned_pages, 3)
    assert.equal(summary.data.summary.meta.execution.truncated, true)
  })

  await t.test("qf_query summary rejects double-stringified amount_column at MCP boundary", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_query", {
      query_mode: "summary",
      app_key: APP_KEY,
      mode: "all",
      select_columns: "\"[1001]\"",
      amount_column: "\"1002\"",
      page_size: "\"2\"",
      scan_max_pages: "\"10\"",
      requested_pages: "\"10\"",
      strict_full: "\"false\""
    })

    assert.match(errorText, /Input validation error/)
    assert.match(errorText, /amount_column|page_size|select_columns/)
  })

  await t.test("qf_query list mode missing select_columns returns local validation error", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_query", {
      query_mode: "list",
      app_key: APP_KEY,
      mode: "all",
      page_size: 2,
      max_rows: 2
    })

    assert.match(errorText, /MISSING_REQUIRED_FIELD/)
    assert.match(errorText, /select_columns/)
  })

  await t.test("qf_query list mode uses default row limit when max_rows is omitted", async () => {
    const listed = await callTool(mcp.client, "qf_query", {
      query_mode: "list",
      app_key: APP_KEY,
      mode: "all",
      page_size: 2,
      select_columns: [1001]
    })

    assert.equal(listed.ok, true)
    assert.equal(listed.data.mode, "list")
    assert.equal(listed.data.list.completeness.returned_items, 2)
    assert.equal(listed.data.list.completeness.is_complete, false)
  })

  await t.test("qf_records_list strict_full fails when result is incomplete", async () => {
    const failed = await callTool(mcp.client, "qf_records_list", {
      app_key: APP_KEY,
      mode: "all",
      page_size: 20,
      max_rows: 2,
      strict_full: true,
      select_columns: [1001]
    })

    assert.equal(failed.ok, false)
    assert.equal(failed.code, "NEED_MORE_DATA")
    assert.equal(failed.status, "need_more_data")
    assert.equal(failed.details.completeness.is_complete, false)
    assert.ok(Array.isArray(failed.example_calls))
    assert.equal(failed.example_calls[0].tool, "qf_records_list")
  })

  await t.test("qf_query summary strict_full fails when scan is incomplete", async () => {
    const failed = await callTool(mcp.client, "qf_query", {
      query_mode: "summary",
      app_key: APP_KEY,
      mode: "all",
      select_columns: [1001],
      amount_column: 1002,
      page_size: 2,
      scan_max_pages: 1,
      requested_pages: 1,
      max_rows: 3,
      strict_full: true
    })

    assert.equal(failed.ok, false)
    assert.equal(failed.code, "NEED_MORE_DATA")
    assert.equal(failed.details.completeness.is_complete, false)
    assert.equal(failed.details.completeness.has_more, true)
    assert.ok(Array.isArray(failed.example_calls))
    assert.equal(failed.example_calls[0].tool, "qf_query")
    assert.equal(failed.example_calls[0].arguments.query_mode, "summary")
  })

  await t.test("qf_records_aggregate returns grouped metrics with evidence", async () => {
    const aggregated = await callTool(mcp.client, "qf_records_aggregate", {
      app_key: APP_KEY,
      mode: "all",
      group_by: [1003],
      amount_column: 1002,
      page_size: 2,
      requested_pages: 10,
      scan_max_pages: 10,
      strict_full: true
    })

    assert.equal(aggregated.ok, true)
    assert.equal(aggregated.data.summary.total_count, 6)
    assert.equal(aggregated.data.summary.total_amount, 350)
    assert.equal(aggregated.data.completeness.is_complete, true)
    assert.ok(Array.isArray(aggregated.data.groups))
    assert.ok(aggregated.data.groups.length > 0)
    assert.ok(
      aggregated.data.groups.every((item) => !Array.isArray(item.group["1003"])),
      "group_by value should come from values, not empty tableValues"
    )
    assert.equal(aggregated.data.evidence.source_pages.length, 3)
  })

  await t.test("qf_records_aggregate compact separates raw scan completeness and output truncation", async () => {
    const aggregated = await callToolRaw(mcp.client, "qf_records_aggregate", {
      app_key: APP_KEY,
      mode: "all",
      group_by: [1003],
      amount_column: 1002,
      page_size: 2,
      requested_pages: 10,
      scan_max_pages: 10,
      max_groups: 1,
      strict_full: false
    })

    assert.equal(aggregated.ok, true)
    assert.equal(aggregated.output_profile, "compact")
    assert.equal(aggregated.data.completeness.raw_scan_complete, true)
    assert.equal(aggregated.data.completeness.output_page_complete, false)
    assert.equal(aggregated.data.completeness.is_complete, false)
    assert.equal(aggregated.data.completeness.scan_limit_hit, false)
    assert.equal(aggregated.data.completeness.raw_next_page_token, null)
    assert.equal(aggregated.next_page_token, null)
  })

  await t.test("qf_records_aggregate raw_next_page_token resumes cumulative scan", async () => {
    const first = await callTool(mcp.client, "qf_records_aggregate", {
      app_key: APP_KEY,
      mode: "all",
      group_by: [1003],
      amount_column: 1002,
      page_size: 2,
      requested_pages: 1,
      scan_max_pages: 1,
      strict_full: false
    })

    assert.equal(first.ok, true)
    assert.equal(first.data.summary.total_count, 2)
    assert.equal(typeof first.next_page_token, "string")

    const resumed = await callTool(mcp.client, "qf_records_aggregate", {
      app_key: APP_KEY,
      mode: "all",
      group_by: [1003],
      amount_column: 1002,
      page_size: 2,
      requested_pages: 2,
      scan_max_pages: 2,
      strict_full: false,
      raw_next_page_token: first.next_page_token
    })

    assert.equal(resumed.ok, true)
    assert.equal(resumed.data.summary.total_count, 6)
    assert.equal(resumed.data.summary.total_amount, 350)
    assert.equal(resumed.data.completeness.raw_scan_complete, true)
    assert.equal(resumed.data.completeness.raw_next_page_token, null)
    assert.equal(resumed.data.completeness.actual_scanned_pages, 3)
    assert.deepEqual(resumed.data.evidence.source_pages, [1, 2, 3])
  })

  await t.test("qf_records_aggregate supports metrics + time_bucket", async () => {
    const aggregated = await callTool(mcp.client, "qf_records_aggregate", {
      app_key: APP_KEY,
      mode: "all",
      group_by: [1003],
      amount_columns: [1002],
      metrics: ["count", "sum", "avg", "min", "max"],
      time_range: {
        column: 1003,
        from: "2026-01-01",
        to: "2026-01-31"
      },
      time_bucket: "week",
      page_size: 2,
      requested_pages: 10,
      scan_max_pages: 10,
      strict_full: true
    })

    assert.equal(aggregated.ok, true)
    assert.equal(aggregated.data.summary.total_count, 5)
    assert.equal(aggregated.data.summary.total_amount, 280)
    assert.equal(aggregated.data.summary.metrics["1002"].sum, 280)
    assert.equal(aggregated.data.summary.metrics["1002"].count, 4)
    assert.equal(aggregated.data.summary.metrics["1002"].avg, 70)
    assert.equal(aggregated.data.summary.metrics["1002"].min, -50)
    assert.equal(aggregated.data.summary.metrics["1002"].max, 200)
    assert.equal(aggregated.data.meta.time_bucket, "week")
    assert.ok(aggregated.data.groups.length > 0)
    assert.ok(
      aggregated.data.groups.every((item) => Object.prototype.hasOwnProperty.call(item.group, "time_bucket_week"))
    )
    assert.ok(aggregated.data.groups.every((item) => item.metrics && item.metrics["1002"]))
  })

  await t.test("qf_records_aggregate rejects stringified group_by at MCP boundary", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_records_aggregate", {
      app_key: APP_KEY,
      mode: "all",
      group_by: "[1003]",
      amount_column: "1002",
      page_size: "2",
      requested_pages: "10",
      scan_max_pages: "10",
      strict_full: "true"
    })

    assert.match(errorText, /Input validation error/)
    assert.match(errorText, /group_by|page_size|strict_full/)
  })

  await t.test("qf_records_aggregate rejects camelCase aliases at MCP boundary", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_records_aggregate", {
      appKey: APP_KEY,
      mode: "all",
      groupBy: [1003],
      amountColumns: 1002,
      pageSize: 2,
      requestedPages: 10,
      scanMaxPages: 10,
      strictFull: true
    })

    assert.match(errorText, /Input validation error/)
    assert.match(errorText, /app_key|group_by|qf_records_aggregate/)
  })

  await t.test("qf_records_aggregate rejects model-style group/date/amount fields at MCP boundary", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_records_aggregate", {
      app_key: APP_KEY,
      mode: "all",
      group_by: [{ que_id: 1003 }],
      amount_que_ids: [1002],
      date_field: 1003,
      date_from: "2026-01-01",
      date_to: "2026-01-31",
      page_size: 2,
      requested_pages: 10,
      scan_max_pages: 10,
      strict_full: true
    })

    assert.match(errorText, /Input validation error/)
    assert.match(errorText, /group_by|amount_que_ids|date_field|date_from|date_to/)
  })

  await t.test("qf_records_aggregate rejects double-stringified group_by/amount at MCP boundary", async () => {
    const errorText = await callToolProtocolError(mcp.client, "qf_records_aggregate", {
      app_key: APP_KEY,
      mode: "all",
      group_by: "\"[1003]\"",
      amount_column: "\"1002\"",
      page_size: "\"2\"",
      requested_pages: "\"10\"",
      scan_max_pages: "\"10\"",
      strict_full: "\"true\""
    })

    assert.match(errorText, /Input validation error/)
    assert.match(errorText, /group_by|amount_column|page_size/)
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
    assert.equal(rowValueByCandidates(beforeUpdate.data.row, ["金额", "1002"]), 123)

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
    assert.equal(rowValueByCandidates(afterUpdate.data.row, ["金额", "1002"]), 456)
  })

  await t.test("create/update enforce explicit member and department write formats", async () => {
    const invalidMember = await callTool(mcp.client, "qf_record_create", {
      app_key: APP_KEY,
      fields: {
        1004: "张三"
      }
    })
    assert.equal(invalidMember.ok, false)
    assert.equal(invalidMember.error_code, "FIELD_VALUE_FORMAT_ERROR")
    assert.match(invalidMember.fix_hint, /userId/)
    assert.equal(invalidMember.details.expected_format.kind, "member_list")

    const invalidDepartment = await callTool(mcp.client, "qf_record_update", {
      apply_id: "5001",
      app_key: APP_KEY,
      answers: [
        {
          queId: 1005,
          values: [{ dept_id: 111 }]
        }
      ]
    })
    assert.equal(invalidDepartment.ok, false)
    assert.equal(invalidDepartment.error_code, "FIELD_VALUE_FORMAT_ERROR")
    assert.match(invalidDepartment.fix_hint, /deptId/)
    assert.equal(invalidDepartment.details.expected_format.kind, "department_list")

    const validSpecialFields = await callTool(mcp.client, "qf_record_create", {
      app_key: APP_KEY,
      fields: {
        客户名称: "带成员部门",
        1004: [{ userId: "u_1", userName: "张三" }],
        1005: [{ deptId: 111, deptName: "销售部" }]
      }
    })
    assert.equal(validSpecialFields.ok, true)
  })
})

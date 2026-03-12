import assert from "node:assert/strict"
import http from "node:http"
import { once } from "node:events"
import test from "node:test"

import { QingflowClient } from "../dist/qingflow-client.js"

const ACCESS_TOKEN = "test-token"

async function startMockServer(handler) {
  const server = http.createServer(async (req, res) => {
    await handler(req, res)
  })

  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const address = server.address()
  if (!address || typeof address === "string") {
    throw new Error("failed to start mock server")
  }

  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
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

function sendJson(res, payload, status = 200) {
  res.statusCode = status
  res.setHeader("content-type", "application/json; charset=utf-8")
  res.end(JSON.stringify(payload))
}

test("qingflow client directory endpoints map request paths and query params", async (t) => {
  const seen = []
  const mock = await startMockServer(async (req, res) => {
    const host = req.headers.host ?? "127.0.0.1"
    const url = new URL(req.url ?? "/", `http://${host}`)
    seen.push({
      method: req.method,
      pathname: url.pathname,
      search: url.search,
      accessToken: req.headers.accesstoken
    })
    sendJson(res, { errCode: 0, errMsg: "ok", result: {} })
  })

  const client = new QingflowClient({
    baseUrl: mock.baseUrl,
    accessToken: ACCESS_TOKEN
  })

  await client.listDepartments({ deptId: 111 })
  await client.listDepartmentUsers("222", { fetchChild: true })
  await client.listUsers({ pageNum: 3, pageSize: 50 })
  await client.getUser("u_123")

  assert.deepEqual(seen, [
    {
      method: "GET",
      pathname: "/department",
      search: "?deptId=111",
      accessToken: ACCESS_TOKEN
    },
    {
      method: "GET",
      pathname: "/department/222/user",
      search: "?fetchChild=true",
      accessToken: ACCESS_TOKEN
    },
    {
      method: "GET",
      pathname: "/user",
      search: "?pageNum=3&pageSize=50",
      accessToken: ACCESS_TOKEN
    },
    {
      method: "GET",
      pathname: "/user/u_123",
      search: "",
      accessToken: ACCESS_TOKEN
    }
  ])

  await mock.close()
  await t.test("cleanup", async () => {})
})

test("qingflow client app and audit endpoints map request paths and query params", async (t) => {
  const seen = []
  const mock = await startMockServer(async (req, res) => {
    const host = req.headers.host ?? "127.0.0.1"
    const url = new URL(req.url ?? "/", `http://${host}`)
    seen.push({
      method: req.method,
      pathname: url.pathname,
      search: url.search,
      accessToken: req.headers.accesstoken
    })
    sendJson(res, { errCode: 0, errMsg: "ok", result: {} })
  })

  const client = new QingflowClient({
    baseUrl: mock.baseUrl,
    accessToken: ACCESS_TOKEN
  })

  await client.listAppsInfo({ appKey: "app_demo", pageNum: 1, pageSize: 50 })
  await client.listAppPackages({ userId: "u_123" })
  await client.listApplyAuditRecords("50001234")
  await client.getApplyAuditRecord("50001234", "1111")

  assert.deepEqual(seen, [
    {
      method: "GET",
      pathname: "/apps",
      search: "?appKey=app_demo&pageNum=1&pageSize=50",
      accessToken: ACCESS_TOKEN
    },
    {
      method: "GET",
      pathname: "/tags",
      search: "?userId=u_123",
      accessToken: ACCESS_TOKEN
    },
    {
      method: "GET",
      pathname: "/apply/50001234/auditRecord",
      search: "",
      accessToken: ACCESS_TOKEN
    },
    {
      method: "GET",
      pathname: "/apply/50001234/auditRecord/1111",
      search: "",
      accessToken: ACCESS_TOKEN
    }
  ])

  await mock.close()
  await t.test("cleanup", async () => {})
})

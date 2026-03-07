import assert from "node:assert/strict"
import http from "node:http"
import { once } from "node:events"
import test from "node:test"

import { QingflowClient } from "../dist/qingflow-client.js"

const ACCESS_TOKEN = "test-token"

test("QingflowClient works when global fetch is unavailable", async (t) => {
  const server = http.createServer(async (req, res) => {
    const host = req.headers.host ?? "127.0.0.1"
    const url = new URL(req.url ?? "/", `http://${host}`)

    if (req.headers.accesstoken !== ACCESS_TOKEN) {
      res.statusCode = 401
      res.setHeader("content-type", "application/json; charset=utf-8")
      res.end(JSON.stringify({ errCode: 401, errMsg: "invalid access token", result: null }))
      return
    }

    if (req.method === "GET" && url.pathname === "/app") {
      res.statusCode = 200
      res.setHeader("content-type", "application/json; charset=utf-8")
      res.end(
        JSON.stringify({
          errCode: 0,
          errMsg: "ok",
          result: {
            appList: [{ appKey: "app_demo", appName: "Demo Sales" }]
          }
        })
      )
      return
    }

    if (req.method === "POST" && url.pathname === "/app/app_demo/apply/filter") {
      const chunks = []
      for await (const chunk of req) {
        chunks.push(Buffer.from(chunk))
      }
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}")
      assert.deepEqual(body, {
        pageNum: 1,
        pageSize: 2,
        type: 8
      })

      res.statusCode = 200
      res.setHeader("content-type", "application/json; charset=utf-8")
      res.end(
        JSON.stringify({
          errCode: 0,
          errMsg: "ok",
          result: {
            pageAmount: 1,
            pageNum: 1,
            pageSize: 2,
            resultAmount: 2,
            result: [{ applyId: "5001" }, { applyId: "5002" }]
          }
        })
      )
      return
    }

    res.statusCode = 404
    res.setHeader("content-type", "application/json; charset=utf-8")
    res.end(JSON.stringify({ errCode: 404, errMsg: "not found", result: null }))
  })

  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => server.close())

  const address = server.address()
  if (!address || typeof address === "string") {
    throw new Error("failed to bind mock server")
  }

  const originalFetchDescriptor = Object.getOwnPropertyDescriptor(globalThis, "fetch")
  Object.defineProperty(globalThis, "fetch", {
    value: undefined,
    configurable: true,
    writable: true
  })

  try {
    const client = new QingflowClient({
      baseUrl: `http://127.0.0.1:${address.port}`,
      accessToken: ACCESS_TOKEN,
      timeoutMs: 5000
    })

    const apps = await client.listApps()
    assert.equal(apps.errCode, 0)
    assert.equal(apps.result.appList?.[0]?.appKey, "app_demo")

    const records = await client.listRecords("app_demo", {
      pageNum: 1,
      pageSize: 2,
      type: 8
    })
    assert.equal(records.errCode, 0)
    assert.equal(records.result.resultAmount, 2)
  } finally {
    if (originalFetchDescriptor) {
      Object.defineProperty(globalThis, "fetch", originalFetchDescriptor)
    } else {
      delete globalThis.fetch
    }
  }
})

export type QingflowPrimitive = string | number | boolean | null

export interface QingflowClientConfig {
  baseUrl: string
  accessToken: string
  timeoutMs?: number
}

export interface RequestOptions {
  userId?: string
  query?: Record<string, QingflowPrimitive | undefined>
  body?: unknown
}

export interface QingflowResponse<T = unknown> {
  errCode: number
  errMsg: string
  result: T
}

export class QingflowApiError extends Error {
  public readonly errCode: number | null
  public readonly errMsg: string
  public readonly httpStatus: number | null
  public readonly details: unknown

  constructor(params: {
    message: string
    errCode?: number | null
    errMsg?: string
    httpStatus?: number | null
    details?: unknown
  }) {
    super(params.message)
    this.name = "QingflowApiError"
    this.errCode = params.errCode ?? null
    this.errMsg = params.errMsg ?? params.message
    this.httpStatus = params.httpStatus ?? null
    this.details = params.details
  }
}

export class QingflowClient {
  private readonly baseUrl: string
  private readonly accessToken: string
  private readonly timeoutMs: number

  constructor(config: QingflowClientConfig) {
    this.baseUrl = normalizeBaseUrl(config.baseUrl)
    this.accessToken = normalizeAccessToken(config.accessToken)
    this.timeoutMs = config.timeoutMs ?? 30_000
  }

  listApps(options: { userId?: string; favourite?: 0 | 1 } = {}) {
    return this.request<{
      appList?: Array<{ appKey: string; appName: string }>
    }>({
      method: "GET",
      path: "/app",
      options: {
        query: {
          userId: options.userId,
          favourite: options.favourite
        }
      }
    })
  }

  getForm(appKey: string, options: { userId?: string } = {}) {
    return this.request<{
      questionBaseInfos?: unknown[]
      questionRelations?: unknown[]
    }>({
      method: "GET",
      path: `/app/${encodeURIComponent(appKey)}/form`,
      options: {
        userId: options.userId
      }
    })
  }

  listRecords(
    appKey: string,
    payload: unknown,
    options: { userId?: string } = {}
  ) {
    return this.request<{
      pageAmount?: number
      pageNum?: number
      pageSize?: number
      resultAmount?: number
      result?: unknown[]
    }>({
      method: "POST",
      path: `/app/${encodeURIComponent(appKey)}/apply/filter`,
      options: {
        userId: options.userId,
        body: payload
      }
    })
  }

  getRecord(applyId: string) {
    return this.request({
      method: "GET",
      path: `/apply/${encodeURIComponent(applyId)}`
    })
  }

  createRecord(
    appKey: string,
    payload: unknown,
    options: { userId?: string } = {}
  ) {
    return this.request<{ requestId?: string; applyId?: number }>({
      method: "POST",
      path: `/app/${encodeURIComponent(appKey)}/apply`,
      options: {
        userId: options.userId,
        body: payload
      }
    })
  }

  updateRecord(
    applyId: string,
    payload: unknown,
    options: { userId?: string } = {}
  ) {
    return this.request<{ requestId?: string }>({
      method: "POST",
      path: `/apply/${encodeURIComponent(applyId)}`,
      options: {
        userId: options.userId,
        body: payload
      }
    })
  }

  getOperation(requestId: string) {
    return this.request<unknown>({
      method: "GET",
      path: `/operation/${encodeURIComponent(requestId)}`
    })
  }

  private async request<T>(params: {
    method: "GET" | "POST"
    path: string
    options?: RequestOptions
  }): Promise<QingflowResponse<T>> {
    const options = params.options ?? {}
    const url = new URL(params.path, this.baseUrl)
    appendQuery(url, options.query)

    const headers = new Headers()
    headers.set("accessToken", this.accessToken)
    if (options.userId) {
      headers.set("userId", options.userId)
    }

    const init: RequestInit = {
      method: params.method,
      headers
    }

    if (options.body !== undefined) {
      headers.set("content-type", "application/json")
      init.body = JSON.stringify(options.body)
    }

    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), this.timeoutMs)
    init.signal = controller.signal

    try {
      const response = await fetch(url, init)
      const text = await response.text()
      const data = safeJsonParse(text)

      if (!response.ok) {
        throw new QingflowApiError({
          message: `Qingflow HTTP ${response.status}`,
          httpStatus: response.status,
          errMsg: extractErrMsg(data, text),
          details: data ?? text
        })
      }

      if (!data || typeof data !== "object") {
        throw new QingflowApiError({
          message: "Qingflow response is not JSON object",
          httpStatus: response.status,
          details: text
        })
      }

      const parsed = parseResponseEnvelope(data as Record<string, unknown>)
      if (parsed.errCode === null) {
        throw new QingflowApiError({
          message: "Qingflow response missing code field",
          httpStatus: response.status,
          details: data
        })
      }

      if (parsed.errCode !== 0) {
        throw new QingflowApiError({
          message: `Qingflow API error ${parsed.errCode}: ${parsed.errMsg}`,
          errCode: parsed.errCode,
          errMsg: parsed.errMsg,
          httpStatus: response.status,
          details: data
        })
      }

      return {
        errCode: parsed.errCode,
        errMsg: parsed.errMsg,
        result: parsed.result as T
      }
    } catch (error) {
      if (error instanceof QingflowApiError) {
        throw error
      }

      if (error instanceof Error && error.name === "AbortError") {
        throw new QingflowApiError({
          message: `Qingflow request timeout after ${this.timeoutMs}ms`
        })
      }

      throw new QingflowApiError({
        message: error instanceof Error ? error.message : "Unknown request error",
        details: error
      })
    } finally {
      clearTimeout(timer)
    }
  }
}

function normalizeBaseUrl(url: string): string {
  const normalized = url.trim()
  if (!normalized) {
    throw new Error("QINGFLOW_BASE_URL is required")
  }
  return normalized.endsWith("/") ? normalized : `${normalized}/`
}

function normalizeAccessToken(token: string): string {
  const normalized = token.trim()
  if (!normalized) {
    throw new Error("QINGFLOW_ACCESS_TOKEN is required")
  }
  if (/[\r\n]/.test(normalized)) {
    throw new Error("QINGFLOW_ACCESS_TOKEN contains newline characters")
  }
  if (/[\u0100-\uFFFF]/.test(normalized)) {
    throw new Error("QINGFLOW_ACCESS_TOKEN contains non-ASCII characters; please paste raw token only")
  }
  return normalized
}

function appendQuery(
  url: URL,
  query?: Record<string, QingflowPrimitive | undefined>
): void {
  if (!query) {
    return
  }

  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null) {
      continue
    }
    url.searchParams.set(key, String(value))
  }
}

function safeJsonParse(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

function extractErrMsg(json: unknown, rawText: string): string {
  if (json && typeof json === "object") {
    const obj = json as Record<string, unknown>
    const msgCandidates = [obj.errMsg, obj.errorMsg, obj.message]
    for (const msg of msgCandidates) {
      if (typeof msg === "string" && msg.trim()) {
        return msg
      }
    }
  }
  const sample = rawText.trim().slice(0, 200)
  return sample || "request failed"
}

function parseResponseEnvelope(data: Record<string, unknown>): {
  errCode: number | null
  errMsg: string
  result: unknown
} {
  const codeCandidates = [data.errCode, data.errorCode, data.statusCode]
  let errCode: number | null = null

  for (const candidate of codeCandidates) {
    const parsed = toFiniteNumber(candidate)
    if (parsed !== null) {
      errCode = parsed
      break
    }
  }

  const messageCandidates = [data.errMsg, data.errorMsg, data.message]
  let errMsg = ""
  for (const candidate of messageCandidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      errMsg = candidate.trim()
      break
    }
  }

  return {
    errCode,
    errMsg,
    result: data.result
  }
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) {
      return parsed
    }
  }
  return null
}

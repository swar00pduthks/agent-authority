import { useCallback, useEffect, useRef, useState } from 'react'

export type Kind = 'dataset' | 'job' | 'session' | 'agent' | 'step' | 'tool' | 'llm' | 'identity'
export type Severity = 'critical' | 'high' | 'medium' | 'low' | 'info'

export interface Stats {
  events_in: number
  bytes_in: number
  bytes_stored: number
  dedup_ratio: number
  facet_refs: number
  facet_new: number
  unique_facets: number
  sampled_out: number
  aggregated: number
  payloads_offloaded: number
  runs_kept_events: number
  runs: number
  nodes: number
  edges: number
  datasets: number
  jobs: number
  agent_runs: number
  cache_hit_rate: number
}

export interface Run {
  run_id: string
  namespace: string
  job: string
  kind: Kind
  root_run_id: string | null
  parent_run_id: string | null
  state: 'START' | 'RUNNING' | 'COMPLETE' | 'FAIL' | 'ABORT' | 'OTHER'
  start_time: string | null
  end_time: string | null
  last_event_time: string | null
  duration_ms: number | null
  event_count: number
  agent_id: string | null
  owner: string | null
  on_behalf_of: string | null
  tool: string | null
  side_effecting: boolean | null
  policy_decision: string | null
  policy_rule: string | null
  model: string | null
  error: string | null
  children?: Run[]
}

export interface Session extends Run {
  size: number
  agents: string[]
  tools: number
  failures: number
  denials: number
}

export interface Agent {
  agent_id: string
  name: string
  namespace: string
  owner: string | null
  runs: number
  failures: number
  denials: number
  tools: string[]
  models: string[]
  principals: string[]
  last_seen: string | null
}

export interface GNode { id: string; kind: Kind; namespace: string; name: string; attrs: Record<string, unknown> }
export interface GEdge { src: string; dst: string; type: string; run_count: number; first_seen: string; last_seen: string }

export interface Finding {
  id: string
  rule: string
  severity: Severity
  title: string
  detail: string
  run_id: string | null
  root_run_id: string | null
  agent_id: string | null
  node_id: string | null
  time: string | null
  evidence: Record<string, unknown>
}

export interface Check { check: string; status: 'pass' | 'warn' | 'fail'; detail: string }
export interface Hop {
  run_id: string; job: string; namespace: string; kind: Kind; agent_id: string | null; principal: string | null
  tool: string | null; model: string | null; state: string; status: 'pass' | 'warn' | 'fail'; checks: Check[]
}
export interface Authority {
  run_id: string
  verdict: 'pass' | 'warn' | 'fail'
  agent_depth: number
  hops: Hop[]
  datasets: { dataset_id: string; direction: 'in' | 'out'; row_count: number | null; tags: string[] }[]
}

export interface BlastRadius {
  node: GNode
  affected: (GNode & { hops: number })[]
  by_kind: Record<string, number>
  sensitive_datasets: string[]
  edges: GEdge[]
}

export interface ReplayEvent { event_type: string; event_time: string; bytes_in: number; bytes_stored: number; event: unknown }

/** Runtime configuration. The standalone app uses defaults; the web component sets these from its attributes. */
export interface AuthorityConfig {
  /** Base URL of the Agent Authority API, e.g. https://authority.internal ('' = same origin). */
  apiBase: string
  /** Scope every view to one namespace, e.g. an app-builder project. */
  namespace?: string
  /** Returns a bearer token (e.g. an Entra access token) for API calls. */
  getToken?: () => string | null | Promise<string | null>
  /** Called once when the API answers 401 (e.g. an expired Entra token); the request is then retried once. */
  onUnauthorized?: () => void | Promise<void>
}

export const config: AuthorityConfig = { apiBase: '' }

export function configure(c: Partial<AuthorityConfig>) {
  Object.assign(config, c)
}

const SCOPED = ['/api/authority/sessions', '/api/authority/agents', '/api/authority/findings', '/api/authority/runs', '/api/authority/graph']

export async function get<T>(path: string, params?: Record<string, string | number | boolean | undefined>): Promise<T> {
  const p: Record<string, string | number | boolean | undefined> = { ...params }
  if (config.namespace && p.namespace === undefined && SCOPED.includes(path)) p.namespace = config.namespace
  const entries = Object.entries(p).filter(([, v]) => v !== undefined && v !== '')
  const q = entries.length ? '?' + entries.map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`).join('&') : ''
  const url = `${config.apiBase.replace(/\/$/, '')}${path}${q}`
  const call = async () => {
    const headers: Record<string, string> = {}
    const token = config.getToken ? await config.getToken() : null
    if (token) headers.Authorization = `Bearer ${token}`
    return fetch(url, { headers })
  }
  let res = await call()
  if (res.status === 401 && config.onUnauthorized) {
    await config.onUnauthorized()
    res = await call()
  }
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${path}`)
  return res.json() as Promise<T>
}

/** Fetch, then re-fetch every `intervalMs` (0 = once). Keeps the last good value while refreshing. */
export function usePoll<T>(fn: () => Promise<T>, deps: unknown[], intervalMs = 0) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const fnRef = useRef(fn)
  fnRef.current = fn
  const load = useCallback(() => {
    fnRef.current().then((d) => { setData(d); setError(null) }).catch((e: Error) => setError(e.message))
  }, [])
  useEffect(() => {
    setData(null)
    load()
    if (!intervalMs) return
    const t = setInterval(() => { if (!document.hidden) load() }, intervalMs)
    return () => clearInterval(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)
  return { data, error, reload: load }
}

export function bytes(n: number): string {
  if (n < 1024) return `${n} B`
  const u = ['KB', 'MB', 'GB', 'TB']
  let i = -1
  do { n /= 1024; i++ } while (n >= 1024 && i < u.length - 1)
  return `${n.toFixed(n < 10 ? 1 : 0)} ${u[i]}`
}

export function ago(iso: string | null): string {
  if (!iso) return '—'
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000)
  if (s < 5) return 'just now'
  if (s < 60) return `${Math.floor(s)}s ago`
  if (s < 3600) return `${Math.floor(s / 60)}m ago`
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`
  return `${Math.floor(s / 86400)}d ago`
}

export function ms(n: number | null): string {
  if (n == null) return '—'
  if (n < 1000) return `${n} ms`
  if (n < 60_000) return `${(n / 1000).toFixed(1)} s`
  return `${Math.floor(n / 60_000)}m ${Math.round((n % 60_000) / 1000)}s`
}

export const short = (id: string | null | undefined, n = 8) => (id ? id.slice(0, n) : '—')
export const compact = (n: number) => Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 }).format(n)

export interface Integrity {
  chain_key: string
  status: 'intact' | 'broken' | 'unanchored' | 'empty'
  events: number
  head_seq: number
  head_hash: string | null
  anchored_seq: number | null
  anchored_at: string | null
  problems: string[]
}

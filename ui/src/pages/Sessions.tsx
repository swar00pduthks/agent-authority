import { useNavigate, useParams } from 'react-router-dom'
import { ago, get, ms, usePoll, type Run, type Session } from '../api'
import { decisionTone, Empty, ErrorNote, KIND_ICON, Pill } from '../components/bits'
import { useRunDrawer } from '../components/RunDrawer'
import './sessions.css'

interface TreeResp { tree: Run; aggregated_tools: { parent_run_id: string; tool: string; calls: number; failures: number; avg_ms: number | null }[] }

function flatten(r: Run, depth = 0, out: { r: Run; depth: number }[] = []) {
  out.push({ r, depth })
  for (const c of r.children ?? []) flatten(c, depth + 1, out)
  return out
}

export function SessionsPage() {
  const { id } = useParams()
  const nav = useNavigate()
  const sessions = usePoll(() => get<Session[]>('/api/hub/sessions', { limit: 60 }), [], 4000)
  const selected = id ?? sessions.data?.[0]?.run_id

  return (
    <div className="page">
      <ErrorNote error={sessions.error} />
      <div className="page-head">
        <div>
          <h1 className="page-title">Sessions</h1>
          <p className="page-sub">One session is everything that happened for one request: each agent, tool call and model call it led to, in order.</p>
        </div>
      </div>
      <div className="sess-layout">
        <aside className="panel sess-list" aria-label="Sessions">
          {sessions.data?.length === 0 && <Empty title="No sessions yet" />}
          {sessions.data?.map((s) => (
            <button key={s.run_id} className={`sess-item ${s.run_id === selected ? 'on' : ''}`} onClick={() => nav(`/sessions/${s.run_id}`)}>
              <div className="si-top">
                <span className="si-name">{s.job}</span>
                <span className="faint mono">{ago(s.last_event_time)}</span>
              </div>
              <div className="si-who">{s.on_behalf_of ?? <span className="si-none">no person attached</span>}</div>
              <div className="si-meta mono faint">
                {s.agents.length} agents · {s.tools} tools
                {s.denials > 0 && <span className="si-bad"> · {s.denials} denied</span>}
                {s.failures > 0 && <span className="si-bad"> · {s.failures} failed</span>}
              </div>
            </button>
          ))}
        </aside>
        {selected ? <SessionTimeline id={selected} /> : <div className="panel"><Empty title="Pick a session" /></div>}
      </div>
    </div>
  )
}

function SessionTimeline({ id }: { id: string }) {
  const open = useRunDrawer()
  const t = usePoll(() => get<TreeResp>(`/api/hub/sessions/${id}/tree`), [id], 5000)
  if (!t.data) return <div className="panel"><div className="panel-body muted">Loading…</div></div>
  const rows = flatten(t.data.tree)
  const times = rows.flatMap(({ r }) => [r.start_time, r.end_time, r.last_event_time]).filter(Boolean).map((x) => new Date(x!).getTime())
  const t0 = Math.min(...times)
  const span = Math.max(1, Math.max(...times) - t0)
  const root = t.data.tree

  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2 className="panel-title" style={{ fontSize: 15 }}>{root.job}</h2>
          <div className="faint mono" style={{ marginTop: 2 }}>{root.on_behalf_of ?? 'no person attached'} · {rows.length} runs · {ms(span)}</div>
        </div>
        <button className="btn" onClick={() => open(root.run_id)}>Chain of authority</button>
      </div>
      <div className="tl" role="list">
        {rows.map(({ r, depth }) => {
          const s = new Date(r.start_time ?? r.last_event_time ?? 0).getTime()
          const e = new Date(r.end_time ?? r.last_event_time ?? 0).getTime()
          const left = ((s - t0) / span) * 100
          const width = Math.max(0.8, ((e - s) / span) * 100)
          const Icon = KIND_ICON[r.kind] ?? KIND_ICON.job
          const deny = decisionTone(r.policy_decision) === 'deny'
          return (
            <div key={r.run_id} role="listitem" className={`tl-row k-${r.kind} ${r.state === 'FAIL' ? 'failed' : ''} ${deny ? 'denied' : ''}`}
              onClick={() => open(r.run_id)} tabIndex={0} onKeyDown={(ev) => ev.key === 'Enter' && open(r.run_id)}>
              <div className="tl-name" style={{ paddingLeft: 12 + depth * 18 }}>
                <Icon size={13} className="tl-icon" />
                <span className="tl-label">{r.job}</span>
                {r.side_effecting && <span className="tl-se" title="Changes state">●</span>}
              </div>
              <div className="tl-track">
                <span className="tl-bar" style={{ left: `${left}%`, width: `${width}%` }} />
              </div>
              <div className="tl-status">
                {r.policy_decision && <Pill tone={decisionTone(r.policy_decision)}>{r.policy_decision}</Pill>}
                {r.state === 'FAIL' && <Pill tone="FAIL">failed</Pill>}
              </div>
            </div>
          )
        })}
      </div>
      {t.data.aggregated_tools.length > 0 && (
        <div className="panel-body rolled">
          <span className="label">Rolled up (read-only tool calls, kept as counts)</span>
          {t.data.aggregated_tools.map((a, i) => (
            <span key={i} className="chip mono">{a.tool} × {a.calls}{a.avg_ms != null ? ` · ${a.avg_ms} ms avg` : ''}</span>
          ))}
        </div>
      )}
    </section>
  )
}

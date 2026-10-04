import { useMemo, useState } from 'react'
import { ago, compact, get, usePoll, type Agent, type Finding, type Run } from '../api'
import { Empty, ErrorNote, Pill } from '../components/bits'
import { useRunDrawer } from '../components/RunDrawer'
import './agents.css'

export function AgentsPage() {
  const open = useRunDrawer()
  const agents = usePoll(() => get<Agent[]>('/api/hub/agents'), [], 5000)
  const findings = usePoll(() => get<Finding[]>('/api/hub/findings'), [], 8000)
  const [q, setQ] = useState('')

  const risk = useMemo(() => {
    const m: Record<string, { n: number; worst: string }> = {}
    const order = ['critical', 'high', 'medium', 'low', 'info']
    for (const f of findings.data ?? []) {
      if (!f.agent_id || f.severity === 'info') continue
      const cur = (m[f.agent_id] ??= { n: 0, worst: 'info' })
      cur.n++
      if (order.indexOf(f.severity) < order.indexOf(cur.worst)) cur.worst = f.severity
    }
    return m
  }, [findings.data])

  const list = (agents.data ?? []).filter((a) =>
    !q || [a.name, a.owner, ...a.tools, ...a.models].join(' ').toLowerCase().includes(q.toLowerCase()))

  const openLatest = async (a: Agent) => {
    const [r] = await get<Run[]>('/api/hub/runs', { agent_id: a.agent_id, kind: 'agent', limit: 1 })
    if (r) open(r.run_id)
  }

  return (
    <div className="page">
      <ErrorNote error={agents.error} />
      <div className="page-head">
        <div>
          <h1 className="page-title">Agents</h1>
          <p className="page-sub">
            Every agent seen in lineage, with its owner, the tools it uses and the people it acts for. Built
            automatically from events, so there is no registry to keep up to date.
          </p>
        </div>
        <input className="input" placeholder="Filter by name, owner, tool or model" value={q} onChange={(e) => setQ(e.target.value)} />
      </div>

      {agents.data && list.length === 0 && (
        <Empty title={q ? 'No agents match that filter' : 'No agents yet'}>
          {!q && <p>Instrument an agent with the Python SDK, or send OpenLineage events with an <code>agent</code> run facet.</p>}
        </Empty>
      )}

      <div className="agent-grid">
        {list.map((a) => {
          const r = risk[a.agent_id]
          return (
            <button key={a.agent_id} className={`agent-card ${r ? `risk-${r.worst}` : ''}`} onClick={() => openLatest(a)}>
              <div className="ac-head">
                <div>
                  <div className="ac-name">{a.name}</div>
                  <div className={`ac-owner mono ${a.owner ? 'faint' : 'missing'}`}>{a.owner ?? 'no owner'}</div>
                </div>
                {r ? <Pill tone={r.worst}>{r.n} finding{r.n > 1 ? 's' : ''}</Pill> : <Pill tone="pass">clean</Pill>}
              </div>
              <div className="ac-stats">
                <div><b>{compact(a.runs)}</b><span>runs</span></div>
                <div className={a.failures ? 'bad' : ''}><b>{a.failures}</b><span>failed</span></div>
                <div className={a.denials ? 'bad' : ''}><b>{a.denials}</b><span>denied</span></div>
                <div><b>{a.principals.length}</b><span>people</span></div>
              </div>
              <div className="ac-row"><span className="label">Tools</span>
                <div className="chips">{a.tools.length ? a.tools.map((t) => <span key={t} className="chip">{t}</span>) : <span className="faint">none</span>}</div>
              </div>
              <div className="ac-row"><span className="label">Models</span>
                <div className="chips">{a.models.map((m) => <span key={m} className="chip mono">{m}</span>)}</div>
              </div>
              <div className="ac-foot faint mono">last active {ago(a.last_seen)}</div>
            </button>
          )
        })}
      </div>
    </div>
  )
}

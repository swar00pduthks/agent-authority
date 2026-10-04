import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { ago, get, usePoll, type Finding, type Severity } from '../api'
import { Empty, ErrorNote, Pill } from '../components/bits'
import { useRunDrawer } from '../components/RunDrawer'
import './sentinel.css'

interface RuleInfo { id: string; severity: Severity; description: string; enabled: boolean }
const LEVELS: (Severity | 'all')[] = ['all', 'critical', 'high', 'medium', 'low', 'info']

export function SentinelPage() {
  const open = useRunDrawer()
  const nav = useNavigate()
  const [level, setLevel] = useState<Severity | 'all'>('all')
  const [rule, setRule] = useState<string | null>(null)
  const findings = usePoll(() => get<Finding[]>('/api/hub/findings'), [], 6000)
  const rules = usePoll(() => get<RuleInfo[]>('/api/hub/rules'), [])

  const byRule = (findings.data ?? []).reduce<Record<string, number>>((m, f) => ({ ...m, [f.rule]: (m[f.rule] ?? 0) + 1 }), {})
  const list = (findings.data ?? []).filter((f) => (level === 'all' || f.severity === level) && (!rule || f.rule === rule))

  return (
    <div className="page">
      <ErrorNote error={findings.error} />
      <div className="page-head">
        <div>
          <h1 className="page-title">Sentinel</h1>
          <p className="page-sub">
            Rules written as code that watch lineage for agents acting without accountability. Rules are plain
            deterministic code, never a model, so a prompt injection can't talk its way past them.
          </p>
        </div>
      </div>

      <div className="sentinel-layout">
        <aside className="panel rules">
          <div className="panel-head"><h2 className="panel-title">Rules</h2><span className="faint mono">{rules.data?.length ?? 0} active</span></div>
          <ul>
            <li className={!rule ? 'on' : ''}><button onClick={() => setRule(null)}><span>All rules</span><span className="mono">{findings.data?.length ?? 0}</span></button></li>
            {rules.data?.map((r) => (
              <li key={r.id} className={rule === r.id ? 'on' : ''}>
                <button onClick={() => setRule(rule === r.id ? null : r.id)} title={r.description}>
                  <span><i className={`dot ${r.severity}`} />{r.id}</span>
                  <span className="mono">{byRule[r.id] ?? 0}</span>
                </button>
                {rule === r.id && <p className="rule-desc">{r.description}</p>}
              </li>
            ))}
          </ul>
        </aside>

        <section>
          <div className="sev-filter" role="group" aria-label="Severity">
            {LEVELS.map((l) => <button key={l} className="btn" aria-pressed={level === l} onClick={() => setLevel(l)}>{l}</button>)}
          </div>
          {findings.data && list.length === 0 && (
            <div className="panel"><Empty title="Nothing to investigate">No findings match this filter.</Empty></div>
          )}
          <div className="findings">
            {list.map((f) => (
              <article key={f.id} className={`finding sev-${f.severity}`}>
                <div className="f-top">
                  <Pill tone={f.severity}>{f.severity}</Pill>
                  <span className="mono faint">{f.rule}</span>
                  {Number(f.evidence.occurrences) > 1 && <span className="pill">seen {String(f.evidence.occurrences)} times</span>}
                  <span className="mono faint" style={{ marginLeft: 'auto' }}>{ago(f.time)}</span>
                </div>
                <h3 className="f-title">{f.title}</h3>
                <p className="f-detail">{f.detail}</p>
                {Array.isArray(f.evidence.chain) && (
                  <div className="f-chain mono">{(f.evidence.chain as string[]).join('  →  ')}</div>
                )}
                <div className="f-actions">
                  {f.run_id && <button className="btn" onClick={() => open(f.run_id!)}>Trace authority</button>}
                  {f.run_id && <button className="btn ghost" onClick={() => open(f.run_id!, 'replay')}>Replay events</button>}
                  {f.root_run_id && <button className="btn ghost" onClick={() => nav(`/sessions/${f.root_run_id}`)}>Open session</button>}
                  {f.node_id && !f.run_id && <button className="btn ghost" onClick={() => nav('/lineage')}>View in lineage</button>}
                </div>
              </article>
            ))}
          </div>
        </section>
      </div>
    </div>
  )
}

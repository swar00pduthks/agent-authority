import { ArrowRight } from 'lucide-react'
import { Link } from 'react-router-dom'
import { ago, bytes, compact, get, usePoll, type Agent, type Finding, type Run, type Severity, type Stats } from '../api'
import { decisionTone, ErrorNote, KindTag, Pill } from '../components/bits'
import { useRunDrawer } from '../components/RunDrawer'
import './overview.css'

const SEV: Severity[] = ['critical', 'high', 'medium', 'low', 'info']

export function OverviewPage({ stats }: { stats: Stats | null }) {
  const open = useRunDrawer()
  const runs = usePoll(() => get<Run[]>('/api/authority/runs', { limit: 16 }), [], 2000)
  const findings = usePoll(() => get<Finding[]>('/api/authority/findings'), [], 6000)
  const agents = usePoll(() => get<Agent[]>('/api/authority/agents'), [], 6000)

  const counts = SEV.map((s) => [s, findings.data?.filter((f) => f.severity === s).length ?? 0] as const)
  const total = counts.reduce((a, [, n]) => a + n, 0)

  return (
    <div className="page">
      <ErrorNote error={runs.error} />
      <div className="page-head">
        <div>
          <h1 className="page-title">What your agents did, and for whom</h1>
          <p className="page-sub">
            Every pipeline run and agent action, captured as OpenLineage, stored once, and checked for accountability.
          </p>
        </div>
      </div>

      <section className="panel meter" aria-label="Storage after dehydration">
        {stats ? <Meter s={stats} /> : <div className="panel-body muted">Waiting for the first events…</div>}
      </section>

      <div className="ov-grid">
        <section className="panel">
          <div className="panel-head">
            <h2 className="panel-title">Live activity</h2>
            <span className="faint mono">newest first</span>
          </div>
          <ul className="tape">
            {runs.data?.map((r) => (
              <li key={r.run_id + r.state} onClick={() => open(r.run_id)} tabIndex={0}
                onKeyDown={(e) => e.key === 'Enter' && open(r.run_id)}>
                <KindTag kind={r.kind} />
                <span className="tape-job">{r.job}</span>
                <span className="tape-who faint">{r.on_behalf_of ?? r.owner ?? r.namespace}</span>
                <span className="tape-pol">{r.policy_decision && <Pill tone={decisionTone(r.policy_decision)}>{r.policy_decision}</Pill>}</span>
                <Pill tone={r.state}>{r.state.toLowerCase()}</Pill>
                <span className="tape-time faint mono">{ago(r.last_event_time)}</span>
              </li>
            ))}
          </ul>
        </section>

        <div className="grid" style={{ alignContent: 'start' }}>
          <section className="panel">
            <div className="panel-head">
              <h2 className="panel-title">Sentinel</h2>
              <Link to="/sentinel" className="faint mono">all findings <ArrowRight size={12} style={{ verticalAlign: -2 }} /></Link>
            </div>
            <div className="panel-body">
              <div className="sevbar" role="img" aria-label={counts.map(([s, n]) => `${n} ${s}`).join(', ')}>
                {counts.filter(([, n]) => n).map(([s, n]) => (
                  <span key={s} className={`seg ${s}`} style={{ flexGrow: n }} />
                ))}
              </div>
              <div className="sevlegend">
                {counts.map(([s, n]) => <span key={s} className={n ? '' : 'faint'}><i className={`dot ${s}`} />{n} {s}</span>)}
              </div>
              <ul className="topfind">
                {findings.data?.slice(0, 5).map((f) => (
                  <li key={f.id} onClick={() => f.run_id && open(f.run_id)} tabIndex={0}>
                    <Pill tone={f.severity}>{f.severity}</Pill>
                    <span>{f.title}</span>
                  </li>
                ))}
                {total === 0 && <li className="faint">No findings. Every action so far traces back to a person.</li>}
              </ul>
            </div>
          </section>

          <section className="panel">
            <div className="panel-head">
              <h2 className="panel-title">Busiest agents</h2>
              <Link to="/agents" className="faint mono">catalog <ArrowRight size={12} style={{ verticalAlign: -2 }} /></Link>
            </div>
            <table className="t">
              <tbody>
                {agents.data?.slice(0, 6).map((a) => (
                  <tr key={a.agent_id}>
                    <td><div style={{ fontWeight: 600 }}>{a.name}</div><div className="faint mono">{a.owner ?? 'no owner'}</div></td>
                    <td className="num">{compact(a.runs)}<div className="faint">runs</div></td>
                    <td className="num" style={{ color: a.denials ? 'var(--color-fail)' : undefined }}>{a.denials}<div className="faint">denied</div></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </div>
      </div>
    </div>
  )
}

function Meter({ s }: { s: Stats }) {
  const pct = s.bytes_in ? Math.max(0.6, (s.bytes_stored / s.bytes_in) * 100) : 100
  return (
    <div className="meter-inner">
      <div className="meter-ratio">
        <span className="ratio-num">{s.dedup_ratio.toFixed(1)}×</span>
        <span className="ratio-cap">smaller than the OpenLineage<br />events it received</span>
      </div>
      <div className="meter-bars">
        <div className="bar-row">
          <span className="label">Received</span>
          <div className="bar"><span className="fill in" style={{ width: '100%' }} /></div>
          <span className="mono">{bytes(s.bytes_in)}</span>
        </div>
        <div className="bar-row">
          <span className="label">Stored</span>
          <div className="bar"><span className="fill out" style={{ width: `${pct}%` }} /></div>
          <span className="mono">{bytes(s.bytes_stored)}</span>
        </div>
        <div className="why">
          <span><b>{compact(s.facet_refs)}</b> facets referenced, <b>{compact(s.unique_facets)}</b> stored</span>
          <span><b>{compact(s.sampled_out)}</b> LLM calls sampled out</span>
          <span><b>{compact(s.aggregated)}</b> read-only tool calls rolled up</span>
          <span><b>{compact(s.payloads_offloaded)}</b> prompts moved to blob storage</span>
          <span><b>{compact(s.events_in)}</b> events merged into <b>{compact(s.runs)}</b> run rows</span>
        </div>
      </div>
    </div>
  )
}

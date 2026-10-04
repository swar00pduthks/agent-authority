import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import { ago, get, ms, usePoll, type Authority, type Finding, type Run } from '../api'
import { AuthorityTrail } from '../components/AuthorityTrail'
import { decisionTone, Empty, ErrorNote, KIND_ICON, Pill } from '../components/bits'
import { useRunDrawer } from '../components/RunDrawer'
import './audit.css'

function flatten(r: Run, out: Run[] = []): Run[] {
  out.push(r)
  for (const c of r.children ?? []) flatten(c, out)
  return out
}

const RANK = { fail: 0, warn: 1, pass: 2 } as const

/**
 * The audit record for one session: who asked, which models and data were used, every action that changed
 * something with its chain of authority, and anything Sentinel found. Designed to be embedded per project.
 */
export function AuditPage({ sessionId }: { sessionId?: string }) {
  const params = useParams()
  const id = sessionId ?? params.id
  const open = useRunDrawer()
  const tree = usePoll(() => get<{ tree: Run }>(`/api/authority/sessions/${id}/tree`), [id])
  const findings = usePoll(() => get<Finding[]>('/api/authority/findings', { session: id }), [id])
  const [chains, setChains] = useState<Authority[] | null>(null)

  const runs = tree.data ? flatten(tree.data.tree) : []
  const actions = runs.filter((r) => r.side_effecting || r.policy_decision || r.state === 'FAIL')
  const actionKey = actions.map((r) => r.run_id).join(',')

  useEffect(() => {
    if (!actionKey) { setChains([]); return }
    let live = true
    Promise.all(actionKey.split(',').map((rid) => get<Authority>(`/api/authority/runs/${rid}/authority`)))
      .then((cs) => { if (live) setChains(cs.sort((a, b) => RANK[a.verdict] - RANK[b.verdict])) })
      .catch(() => live && setChains([]))
    return () => { live = false }
  }, [actionKey])

  if (!id) return <div className="page"><Empty title="No session selected" /></div>
  if (tree.error) return <div className="page"><ErrorNote error={tree.error} /></div>
  if (!tree.data) return <div className="page muted">Loading…</div>

  const root = tree.data.tree
  const models = [...new Set(runs.map((r) => r.model).filter(Boolean))] as string[]
  const datasets = chains ? [...new Map(chains.flatMap((c) => c.datasets).map((d) => [d.dataset_id + d.direction, d])).values()] : []
  const verdict = chains?.length ? chains[0].verdict : 'pass'
  const span = runs.length ? ms(new Date(runs.map((r) => r.last_event_time ?? '').sort().at(-1)!).getTime() - new Date(root.start_time ?? root.last_event_time!).getTime()) : '—'

  return (
    <div className="page audit">
      <header className="audit-head">
        <div>
          <div className="label">Audit record · session {root.run_id.slice(0, 8)}</div>
          <h1 className="page-title">{root.job}</h1>
          <dl className="audit-facts">
            <div><dt>Requested by</dt><dd className={root.on_behalf_of ? '' : 'bad'}>{root.on_behalf_of ?? 'nobody'}</dd></div>
            <div><dt>Project</dt><dd className="mono">{root.namespace}</dd></div>
            <div><dt>When</dt><dd>{ago(root.start_time ?? root.last_event_time)} · {span}</dd></div>
            <div><dt>Models</dt><dd>{models.length ? models.join(', ') : '—'}</dd></div>
            <div><dt>Steps</dt><dd>{runs.length} runs · {actions.length} actions</dd></div>
          </dl>
        </div>
        <div className={`stamp big ${verdict}`}>{verdict === 'pass' ? 'Accountable' : verdict === 'warn' ? 'Review' : 'Unaccountable'}</div>
      </header>

      <div className="audit-grid">
        <section>
          <h2 className="audit-h">Actions and their authority</h2>
          {chains === null && <p className="muted">Checking each action…</p>}
          {chains?.length === 0 && <Empty title="No actions changed anything">This session only read and reasoned.</Empty>}
          <div className="audit-actions">
            {chains?.map((c) => {
              const last = c.hops[c.hops.length - 1]
              return (
                <details key={c.run_id} className={`audit-action v-${c.verdict}`} open={c.verdict !== 'pass'}>
                  <summary>
                    <span className={`dot-v ${c.verdict}`} />
                    <span className="aa-name">{last.job}</span>
                    <span className="faint mono">{last.kind}</span>
                    <span style={{ marginLeft: 'auto' }}><Pill tone={c.verdict}>{c.verdict}</Pill></span>
                  </summary>
                  <div className="aa-body"><AuthorityTrail a={c} onSelect={(rid) => open(rid)} /></div>
                </details>
              )
            })}
          </div>
        </section>

        <aside className="grid" style={{ alignContent: 'start' }}>
          <section className="panel">
            <div className="panel-head"><h2 className="panel-title">Data touched</h2></div>
            <div className="panel-body audit-data">
              {datasets.length === 0 && <span className="faint">No datasets recorded.</span>}
              {datasets.map((d) => (
                <div key={d.dataset_id + d.direction}>
                  <span className={`dir ${d.direction}`}>{d.direction === 'out' ? 'wrote' : 'read'}</span>
                  <span className="mono">{d.dataset_id.replace(/^dataset:/, '')}</span>
                  {d.tags.map((t) => <span key={t} className="tag">{t}</span>)}
                </div>
              ))}
            </div>
          </section>
          <section className="panel">
            <div className="panel-head"><h2 className="panel-title">Sentinel</h2><span className="faint mono">{findings.data?.length ?? 0}</span></div>
            <div className="panel-body" style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {findings.data?.length === 0 && <span className="faint">No findings for this session.</span>}
              {findings.data?.map((f) => (
                <button key={f.id} className="audit-finding" onClick={() => f.run_id && open(f.run_id)}>
                  <Pill tone={f.severity}>{f.severity}</Pill><span>{f.title}</span>
                </button>
              ))}
            </div>
          </section>
          <section className="panel">
            <div className="panel-head"><h2 className="panel-title">Every step</h2></div>
            <ul className="audit-steps">
              {runs.map((r) => {
                const Icon = KIND_ICON[r.kind] ?? KIND_ICON.job
                return (
                  <li key={r.run_id} onClick={() => open(r.run_id)}>
                    <Icon size={13} /><span>{r.job}</span>
                    {r.policy_decision && <Pill tone={decisionTone(r.policy_decision)}>{r.policy_decision}</Pill>}
                    {r.state === 'FAIL' && <Pill tone="FAIL">failed</Pill>}
                  </li>
                )
              })}
            </ul>
          </section>
        </aside>
      </div>
    </div>
  )
}

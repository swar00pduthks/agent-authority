import { X } from 'lucide-react'
import { createContext, useContext, useEffect, useState, type ReactNode } from 'react'
import { get, ms, short, usePoll, type Authority, type ReplayEvent, type Run } from '../api'
import { AuthorityTrail } from './AuthorityTrail'
import { decisionTone, Json, KIND_LABEL, KindTag, Pill } from './bits'

type Tab = 'authority' | 'details' | 'replay'
const Ctx = createContext<(runId: string, tab?: Tab) => void>(() => {})
export const useRunDrawer = () => useContext(Ctx)

export function RunDrawerProvider({ children, onOpen }: { children: ReactNode; onOpen?: (runId: string) => void }) {
  const [state, setState] = useState<{ id: string; tab: Tab } | null>(null)
  return (
    <Ctx.Provider value={(id, tab = 'authority') => { setState({ id, tab }); onOpen?.(id) }}>
      {children}
      {state && <RunDrawer key={state.id} runId={state.id} initialTab={state.tab} onClose={() => setState(null)}
        onSelect={(id) => setState({ id, tab: 'authority' })} />}
    </Ctx.Provider>
  )
}

function RunDrawer({ runId, initialTab, onClose, onSelect }: {
  runId: string; initialTab: Tab; onClose: () => void; onSelect: (id: string) => void
}) {
  const [tab, setTab] = useState<Tab>(initialTab)
  const run = usePoll(() => get<Run & { dynamic_facets: Record<string, unknown>; io: unknown[] }>(`/api/authority/runs/${runId}`), [runId])
  const auth = usePoll(() => get<Authority>(`/api/authority/runs/${runId}/authority`), [runId])
  const [withPayloads, setWithPayloads] = useState(false)
  const replay = usePoll(() => get<ReplayEvent[]>(`/api/authority/runs/${runId}/replay`, { payloads: withPayloads }), [runId, withPayloads])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const r = run.data
  return (
    <>
      <div className="scrim" onClick={onClose} />
      <aside className="drawer" role="dialog" aria-label="Run details">
        <div className="drawer-head">
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 4 }}>
              {r && <KindTag kind={r.kind} label={KIND_LABEL[r.kind]} />}
              <span className="mono faint">run {short(runId)}</span>
            </div>
            <div style={{ font: '700 21px/1.2 var(--font-display)', letterSpacing: '-0.02em' }}>{r?.job ?? '…'}</div>
            {r && (
              <div style={{ display: 'flex', gap: 8, marginTop: 8, flexWrap: 'wrap' }}>
                <Pill tone={r.state}>{r.state.toLowerCase()}</Pill>
                {r.policy_decision && <Pill tone={decisionTone(r.policy_decision)}>policy: {r.policy_decision}{r.policy_rule ? ` · ${r.policy_rule}` : ''}</Pill>}
                {r.side_effecting && <Pill tone="warn">changes state</Pill>}
                <Pill>{ms(r.duration_ms)}</Pill>
              </div>
            )}
          </div>
          <button className="btn ghost" onClick={onClose} aria-label="Close"><X size={18} /></button>
        </div>
        <div className="tabs" role="tablist">
          {(['authority', 'details', 'replay'] as Tab[]).map((t) => (
            <button key={t} role="tab" className="tab" aria-selected={tab === t} onClick={() => setTab(t)}>
              {t === 'authority' ? 'Chain of authority' : t === 'details' ? 'Details' : 'Auditor replay'}
            </button>
          ))}
        </div>
        <div className="drawer-body">
          {tab === 'authority' && (auth.data ? <AuthorityTrail a={auth.data} onSelect={onSelect} /> : <p className="muted">Loading…</p>)}
          {tab === 'details' && r && (
            <>
              <dl className="kv">
                <dt>Namespace</dt><dd className="mono">{r.namespace}</dd>
                <dt>Run id</dt><dd className="mono">{r.run_id}</dd>
                <dt>Session</dt><dd className="mono">{r.root_run_id === r.run_id ? 'this run is the root' : r.root_run_id}</dd>
                <dt>Agent</dt><dd>{r.agent_id ?? '—'}</dd>
                <dt>Owner</dt><dd>{r.owner ?? '—'}</dd>
                <dt>Acting for</dt><dd>{r.on_behalf_of ?? 'inherited / none'}</dd>
                <dt>Tool</dt><dd>{r.tool ?? '—'}</dd>
                <dt>Model</dt><dd>{r.model ?? '—'}</dd>
                <dt>Events merged</dt><dd>{r.event_count} → 1 row</dd>
                <dt>Started</dt><dd className="mono">{r.start_time ?? '—'}</dd>
                <dt>Ended</dt><dd className="mono">{r.end_time ?? '—'}</dd>
                {r.error && <><dt>Error</dt><dd style={{ color: 'var(--color-fail)' }}>{r.error}</dd></>}
              </dl>
              <div>
                <div className="label" style={{ marginBottom: 8 }}>Dynamic facets (kept on the run row)</div>
                <Json value={run.data?.dynamic_facets} />
              </div>
            </>
          )}
          {tab === 'replay' && (
            <>
              <p className="muted" style={{ margin: 0 }}>
                Stored events are rebuilt from their content-addressed facets, giving the exact OpenLineage events
                that were received. <span style={{ color: 'var(--color-accent)' }}>Teal refs</span> show what was deduplicated.
              </p>
              <label style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <input type="checkbox" checked={withPayloads} onChange={(e) => setWithPayloads(e.target.checked)} />
                Include offloaded prompts and completions
              </label>
              {replay.data?.length === 0 && <p className="muted">This run was aggregated or sampled, so no raw events were kept.</p>}
              {replay.data?.map((e, i) => (
                <div key={i}>
                  <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 8 }}>
                    <Pill tone={e.event_type}>{e.event_type}</Pill>
                    <span className="mono faint">{e.event_time}</span>
                    <span className="mono faint" style={{ marginLeft: 'auto' }}>
                      received {e.bytes_in.toLocaleString()} B · stored {e.bytes_stored.toLocaleString()} B
                    </span>
                  </div>
                  <Json value={e.event} />
                </div>
              ))}
            </>
          )}
        </div>
      </aside>
    </>
  )
}

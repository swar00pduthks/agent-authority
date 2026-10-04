import { Check, CircleAlert, Fingerprint, X } from 'lucide-react'
import type { Authority } from '../api'
import { KIND_ICON, KIND_LABEL } from './bits'
import './trail.css'

const STAMP = { pass: 'Accountable', warn: 'Review', fail: 'Unaccountable' } as const
const MARK = { pass: Check, warn: CircleAlert, fail: X }

/**
 * The chain of custody for one action: who it was done for, every agent that
 * handed it on, and the data it touched. The thread stays solid while a human
 * principal is attached and breaks when nobody is accountable.
 */
export function AuthorityTrail({ a, onSelect }: { a: Authority; onSelect?: (runId: string) => void }) {
  const principal = a.hops.find((h) => h.principal)?.principal ?? null
  return (
    <div className={`trail v-${a.verdict}`}>
      <div className="trail-top">
        <div className="trail-origin">
          <span className={`origin-seal ${principal ? 'ok' : 'none'}`}><Fingerprint size={16} /></span>
          <div>
            <div className="label">Acting for</div>
            <div className="origin-name">{principal ?? 'No accountable person'}</div>
          </div>
        </div>
        <div className={`stamp ${a.verdict}`} role="status" aria-label={`Verdict: ${STAMP[a.verdict]}`}>
          {STAMP[a.verdict]}
        </div>
      </div>

      <ol className="hops">
        {a.hops.map((h, i) => {
          const Icon = KIND_ICON[h.kind] ?? KIND_ICON.job
          const isLast = i === a.hops.length - 1
          return (
            <li key={h.run_id} className={`hop ${h.status} ${h.principal ? 'held' : 'broken'} ${isLast ? 'last' : ''}`}>
              <button className="seal" onClick={() => onSelect?.(h.run_id)} title="Open this run">
                <Icon size={15} />
              </button>
              <div className="hop-body">
                <div className="hop-line">
                  <span className="hop-name">{h.job}</span>
                  <span className="hop-kind">{KIND_LABEL[h.kind]}</span>
                  {h.model && <span className="hop-meta mono">{h.model}</span>}
                  {h.state === 'FAIL' && <span className="pill fail">failed</span>}
                </div>
                <ul className="checks">
                  {h.checks.map((c) => {
                    const M = MARK[c.status]
                    return (
                      <li key={c.check} className={c.status}>
                        <M size={12} strokeWidth={2.5} />
                        <span className="check-name">{c.check}</span>
                        <span className="check-detail">{c.detail}</span>
                      </li>
                    )
                  })}
                </ul>
              </div>
            </li>
          )
        })}
      </ol>

      {a.datasets.length > 0 && (
        <div className="touched">
          <div className="label">Touched</div>
          {a.datasets.map((d) => (
            <div key={d.dataset_id + d.direction} className="touched-row">
              <span className={`dir ${d.direction}`}>{d.direction === 'out' ? 'wrote' : 'read'}</span>
              <span className="mono">{d.dataset_id.replace(/^dataset:/, '')}</span>
              {d.tags.map((t) => <span key={t} className="tag">{t}</span>)}
              {d.row_count != null && <span className="faint mono">{d.row_count.toLocaleString()} rows</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

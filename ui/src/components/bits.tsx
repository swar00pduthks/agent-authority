import { Bot, Boxes, Cog, Database, Fingerprint, MessageSquareText, Workflow, Wrench } from 'lucide-react'
import type { ReactNode } from 'react'
import type { Kind } from '../api'

export const KIND_ICON: Record<Kind, typeof Bot> = {
  dataset: Database,
  job: Cog,
  session: Workflow,
  agent: Bot,
  step: Boxes,
  tool: Wrench,
  llm: MessageSquareText,
  identity: Fingerprint,
}

export const KIND_LABEL: Record<Kind, string> = {
  dataset: 'Dataset', job: 'Pipeline job', session: 'Session', agent: 'Agent', step: 'Step', tool: 'Tool',
  llm: 'LLM call', identity: 'Person',
}

export function KindTag({ kind, label }: { kind: Kind; label?: string }) {
  return <span className={`kind ${kind}`}>{label ?? kind}</span>
}

export function Pill({ tone, children }: { tone?: string | null; children: ReactNode }) {
  return <span className={`pill ${tone ?? ''}`}>{children}</span>
}

export function decisionTone(d: string | null): string | undefined {
  if (!d) return undefined
  const x = d.toLowerCase()
  if (['deny', 'denied', 'block', 'blocked'].includes(x)) return 'deny'
  if (['allow', 'allowed', 'approved'].includes(x)) return 'allow'
  return undefined
}

export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <h3>{title}</h3>
      {children}
    </div>
  )
}

export function ErrorNote({ error }: { error: string | null }) {
  if (!error) return null
  return (
    <div className="panel" style={{ padding: 14, borderColor: 'var(--fail)', color: 'var(--fail)', marginBottom: 16 }}>
      Can't reach the Agent Authority API ({error}). Start it with <code className="mono">authority demo</code> and reload.
    </div>
  )
}

/** Minimal JSON highlighter that makes dehydration refs visible. */
export function Json({ value }: { value: unknown }) {
  const text = JSON.stringify(value, null, 2)
  const html = text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/"(\$ref|\$meta|\$blob)": "([0-9a-f]+)"/g, '<span class="ref">"$1": "$2"</span>')
    .replace(/^(\s*)"([^"]+)":/gm, '$1<span class="k">"$2"</span>:')
  return <pre className="json" dangerouslySetInnerHTML={{ __html: html }} />
}

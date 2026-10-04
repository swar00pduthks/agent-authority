/**
 * <agent-authority> — Agent Authority as a framework-agnostic micro-frontend.
 *
 *   <script type="module" src="https://authority.internal/embed/agent-authority.js"></script>
 *   <agent-authority view="audit" session-id="…" namespace="appbuilder/my-project"
 *                    api-base="https://authority.internal" theme="dark"></agent-authority>
 *
 * Renders into a shadow root, so its styles never leak into the host and the host's never leak in.
 * Design tokens are CSS custom properties, so a host can restyle it from outside:
 *   agent-authority { --verified: #0b7; --font-body: Inter, sans-serif; }
 *
 * Properties: `getToken` (async () => bearer token, e.g. from MSAL for Entra).
 * Events (bubbling, composed): `agent-authority:navigate` {path}, `agent-authority:open-run` {runId}.
 */
import '@xyflow/react/dist/style.css'
import './theme.css'
import { StrictMode, useCallback } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { MemoryRouter, NavLink } from 'react-router-dom'
import { configure } from './api'
import { RunDrawerProvider } from './components/RunDrawer'
import { Seal } from './components/Seal'
import { AuthorityRoutes, NavigationReporter } from './Routes'

const VIEWS: Record<string, (el: AgentAuthorityElement) => string> = {
  overview: () => '/',
  agents: () => '/agents',
  sessions: (el) => (el.getAttribute('session-id') ? `/sessions/${el.getAttribute('session-id')}` : '/sessions'),
  audit: (el) => `/audit/${el.getAttribute('session-id') ?? ''}`,
  lineage: () => '/lineage',
  sentinel: () => '/sentinel',
}

function Embedded({ path, tabs, scope, onNavigate, onOpenRun }: {
  path: string; tabs: boolean; scope: string | null; onNavigate: (p: string) => void; onOpenRun: (id: string) => void
}) {
  const report = useCallback((p: string) => onNavigate(p), [onNavigate])
  return (
    <MemoryRouter initialEntries={[path]}>
      <RunDrawerProvider onOpen={onOpenRun}>
        <NavigationReporter onNavigate={report} />
        <div className="embedded">
          <div className="embed-bar">
            <span className="embed-brand"><Seal size={20} />Agent Authority</span>
            {tabs && (
              <nav className="embed-tabs" aria-label="Agent Authority">
                <NavLink to="/" end>Overview</NavLink>
                <NavLink to="/sessions">Sessions</NavLink>
                <NavLink to="/agents">Agents</NavLink>
                <NavLink to="/lineage">Lineage</NavLink>
                <NavLink to="/sentinel">Sentinel</NavLink>
              </nav>
            )}
            {scope && <span className="scope-chip" title="Everything here is scoped to this namespace">{scope}</span>}
          </div>
          <AuthorityRoutes stats={null} />
        </div>
      </RunDrawerProvider>
    </MemoryRouter>
  )
}

export class AgentAuthorityElement extends HTMLElement {
  static observedAttributes = ['view', 'session-id', 'namespace', 'api-base', 'theme', 'tabs']
  private root: Root | null = null
  private mount: HTMLDivElement | null = null
  private _getToken: (() => string | null | Promise<string | null>) | undefined

  set getToken(fn: (() => string | null | Promise<string | null>) | undefined) {
    this._getToken = fn
    configure({ getToken: fn })
  }

  get getToken() {
    return this._getToken
  }

  connectedCallback() {
    if (!this.shadowRoot) {
      const shadow = this.attachShadow({ mode: 'open' })
      const css = document.createElement('link')
      css.rel = 'stylesheet'
      css.href = new URL(/* @vite-ignore */ './agent-authority.css', import.meta.url).href
      this.mount = document.createElement('div')
      shadow.append(css, this.mount)
      this.root = createRoot(this.mount)
    }
    this.render()
  }

  disconnectedCallback() {
    this.root?.unmount()
    this.root = null
  }

  attributeChangedCallback() {
    if (this.root) this.render()
  }

  private emit(name: string, detail: unknown) {
    this.dispatchEvent(new CustomEvent(`agent-authority:${name}`, { detail, bubbles: true, composed: true }))
  }

  private render() {
    configure({
      apiBase: this.getAttribute('api-base') ?? '',
      namespace: this.getAttribute('namespace') ?? undefined,
      getToken: this._getToken,
    })
    const view = this.getAttribute('view') ?? 'overview'
    const path = (VIEWS[view] ?? VIEWS.overview)(this)
    const tabs = this.hasAttribute('tabs') ? this.getAttribute('tabs') !== 'false' : view !== 'audit'
    const key = `${view}|${this.getAttribute('session-id')}|${this.getAttribute('namespace')}`
    this.root!.render(
      <StrictMode>
        <Embedded key={key} path={path} tabs={tabs} scope={this.getAttribute('namespace')}
          onNavigate={(p) => this.emit('navigate', { path: p })} onOpenRun={(id) => this.emit('open-run', { runId: id })} />
      </StrictMode>,
    )
  }
}

if (!customElements.get('agent-authority')) customElements.define('agent-authority', AgentAuthorityElement)

declare global {
  interface HTMLElementTagNameMap { 'agent-authority': AgentAuthorityElement }
}

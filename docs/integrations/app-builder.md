# Integrating with the Governed AI App Builder

Agent Authority is the app builder's lineage store and audit-record UI. Two touch points:

1. **Emit lineage** from the app builder's agent loop with the Python SDK.
2. **Embed** the `<agent-authority>` web component in the app builder's React shell.

## 1. Emit lineage

| App-builder concept | Lineage |
|---|---|
| Project | Namespace, e.g. `appbuilder/trade-break-dashboard` |
| Build request | `session` run, `onBehalfOf` = the Entra user (UPN or object id) |
| Planner / coder loops | `agent` runs, `owner` = the platform team |
| Model call via the LiteLLM gateway | `llm` run with `model` and the gateway's `policy` decision (`allow` / `deny`, rule `model-allowlist`) |
| Data connector query | `tool` run with the connector table as an **input** dataset |
| File writes, commands, tests in the sandbox | read-only `tool` runs (rolled up into counts by the capture policy) |
| Export as pull request | side-effecting `tool` run, **output** dataset = the Git repo, policy `human-review-required` |

```python
from agent_authority import Authority, Dataset

aa = Authority("https://authority.internal", namespace=f"appbuilder/{project.slug}")

with aa.session(f"build: {project.title}", on_behalf_of=f"user:{entra_user.upn}"):
    with aa.agent("planner", owner="app-builder-platform", model=route.model):
        with aa.tool("query_schema", inputs=[Dataset(conn.uri, conn.table)]) as t:
            t.allow("approved-connector")
        ...
    with aa.agent("coder", owner="app-builder-platform", model=route.model):
        ...
        with aa.tool("open_pull_request", side_effecting=True,
                     outputs=[Dataset("git://github.com/acme-apps", project.slug)]) as t:
            t.allow("human-review-required")
```

Model-gateway decisions can also be emitted by the gateway itself (a LiteLLM callback posting OpenLineage
events with the same `ParentRunFacet`), which keeps the agent loop free of policy code.

## 2. Embed the audit record

```html
<script type="module" src="https://authority.internal/embed/agent-authority.js"></script>
```

```tsx
// React 19 passes properties (getToken) to custom elements directly
function AuditRecord({ project, sessionId }: { project: Project; sessionId: string }) {
  const ref = useRef<HTMLElement>(null)
  useEffect(() => {
    const onNav = (e: Event) => setSubPath((e as CustomEvent).detail.path)
    ref.current?.addEventListener('agent-authority:navigate', onNav)
    return () => ref.current?.removeEventListener('agent-authority:navigate', onNav)
  }, [])
  return (
    <agent-authority
      ref={ref}
      view="audit"                          // audit | sessions | agents | lineage | sentinel | overview
      session-id={sessionId}
      namespace={`appbuilder/${project.slug}`}
      api-base="https://authority.internal"
      theme="light"
      getToken={() => msal.acquireTokenSilent(req).then((r) => r.accessToken)}
    />
  )
}
```


| Attribute / property | Meaning |
|---|---|
| `view` | Which screen to show |
| `namespace` | Scope every view to one project |
| `session-id` | Session for `audit` (and deep-links `sessions`) |
| `api-base` | Agent Authority API origin (CORS is open by default; restrict in production) |
| `theme` | `dark` (default) or `light` |
| `tabs` | `true`/`false` to show the internal tab bar (default: on, except for `audit`) |
| `getToken` (property) | Async bearer-token provider, e.g. MSAL for Entra |
| `agent-authority:navigate` | Fired on internal navigation, `detail.path` |
| `agent-authority:open-run` | Fired when a run's chain of authority opens, `detail.runId` |
| `agent-authority:token-expired` | The API answered 401. Refresh the token (MSAL `acquireTokenSilent`), then call `detail.retry()`; the request retries once |

**Styling.** The component renders in a shadow root and has two token layers:

- **Brand** tokens the host may override from outside: `--color-accent`, `--color-ground`, `--color-surface`,
  `--color-sunken`, `--color-border*`, `--color-ink*`, `--font-*`, `--r-*`.
- **Meaning** tokens the host can't override: `--color-verified`, `--color-warn`, `--color-fail`,
  `--color-critical`, `--color-info` and `--color-kind-*`. They are re-declared inside the shadow root, so an
  auditor sees the same colour for Accountable / Review / Unaccountable in every product.

```css
agent-authority { --color-accent: #2f6fdb; --font-body: "Segoe UI", system-ui, sans-serif; --font-display: inherit; }
```

The single source is `ui/src/tokens.css`.

**Try it:** run `authority demo --no-browser`, then
`python -m http.server 5500 -d examples/microfrontend` and open http://localhost:5500. The page is a stand-in
app-builder shell in plain HTML, embedding the audit record, sessions and findings per project.

## 3. Close the chain with the human approver

Write the session marker into every pull request the app builder opens:

```python
with aa.session(...) as session:
    ...
pr_body = f"{summary}\n\n{session.pr_marker}"   # "agent-authority-session: <session id>"
```

Point the repository's webhooks at Agent Authority:

| Provider | URL | Events | Secret |
|---|---|---|---|
| GitHub | `/api/authority/webhooks/github` | Pull requests, Pull request reviews | `AUTHORITY_GITHUB_WEBHOOK_SECRET` (HMAC) |
| GitLab | `/api/authority/webhooks/gitlab` | Merge request events | `AUTHORITY_GITLAB_WEBHOOK_TOKEN` |

Reviews and merges become `approval` runs under the session, and the chain of authority for the PR ends at the
named reviewer. If the app builder already receives these webhooks, it can `POST /api/authority/approvals`
instead.

## Security notes
- Set `AUTHORITY_OIDC_ISSUER` (e.g. `https://login.microsoftonline.com/<tenant>/v2.0`) and
  `AUTHORITY_OIDC_AUDIENCE` (the app registration's Application ID URI). Every API call then needs an Entra
  token; optional app roles: `AUTHORITY_OIDC_READ_ROLE`, `AUTHORITY_OIDC_INGEST_ROLE`. Emitters that can't get
  tokens (Spark) use `AUTHORITY_API_KEY` on ingest only.
- Set `AUTHORITY_ANCHOR_DIR` (a WORM-mounted directory) or `AUTHORITY_ANCHOR_AZURE_CONNECTION_STRING` +
  `AUTHORITY_ANCHOR_AZURE_CONTAINER` (a container with a time-based immutability policy) on **both** the API
  and the collector, so audit records verify as intact rather than unanchored.
- Run Agent Authority in the same tenant as the app builder; prompts and completions are stored in
  `payload_blob` and fall under the same data-residency rules.

import { Activity, Bot, Moon, Network, ShieldAlert, Sun, Workflow } from 'lucide-react'
import { useEffect, useState } from 'react'
import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import { bytes, compact, get, usePoll, type Finding, type Stats } from './api'
import { RunDrawerProvider } from './components/RunDrawer'
import { AgentsPage } from './pages/Agents'
import { LineagePage } from './pages/Lineage'
import { OverviewPage } from './pages/Overview'
import { SentinelPage } from './pages/Sentinel'
import { SessionsPage } from './pages/Sessions'

function Logo() {
  return (
    <svg width="26" height="26" viewBox="0 0 32 32" aria-hidden>
      <circle cx="9" cy="10" r="3.4" fill="var(--k-agent)" />
      <circle cx="23" cy="10" r="3.4" fill="var(--k-dataset)" />
      <circle cx="16" cy="23" r="3.8" fill="var(--ink)" />
      <path d="M9 10 L16 23 L23 10" stroke="var(--ink)" strokeWidth="1.6" fill="none" strokeLinecap="round" />
    </svg>
  )
}

function useTheme() {
  const [theme, setTheme] = useState<'dark' | 'light'>(() => {
    try { return (localStorage.getItem('agenthub-theme') as 'dark' | 'light') || 'dark' } catch { return 'dark' }
  })
  useEffect(() => {
    document.documentElement.dataset.theme = theme
    try { localStorage.setItem('agenthub-theme', theme) } catch { /* private mode */ }
  }, [theme])
  return [theme, () => setTheme(theme === 'dark' ? 'light' : 'dark')] as const
}

function Shell() {
  const [theme, toggle] = useTheme()
  const stats = usePoll(() => get<Stats>('/api/hub/stats'), [], 2000)
  const findings = usePoll(() => get<Finding[]>('/api/hub/findings', { severity: 'high' }), [], 8000)
  const hot = findings.data?.length ?? 0
  const s = stats.data

  return (
    <div className="shell">
      <nav className="rail" aria-label="Main">
        <div className="brand"><Logo /><span className="brand-word">Agent<span>Hub</span></span></div>
        <NavLink to="/" end className="nav-link"><Activity size={16} />Overview</NavLink>
        <NavLink to="/agents" className="nav-link"><Bot size={16} />Agents</NavLink>
        <NavLink to="/sessions" className="nav-link"><Workflow size={16} />Sessions</NavLink>
        <NavLink to="/lineage" className="nav-link"><Network size={16} />Lineage</NavLink>
        <NavLink to="/sentinel" className="nav-link"><ShieldAlert size={16} />Sentinel
          {hot > 0 && <span className="count hot">{hot}</span>}
        </NavLink>
        <div className="rail-foot">
          <button className="btn ghost" onClick={toggle} aria-label="Toggle colour theme">
            {theme === 'dark' ? <Sun size={15} /> : <Moon size={15} />} {theme === 'dark' ? 'Light' : 'Dark'} theme
          </button>
          <a className="faint mono" href="/docs" style={{ padding: '0 12px' }}>API docs →</a>
        </div>
      </nav>
      <div className="main">
        <header className="topbar">
          <span className="live"><i />{stats.error ? 'offline' : 'ingesting'}</span>
          {s && <>
            <span className="topstat"><b>{compact(s.events_in)}</b> events</span>
            <span className="topstat"><b>{bytes(s.bytes_in)}</b> received → <b>{bytes(s.bytes_stored)}</b> stored</span>
            <span className="topstat"><b>{compact(s.runs)}</b> runs · <b>{s.nodes}</b> nodes · <b>{s.edges}</b> edges</span>
          </>}
        </header>
        <Routes>
          <Route path="/" element={<OverviewPage stats={s} />} />
          <Route path="/agents" element={<AgentsPage />} />
          <Route path="/sessions" element={<SessionsPage />} />
          <Route path="/sessions/:id" element={<SessionsPage />} />
          <Route path="/lineage" element={<LineagePage />} />
          <Route path="/sentinel" element={<SentinelPage />} />
        </Routes>
      </div>
    </div>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <RunDrawerProvider>
        <Shell />
      </RunDrawerProvider>
    </BrowserRouter>
  )
}

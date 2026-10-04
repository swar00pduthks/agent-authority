import { useEffect } from 'react'
import { Route, Routes, useLocation } from 'react-router-dom'
import type { Stats } from './api'
import { AgentsPage } from './pages/Agents'
import { AuditPage } from './pages/Audit'
import { LineagePage } from './pages/Lineage'
import { OverviewPage } from './pages/Overview'
import { SentinelPage } from './pages/Sentinel'
import { SessionsPage } from './pages/Sessions'

export function AuthorityRoutes({ stats }: { stats: Stats | null }) {
  return (
    <Routes>
      <Route path="/" element={<OverviewPage stats={stats} />} />
      <Route path="/agents" element={<AgentsPage />} />
      <Route path="/sessions" element={<SessionsPage />} />
      <Route path="/sessions/:id" element={<SessionsPage />} />
      <Route path="/audit/:id" element={<AuditPage />} />
      <Route path="/lineage" element={<LineagePage />} />
      <Route path="/sentinel" element={<SentinelPage />} />
    </Routes>
  )
}

/** Reports in-app navigation to a listener (the web component forwards it to the host shell). */
export function NavigationReporter({ onNavigate }: { onNavigate?: (path: string) => void }) {
  const loc = useLocation()
  useEffect(() => { onNavigate?.(loc.pathname) }, [loc.pathname, onNavigate])
  return null
}

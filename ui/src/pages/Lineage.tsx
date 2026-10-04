import dagre from '@dagrejs/dagre'
import {
  Background, Controls, Handle, MarkerType, Position, ReactFlow, ReactFlowProvider,
  type Edge, type Node, type NodeProps, useReactFlow,
} from '@xyflow/react'
import { Crosshair, Radiation, X } from 'lucide-react'
import { Fragment, useEffect, useMemo, useState } from 'react'
import { get, usePoll, type BlastRadius, type GEdge, type GNode, type Kind } from '../api'
import { ErrorNote, KIND_ICON, KIND_LABEL, KindTag } from '../components/bits'
import './lineage.css'

type NodeData = { g: GNode; dim: boolean; hot: boolean; origin: boolean }
const W = 176
const H = 46

function LNode({ data }: NodeProps<Node<NodeData>>) {
  const { g, dim, hot, origin } = data
  const Icon = KIND_ICON[g.kind as Kind] ?? KIND_ICON.job
  const tags = (g.attrs.tags as string[] | undefined) ?? []
  return (
    <div className={`ln k-${g.kind} ${dim ? 'dim' : ''} ${hot ? 'hot' : ''} ${origin ? 'origin' : ''}`}>
      <Handle type="target" position={Position.Left} />
      <Icon size={14} className="ln-icon" />
      <div className="ln-text">
        <div className="ln-name" title={g.name}>{g.name}</div>
        <div className="ln-ns">{g.kind === 'identity' ? 'person' : g.namespace}</div>
      </div>
      {tags.length > 0 && <span className="ln-tag">{tags[0]}</span>}
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
const nodeTypes = { l: LNode }

function layout(nodes: GNode[], edges: GEdge[]) {
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: 'LR', nodesep: 12, ranksep: 48, marginx: 20, marginy: 20 })
  g.setDefaultEdgeLabel(() => ({}))
  nodes.forEach((n) => g.setNode(n.id, { width: W, height: H }))
  edges.forEach((e) => g.setEdge(e.src, e.dst))
  dagre.layout(g)
  return Object.fromEntries(nodes.map((n) => {
    const p = g.node(n.id)
    return [n.id, { x: p.x - W / 2, y: p.y - H / 2 }]
  }))
}

const EDGE_STYLE: Record<string, React.CSSProperties> = {
  INPUT: { stroke: 'var(--k-dataset)' },
  OUTPUT: { stroke: 'var(--k-job)' },
  INVOKES: { stroke: 'var(--k-agent)', strokeDasharray: '5 4' },
  ON_BEHALF_OF: { stroke: 'var(--ink-3)', strokeDasharray: '1 4', strokeLinecap: 'round' },
}

function Graph() {
  const [ns, setNs] = useState<string>('')
  const [q, setQ] = useState('')
  const [selected, setSelected] = useState<GNode | null>(null)
  const [blast, setBlast] = useState<BlastRadius | null>(null)
  const [people, setPeople] = useState(false)
  const nsList = usePoll(() => get<{ namespaces: { name: string }[] }>('/api/v1/namespaces'), [])
  const raw = usePoll(() => get<{ nodes: GNode[]; edges: GEdge[] }>('/api/hub/graph', { namespace: ns || undefined }), [ns], 10000)
  const data = useMemo(() => {
    if (!raw.data || people) return raw.data
    const keep = new Set(raw.data.nodes.filter((n) => n.kind !== 'identity').map((n) => n.id))
    return { nodes: raw.data.nodes.filter((n) => keep.has(n.id)), edges: raw.data.edges.filter((e) => keep.has(e.src) && keep.has(e.dst)) }
  }, [raw.data, people])
  const graph = { data, error: raw.error }
  const flow = useReactFlow()

  const pos = useMemo(() => graph.data ? layout(graph.data.nodes, graph.data.edges) : {}, [graph.data])
  const hotIds = useMemo(() => new Set(blast?.affected.map((a) => a.id) ?? []), [blast])

  const nodes: Node<NodeData>[] = useMemo(() => (graph.data?.nodes ?? []).map((g) => {
    const match = q && g.id.toLowerCase().includes(q.toLowerCase())
    return {
      id: g.id, type: 'l', position: pos[g.id] ?? { x: 0, y: 0 },
      data: {
        g,
        origin: blast?.node.id === g.id,
        hot: blast ? hotIds.has(g.id) : !!match,
        dim: blast ? !hotIds.has(g.id) && blast.node.id !== g.id : !!q && !match,
      },
    }
  }), [graph.data, pos, blast, hotIds, q])

  const edges: Edge[] = useMemo(() => (graph.data?.edges ?? []).map((e) => {
    const inBlast = blast && (hotIds.has(e.dst) && (hotIds.has(e.src) || e.src === blast.node.id))
    return {
      id: `${e.src}|${e.dst}|${e.type}`, source: e.src, target: e.dst,
      style: { ...EDGE_STYLE[e.type], strokeWidth: Math.min(1 + Math.log10(e.run_count + 1), 3.2), opacity: blast && !inBlast ? 0.12 : 0.8,
        ...(inBlast ? { stroke: 'var(--fail)' } : {}) },
      markerEnd: { type: MarkerType.ArrowClosed, width: 14, height: 14, color: inBlast ? 'var(--fail)' : 'var(--ink-3)' },
      animated: !!inBlast,
    }
  }), [graph.data, blast, hotIds])

  useEffect(() => { const t = setTimeout(() => flow.fitView({ padding: 0.12, duration: 300 }), 60); return () => clearTimeout(t) }, [ns, graph.data?.nodes.length, flow])

  const runBlast = async (id: string) => setBlast(await get<BlastRadius>('/api/hub/blast-radius', { node: id }))

  return (
    <div className="page lineage-page">
      <ErrorNote error={graph.error} />
      <div className="page-head">
        <div>
          <h1 className="page-title">Lineage</h1>
          <p className="page-sub">Pipelines, agents, tools and the data between them, in one graph. Thicker lines mean more runs. Pick a node, then check its blast radius.</p>
        </div>
        <input className="input" placeholder="Find a node" value={q} onChange={(e) => setQ(e.target.value)} />
      </div>
      <div className="ns-chips" role="group" aria-label="Namespace">
        <button className="btn" aria-pressed={!ns} onClick={() => { setNs(''); setBlast(null) }}>All</button>
        {nsList.data?.namespaces.map((n) => (
          <button key={n.name} className="btn" aria-pressed={ns === n.name} onClick={() => { setNs(n.name); setBlast(null) }}>{n.name}</button>
        ))}
        <button className="btn" aria-pressed={people} onClick={() => setPeople(!people)}>Show people</button>
        <span className="legend">
          <span><i style={{ background: 'var(--k-dataset)' }} />reads</span>
          <span><i style={{ background: 'var(--k-job)' }} />writes</span>
          <span><i className="dash" style={{ borderColor: 'var(--k-agent)' }} />invokes</span>
          <span><i className="dot" />acts for</span>
        </span>
      </div>
      <div className="lineage-wrap panel">
        <ReactFlow nodes={nodes} edges={edges} nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.08 }} minZoom={0.15} proOptions={{ hideAttribution: true }}
          onNodeClick={(_, n) => { setSelected((n.data as NodeData).g); setBlast(null) }} onPaneClick={() => setSelected(null)}
          nodesConnectable={false} colorMode="system">
          <Background gap={24} size={1} color="var(--line)" />
          <Controls showInteractive={false} />
        </ReactFlow>

        {selected && (
          <aside className="node-panel">
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
              <KindTag kind={selected.kind} label={KIND_LABEL[selected.kind]} />
              <button className="btn ghost" aria-label="Close" onClick={() => { setSelected(null); setBlast(null) }}><X size={15} /></button>
            </div>
            <div className="np-name">{selected.name}</div>
            <div className="mono faint" style={{ overflowWrap: 'anywhere' }}>{selected.id}</div>
            {Object.keys(selected.attrs).length > 0 && (
              <dl className="kv" style={{ gridTemplateColumns: '90px 1fr', fontSize: 12.5 }}>
                {Object.entries(selected.attrs).filter(([k]) => k !== 'job_type_ref').map(([k, v]) => (
                  <Fragment key={k}><dt>{k.replace(/_/g, ' ')}</dt><dd>{Array.isArray(v) ? v.join(', ') : String(v)}</dd></Fragment>
                ))}
              </dl>
            )}
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              <button className="btn" onClick={() => runBlast(selected.id)} aria-pressed={!!blast}><Radiation size={14} />Blast radius</button>
              <button className="btn ghost" onClick={() => flow.fitView({ nodes: [{ id: selected.id }], duration: 300, maxZoom: 1.4 })}><Crosshair size={14} />Center</button>
            </div>
            {blast && (
              <div className="blast">
                <div className="blast-num"><b>{blast.affected.length}</b> things downstream</div>
                <div className="blast-kinds">
                  {Object.entries(blast.by_kind).map(([k, n]) => <span key={k}><KindTag kind={k as Kind} label={`${n} ${KIND_LABEL[k as Kind]?.toLowerCase() ?? k}`} /></span>)}
                </div>
                {blast.sensitive_datasets.length > 0 && (
                  <div className="blast-sens">
                    <div className="label" style={{ color: 'var(--fail)' }}>Sensitive data exposed</div>
                    {blast.sensitive_datasets.map((d) => <div key={d} className="mono">{d.replace(/^dataset:/, '')}</div>)}
                  </div>
                )}
                <p className="faint" style={{ margin: 0, fontSize: 12.5 }}>
                  If this {selected.kind === 'dataset' ? 'dataset is corrupted' : 'component is compromised'}, everything highlighted could be affected.
                </p>
              </div>
            )}
          </aside>
        )}
      </div>
    </div>
  )
}

export function LineagePage() {
  return <ReactFlowProvider><Graph /></ReactFlowProvider>
}

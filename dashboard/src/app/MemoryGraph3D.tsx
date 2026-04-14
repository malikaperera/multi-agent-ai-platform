"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useRef, useState } from "react";

const ForceGraph3D = dynamic(() => import("react-force-graph-3d"), { ssr: false }) as any;

type NodeLike = {
  id: string;
  label: string;
  type: string;
  agent?: string | null;
  summary?: string;
  evidence: "verified" | "inferred" | "unknown";
  timestamp?: string;
};

type EdgeLike = {
  id: string;
  source: string;
  target: string;
  label: string;
  type: string;
  evidence: "verified" | "inferred" | "unknown";
  timestamp?: string;
};

type GraphLike = {
  nodes: NodeLike[];
  edges: EdgeLike[];
};

const AGENT_COLORS: Record<string, string> = {
  roderick: "#2dd4bf",
  merlin: "#818cf8",
  forge: "#fbbf24",
  venture: "#4ade80",
  atlas: "#60a5fa",
  sentinel: "#f87171",
  zuko: "#a78bfa",
  operator: "#fb923c",
};

const TYPE_COLORS: Record<string, string> = {
  approval: "#fbbf24",
  validation: "#f87171",
  artifact: "#60a5fa",
  github_run: "#2dd4bf",
  improvement: "#fbbf24",
  policy: "#94a3b8",
  report: "#34d399",
  event: "#2dd4bf",
  message: "#c084fc",
  task: "#5dff9f",
  memory_note: "#14b8a6",
  opportunity: "#4ade80",
  lesson: "#60a5fa",
  skill: "#818cf8",
};

function recent(timestamp?: string): boolean {
  if (!timestamp) return false;
  const t = new Date(timestamp).getTime();
  return Number.isFinite(t) && Date.now() - t < 60 * 60 * 1000;
}

function nodeColor(node: NodeLike): string {
  if (node.agent && AGENT_COLORS[node.agent]) return AGENT_COLORS[node.agent];
  if (TYPE_COLORS[node.type]) return TYPE_COLORS[node.type];
  if (node.evidence === "unknown") return "#64748b";
  return "#5dff9f";
}

export default function MemoryGraph3D({
  graph,
  selectedId,
  onSelect,
}: {
  graph: GraphLike;
  selectedId?: string | null;
  onSelect: (node: NodeLike) => void;
}) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const graphRef = useRef<any>(null);
  const [size, setSize] = useState({ width: 860, height: 650 });

  useEffect(() => {
    if (!hostRef.current) return;
    const el = hostRef.current;
    const update = () => {
      const rect = el.getBoundingClientRect();
      setSize({
        width: Math.max(320, Math.floor(rect.width)),
        height: Math.max(420, Math.floor(rect.height)),
      });
    };
    update();
    const observer = new ResizeObserver(update);
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  // Structural key: only changes when nodes/edges are actually added or removed.
  // This prevents the physics simulation from restarting on every 30s data refresh.
  const structureKey = useMemo(() => {
    const nk = graph.nodes.map(n => n.id).sort().join("|");
    const ek = graph.edges.map(e => e.id).sort().join("|");
    return `${nk}::${ek}`;
  }, [graph.nodes, graph.edges]);

  // Keep a ref so the data memo below always reads the latest graph when structure changes.
  const latestGraph = useRef(graph);
  latestGraph.current = graph;

  const data = useMemo(() => {
    const g = latestGraph.current;
    return {
      nodes: g.nodes.map(node => ({
        ...node,
        raw: node,
        color: nodeColor(node),
        val: node.type === "agent" || node.type === "user" ? 7 : recent(node.timestamp) ? 5 : 3.8,
      })),
      links: g.edges.map(edge => ({
        ...edge,
        color: nodeColor(g.nodes.find(node => node.id === edge.source) || { id: "", label: "", type: "task", evidence: "verified" }),
        particleCount: recent(edge.timestamp) ? 2 : 0,
      })),
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [structureKey]);

  useEffect(() => {
    if (!graphRef.current || !data.nodes.length) return;
    const timer = window.setTimeout(() => {
      try {
        graphRef.current.zoomToFit(600, 50);
      } catch {
        // no-op
      }
    }, 350);
    return () => window.clearTimeout(timer);
  }, [data.nodes.length]);

  useEffect(() => {
    if (!selectedId || !graphRef.current) return;
    const selected = data.nodes.find((node: any) => node.id === selectedId) as any;
    if (!selected) return;
    const distance = 160;
    const distRatio = 1 + distance / Math.hypot(selected.x || 1, selected.y || 1, selected.z || 1);
    try {
      graphRef.current.cameraPosition(
        { x: (selected.x || 0) * distRatio, y: (selected.y || 0) * distRatio, z: (selected.z || 0) * distRatio },
        selected,
        800,
      );
    } catch {
      // no-op
    }
  }, [data.nodes, selectedId]);

  return (
    <div ref={hostRef} className="memory-3d-shell">
      <ForceGraph3D
        ref={graphRef}
        graphData={data}
        width={size.width}
        height={size.height}
        backgroundColor="#07110c"
        nodeLabel={(node: any) =>
          `<div><strong>${node.label}</strong><br/>${node.type}<br/>${(node.summary || "No summary").slice(0, 180)}</div>`
        }
        nodeColor={(node: any) => node.color}
        nodeVal={(node: any) => node.val}
        linkColor={(link: any) => link.color}
        linkWidth={(link: any) => (link.evidence === "verified" ? 1.5 : 0.7)}
        linkOpacity={0.34}
        linkDirectionalParticles={(link: any) => link.particleCount}
        linkDirectionalParticleWidth={2.2}
        linkDirectionalParticleColor={(link: any) => link.color}
        linkDirectionalParticleSpeed={(link: any) => (link.particleCount ? 0.012 : 0)}
        onNodeClick={(node: any) => onSelect(node.raw)}
        cooldownTicks={180}
        warmupTicks={20}
        d3AlphaDecay={0.08}
        d3VelocityDecay={0.4}
      />
    </div>
  );
}

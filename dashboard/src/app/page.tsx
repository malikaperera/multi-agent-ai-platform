"use client";

import { Fragment, useEffect, useMemo, useCallback, useReducer, useRef, useState, type Dispatch, type PointerEvent, type ReactNode, type SetStateAction, type WheelEvent } from "react";
import MemoryGraph3D from "./MemoryGraph3D";

// ── Types ─────────────────────────────────────────────────────────────────────

type Agent = {
  name: string; display_name: string; purpose: string;
  status: string; autonomy_level: string; model_used: string;
  current_model?: string;
  task_types_accepted: string[]; last_heartbeat?: string;
  last_message?: string; last_success?: string; last_error?: string;
};

type AgentDetail = Agent & {
  active_task?: { id: number; task_type: string; description: string; status: string; updated_at: string } | null;
  recent_tasks: TaskRow[];
  task_counts: Record<string, number>;
  intentions: TaskRow[];
  recent_events: AgentEvent[];
  recent_messages: AgentMessage[];
  recent_reports: AgentReport[];
  learning_note?: string;
  resource_snapshot?: AgentResourceSnapshot;
  stage?: string;
  state_evidence?: string;
  planner_model?: string;
  coder_model?: string;
  research_model?: string;
  diagnostic_model?: string;
  deep_model?: string;
  routine_model?: string;
};

type AgentEvent = {
  id: number; event_type: string; agent: string; payload?: Record<string, unknown>; created_at?: string;
};

type AgentMessage = {
  id: number; from_agent: string; to_agent: string; message: string; priority: string; read: number; created_at?: string;
};

type AgentReport = {
  file: string; path: string; summary: string; updated_at?: string;
};

type AgentResourceSnapshot = {
  scope: string; process_cpu_percent: number; process_memory_rss_mb: number; process_threads: number;
  host_cpu_percent: number; host_memory_percent: number; disk_percent: number; evidence: string;
};

type TaskRow = {
  id: number; to_agent: string; from_agent: string; task_type: string;
  description: string; status: string; priority: string; urgency: string;
  domain: string; result?: unknown; created_at?: string; updated_at?: string;
};

type Approval = {
  id: number; task_id?: number; request_type: string;
  description: string; status: string; created_at: string;
  payload?: Record<string, unknown>;
  task?: TaskRow | null;
  improvement?: ImprovementItem | null;
  evidence_events?: AgentEvent[];
  decision_packet?: { why?: string; if_declined?: string; verified: string[]; risks: string[]; checks: string[]; unknowns: string[] };
};

type SystemStats = {
  tasks_by_status: Record<string, number>; tasks_by_agent: Record<string, number>;
  total_events: number; pending_approvals: number;
  agents_busy: number; agents_idle: number;
};

type SystemMetrics = {
  process: { cpu_percent: number; memory_rss_mb: number; threads: number };
  host: { cpu_percent: number; memory_used_mb: number; memory_total_mb: number; memory_percent: number; disk_used_gb: number; disk_total_gb: number; disk_percent: number };
  gpu?: { status: string; evidence?: string; error?: string; gpus: Array<Record<string, number | string | null>>; ollama_residency?: { status: string; residency_percent: number; evidence: string; models: Array<Record<string, number | string | null>> } };
  ollama?: { status: string; base_url: string; error?: string; loaded_models: Array<Record<string, number | string | null>> };
  langfuse?: { status: string; host: string; public_key_configured: boolean; secret_key_configured: boolean; ui: string; evidence: string };
  db_size_mb: number;
  generated_at?: string;
};

type MetricPoint = {
  ts: string; cpu: number; memory: number; disk: number; gpu: number | null; vram: number | null;
};

type LogEntry = {
  kind: "event" | "task" | "message";
  id: string; agent: string; type: string;
  message: string; detail: Record<string, unknown>; ts: string;
};

type OppData = { recent: Array<Record<string, unknown>>; log: string; reports: Array<Record<string, unknown>> };

type OperatorInitiative = {
  title: string; status: string; priority: string;
  next_actions: string[]; blockers: string[];
};

type OperatorReport = {
  task_id?: number; task_type?: string; initiative?: string; status?: string;
  task_summary?: string; approval_required?: boolean; executed_at?: string;
};

type OperatorData = {
  initiatives: OperatorInitiative[];
  recent_reports: OperatorReport[];
  business_ops_summary: string;
  pending_approvals: Approval[];
  blocked_tasks: TaskRow[];
  chat_messages: AgentMessage[];
  loaded: boolean;
};

type AtlasLearning = {
  status: Record<string, unknown>;
  today: Record<string, unknown>;
  entries: Array<Record<string, unknown>>;
  coaching_recommendations?: Array<Record<string, string>>;
  linkedin_learning_note: string;
};

type ImprovementItem = {
  id: number; title: string; origin_agent: string; origin_signal: string;
  status: string; priority: string; risk_level: string; forge_recommended: boolean;
  description?: string; created_at?: string; updated_at?: string;
};

type PipelineData = {
  by_status: Record<string, ImprovementItem[]>;
  active_count: number; total_count: number; generated_at: string;
};

type ForgeArtifact = {
  id: number; task_id: number; artifact_type: string; artifact_root: string;
  relative_path: string; path: string; summary: string;
  approval_state: string; validation_state: string;
  metadata?: Record<string, unknown>; created_at?: string;
};

type ForgeWorkflow = {
  git_enabled: boolean; status: string; repo_root: string; artifact_root: string;
  source_of_truth: string; promotion_flow: string[]; sentinel_gate: string[];
  local_git_enabled?: boolean; github_connected?: boolean; workflow_configured?: boolean;
  github_repository?: string; github_url?: string; github_branch?: string; github_workflow?: string;
  phone_approval_flow?: string[];
  truthfulness: string;
};

type SentinelStatus = {
  agent?: Agent | null;
  active_task?: TaskRow | null;
  queued_tasks: TaskRow[];
  recent_events: AgentEvent[];
  merlin_research_messages: AgentMessage[];
  recent_reports: AgentReport[];
  latest_summary: string;
  checks: string[];
  evidence: string;
  generated_at: string;
};

type BehaviorPolicy = {
  id: number; agent: string; policy_key: string; policy_value: string;
  description: string; status: string; origin: string; changed_by: string;
  requires_approval: boolean; approved_by?: string; applied_at?: string;
  expires_at?: string; audit_notes?: string; created_at: string; updated_at: string;
};

type GraphNode = {
  id: string; label: string; type: string; agent?: string | null; summary?: string;
  timestamp?: string; evidence: "verified" | "inferred" | "unknown"; source?: string;
  status?: string; meta?: Record<string, unknown>;
};

type GraphEdge = {
  id: string; source: string; target: string; label: string; type: string;
  timestamp?: string; evidence: "verified" | "inferred" | "unknown"; source_ref?: string;
};

type MemoryGraphData = {
  nodes: GraphNode[];
  edges: GraphEdge[];
  stats: {
    node_count: number; edge_count: number; verified_count: number; inferred_count: number;
    unknown_count: number; type_counts: Record<string, number>; window_hours: number;
    warnings?: string[]; generated_at: string;
  };
};

type NodeTraffic = {
  inbound: GraphEdge[];
  outbound: GraphEdge[];
  related: GraphNode[];
};

type OperatorRecommendation = {
  id: string; severity: "info" | "warn" | "danger"; title: string; summary: string;
  why: string; action_label?: string; action?: Record<string, unknown>; evidence: string;
};

type TaskFocus = {
  label: string;
  agent?: string;
  status?: string;
  stuckOnly?: boolean;
};

type TaskDetail = {
  task: TaskRow;
  stage: string;
  output: {
    stage: string;
    forge_mode?: string | null;
    files_created?: string[];
    patches_applied?: Array<Record<string, unknown>>;
    sentinel_task_id?: number | null;
    artifact_root?: string | null;
    artifact_files_dir?: string | null;
    validation_state?: string | null;
    deployment?: Record<string, any> | null;
  };
  improvement?: ImprovementItem | Record<string, unknown> | null;
  related_sentinel_task?: TaskRow | null;
  events: AgentEvent[];
  approvals: Approval[];
  artifacts: ForgeArtifact[];
  messages: AgentMessage[];
  report?: { path: string; content: Record<string, unknown>; updated_at?: string } | null;
  generated_at?: string;
  evidence?: string;
};

type Tab = "Overview" | "Agents" | "Tasks" | "Logs" | "Approvals" | "Atlas" | "Opportunities" | "Operator" | "Pipeline" | "Forge" | "Behaviors" | "Memory" | "Jobs";

type JobApplication = {
  id: number; job_id: string; title: string; company: string; location: string;
  url: string; salary: string; status: string; source: string; apply_type: string;
  cover_letter?: string; created_at?: string; updated_at?: string;
};

type JobsData = {
  applications: JobApplication[];
  stats: Record<string, number>;
  total: number;
};

const DISPLAY_TZ = Intl.DateTimeFormat().resolvedOptions().timeZone;

function getApiBase(): string {
  if (typeof window !== "undefined") {
    const { protocol, hostname } = window.location;
    const configured = process.env.NEXT_PUBLIC_API_URL;
    if (configured && !configured.includes("localhost") && !configured.includes("127.0.0.1")) {
      return configured;
    }
    return `${protocol}//${hostname}:8000`;
  }
  if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL;
  return "http://localhost:8000";
}

function formatDateTime(value?: string | Date | null, opts?: Intl.DateTimeFormatOptions): string {
  if (!value) return "unknown";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "unknown";
  return new Intl.DateTimeFormat("en-AU", {
    timeZone: DISPLAY_TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    ...opts,
  }).format(date);
}

function formatTime(value?: string | Date | null, opts?: Intl.DateTimeFormatOptions): string {
  return formatDateTime(value, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    ...opts,
  });
}

function formatDateOnly(value?: string | Date | null): string {
  return formatDateTime(value, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: undefined,
    minute: undefined,
    second: undefined,
  });
}

const AGENT_COLORS: Record<string, string> = {
  roderick: "#2dd4bf", merlin: "#818cf8", forge: "#fbbf24",
  venture: "#4ade80", atlas: "#60a5fa", sentinel: "#f87171", zuko: "#a78bfa",
  operator: "#fb923c",
};

function agentAccent(agent?: string | null): string | null {
  if (!agent) return null;
  return AGENT_COLORS[agent.toLowerCase()] || null;
}

function metricGpuValue(metrics: SystemMetrics): number | null {
  const gpu = metrics.gpu?.gpus?.[0];
  if (typeof gpu?.utilization_percent === "number") return gpu.utilization_percent;
  if (typeof metrics.gpu?.ollama_residency?.residency_percent === "number") {
    return metrics.gpu.ollama_residency.residency_percent;
  }
  return null;
}

function metricVramValue(metrics: SystemMetrics): number | null {
  const gpu = metrics.gpu?.gpus?.[0];
  if (typeof gpu?.memory_percent === "number") return gpu.memory_percent;
  return null;
}

function summarizeObject(value: Record<string, unknown> | null | undefined, limit = 6): string[] {
  if (!value) return [];
  const preferred = ["summary", "task_summary", "description", "initiative", "status", "approval_reason", "error", "why", "if_declined"];
  const lines: string[] = [];
  for (const key of preferred) {
    const item = value[key];
    if (item === undefined || item === null || item === "") continue;
    lines.push(`${key}: ${String(item).slice(0, 220)}`);
  }
  if (lines.length >= limit) return lines.slice(0, limit);
  for (const [key, item] of Object.entries(value)) {
    if (preferred.includes(key) || item === undefined || item === null || item === "") continue;
    if (Array.isArray(item)) {
      if (item.length) lines.push(`${key}: ${item.slice(0, 4).map(v => String(v)).join(" · ").slice(0, 220)}`);
    } else if (typeof item === "object") {
      continue;
    } else {
      lines.push(`${key}: ${String(item).slice(0, 220)}`);
    }
    if (lines.length >= limit) break;
  }
  return lines;
}

// ── API ───────────────────────────────────────────────────────────────────────

async function api<T>(path: string): Promise<T> {
  const res = await fetch(`${getApiBase()}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

// ── Root ──────────────────────────────────────────────────────────────────────

type State = {
  agents: Agent[]; tasks: TaskRow[]; approvals: Approval[];
  stats: SystemStats | null; metrics: SystemMetrics | null;
  logs: LogEntry[]; presence: string;
  skills: Record<string, string>; lesson: Record<string, unknown>;
  atlasLearning: AtlasLearning | null;
  opps: OppData; pipeline: PipelineData | null; artifacts: ForgeArtifact[];
  forgeWorkflow: ForgeWorkflow | null;
  sentinelStatus: SentinelStatus | null;
  behaviors: BehaviorPolicy[];
  memoryGraph: MemoryGraphData | null;
  recommendations: OperatorRecommendation[];
  operatorData: OperatorData;
  jobs: JobsData | null;
  error: string; lastRefresh: Date | null;
};

const init: State = {
  agents: [], tasks: [], approvals: [], stats: null, metrics: null,
  logs: [], presence: "at_pc", skills: {}, lesson: {}, atlasLearning: null,
  opps: { recent: [], log: "", reports: [] }, pipeline: null, artifacts: [],
  forgeWorkflow: null, sentinelStatus: null, behaviors: [],
  memoryGraph: null, recommendations: [],
  operatorData: { initiatives: [], recent_reports: [], business_ops_summary: "", pending_approvals: [], blocked_tasks: [], chat_messages: [], loaded: false },
  jobs: null,
  error: "", lastRefresh: null,
};

export default function App() {
  const [state, setState] = useReducer((s: State, p: Partial<State>) => ({ ...s, ...p }), init);
  const [tab, setTab] = useState<Tab>("Overview");
  const [tabHistory, setTabHistory] = useState<Tab[]>([]);
  const [messageText, setMessageText] = useState("");
  const [messageReply, setMessageReply] = useState("");
  const [messageSending, setMessageSending] = useState(false);
  const [metricHistory, setMetricHistory] = useState<MetricPoint[]>([]);
  const [taskFocus, setTaskFocus] = useState<TaskFocus | null>(null);

  // Fast path (5s): operational state — agents, tasks, approvals, stats, metrics
  const loadFast = useCallback(async () => {
    try {
      const [agents, tasks, approvals, presence, stats, metrics, recs] = await Promise.all([
        api<Agent[]>("/agents"),
        api<TaskRow[]>("/tasks?limit=200"),
        api<Approval[]>("/approvals?status=pending"),
        api<{ mode: string }>("/presence"),
        api<SystemStats>("/system/stats"),
        api<SystemMetrics>("/system/metrics"),
        api<{ items: OperatorRecommendation[] }>("/operator/recommendations"),
      ]);
      setMetricHistory(prev => [
        ...prev.slice(-29),
        {
          ts: metrics.generated_at || new Date().toISOString(),
          cpu: metrics.host.cpu_percent,
          memory: metrics.host.memory_percent,
          disk: metrics.host.disk_percent,
          gpu: metricGpuValue(metrics),
          vram: metricVramValue(metrics),
        },
      ]);
      setState({ agents, tasks, approvals, presence: presence.mode, stats, metrics, recommendations: recs.items, error: "", lastRefresh: new Date() });
    } catch (e) {
      setState({ error: e instanceof Error ? e.message : "Cannot reach API" });
    }
  }, []);

  // Slow path (30s): reference data — logs, behaviors, pipeline, atlas, opportunities, forge
  const loadSlow = useCallback(async () => {
    try {
      const [skills, lesson, atlasLearning, opps, logs, pipeline, artifacts, forgeWorkflow, sentinelStatus, behaviors, memoryGraph, opInit, opPending, opChat, jobs] =
        await Promise.all([
          api<Record<string, string>>("/atlas/skills"),
          api<Record<string, unknown>>("/atlas/today"),
          api<AtlasLearning>("/atlas/learning"),
          api<OppData>("/opportunities"),
          api<LogEntry[]>("/logs?limit=150"),
          api<PipelineData>("/pipeline"),
          api<ForgeArtifact[]>("/forge/artifacts?limit=150"),
          api<ForgeWorkflow>("/forge/workflow"),
          api<SentinelStatus>("/sentinel/status"),
          api<BehaviorPolicy[]>("/behaviors"),
          api<MemoryGraphData>("/memory/graph?window_hours=24&limit=220"),
          api<{ initiatives: OperatorInitiative[]; recent_reports: OperatorReport[]; business_ops_summary: string }>("/operator/initiatives"),
          api<{ pending_approvals: Approval[]; blocked_tasks: TaskRow[] }>("/operator/pending"),
          api<{ messages: AgentMessage[] }>("/agents/operator/messages?limit=40"),
          api<JobsData>("/jobs"),
        ]);
      const operatorData: OperatorData = {
        initiatives: opInit.initiatives,
        recent_reports: opInit.recent_reports,
        business_ops_summary: opInit.business_ops_summary,
        pending_approvals: opPending.pending_approvals,
        blocked_tasks: opPending.blocked_tasks,
        chat_messages: opChat.messages || [],
        loaded: true,
      };
      setState({ skills, lesson, atlasLearning, opps, logs, pipeline, artifacts, forgeWorkflow, sentinelStatus, behaviors, memoryGraph, operatorData, jobs });
    } catch {
      // slow path errors are non-fatal — keep stale data
    }
  }, []);

  const load = useCallback(async () => { await Promise.all([loadFast(), loadSlow()]); }, [loadFast, loadSlow]);

  useEffect(() => {
    load();
    let fastTimer: ReturnType<typeof setInterval>;
    let slowTimer: ReturnType<typeof setInterval>;

    function startTimers() {
      fastTimer = setInterval(loadFast, 5_000);
      slowTimer = setInterval(loadSlow, 30_000);
    }
    function clearTimers() {
      clearInterval(fastTimer);
      clearInterval(slowTimer);
    }

    startTimers();

    function onVisibility() {
      if (document.hidden) {
        clearTimers();
      } else {
        loadFast();
        startTimers();
      }
    }
    document.addEventListener("visibilitychange", onVisibility);
    return () => { clearTimers(); document.removeEventListener("visibilitychange", onVisibility); };
  }, [load, loadFast, loadSlow]);

  async function setPresence(mode: string) {
    await fetch(`${getApiBase()}/presence`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ mode }) });
    setState({ presence: mode });
  }

  async function resolveApproval(id: number, action: "approve" | "reject" | "defer") {
    await fetch(`${getApiBase()}/approvals/${id}/resolve`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action }) });
    load();
  }

  function navigateTab(next: Tab) {
    setTab(current => {
      if (current !== next) setTabHistory(history => [...history.slice(-8), current]);
      return next;
    });
  }

  function goBack() {
    setTabHistory(history => {
      const previous = history[history.length - 1];
      if (previous) setTab(previous);
      return history.slice(0, -1);
    });
  }

  async function runControlAction(action: Record<string, unknown>) {
    if (action.action === "open_tab" && typeof action.tab === "string") {
      navigateTab(action.tab as Tab);
      return;
    }
    const res = await fetch(`${getApiBase()}/control/actions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(action),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "Control action failed");
    setMessageReply(String(data.message || "Control action queued."));
    load();
  }

  async function sendDashboardMessage() {
    const message = messageText.trim();
    if (!message || messageSending) return;
    setMessageSending(true);
    setMessageReply("");
    try {
      const res = await fetch(`${getApiBase()}/roderick/message`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "Roderick message failed");
      setMessageReply(String(data.response || ""));
      setMessageText("");
      load();
    } catch (e) {
      setMessageReply(e instanceof Error ? e.message : "Could not reach Roderick");
    } finally {
      setMessageSending(false);
    }
  }

  async function sendAgentMessage(agent: string, message: string): Promise<string> {
    const res = await fetch(`${getApiBase()}/agents/${agent}/message`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || `Could not reach ${agent}`);
    await load();
    return String(data.message || `${agent} message queued.`);
  }

  const { agents, tasks, approvals, stats, metrics, logs, presence, skills, lesson, atlasLearning, opps, pipeline, artifacts, forgeWorkflow, sentinelStatus, behaviors, memoryGraph, recommendations, operatorData, jobs, error, lastRefresh } = state;
  const busyCount = agents.filter(a => a.status === "busy").length;

  const NAV: { id: Tab; icon: string; label: string }[] = [
    { id: "Overview",      icon: "⬡",  label: "Overview" },
    { id: "Agents",        icon: "◎",  label: "Agents" },
    { id: "Tasks",         icon: "≡",  label: "Tasks" },
    { id: "Logs",          icon: "⌁",  label: "Activity Log" },
    { id: "Approvals",     icon: "✓",  label: "Approvals" },
    { id: "Atlas",         icon: "◈",  label: "Atlas" },
    { id: "Opportunities", icon: "◇",  label: "Opportunities" },
    { id: "Operator",      icon: "⚙",  label: "Operator" },
    { id: "Pipeline",      icon: "⟳",  label: "Pipeline" },
    { id: "Forge",         icon: "F",   label: "Forge Files" },
    { id: "Behaviors",     icon: "⚙",  label: "Behaviors" },
    { id: "Memory",        icon: "*",   label: "Memory Graph" },
    { id: "Jobs",          icon: "◈",  label: "Zuko Jobs" },
  ];

  return (
    <div className="app">
      {/* ── Sidebar ── */}
      <aside className="sidebar">
        <div className="sidebar-brand">
          <h1>Roderick</h1>
          <p>Operator Console</p>
        </div>

        <div className="sidebar-section">
          <div className="sidebar-label">Navigation</div>
          {NAV.map(({ id, icon, label }) => (
            <button key={id} className={`nav-item ${tab === id ? "active" : ""}`} onClick={() => navigateTab(id)}>
              <span className="nav-icon">{icon}</span>
              {label}
              {id === "Approvals" && approvals.length > 0 && (
                <span className="nav-badge">{approvals.length}</span>
              )}
            </button>
          ))}
        </div>

        {/* Agent status mini-list */}
        <div className="sidebar-section">
          <div className="sidebar-label">Agents</div>
          {agents.map(a => (
            <div key={a.name} style={{ display: "flex", alignItems: "center", gap: 8, padding: "5px 10px", fontSize: 12 }}>
              <span className={`dot sm ${a.status}`} />
              <span style={{ color: AGENT_COLORS[a.name] ?? "var(--text2)", fontWeight: 500 }}>{a.display_name}</span>
              {a.status === "busy" && <span style={{ marginLeft: "auto", fontSize: 10, color: "var(--amber)" }}>●</span>}
            </div>
          ))}
        </div>

        <OperatorPanel
          className="sidebar-footer"
          recommendations={recommendations}
          onAction={runControlAction}
          onOpenTab={(target) => navigateTab(target)}
          messageText={messageText}
          setMessageText={setMessageText}
          messageReply={messageReply}
          messageSending={messageSending}
          onSend={sendDashboardMessage}
          presence={presence}
          setPresence={setPresence}
        />
      </aside>

      {/* ── Main ── */}
      <div className="main-wrap">
        <header className="topbar">
          <div className="topbar-left">
            <button className="back-btn" onClick={goBack} disabled={!tabHistory.length} title={tabHistory.length ? `Back to ${tabHistory[tabHistory.length - 1]}` : "No previous page"}>
              ← Back
            </button>
            <span className="topbar-title">{NAV.find(n => n.id === tab)?.label}</span>
            {busyCount > 0 && (
              <span style={{ fontSize: 12, color: "var(--amber)", display: "flex", alignItems: "center", gap: 5 }}>
                <span className="dot sm busy" />{busyCount} agent{busyCount > 1 ? "s" : ""} busy
              </span>
            )}
          </div>
          <div className="topbar-right">
            {lastRefresh && (
              <span style={{ fontSize: 11, color: "var(--text3)" }}>
                {formatTime(lastRefresh)} local
              </span>
            )}
            <div className="live-dot" title="Auto-refresh every 10s" />
            <button className="refresh-btn" onClick={load}>↻ Refresh</button>
          </div>
        </header>

        <OperatorPanel
          className="mobile-operator-panel"
          recommendations={recommendations}
          onAction={runControlAction}
          onOpenTab={(target) => navigateTab(target)}
          messageText={messageText}
          setMessageText={setMessageText}
          messageReply={messageReply}
          messageSending={messageSending}
          onSend={sendDashboardMessage}
          presence={presence}
          setPresence={setPresence}
          compact
        />

        <main className="content">
          {error && <div className="alert" style={{ marginBottom: 16 }}>{error}</div>}
          {tab === "Overview"      && <Overview agents={agents} tasks={tasks} approvals={approvals} stats={stats} metrics={metrics} logs={logs} sentinel={sentinelStatus} metricHistory={metricHistory} onOpenTab={navigateTab} onTaskFocus={(focus) => { setTaskFocus(focus); navigateTab("Tasks"); }} onControlAction={runControlAction} />}
            {tab === "Agents"        && <Agents agents={agents} tasks={tasks} behaviors={behaviors} onControlAction={runControlAction} onSendMessage={sendAgentMessage} onBehaviorAction={async (agent, key, action) => {
              await fetch(`${getApiBase()}/behaviors/${agent}/${key}/action`, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ action }) });
              load();
            }} />}
          {tab === "Tasks"         && <Tasks tasks={tasks} focus={taskFocus} onClearFocus={() => setTaskFocus(null)} onControlAction={runControlAction} />}
          {tab === "Logs"          && <Logs logs={logs} />}
          {tab === "Approvals"     && <Approvals approvals={approvals} onResolve={resolveApproval} />}
          {tab === "Atlas"         && <Atlas skills={skills} lesson={lesson} learning={atlasLearning} />}
          {tab === "Opportunities" && <Opportunities data={opps} />}
          {tab === "Operator"      && <OperatorExecutionPanel data={operatorData} tasks={tasks} onControlAction={runControlAction} onSendMessage={sendAgentMessage} />}
          {tab === "Pipeline"      && <Pipeline data={pipeline} />}
          {tab === "Forge"         && <ForgeArtifacts artifacts={artifacts} workflow={forgeWorkflow} onControlAction={runControlAction} />}
          {tab === "Behaviors"     && <Behaviors policies={behaviors} onAction={async (agent, key, action) => {
            await fetch(`${getApiBase()}/behaviors/${agent}/${key}/action`, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ action }) });
            load();
          }} />}
          {tab === "Memory"        && <MemoryGraph graph={memoryGraph} onControlAction={runControlAction} />}
          {tab === "Jobs"          && <Jobs data={jobs} onScanNow={async () => { await fetch(`${getApiBase()}/zuko/scan`, { method: "POST" }); loadSlow(); }} />}
        </main>

        <nav className="mobile-tabbar" aria-label="Mobile navigation">
          {NAV.map(({ id, icon, label }) => (
            <button key={id} className={`mobile-tab ${tab === id ? "active" : ""}`} onClick={() => navigateTab(id)}>
              <span className="mobile-tab-icon">{icon}</span>
              <span className="mobile-tab-label">{label}</span>
              {id === "Approvals" && approvals.length > 0 && <span className="mobile-tab-badge">{approvals.length}</span>}
            </button>
          ))}
        </nav>
      </div>
    </div>
  );
}

// ── Overview ──────────────────────────────────────────────────────────────────

function OperatorPanel({
  className,
  recommendations,
  onAction,
  onOpenTab,
  messageText,
  setMessageText,
  messageReply,
  messageSending,
  onSend,
  presence,
  setPresence,
  compact = false,
}: {
  className: string;
  recommendations: OperatorRecommendation[];
  onAction: (action: Record<string, unknown>) => Promise<void>;
  onOpenTab: (tab: Tab) => void;
  messageText: string;
  setMessageText: (value: string) => void;
  messageReply: string;
  messageSending: boolean;
  onSend: () => Promise<void>;
  presence: string;
  setPresence: (mode: string) => Promise<void>;
  compact?: boolean;
}) {
  return (
    <div className={className}>
      {compact && (
        <div className="mobile-app-card">
          <strong>Install on iPhone</strong>
          <p>Open this in Safari, tap Share, then Add to Home Screen for an app-like dashboard.</p>
        </div>
      )}
      <RecommendationStrip
        recommendations={recommendations}
        onAction={onAction}
        onOpenTab={onOpenTab}
      />
      <div className="dashboard-message">
        <div className="sidebar-label" style={{ paddingLeft: 0 }}>Message</div>
        <textarea
          className="message-input"
          rows={compact ? 2 : 3}
          placeholder="Talk to Roderick..."
          value={messageText}
          onChange={e => setMessageText(e.target.value)}
          onKeyDown={e => {
            if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) void onSend();
          }}
        />
        <button className="message-send" onClick={() => void onSend()} disabled={messageSending || !messageText.trim()}>
          {messageSending ? "Sending..." : "Send to Roderick"}
        </button>
        {messageReply && <div className="message-reply">{messageReply.replace(/<[^>]*>/g, "")}</div>}
      </div>
      <div style={{ fontSize: 11, color: "var(--text3)", marginBottom: 8 }}>Presence</div>
      <div className="presence-row">
        {["at_pc", "away", "focus", "dnd"].map(m => (
          <button key={m} className={`presence-btn ${presence === m ? "active" : ""}`} onClick={() => void setPresence(m)}>
            {m === "at_pc" ? "PC" : m === "dnd" ? "DND" : m.charAt(0).toUpperCase() + m.slice(1)}
          </button>
        ))}
      </div>
    </div>
  );
}

function RecommendationStrip({ recommendations, onAction, onOpenTab }: {
  recommendations: OperatorRecommendation[];
  onAction: (action: Record<string, unknown>) => Promise<void>;
  onOpenTab: (tab: Tab) => void;
}) {
  const [error, setError] = useState("");
  const [actionState, setActionState] = useState<Record<string, "working" | "sent" | "failed">>({});
  const visible = recommendations.slice(0, 3);
  if (!visible.length) return null;
  return (
    <div className="operator-recs">
      <div className="sidebar-label" style={{ paddingLeft: 0 }}>Recommended</div>
      {visible.map(rec => (
        <div key={rec.id} className={`operator-rec ${rec.severity}`}>
          <strong>{rec.title}</strong>
          <p>{rec.summary}</p>
          <small>{rec.why}</small>
          {rec.action_label && rec.action && (
            <button
              className={`message-send rec-action ${actionState[rec.id] === "sent" ? "sent" : ""}`}
              disabled={actionState[rec.id] === "working"}
              onClick={async () => {
                try {
                  setError("");
                  setActionState(s => ({ ...s, [rec.id]: "working" }));
                  if (rec.action?.action === "open_tab" && typeof rec.action.tab === "string") {
                    onOpenTab(rec.action.tab as Tab);
                  } else {
                    await onAction(rec.action || {});
                  }
                  setActionState(s => ({ ...s, [rec.id]: "sent" }));
                } catch (e) {
                  setActionState(s => ({ ...s, [rec.id]: "failed" }));
                  setError(e instanceof Error ? e.message : "Action failed");
                }
              }}
            >
              {actionState[rec.id] === "working"
                ? "Sending..."
                : actionState[rec.id] === "sent"
                  ? (rec.action?.action === "open_tab" ? "Opened" : "Sent to Roderick")
                  : rec.action_label}
            </button>
          )}
        </div>
      ))}
      {error && <div className="message-reply danger">{error}</div>}
    </div>
  );
}

function Overview({ agents, tasks, approvals, stats, metrics, logs, sentinel, metricHistory, onOpenTab, onTaskFocus, onControlAction }: {
  agents: Agent[]; tasks: TaskRow[]; approvals: Approval[];
  stats: SystemStats | null; metrics: SystemMetrics | null; logs: LogEntry[];
  sentinel: SentinelStatus | null; metricHistory: MetricPoint[];
  onOpenTab: (tab: Tab) => void;
  onTaskFocus: (focus: TaskFocus) => void;
  onControlAction: (action: Record<string, unknown>) => Promise<void>;
}) {
  const [expanded, setExpanded] = useState<string | null>(null);
  const busy = agents.filter(a => a.status === "busy");
  const failed = agents.filter(a => a.status === "failed" || a.status === "offline");
  const recentLogs = logs.slice(0, expanded === "activity" ? 18 : 10);
  const sentinelAgent = agents.find(a => a.name === "sentinel");
  const loadedModels = metrics?.ollama?.loaded_models || [];
  const gpu = metrics?.gpu?.gpus?.[0];
  const gpuValue = metrics ? metricGpuValue(metrics) : null;
  const gpuLabel = metrics?.gpu?.status === "verified"
    ? String(gpu?.name || "GPU")
    : metrics?.gpu?.ollama_residency
      ? "Ollama residency"
      : "no data";
  const toggleExpanded = (id: string) => setExpanded(current => current === id ? null : id);
  const cardExpanded = (id: string) => expanded === id;

  // Stuck task detection (client-side — no extra API)
  const now = Date.now();
  const stuckTasks = tasks.filter(t => {
    if (t.status !== "in_progress") return false;
    const updated = t.updated_at ? new Date(t.updated_at).getTime() : 0;
    return now - updated > 10 * 60 * 1000; // stuck > 10 min
  });

  // SRE status banner — symptoms first
  const bannerSeverity = failed.length > 0 || stuckTasks.length > 2 ? "danger"
    : approvals.length > 0 || stuckTasks.length > 0 ? "warn"
    : "ok";
  const bannerMsg = failed.length > 0
    ? `${failed.length} agent${failed.length > 1 ? "s" : ""} offline or failed — ${failed.map(a => a.display_name).join(", ")}`
    : stuckTasks.length > 2
    ? `${stuckTasks.length} tasks stuck >10 min — possible deadlock`
    : approvals.length > 0
    ? `${approvals.length} approval${approvals.length > 1 ? "s" : ""} pending — action required`
    : stuckTasks.length > 0
    ? `${stuckTasks.length} task${stuckTasks.length > 1 ? "s" : ""} stuck >10 min`
    : "All systems nominal";
  const bannerIcon = bannerSeverity === "danger" ? "✕" : bannerSeverity === "warn" ? "⚠" : "✓";

  // Forge CI stage from tasks
  const forgeTasks = tasks.filter(t => t.to_agent === "forge" || t.from_agent === "forge");
  const activeForge = forgeTasks.find(t => t.status === "in_progress");
  const forgeStages = ["pending", "approved", "plan_ready", "plan_approved", "in_progress", "awaiting_validation", "completed"];
  const currentForgeStage = activeForge?.status || null;
  const forgeStageCounts = forgeStages.reduce<Record<string, number>>((acc, stage) => {
    acc[stage] = forgeTasks.filter(t => t.status === stage).length;
    return acc;
  }, {});
  const taskStatusEntries = stats ? Object.entries(stats.tasks_by_status).sort((a, b) => b[1] - a[1]) : [];
  const taskAgentEntries = stats ? Object.entries(stats.tasks_by_agent).sort((a, b) => b[1] - a[1]) : [];

  return (
    <div className="bento">
      {/* ── Status banner ── */}
      <div className={`status-banner ${bannerSeverity}`} style={{ gridColumn: "span 12" }}>
        <span className="status-banner-icon">{bannerIcon}</span>
        <span>{bannerMsg}</span>
        {stuckTasks.length > 0 && bannerSeverity !== "danger" && (
          <span className="status-banner-detail">
            {stuckTasks.map(t => `#${t.id} ${t.to_agent}`).join(" · ")}
          </span>
        )}
      </div>

      {/* ── Stat row ── */}
      <div role="button" tabIndex={0} className={`bento-span3 card overview-action-card ${cardExpanded("agents") ? "expanded" : ""}`} onClick={() => toggleExpanded("agents")} onKeyDown={e => e.key === "Enter" && toggleExpanded("agents")}>
        <div className="card-title">Agents</div>
        <div className="stat-val">{agents.length}</div>
        <div className="stat-sub">{busy.length} active · {failed.length} failed</div>
        {cardExpanded("agents") && (
          <OverviewDrilldown>
            {busy.concat(failed).slice(0, 5).map(agent => (
              <MiniRow key={agent.name} label={agent.display_name} value={agent.status} tone={agent.status === "busy" ? "warn" : "danger"} />
            ))}
            {busy.length === 0 && failed.length === 0 && <div className="muted sm">All registered agents are idle or healthy.</div>}
            <button className="btn sm primary" onClick={(e) => { e.stopPropagation(); onOpenTab("Agents"); }}>Open agent cockpit</button>
          </OverviewDrilldown>
        )}
      </div>
      <div role="button" tabIndex={0} className={`bento-span3 card overview-action-card ${cardExpanded("queue") ? "expanded" : ""}`} onClick={() => toggleExpanded("queue")} onKeyDown={e => e.key === "Enter" && toggleExpanded("queue")}>
        <div className="card-title">Queue</div>
        <div className="stat-val" style={{ color: (stats?.tasks_by_status?.["pending"] ?? 0) > 5 ? "var(--amber)" : undefined }}>
          {stats?.tasks_by_status?.["pending"] ?? 0}
        </div>
        <div className="stat-sub">pending tasks</div>
        {cardExpanded("queue") && (
          <OverviewDrilldown>
            {taskStatusEntries.slice(0, 5).map(([status, count]) => <MiniRow key={status} label={status} value={count} />)}
            {tasks.filter(t => t.status === "pending").slice(0, 3).map(t => <MiniRow key={t.id} label={`#${t.id} ${t.to_agent}`} value={t.task_type} />)}
            <button className="btn sm primary" onClick={(e) => { e.stopPropagation(); onTaskFocus({ label: "Pending Queue", status: "pending" }); }}>Open pending queue</button>
          </OverviewDrilldown>
        )}
      </div>
      <div role="button" tabIndex={0} className={`bento-span3 card overview-action-card ${cardExpanded("approvals") ? "expanded" : ""}`} onClick={() => toggleExpanded("approvals")} onKeyDown={e => e.key === "Enter" && toggleExpanded("approvals")}>
        <div className="card-title">Approvals</div>
        <div className="stat-val" style={{ color: approvals.length > 0 ? "var(--amber)" : undefined }}>
          {approvals.length}
        </div>
        <div className="stat-sub">awaiting review</div>
        {cardExpanded("approvals") && (
          <OverviewDrilldown>
            {approvals.slice(0, 4).map(a => <MiniRow key={a.id} label={`#${a.id} ${a.request_type}`} value={`task #${a.task_id ?? "?"}`} />)}
            {approvals.length === 0 && <div className="muted sm">No approvals waiting.</div>}
            <button className="btn sm primary" onClick={(e) => { e.stopPropagation(); onOpenTab("Approvals"); }}>Review approvals</button>
          </OverviewDrilldown>
        )}
      </div>
      <div role="button" tabIndex={0} className={`bento-span3 card overview-action-card ${cardExpanded("stuck") ? "expanded" : ""}`} onClick={() => toggleExpanded("stuck")} onKeyDown={e => e.key === "Enter" && toggleExpanded("stuck")}>
        <div className="card-title">Stuck</div>
        <div className="stat-val" style={{ color: stuckTasks.length > 0 ? "var(--red)" : undefined }}>
          {stuckTasks.length}
        </div>
        <div className="stat-sub">tasks &gt;10 min in_progress</div>
        {cardExpanded("stuck") && (
          <OverviewDrilldown>
            {stuckTasks.slice(0, 5).map(t => <MiniRow key={t.id} label={`#${t.id} ${t.to_agent}`} value={t.task_type} tone="danger" />)}
            {stuckTasks.length === 0 && <div className="muted sm">No stuck in-progress tasks detected.</div>}
            <div className="mini-actions">
              <button className="btn sm primary" onClick={(e) => { e.stopPropagation(); onTaskFocus({ label: "Stuck Tasks", status: "in_progress", stuckOnly: true }); }}>Open stuck jobs</button>
              {stuckTasks[0] && <button className="btn sm" onClick={(e) => { e.stopPropagation(); onControlAction({ action: "diagnose_agent", agent: stuckTasks[0].to_agent, task_id: stuckTasks[0].id, reason: "stuck task from overview" }); }}>Diagnose first</button>}
            </div>
          </OverviewDrilldown>
        )}
      </div>

      {/* ── Agent topology ── */}
      <div className="bento-span6 card">
        <div className="card-title">Agent Topology</div>
        <div className="stagger" style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          {agents.map(a => (
            <button
              key={a.name}
              type="button"
              className="topology-row"
              onClick={() => onTaskFocus({ label: `${a.display_name} tasks`, agent: a.name })}
            >
              <span className={`dot ${a.status}`} />
              <span style={{ fontWeight: 600, fontSize: 13, minWidth: 90, color: AGENT_COLORS[a.name] ?? "var(--text)" }}>{a.display_name}</span>
              <span className={`badge ${a.status}`}>{a.status}</span>
              {stuckTasks.find(t => t.to_agent === a.name) && (
                <span className="badge failed" style={{ marginLeft: 4 }}>stuck</span>
              )}
              <span style={{ marginLeft: "auto", fontSize: 11, color: "var(--text3)" }}>
                {a.last_message ? a.last_message.slice(0, 48) + (a.last_message.length > 48 ? "…" : "") : "—"}
              </span>
            </button>
          ))}
          {agents.length === 0 && <div className="empty" style={{ padding: "16px 0" }}>No agents registered</div>}
        </div>
      </div>

      {/* ── Approvals action queue ── */}
      <div className="bento-span6 card">
        <div className="card-title">Approvals Queue</div>
        {approvals.length === 0 ? (
          <div className="empty" style={{ padding: "16px 0" }}>No pending approvals</div>
        ) : (
          <div className="stagger" style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {approvals.slice(0, 5).map(a => (
              <button
                key={a.id}
                type="button"
                className="approval-queue-row"
                onClick={() => onOpenTab("Approvals")}
              >
                <span style={{ fontSize: 11, fontWeight: 600, color: "var(--amber)", minWidth: 20 }}>#{a.id}</span>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: "var(--amber)", textTransform: "uppercase", letterSpacing: "0.05em" }}>{a.request_type}</div>
                  <div style={{ fontSize: 12, color: "var(--text2)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{a.description}</div>
                </div>
                <span style={{ fontSize: 11, color: "var(--text3)", flexShrink: 0 }}>{formatTime(a.created_at, { hour: "2-digit", minute: "2-digit", second: undefined })}</span>
              </button>
            ))}
            {approvals.length > 5 && (
              <div style={{ fontSize: 12, color: "var(--text3)", paddingTop: 4 }}>+{approvals.length - 5} more — see Approvals tab</div>
            )}
          </div>
        )}
      </div>

      {/* ── Forge CI lane ── */}
      <div className={`bento-span8 card overview-expandable ${cardExpanded("forge") ? "expanded" : ""}`} onClick={() => toggleExpanded("forge")}>
        <div className="card-title-lg">Forge CI Lane <span className="muted">{activeForge ? `task #${activeForge.id}` : "idle"}</span><span className="drill-hint">{cardExpanded("forge") ? "collapse" : "details"}</span></div>
        <div className="pipeline-lane">
          {forgeStages.map((stage, i) => {
            const isActive = stage === currentForgeStage;
            const isDone = currentForgeStage
              ? forgeStages.indexOf(currentForgeStage) > i
              : false;
            const isFailed = activeForge?.status === "failed" && isActive;
            const stageClass = isFailed ? "failed" : isDone ? "done" : isActive ? "active" : "blocked";
            const stageIcon = isDone ? "✓" : isActive ? "●" : isFailed ? "✕" : "";
            return [
              i > 0 && <div key={`conn-${i}`} className={`pipeline-connector ${isDone ? "done" : ""}`} />,
              <button key={stage} className={`pipeline-stage ${stageClass}`} onClick={(e) => { e.stopPropagation(); onTaskFocus({ label: `Forge ${stage.replace("_", " ")}`, agent: "forge", status: stage }); }}>
                <div className="pipeline-stage-dot">{stageIcon}</div>
                <div className="pipeline-stage-label">{stage.replace("_", " ")}{forgeStageCounts[stage] ? ` (${forgeStageCounts[stage]})` : ""}</div>
              </button>
            ];
          })}
        </div>
        {activeForge && (
          <div style={{ marginTop: 10, fontSize: 12, color: "var(--text2)", borderTop: "1px solid rgba(255,255,255,0.06)", paddingTop: 10 }}>
            {activeForge.description.slice(0, 120)}{activeForge.description.length > 120 ? "…" : ""}
          </div>
        )}
        {!activeForge && forgeTasks.length > 0 && (
          <div style={{ marginTop: 10, fontSize: 12, color: "var(--text3)" }}>
            Last: #{forgeTasks[0].id} · {forgeTasks[0].status} · {forgeTasks[0].description.slice(0, 80)}
          </div>
        )}
        {cardExpanded("forge") && (
          <OverviewDrilldown>
            <div className="stage-detail-grid">
              {forgeStages.map(stage => {
                const stageTasks = forgeTasks.filter(t => t.status === stage).slice(0, 2);
                return (
                  <button
                    key={stage}
                    className="stage-detail stage-detail-button"
                    onClick={(e) => {
                      e.stopPropagation();
                      onTaskFocus({
                        label: `Forge ${stage.replaceAll("_", " ")}`,
                        agent: "forge",
                        status: stage,
                      });
                    }}
                  >
                    <strong>{stage.replace("_", " ")}</strong>
                    <span>{forgeStageCounts[stage] || 0} task{forgeStageCounts[stage] === 1 ? "" : "s"}</span>
                    {stageTasks.map(t => <small key={t.id}>#{t.id} {t.task_type}</small>)}
                  </button>
                );
              })}
            </div>
            <div className="mini-actions">
              <button className="btn sm primary" onClick={(e) => { e.stopPropagation(); onTaskFocus({ label: "Forge workflow", agent: "forge" }); }}>Open Forge tasks</button>
              <button className="btn sm" onClick={(e) => { e.stopPropagation(); onOpenTab("Forge"); }}>Open Forge files</button>
              {activeForge && <button className="btn sm" onClick={(e) => { e.stopPropagation(); onControlAction({ action: "diagnose_agent", agent: "forge", task_id: activeForge.id, reason: "Forge CI lane drilldown" }); }}>Diagnose active</button>}
            </div>
          </OverviewDrilldown>
        )}
      </div>

      {/* ── Sentinel guard ── */}
      <div className={`bento-span4 card sentinel-card overview-expandable ${cardExpanded("sentinel") ? "expanded" : ""}`} onClick={() => toggleExpanded("sentinel")}>
        <div className="card-title-lg">Sentinel <span className="muted">{sentinel?.agent?.status || sentinelAgent?.status || "unknown"}</span><span className="drill-hint">{cardExpanded("sentinel") ? "collapse" : "checks"}</span></div>
        <div className="sentinel-status-line">
          <span className={`dot ${sentinel?.agent?.status || sentinelAgent?.status || "unknown"}`} />
          <div>
            <strong style={{ fontSize: 13 }}>{sentinel?.active_task ? `Task #${sentinel.active_task.id}` : "Idle"}</strong>
            <p>{(sentinel?.latest_summary || sentinelAgent?.last_message || "No active validation").slice(0, 120)}</p>
          </div>
        </div>
        <div className="sentinel-checks">
          {(sentinel?.checks || []).slice(0, 4).map(check => <span key={check}>{check}</span>)}
        </div>
        {cardExpanded("sentinel") && (
          <OverviewDrilldown>
            {sentinel?.active_task && <MiniRow label={`Active #${sentinel.active_task.id}`} value={sentinel.active_task.task_type} tone="warn" />}
            {sentinel?.queued_tasks?.slice(0, 4).map(t => <MiniRow key={t.id} label={`Queued #${t.id}`} value={t.task_type} />)}
            {(sentinel?.recent_events || []).slice(0, 3).map(e => <MiniRow key={e.id} label={e.event_type} value={formatTime(e.created_at, { hour: "2-digit", minute: "2-digit", second: undefined })} />)}
            <div className="mini-actions">
              <button className="btn sm primary" onClick={(e) => { e.stopPropagation(); onTaskFocus({ label: "Sentinel work", agent: "sentinel" }); }}>Open Sentinel tasks</button>
              <button className="btn sm" onClick={(e) => { e.stopPropagation(); onOpenTab("Logs"); }}>Open events</button>
            </div>
          </OverviewDrilldown>
        )}
      </div>

      {/* ── System metrics ── */}
      {metrics && (
        <div className={`bento-span8 card metrics-cockpit overview-expandable ${cardExpanded("metrics") ? "expanded" : ""}`} onClick={() => toggleExpanded("metrics")}>
          <div className="card-title-lg">System Health <span className="muted">USE methodology</span><span className="drill-hint">{cardExpanded("metrics") ? "collapse" : "details"}</span></div>
          <MetricSparkline history={metricHistory} />
          <div className="metric-grid">
            <MetricDial label="CPU" value={metrics.host.cpu_percent} />
            <MetricDial label="Memory" value={metrics.host.memory_percent} sub={`${Math.round(metrics.host.memory_used_mb / 1024 * 10) / 10} GB`} />
            <MetricDial label="Disk" value={metrics.host.disk_percent} sub={`${metrics.host.disk_used_gb}/${metrics.host.disk_total_gb} GB`} />
            <MetricDial label={metrics.gpu?.status === "verified" ? "GPU" : "GPU Offload"} value={gpuValue} sub={gpuLabel} />
          </div>
          <div className="model-strip">
            {loadedModels.length ? loadedModels.map((m, i) => (
              <div key={`${m.name}-${i}`} className="model-chip">
                <strong>{String(m.name || "model")}</strong>
                <span>{String(m.processor || "processor unknown")}</span>
                {m.size_vram ? <small>VRAM {Math.round(Number(m.size_vram) / 1024 / 1024 / 1024 * 10) / 10} GB</small> : null}
              </div>
            )) : (
              <div className="model-chip dim">
                <strong>No loaded Ollama models</strong>
                <span>{metrics.ollama?.status === "unknown" ? String(metrics.ollama.error || "") : "idle"}</span>
              </div>
            )}
          </div>
          <div className="metric-foot">
            <span>DB {metrics.db_size_mb} MB</span>
            <span>{metrics.gpu?.evidence || "GPU evidence unavailable"}</span>
          </div>
          {cardExpanded("metrics") && (
            <OverviewDrilldown>
              <MiniRow label="Host memory" value={`${metrics.host.memory_used_mb}/${metrics.host.memory_total_mb} MB`} />
              <MiniRow label="Disk" value={`${metrics.host.disk_used_gb}/${metrics.host.disk_total_gb} GB`} />
              <MiniRow label="GPU evidence" value={metrics.gpu?.status || "unknown"} tone={metrics.gpu?.status === "verified" ? "ok" : "warn"} />
              {metrics.gpu?.ollama_residency && <MiniRow label="Ollama GPU residency" value={`${metrics.gpu.ollama_residency.residency_percent}%`} tone="ok" />}
              {metrics.gpu?.error && <MiniRow label="GPU utility" value={metrics.gpu.error} tone="warn" />}
              {metrics.langfuse && <MiniRow label="Langfuse" value={`${metrics.langfuse.status} · ${metrics.langfuse.host}`} tone={metrics.langfuse.status === "configured" ? "ok" : "warn"} />}
              <MiniRow label="Ollama" value={`${loadedModels.length} loaded model${loadedModels.length === 1 ? "" : "s"}`} />
              {loadedModels.slice(0, 4).map((m, i) => <MiniRow key={`${m.name}-${i}`} label={String(m.name || "model")} value={String(m.processor || "processor unknown")} />)}
            </OverviewDrilldown>
          )}
        </div>
      )}

      {/* ── Task distribution ── */}
      {stats && (
        <div className={`bento-span4 card overview-expandable ${cardExpanded("distribution") ? "expanded" : ""}`} onClick={() => toggleExpanded("distribution")}>
          <div className="card-title">Task Distribution <span className="drill-hint">{cardExpanded("distribution") ? "collapse" : "agents"}</span></div>
          {taskStatusEntries.map(([s, n]) => (
            <button key={s} className="kv-row overview-link-row" onClick={(e) => { e.stopPropagation(); onTaskFocus({ label: `${s} tasks`, status: s }); }}>
              <span className={`badge ${s}`}>{s}</span>
              <span className="kv-val">{n}</span>
            </button>
          ))}
          {cardExpanded("distribution") && (
            <OverviewDrilldown>
              {taskAgentEntries.slice(0, 6).map(([agent, count]) => <MiniRow key={agent} label={agent} value={count} />)}
              <button className="btn sm primary" onClick={(e) => { e.stopPropagation(); onTaskFocus({ label: "All tasks" }); }}>Open all tasks</button>
            </OverviewDrilldown>
          )}
        </div>
      )}

      {/* ── Recent activity ── */}
      <div className={`bento-span12 card overview-expandable ${cardExpanded("activity") ? "expanded" : ""}`} onClick={() => toggleExpanded("activity")}>
        <div className="card-title-lg">Recent Activity <span className="muted">live feed · 5s</span></div>
        <div className="activity-drill-hint">{cardExpanded("activity") ? "Showing expanded activity. Click Open full log for the complete feed." : "Click this card to expand recent activity."}</div>
        <div className="log-feed">
          {recentLogs.length === 0 && <div className="empty">No activity yet.</div>}
          {recentLogs.map(e => <LogRow key={e.id} entry={e} />)}
        </div>
        {cardExpanded("activity") && (
          <div className="mini-actions" style={{ marginTop: 12 }}>
            <button className="btn sm primary" onClick={(e) => { e.stopPropagation(); onOpenTab("Logs"); }}>Open full log</button>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Agents ────────────────────────────────────────────────────────────────────

function OverviewDrilldown({ children }: { children: ReactNode }) {
  return (
    <div className="overview-drilldown" onClick={(e) => e.stopPropagation()}>
      {children}
    </div>
  );
}

function MiniRow({ label, value, tone }: { label: string; value: string | number; tone?: "ok" | "warn" | "danger" }) {
  return (
    <div className={`mini-row ${tone || ""}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function AgentControlStrip({ agent, runtimeState, onAction }: {
  agent: string;
  runtimeState: string;
  onAction: (agent: string, action: string) => Promise<void>;
}) {
  const [working, setWorking] = useState("");
  const run = async (action: string) => {
    setWorking(action);
    try {
      await onAction(agent, action);
    } finally {
      setWorking("");
    }
  };
  const disabled = Boolean(working);
  return (
    <div className="agent-control-strip" onClick={(e) => e.stopPropagation()}>
      <button className="btn sm" disabled={disabled || runtimeState === "paused"} onClick={() => run("pause_agent")}>{working === "pause_agent" ? "Pausing..." : "Pause"}</button>
      <button className="btn sm danger" disabled={disabled || runtimeState === "stopped"} onClick={() => run("stop_agent")}>{working === "stop_agent" ? "Stopping..." : "Stop"}</button>
      <button className="btn sm primary" disabled={disabled || runtimeState === "active"} onClick={() => run("start_agent")}>{working === "start_agent" ? "Starting..." : "Start"}</button>
      <button className="btn sm" disabled={disabled} onClick={() => run("restart_agent")}>{working === "restart_agent" ? "Restarting..." : "Restart"}</button>
    </div>
  );
}

function supportsDirectChat(name: string | null) {
  return name === "operator" || name === "zuko";
}

function AgentDetailContent({
  selected,
  detail,
  loading,
  behaviors,
  runtimeState,
  chatMessages,
  chatLoading,
  onControlAgent,
  onBehaviorAction,
  onSendMessage,
  setChatMessages,
  setChatLoading,
}: {
  selected: string;
  detail: AgentDetail | null;
  loading: boolean;
  behaviors: BehaviorPolicy[];
  runtimeState: string;
  chatMessages: AgentMessage[];
  chatLoading: boolean;
  onControlAgent: (agent: string, action: string) => Promise<void>;
  onBehaviorAction: (agent: string, key: string, action: "approve" | "reject" | "rollback") => void;
  onSendMessage: (agent: string, message: string) => Promise<string>;
  setChatMessages: Dispatch<SetStateAction<AgentMessage[]>>;
  setChatLoading: Dispatch<SetStateAction<boolean>>;
}) {
  return (
    <>
      {loading && <div className="muted sm" style={{ padding: "20px 0" }}>Loading...</div>}

      {detail && !loading && (
        <div className="stack">
          <div className="ops-panel agent-control-panel">
            <div>
              <div className="card-title-lg">Agent Controls</div>
              <p className="muted sm">Runtime policy: <strong>{runtimeState}</strong>. Pause/stop affects new task pickup; restart is a soft restart that requeues active rows.</p>
            </div>
            <AgentControlStrip agent={selected} runtimeState={runtimeState} onAction={onControlAgent} />
          </div>

          <div className="agent-ops-grid">
            <div className="ops-tile">
              <span>Stage</span>
              <strong>{detail.stage || detail.status}</strong>
              <small>{detail.state_evidence || "unknown evidence"}</small>
            </div>
            <div className="ops-tile">
              <span>Model</span>
              {detail.planner_model && detail.coder_model ? (
                <>
                  <strong style={{ fontSize: 11 }}>planner: {detail.planner_model}</strong>
                  <small>coder: {detail.coder_model}</small>
                </>
              ) : detail.research_model && detail.diagnostic_model ? (
                <>
                  <strong style={{ fontSize: 11 }}>research: {detail.research_model}</strong>
                  <small>diagnostics: {detail.diagnostic_model}</small>
                </>
              ) : detail.deep_model && detail.routine_model ? (
                <>
                  <strong style={{ fontSize: 11 }}>deep: {detail.deep_model}</strong>
                  <small>routine: {detail.routine_model}</small>
                </>
              ) : (
                <>
                  <strong>{detail.model_used || "unknown"}</strong>
                  <small>from registry</small>
                </>
              )}
            </div>
            <div className="ops-tile">
              <span>Heartbeat</span>
              <strong>{formatTime(detail.last_heartbeat)}</strong>
              <small>{detail.last_heartbeat ? `${formatDateOnly(detail.last_heartbeat)} local` : "no heartbeat recorded"}</small>
            </div>
            <div className="ops-tile">
              <span>Host Usage</span>
              <strong>{detail.resource_snapshot?.host_cpu_percent ?? 0}% CPU</strong>
              <small>{detail.resource_snapshot?.host_memory_percent ?? 0}% memory, {detail.resource_snapshot?.disk_percent ?? 0}% disk</small>
            </div>
          </div>

          {detail.active_task ? (
            <div className="working-box" style={{ padding: "12px 16px" }}>
              <strong>Current work #{detail.active_task.id}</strong>
              <div style={{ marginTop: 6, color: "var(--text2)" }}>{detail.active_task.description}</div>
              <div className="muted sm">{detail.active_task.task_type} / updated {formatDateTime(detail.active_task.updated_at)} local</div>
            </div>
          ) : (
            <div className="ops-panel">
              <div className="card-title">Current Work</div>
              <div className="muted sm">No verified in-progress task row for this agent.</div>
            </div>
          )}

          <div className="agent-detail-grid">
            <div className="ops-panel">
              <div className="card-title-lg">Thinking / Learning Notes</div>
              <pre className="thought-box">{detail.learning_note || "No learning note has been written for this agent yet."}</pre>
            </div>

            <div className="ops-panel">
              <div className="card-title-lg">Intentions / Queue</div>
              {detail.intentions?.length ? detail.intentions.map(t => (
                <div key={t.id} className="intent-row">
                  <span className={`badge ${t.status}`}>{t.status}</span>
                  <div>
                    <strong>#{t.id} {t.task_type}</strong>
                    <p>{t.description}</p>
                  </div>
                </div>
              )) : <div className="empty compact">No queued intentions.</div>}
            </div>
          </div>

          <div className="agent-detail-grid">
            <div className="ops-panel">
              <div className="card-title-lg">Inter-agent Messages</div>
              {detail.recent_messages?.length ? detail.recent_messages.map(m => (
                <div key={m.id} className="message-line">
                  <div><AgentPill name={m.from_agent} /> <span className="muted sm">to</span> <AgentPill name={m.to_agent} /></div>
                  <p>{m.message}</p>
                  <small>{m.priority} / {formatDateTime(m.created_at)} local</small>
                </div>
              )) : <div className="empty compact">No recent messages.</div>}
            </div>

            <div className="ops-panel">
              <div className="card-title-lg">Reports</div>
              {detail.recent_reports?.length ? detail.recent_reports.map(r => (
                <div key={r.path} className="report-link">
                  <strong>{r.file}</strong>
                  <p>{r.summary || "No summary available."}</p>
                  <small>{formatDateTime(r.updated_at)} local / {r.path}</small>
                </div>
              )) : <div className="empty compact">No reports found.</div>}
            </div>
          </div>

          <div className="ops-panel">
            <div className="card-title-lg">Event Timeline</div>
            <div className="mini-timeline">
              {detail.recent_events?.length ? detail.recent_events.map(e => (
                <div key={e.id} className="timeline-row">
                  <span>{formatTime(e.created_at)}</span>
                  <strong>{e.event_type}</strong>
                  <code>{
                    (() => {
                      const p = e.payload || {};
                      const summary = (p as Record<string, unknown>).summary || (p as Record<string, unknown>).task_summary || (p as Record<string, unknown>).description || (p as Record<string, unknown>).initiative || (p as Record<string, unknown>).error;
                      if (summary) return String(summary).slice(0, 180);
                      const keys = Object.keys(p as Record<string, unknown>);
                      if (keys.length === 0) return "-";
                      return keys.map(k => `${k}: ${String((p as Record<string, unknown>)[k]).slice(0, 40)}`).join(" · ").slice(0, 200);
                    })()
                  }</code>
                </div>
              )) : <div className="empty compact">No structured events for this agent yet.</div>}
            </div>
          </div>

          <div className="grid4">
            {Object.entries(detail.task_counts).map(([s, n]) => (
              <div key={s} className="ops-tile">
                <span>{s}</span>
                <strong>{n}</strong>
                <small>tasks</small>
              </div>
            ))}
          </div>

          {(() => {
            const agentPolicies = behaviors.filter(p => p.agent === selected);
            const STATUS_COLOR: Record<string, string> = {
              proposed: "var(--amber)", applied: "var(--green)",
              rejected: "var(--red)", rolled_back: "var(--text3)",
            };
            if (!agentPolicies.length) return (
              <div className="ops-panel">
                <div className="card-title-lg">Behavior Policies</div>
                <div className="muted sm">No policies set. Tell Roderick: <em>&quot;{detail.display_name} should ...&quot;</em></div>
              </div>
            );
            return (
              <div className="ops-panel">
                <div className="card-title-lg">Behavior Policies
                  <span className="muted" style={{ marginLeft: 8 }}>{agentPolicies.length} active</span>
                </div>
                <table className="table">
                  <thead>
                    <tr><th>Key</th><th>Value</th><th>Status</th><th>Description</th><th></th></tr>
                  </thead>
                  <tbody>
                    {agentPolicies.map(p => (
                      <tr key={p.policy_key}>
                        <td><code style={{ fontSize: 11 }}>{p.policy_key}</code></td>
                        <td><code style={{ fontSize: 11, color: "var(--teal)" }}>{p.policy_value}</code></td>
                        <td><span style={{ color: STATUS_COLOR[p.status] ?? "var(--text2)", fontWeight: 600, fontSize: 11 }}>{p.status}</span></td>
                        <td style={{ fontSize: 12, color: "var(--text2)", maxWidth: 180 }}>{p.description}</td>
                        <td>
                          <div style={{ display: "flex", gap: 4 }}>
                            {p.status === "proposed" && (
                              <>
                                <button className="btn sm primary" onClick={() => onBehaviorAction(p.agent, p.policy_key, "approve")}>Apply</button>
                                <button className="btn sm danger" onClick={() => onBehaviorAction(p.agent, p.policy_key, "reject")}>Reject</button>
                              </>
                            )}
                            {p.status === "applied" && (
                              <button className="btn sm" onClick={() => onBehaviorAction(p.agent, p.policy_key, "rollback")}>Rollback</button>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            );
          })()}

          {supportsDirectChat(selected) && (
            <DirectAgentChatPanel
              agent={selected}
              displayName={detail.display_name}
              accent={agentAccent(selected) || "var(--text)"}
              messages={chatMessages}
              loading={chatLoading}
              onSendMessage={onSendMessage}
              onRefresh={async () => {
                setChatLoading(true);
                try {
                  const thread = await api<{ messages: AgentMessage[] }>(`/agents/${selected}/messages?limit=40`);
                  setChatMessages(thread.messages || []);
                } finally {
                  setChatLoading(false);
                }
              }}
            />
          )}
        </div>
      )}
    </>
  );
}

function Agents({ agents, tasks, behaviors, onControlAction, onBehaviorAction, onSendMessage }: {
  agents: Agent[]; tasks: TaskRow[];
  behaviors: BehaviorPolicy[];
  onControlAction: (action: Record<string, unknown>) => Promise<void>;
  onBehaviorAction: (agent: string, key: string, action: "approve" | "reject" | "rollback") => void;
  onSendMessage: (agent: string, message: string) => Promise<string>;
}) {
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail]   = useState<AgentDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [chatMessages, setChatMessages] = useState<AgentMessage[]>([]);
  const [chatLoading, setChatLoading] = useState(false);

  async function loadDetail(name: string) {
    if (selected === name) {
      setSelected(null); setDetail(null); setChatMessages([]);
      return;
    }
    setSelected(name); setLoading(true);
    try {
      const detailData = await api<AgentDetail>(`/agents/${name}`);
      setDetail(detailData);
      if (supportsDirectChat(name)) {
        setChatLoading(true);
        try {
          const thread = await api<{ messages: AgentMessage[] }>(`/agents/${name}/messages?limit=40`);
          setChatMessages(thread.messages || []);
        } finally {
          setChatLoading(false);
        }
      } else {
        setChatMessages([]);
      }
    } finally { setLoading(false); }
  }

  const activeByAgent = useMemo(() => {
    const m: Record<string, TaskRow> = {};
    for (const t of tasks)
      if (t.status === "in_progress" && !m[t.to_agent]) m[t.to_agent] = t;
    return m;
  }, [tasks]);
  const runtimeByAgent = useMemo(() => {
    const map: Record<string, string> = {};
    for (const p of behaviors) {
      if (p.policy_key === "runtime_state" && p.status === "applied") map[p.agent] = p.policy_value;
    }
    return map;
  }, [behaviors]);
  const controlAgent = async (agent: string, action: string) => {
    await onControlAction({ action, agent, reason: `operator clicked ${action.replace("_", " ")} in Agents tab` });
    if (selected === agent) await loadDetail(agent);
  };

  return (
    <div className="stack">
      <div className="grid2">
        {agents.map(agent => {
          const active = activeByAgent[agent.name];
          const accent = AGENT_COLORS[agent.name];
          const runtimeState = runtimeByAgent[agent.name] || "active";
          const isSelected = selected === agent.name;
          return (
            <div key={agent.name}
              className={`agent-card ${isSelected ? "selected" : ""}`}
              onClick={() => loadDetail(agent.name)}
              style={isSelected ? { borderColor: accent, boxShadow: `0 0 0 1px ${accent}33` } : {}}>
              <div className="agent-card-hdr">
                <div className="agent-card-name">
                  <span className={`dot ${agent.status}`} />
                  <span style={{ color: accent }}>{agent.display_name}</span>
                  <span className="chip" style={{ fontSize: 10 }}>{agent.autonomy_level}</span>
                  {runtimeState !== "active" && <span className={`chip runtime ${runtimeState}`}>{runtimeState}</span>}
                </div>
                <span className={`badge ${agent.status}`}>{agent.status}</span>
              </div>

              <p className="agent-purpose">{agent.purpose}</p>

              <div className="agent-meta-row">
                <span className="chip accent" style={{ borderColor: `${accent}44`, color: accent }} title={agent.current_model || agent.model_used}>
                  {agent.current_model && agent.current_model !== agent.model_used ? agent.current_model : agent.model_used}
                </span>
                {agent.task_types_accepted?.map(t => (
                  <span key={t} className="chip">{t}</span>
                ))}
              </div>

              {active ? (
                <div className="working-box">
                  <strong>Working</strong>
                  <span>#{active.id} · {active.description.slice(0, 70)}{active.description.length > 70 ? "…" : ""}</span>
                </div>
              ) : agent.last_message ? (
                <div className="last-msg">↳ {agent.last_message.slice(0, 100)}</div>
              ) : null}

              {agent.last_error && (
                <div className="error-msg">⚠ {agent.last_error.slice(0, 80)}</div>
              )}
              <AgentControlStrip agent={agent.name} runtimeState={runtimeState} onAction={controlAgent} />
              {isSelected && (
                <div className="detail-panel mobile-inline-agent-detail" onClick={(e) => e.stopPropagation()}>
                  <div className="card-title-lg" style={{ marginBottom: 16 }}>
                    <span style={{ color: AGENT_COLORS[agent.name] ?? "var(--teal)" }}>
                      {detail?.display_name ?? agent.display_name}
                    </span>
                    <span className="muted">- detail view</span>
                    <button className="btn sm ml-auto" onClick={() => { setSelected(null); setDetail(null); }}>Close</button>
                  </div>
                  <AgentDetailContent
                    selected={agent.name}
                    detail={detail}
                    loading={loading}
                    behaviors={behaviors}
                    runtimeState={runtimeState}
                    chatMessages={chatMessages}
                    chatLoading={chatLoading}
                    onControlAgent={controlAgent}
                    onBehaviorAction={onBehaviorAction}
                    onSendMessage={onSendMessage}
                    setChatMessages={setChatMessages}
                    setChatLoading={setChatLoading}
                  />
                </div>
              )}
            </div>
          );
        })}
      </div>

      {selected && (
        <div className="detail-panel desktop-agent-detail">
          <div className="card-title-lg" style={{ marginBottom: 16 }}>
            <span style={{ color: AGENT_COLORS[selected] ?? "var(--teal)" }}>
              {detail?.display_name ?? selected}
            </span>
            <span className="muted">- detail view</span>
            <button className="btn sm ml-auto" onClick={() => { setSelected(null); setDetail(null); }}>Close</button>
          </div>
          <AgentDetailContent
            selected={selected}
            detail={detail}
            loading={loading}
            behaviors={behaviors}
            runtimeState={runtimeByAgent[selected] || "active"}
            chatMessages={chatMessages}
            chatLoading={chatLoading}
            onControlAgent={controlAgent}
            onBehaviorAction={onBehaviorAction}
            onSendMessage={onSendMessage}
            setChatMessages={setChatMessages}
            setChatLoading={setChatLoading}
          />
        </div>
      )}
    </div>
  );
}

function DirectAgentChatPanel({
  agent,
  displayName,
  accent,
  messages,
  loading,
  onSendMessage,
  onRefresh,
}: {
  agent: string;
  displayName: string;
  accent: string;
  messages: AgentMessage[];
  loading: boolean;
  onSendMessage: (agent: string, message: string) => Promise<string>;
  onRefresh: () => Promise<void>;
}) {
  const [chatText, setChatText] = useState("");
  const [chatState, setChatState] = useState<"idle" | "sending" | "sent" | "failed">("idle");
  const [chatReply, setChatReply] = useState("");
  const agentTitle = agent === "zuko" ? "Direct Zuko Chat" : `Direct ${displayName} Chat`;
  const subtitle = agent === "zuko"
    ? "Talk to Zuko directly from the dashboard about job scans, shortlist movement, recruiter signals, and applications in motion."
    : `Talk to ${displayName} directly from the dashboard.`;
  const placeholder = agent === "zuko"
    ? "Ask Zuko about shortlist movement, applications, recruiters, or the next scan..."
    : `Ask ${displayName} about current work, blockers, or next steps...`;

  async function sendChat() {
    const message = chatText.trim();
    if (!message || chatState === "sending") return;
    setChatState("sending");
    setChatReply("");
    try {
      const reply = await onSendMessage(agent, message);
      setChatReply(reply);
      setChatText("");
      setChatState("sent");
      await onRefresh();
    } catch (error) {
      setChatReply(error instanceof Error ? error.message : `Could not reach ${displayName}`);
      setChatState("failed");
    }
  }

  return (
    <div className="ops-panel">
      <div className="card-title-lg" style={{ color: accent }}>{agentTitle}</div>
      <div className="muted sm" style={{ marginBottom: 10 }}>{subtitle}</div>
      <div className="dashboard-message" style={{ marginBottom: 12 }}>
        <textarea
          value={chatText}
          onChange={e => setChatText(e.target.value)}
          placeholder={placeholder}
          rows={4}
        />
        <button className="message-send" onClick={() => void sendChat()} disabled={!chatText.trim() || chatState === "sending"}>
          {chatState === "sending" ? `Sending to ${displayName}...` : `Send to ${displayName}`}
        </button>
      </div>
      {chatReply && (
        <div className={`operator-chat-reply ${chatState === "failed" ? "failed" : ""}`} style={{ borderColor: `${accent}33`, background: `${accent}14` }}>
          {chatReply}
        </div>
      )}
      <div className="operator-chat-thread">
        {loading ? (
          <div className="empty compact">Loading {displayName} conversation...</div>
        ) : messages.length === 0 ? (
          <div className="empty compact">No {displayName} dashboard conversation yet.</div>
        ) : messages.slice(-12).map(message => {
          const outbound = message.from_agent === "dashboard";
          return (
            <div key={message.id} className={`operator-chat-row ${outbound ? "outbound" : "inbound"}`}>
              <div className="operator-chat-meta">
                <AgentPill name={outbound ? "dashboard" : agent} />
                <span>{formatDateTime(message.created_at)}</span>
              </div>
              <div
                className="operator-chat-bubble"
                style={outbound ? {
                  borderColor: `${accent}55`,
                  background: `${accent}1A`,
                  color: "var(--text)",
                } : undefined}
              >
                {message.message}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Tasks ─────────────────────────────────────────────────────────────────────

function Tasks({ tasks, focus, onClearFocus, onControlAction }: {
  tasks: TaskRow[];
  focus: TaskFocus | null;
  onClearFocus: () => void;
  onControlAction: (action: Record<string, unknown>) => Promise<void>;
}) {
  const [fa, setFa] = useState("all");
  const [fs, setFs] = useState("all");
  const [stuckOnly, setStuckOnly] = useState(false);
  const [selectedTaskId, setSelectedTaskId] = useState<number | null>(null);
  const [detailCache, setDetailCache] = useState<Record<number, TaskDetail>>({});
  const [loadingTaskId, setLoadingTaskId] = useState<number | null>(null);
  useEffect(() => {
    if (!focus) return;
    setFa(focus.agent || "all");
    setFs(focus.status || "all");
    setStuckOnly(Boolean(focus.stuckOnly));
  }, [focus]);
  const agentList = Array.from(new Set(tasks.map(t => t.to_agent))).sort();
  const statusList = Array.from(new Set(tasks.map(t => t.status))).sort();
  const now = Date.now();
  const isStuck = (t: TaskRow) => {
    if (t.status !== "in_progress") return false;
    const updated = t.updated_at ? new Date(t.updated_at).getTime() : 0;
    return now - updated > 10 * 60 * 1000;
  };
  const filtered = tasks.filter(t =>
    (fa === "all" || t.to_agent === fa) && (fs === "all" || t.status === fs) && (!stuckOnly || isStuck(t))
  );

  async function toggleTaskDetail(taskId: number) {
    if (selectedTaskId === taskId) {
      setSelectedTaskId(null);
      return;
    }
    setSelectedTaskId(taskId);
    if (detailCache[taskId]) return;
    setLoadingTaskId(taskId);
    try {
      const detail = await api<TaskDetail>(`/tasks/${taskId}`);
      setDetailCache(prev => ({ ...prev, [taskId]: detail }));
    } finally {
      setLoadingTaskId(null);
    }
  }

  return (
    <div className="card">
      <div className="filter-bar">
        <span style={{ fontWeight: 600 }}>Tasks <span className="muted sm">({filtered.length})</span></span>
        <select className="select" value={fa} onChange={e => setFa(e.target.value)}>
          <option value="all">All agents</option>
          {agentList.map(a => <option key={a} value={a}>{a}</option>)}
        </select>
        <select className="select" value={fs} onChange={e => setFs(e.target.value)}>
          <option value="all">All statuses</option>
          {statusList.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <button className={`btn sm ${stuckOnly ? "danger" : ""}`} onClick={() => setStuckOnly(v => !v)}>
          {stuckOnly ? "Showing stuck" : "Stuck only"}
        </button>
        {focus && (
          <button className="btn sm" onClick={() => { onClearFocus(); setFa("all"); setFs("all"); setStuckOnly(false); }}>
            Clear focus
          </button>
        )}
      </div>
      {focus && (
        <div className="task-focus-banner">
          <strong>{focus.label}</strong>
          <span>{filtered.length} matching task{filtered.length === 1 ? "" : "s"} ready for action.</span>
        </div>
      )}
      <table className="table">
        <thead>
          <tr><th>#</th><th>Agent</th><th>Status</th><th>Type</th><th>Description</th><th>Updated</th><th>Controls</th></tr>
        </thead>
        <tbody>
          {filtered.map(t => (
            <Fragment key={t.id}>
              <tr className={selectedTaskId === t.id ? "task-row-selected" : ""}>
                <td className="muted sm">
                  <button className="task-id-btn" onClick={() => toggleTaskDetail(t.id)}>#{t.id}</button>
                </td>
                <td><AgentPill name={t.to_agent} /></td>
                <td><span className={`badge ${t.status}`}>{t.status}</span></td>
                <td><span className="chip">{t.task_type}</span></td>
                <td className="truncate">{t.description}</td>
                <td className="muted sm">{formatDateTime(t.updated_at)}</td>
                <td>
                  <div className="mini-actions">
                    <button className={`btn sm ${selectedTaskId === t.id ? "primary" : ""}`} onClick={() => toggleTaskDetail(t.id)}>
                      {selectedTaskId === t.id ? "Hide" : "Inspect"}
                    </button>
                    {t.status === "in_progress" && (
                      <button className="btn sm" onClick={() => onControlAction({ action: "diagnose_agent", agent: t.to_agent, task_id: t.id, reason: "task appears active from dashboard" })}>Diagnose</button>
                    )}
                    {["in_progress", "failed", "cancelled"].includes(t.status) && (
                      <button className="btn sm" onClick={() => onControlAction({ action: "requeue_task", task_id: t.id, reason: "operator requested retry from dashboard" })}>Requeue</button>
                    )}
                    {["pending", "in_progress", "approved"].includes(t.status) && (
                      <button className="btn sm danger" onClick={() => onControlAction({ action: "cancel_task", task_id: t.id, reason: "operator cancelled from dashboard" })}>Cancel</button>
                    )}
                  </div>
                </td>
              </tr>
              {selectedTaskId === t.id && (
                <tr className="task-detail-row">
                  <td colSpan={7}>
                    {loadingTaskId === t.id && !detailCache[t.id] ? (
                      <div className="task-detail-panel"><div className="empty">Loading live task activity…</div></div>
                    ) : detailCache[t.id] ? (
                      <TaskExecutionPanel detail={detailCache[t.id]} onControlAction={onControlAction} />
                    ) : (
                      <div className="task-detail-panel"><div className="empty">No live detail available for this task yet.</div></div>
                    )}
                  </td>
                </tr>
              )}
            </Fragment>
          ))}
          {filtered.length === 0 && (
            <tr><td colSpan={7} className="empty">No tasks match the filter.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function TaskExecutionPanel({ detail, onControlAction }: {
  detail: TaskDetail;
  onControlAction: (action: Record<string, unknown>) => Promise<void>;
}) {
  const reportSummary = detail.report?.content ? summarizeObject(detail.report.content, 8) : [];
  const improvementSummary = detail.improvement ? summarizeObject(detail.improvement as Record<string, unknown>, 6) : [];
  const executionMode =
    detail.output.forge_mode === "repo_patch" ? "Repo/System Patch"
      : detail.output.forge_mode === "artifact_build" ? "Artifact Build"
      : detail.output.forge_mode === "artifact_markdown" ? "Markdown Artifact"
      : "General Task";

  return (
    <div className="task-detail-panel">
      <div className="task-detail-head">
        <div>
          <div className="card-title-lg">Live Task Console</div>
          <div className="muted sm">Real task evidence from the database, events, approvals, artifacts, messages, and reports.</div>
        </div>
        <div className="mini-actions">
          <button className="btn sm" onClick={() => onControlAction({ action: "diagnose_agent", agent: detail.task.to_agent, task_id: detail.task.id, reason: "live task console inspection" })}>
            Diagnose
          </button>
        </div>
      </div>

      <div className="task-detail-grid">
        <div className="task-detail-card">
          <div className="task-detail-title">Task</div>
          <div className="task-detail-list">
            <div><span>ID</span><strong>#{detail.task.id}</strong></div>
            <div><span>Agent</span><strong>{detail.task.to_agent}</strong></div>
            <div><span>Type</span><strong>{detail.task.task_type}</strong></div>
            <div><span>Status</span><strong>{detail.task.status}</strong></div>
            <div><span>Stage</span><strong>{detail.stage.replaceAll("_", " ")}</strong></div>
            <div><span>Updated</span><strong>{formatDateTime(detail.task.updated_at)}</strong></div>
          </div>
          <div className="task-detail-body">{detail.task.description}</div>
        </div>

        <div className="task-detail-card">
          <div className="task-detail-title">Execution</div>
          <div className="task-detail-list">
            <div><span>Mode</span><strong>{executionMode}</strong></div>
            {detail.output.sentinel_task_id && <div><span>Sentinel</span><strong>Task #{detail.output.sentinel_task_id}</strong></div>}
            {detail.related_sentinel_task && <div><span>Sentinel status</span><strong>{detail.related_sentinel_task.status}</strong></div>}
            {detail.output.deployment?.state && <div><span>Deployment</span><strong>{String(detail.output.deployment.state)}</strong></div>}
            <div><span>Approvals</span><strong>{detail.approvals.length}</strong></div>
            <div><span>Artifacts</span><strong>{detail.artifacts.length}</strong></div>
          </div>
          <div className="task-paths">
            {detail.output.artifact_root && <div>Root: {detail.output.artifact_root}</div>}
            {detail.output.artifact_files_dir && <div>Files: {detail.output.artifact_files_dir}</div>}
            {detail.output.files_created?.length ? <div>Files created: {detail.output.files_created.slice(0, 6).join(", ")}</div> : null}
            {detail.output.patches_applied?.length ? <div>Patches: {detail.output.patches_applied.length}</div> : null}
            {detail.output.deployment?.services?.length ? <div>Services refreshed: {detail.output.deployment.services.join(", ")}</div> : null}
          </div>
        </div>

        <div className="task-detail-card">
          <div className="task-detail-title">Linked Work</div>
          <div className="task-detail-list">
            <div><span>Events</span><strong>{detail.events.length}</strong></div>
            <div><span>Messages</span><strong>{detail.messages.length}</strong></div>
            <div><span>Report</span><strong>{detail.report ? "available" : "none"}</strong></div>
          </div>
          {improvementSummary.length > 0 && (
            <div className="task-detail-body">
              {improvementSummary.map(line => <div key={line}>{line}</div>)}
            </div>
          )}
        </div>
      </div>

      <div className="task-detail-streams">
        <div className="task-stream-card">
          <div className="task-detail-title">Live Activity</div>
          <div className="task-stream-list">
            {detail.events.length ? detail.events.slice(0, 12).map(event => (
              <div key={event.id} className="task-stream-row">
                <span>{formatTime(event.created_at)}</span>
                <strong>{event.event_type}</strong>
                <div>{summarizeObject(event.payload || {}, 2).join(" · ") || "No structured payload summary"}</div>
              </div>
            )) : <div className="empty compact">No structured events recorded yet.</div>}
          </div>
        </div>

        <div className="task-stream-card">
          <div className="task-detail-title">Inter-Agent Traffic</div>
          <div className="task-stream-list">
            {detail.messages.length ? detail.messages.slice(0, 10).map(message => (
              <div key={message.id} className="task-stream-row">
                <span>{formatTime(message.created_at)}</span>
                <strong>{message.from_agent} → {message.to_agent}</strong>
                <div>{message.message.slice(0, 220)}</div>
              </div>
            )) : <div className="empty compact">No recent agent traffic tied to this task’s lane.</div>}
          </div>
        </div>

        <div className="task-stream-card">
          <div className="task-detail-title">Report / Evidence</div>
          <div className="task-stream-list">
            {reportSummary.length ? reportSummary.map(line => (
              <div key={line} className="task-stream-row report">
                <strong>{line.split(":")[0]}</strong>
                <div>{line.slice(line.indexOf(":") + 1).trim()}</div>
              </div>
            )) : <div className="empty compact">No structured report summary available yet.</div>}
            {detail.report?.path && <div className="muted sm">Source: {detail.report.path}</div>}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Logs ──────────────────────────────────────────────────────────────────────

function Logs({ logs }: { logs: LogEntry[] }) {
  const [filterAgent, setFilterAgent] = useState("all");
  const [filterKind, setFilterKind]   = useState("all");
  const agentList = Array.from(new Set(logs.map(l => l.agent))).sort();
  const filtered  = logs.filter(l =>
    (filterAgent === "all" || l.agent === filterAgent) &&
    (filterKind  === "all" || l.kind  === filterKind)
  );

  return (
    <div className="card">
      <div className="filter-bar">
        <span style={{ fontWeight: 600 }}>Activity Log <span className="muted sm">({filtered.length} entries)</span></span>
        <select className="select" value={filterAgent} onChange={e => setFilterAgent(e.target.value)}>
          <option value="all">All agents</option>
          {agentList.map(a => <option key={a} value={a}>{a}</option>)}
        </select>
        <select className="select" value={filterKind} onChange={e => setFilterKind(e.target.value)}>
          <option value="all">All types</option>
          <option value="event">Events</option>
          <option value="task">Tasks</option>
          <option value="message">Messages</option>
        </select>
      </div>

      {filtered.length === 0 && <div className="empty">No activity yet. Agents will log here as they work.</div>}

      <div className="log-feed">
        {filtered.map(e => <LogRow key={e.id} entry={e} expanded />)}
      </div>
    </div>
  );
}

function LogRow({ entry, expanded }: { entry: LogEntry; expanded?: boolean }) {
  const [open, setOpen] = useState(false);
  const accent = AGENT_COLORS[entry.agent] ?? "var(--text3)";
  const time = formatTime(entry.ts);

  return (
    <div>
      <div
        className={`log-entry ${entry.kind}`}
        onClick={() => setOpen(o => !o)}
        style={{ cursor: "pointer" }}
      >
        <span className="log-time">{time}</span>
        <AgentPill name={entry.agent} />
        <span className="log-msg">{entry.message}</span>
        {expanded && entry.detail && Object.keys(entry.detail).length > 0 && (
          <span style={{ fontSize: 11, color: "var(--text3)" }}>{open ? "▴" : "▾"}</span>
        )}
      </div>
      {open && entry.detail && (
        <div style={{ padding: "6px 14px 10px 134px", fontSize: 12, color: "var(--text3)", background: "rgba(255,255,255,0.02)", borderRadius: "0 0 6px 6px" }}>
          {Object.entries(entry.detail).map(([k, v]) => v ? (
            <div key={k} style={{ marginBottom: 3 }}>
              <span style={{ color: accent, marginRight: 6 }}>{k}:</span>
              <span>{String(v).slice(0, 200)}</span>
            </div>
          ) : null)}
        </div>
      )}
    </div>
  );
}

// ── Approvals ─────────────────────────────────────────────────────────────────

function Approvals({ approvals, onResolve }: {
  approvals: Approval[];
  onResolve: (id: number, action: "approve" | "reject" | "defer") => Promise<void>;
}) {
  if (approvals.length === 0) {
    return (
      <div className="card">
        <div className="empty">No pending approvals. When agents need your sign-off, they'll appear here.</div>
      </div>
    );
  }

  return (
    <div className="stack">
      <div className="approval-command">
        <div>
          <div className="card-title-lg">Approval Command Queue</div>
          <p>Decision packets show linked tasks, improvement context, risks, checks, and unknowns from the same evidence model Roderick uses.</p>
        </div>
        <div className="approval-count">{approvals.length}</div>
      </div>
      {approvals.map(a => (
        <div key={a.id} className="approval-card">
          <div className="approval-type">{a.request_type.replace(/_/g, " ")}</div>
          <div className="approval-meta">
            #{a.id} · {formatDateTime(a.created_at)} local
            {a.task_id ? ` · Task #${a.task_id}` : ""}
          </div>
          <div className="approval-desc">{a.description}</div>
          <div className="approval-grid">
            <div className="approval-section">
              <span>Linked work</span>
              {a.task ? (
                <>
                  <strong>#{a.task.id} {a.task.task_type}</strong>
                  <p>{a.task.description}</p>
                  <div className="approval-mini-row">
                    <AgentPill name={a.task.from_agent} />
                    <span>to</span>
                    <AgentPill name={a.task.to_agent} />
                    <span className={`badge ${a.task.status}`}>{a.task.status}</span>
                  </div>
                </>
              ) : <p>No linked task row was found.</p>}
            </div>
            <div className="approval-section">
              <span>Decision packet</span>
              {a.decision_packet?.why && <p className="verified-line">{a.decision_packet.why}</p>}
              {a.decision_packet?.if_declined && <p className="risk-line">If declined: {a.decision_packet.if_declined}</p>}
              {(a.decision_packet?.verified || []).slice(0, 4).map(v => <p key={v} className="verified-line">{v}</p>)}
              {(a.decision_packet?.checks || []).slice(0, 4).map(v => <p key={v}>{v}</p>)}
              {(a.decision_packet?.risks || []).slice(0, 3).map(v => <p key={v} className="risk-line">{v}</p>)}
              {(a.decision_packet?.unknowns || []).slice(0, 3).map(v => <p key={v} className="unknown-line">{v}</p>)}
            </div>
          </div>
          {a.improvement && (
            <div className="approval-improvement">
              <strong>Improvement #{a.improvement.id}: {a.improvement.title}</strong>
              <p>{a.improvement.description || a.improvement.origin_signal}</p>
              <span>{a.improvement.priority} priority / {a.improvement.risk_level} risk / {a.improvement.status}</span>
            </div>
          )}
          {!!a.evidence_events?.length && (
            <div className="approval-events">
              {a.evidence_events.slice(0, 4).map(e => (
                <span key={e.id}>{formatTime(e.created_at)} {e.event_type}</span>
              ))}
            </div>
          )}
          <div className="approval-actions">
            <button className="btn primary" onClick={() => onResolve(a.id, "approve")}>✓ Approve</button>
            <button className="btn" onClick={() => onResolve(a.id, "defer")}>⏸ Defer</button>
            <button className="btn danger" onClick={() => onResolve(a.id, "reject")}>✕ Reject</button>
          </div>
        </div>
      ))}
    </div>
  );
}

// ── Atlas ─────────────────────────────────────────────────────────────────────

const STAGES = [
  { id: "interview_ready", label: "Interview Ready", color: "#2dd4bf" },
  { id: "project_used",    label: "Project Used",    color: "#4ade80" },
  { id: "practiced",       label: "Practiced",       color: "#60a5fa" },
  { id: "learning",        label: "Learning",        color: "#818cf8" },
  { id: "introduced",      label: "Introduced",      color: "#a78bfa" },
  { id: "unknown",         label: "Queued",          color: "#4b5563" },
];

function Atlas({ skills, lesson, learning }: { skills: Record<string, string>; lesson: Record<string, unknown>; learning: AtlasLearning | null }) {
  const grouped = useMemo(() => {
    const g: Record<string, string[]> = {};
    for (const [sk, st] of Object.entries(skills)) {
      if (!g[st]) g[st] = [];
      g[st].push(sk);
    }
    return g;
  }, [skills]);

  const total = Object.keys(skills).length;

  const entries = learning?.entries || [];
  const linkedInUrl = String(lesson.linkedin_learning_search_url || "");
  const linkedInQuery = String(lesson.linkedin_learning_search_query || "");
  const outcome = String(lesson.shareable_outcome || "");
  const proof = String(lesson.portfolio_receipt || "");
  const coaching = learning?.coaching_recommendations || [];
  const lessonStatus = learning?.status || {};
  const todayStatus = String(lessonStatus.status || "no status today");
  const postponedUntil = String(lessonStatus.postponed_until || "");
  const lastLessonAt = String((lesson as Record<string, unknown>)._generated_at || lessonStatus.last_generated_at || "");
  const currentTopic = String(lesson.topic || lessonStatus.current_topic || lessonStatus.next_lesson_topic || "");

  return (
    <div className="stack">
      <div className="atlas-hero">
        <div>
          <div className="card-title-lg">Atlas Learning Studio</div>
          <p>
            Lessons are tied to the Roderick ecosystem, job-market evidence, and shareable proof you can show recruiters.
          </p>
          <div className="workflow-meta">
            <span className="chip accent">{total} tracked skills</span>
            <span className={`badge ${String(learning?.status?.status || "unknown")}`}>{String(learning?.status?.status || "no status today")}</span>
          </div>
        </div>
        <div className="atlas-proof-card">
          <span>Credential path</span>
          <strong>LinkedIn Learning + project proof</strong>
          <small>{learning?.linkedin_learning_note || "Atlas tracks completions only when you record them."}</small>
        </div>
      </div>

      <div className="grid2">
        <div className="card">
          <div className="card-title-lg">Session State</div>
          <div className="task-detail-list">
            <div><span>Status</span><strong>{todayStatus}</strong></div>
            <div><span>Current topic</span><strong>{currentTopic || "none queued"}</strong></div>
            <div><span>Last lesson</span><strong>{lastLessonAt ? formatDateTime(lastLessonAt) : "not recorded"}</strong></div>
            <div><span>Receipts</span><strong>{entries.length}</strong></div>
          </div>
          {postponedUntil && (
            <div className="task-detail-body" style={{ color: "var(--amber)" }}>
              Postponed until {formatDateTime(postponedUntil)}.
            </div>
          )}
        </div>

        <div className="card">
        <div className="card-title-lg">Skill Board <span className="muted">({total} skills)</span></div>
        <div className="skill-board">
          {STAGES.filter(s => grouped[s.id]?.length).map(({ id, label, color }) => (
            <div key={id} className="skill-stage">
              <div className="skill-stage-label" style={{ color }}>{label} ({grouped[id].length})</div>
              <div className="skill-chips">
                {grouped[id].map(sk => (
                  <span key={sk} className="skill-chip"
                    style={{ borderColor: `${color}44`, color, background: `${color}10` }}>
                    {sk}
                  </span>
                ))}
              </div>
            </div>
          ))}
          {total === 0 && <div className="empty" style={{ padding: "20px 0" }}>No skills tracked yet. Atlas will populate this as lessons are delivered.</div>}
        </div>
      </div>

        <div className="card">
        <div className="card-title-lg">Today's Lesson</div>
        {Object.keys(lesson).length === 0 ? (
          <div className="empty" style={{ padding: "20px 0" }}>No lesson today. Triggers at 09:00 local time or message Roderick.</div>
        ) : (
          <div className="stack" style={{ gap: 12 }}>
            {(lesson.topic as string) && (
              <div style={{ fontSize: 16, fontWeight: 600, color: "var(--blue)" }}>{lesson.topic as string}</div>
            )}
            {(lesson.summary as string) && (
              <p style={{ color: "var(--text2)", fontSize: 13, lineHeight: 1.7 }}>{lesson.summary as string}</p>
            )}
            {linkedInQuery && (
              <div className="atlas-credential-box">
                <div className="sidebar-label" style={{ paddingLeft: 0 }}>LinkedIn Learning</div>
                <strong>{linkedInQuery}</strong>
                <p>{outcome || "Use this lesson to create a recruiter-visible proof point."}</p>
                {linkedInUrl && <a className="btn sm primary" href={linkedInUrl} target="_blank" rel="noreferrer">Open search</a>}
              </div>
            )}
            {Array.isArray(lesson.key_points) && (
              <ul style={{ paddingLeft: 18, color: "var(--text2)", fontSize: 13, lineHeight: 1.8 }}>
                {(lesson.key_points as string[]).map((p, i) => <li key={i}>{p}</li>)}
              </ul>
            )}
            {(lesson.exercise as string) && (
              <div style={{ background: "rgba(96,165,250,0.07)", border: "1px solid rgba(96,165,250,0.2)", borderRadius: 8, padding: "12px 14px" }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: "var(--blue)", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>Exercise</div>
                <p style={{ fontSize: 13, color: "var(--text2)", lineHeight: 1.7 }}>{lesson.exercise as string}</p>
              </div>
            )}
            {proof && (
              <div className="atlas-proof-note">
                <div className="sidebar-label" style={{ paddingLeft: 0 }}>Proof Receipt</div>
                {proof}
              </div>
            )}
            {!lesson.topic && <div className="empty" style={{ padding: "12px 0" }}>Lesson not yet loaded — Atlas generates one at 09:00 local time or when you ask Roderick.</div>}
          </div>
        )}
        </div>
      </div>

      <div className="grid2">
        <div className="card">
          <div className="card-title-lg">Atlas Next Moves</div>
          {coaching.length === 0 ? (
            <div className="empty compact">Atlas will suggest next moves when lesson evidence is available.</div>
          ) : coaching.slice(0, 4).map((rec, i) => (
            <div key={i} className="learning-receipt">
              <strong>{String(rec.title || "Learning move")}</strong>
              <p>{String(rec.summary || "")}</p>
              <small>{String(rec.evidence || "atlas evidence")}</small>
            </div>
          ))}
        </div>

        <div className="card">
          <div className="card-title-lg">Learning Receipts <span className="muted">({entries.length})</span></div>
          {entries.length === 0 ? (
            <div className="empty" style={{ padding: "20px 0" }}>No recorded completions yet. Finish a lesson or paste a LinkedIn Learning certificate link through Atlas.</div>
          ) : entries.slice(0, 12).map((entry, i) => (
            <div key={String(entry.id || i)} className="learning-receipt">
              <strong>{String(entry.topic || "Learning item")}</strong>
              <p>{String(entry.shareable_outcome || entry.note || "Completion recorded.")}</p>
              {entry.linkedin_certificate_url ? (
                <a href={String(entry.linkedin_certificate_url)} target="_blank" rel="noreferrer">Certificate</a>
              ) : <span className="muted sm">No certificate link recorded</span>}
              <small>{formatDateTime(String(entry.created_at || ""))} local</small>
            </div>
          ))}
        </div>

        <div className="card">
          <div className="card-title-lg">Recruiter Visibility</div>
          <div className="atlas-path">
            <div><span>1</span>Study with Atlas</div>
            <div><span>2</span>Complete a LinkedIn Learning course or path</div>
            <div><span>3</span>Record the certificate/share URL in Atlas</div>
            <div><span>4</span>Connect it to a Forge portfolio artifact</div>
            <div><span>5</span>Use it in LinkedIn, applications, and interviews</div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Opportunities ─────────────────────────────────────────────────────────────

function OperatorExecutionPanel({ data, tasks, onControlAction, onSendMessage }: {
  data: OperatorData;
  tasks: TaskRow[];
  onControlAction: (payload: Record<string, unknown>) => Promise<void>;
  onSendMessage: (agent: string, message: string) => Promise<string>;
}) {
  const [actionState, setActionState] = useState<Record<string, "working" | "sent" | "failed">>({});
  const [chatText, setChatText] = useState("");
  const [chatState, setChatState] = useState<"idle" | "sending" | "sent" | "failed">("idle");
  const [chatReply, setChatReply] = useState("");
  const operatorTasks = tasks.filter(t => t.to_agent === "operator");
  const pendingTasks = operatorTasks.filter(t => ["pending", "in_progress", "blocked"].includes(t.status));
  const uniqueInitiatives = useMemo(() => {
    const seen = new Set<string>();
    return data.initiatives.filter(initiative => {
      const key = `${initiative.title}::${initiative.status}::${initiative.priority}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }, [data.initiatives]);

  if (!data.loaded) {
    return <div className="card"><div className="empty">Loading Operator data...</div></div>;
  }

  async function retryTask(taskId: number) {
    const key = `retry_${taskId}`;
    setActionState(s => ({ ...s, [key]: "working" }));
    try {
      await onControlAction({ action: "requeue_task", task_id: taskId, reason: "operator dashboard retry" });
      setActionState(s => ({ ...s, [key]: "sent" }));
    } catch {
      setActionState(s => ({ ...s, [key]: "failed" }));
    }
  }

  const statusColor = (s: string) => {
    if (s.includes("CRITICAL") || s.includes("blocked")) return "var(--red)";
    if (s.includes("HIGH") || s.includes("in_progress")) return "var(--amber)";
    if (s.includes("needs_approval")) return "var(--amber)";
    if (s.includes("completed")) return "var(--green)";
    return "var(--text2)";
  };

  async function sendChat() {
    const message = chatText.trim();
    if (!message || chatState === "sending") return;
    setChatState("sending");
    setChatReply("");
    try {
      const reply = await onSendMessage("operator", message);
      setChatReply(reply);
      setChatText("");
      setChatState("sent");
    } catch (error) {
      setChatReply(error instanceof Error ? error.message : "Could not reach Operator");
      setChatState("failed");
    }
  }

  return (
    <div className="stack">
      {/* Header status bar */}
      <div className="card" style={{ padding: "10px 16px", display: "flex", gap: 24, alignItems: "center", flexWrap: "wrap" }}>
        <div>
          <span style={{ fontSize: 11, color: "var(--text3)", marginRight: 6 }}>INITIATIVES</span>
          <span style={{ fontWeight: 700, color: "var(--operator, #fb923c)" }}>{uniqueInitiatives.length}</span>
        </div>
        <div>
          <span style={{ fontSize: 11, color: "var(--text3)", marginRight: 6 }}>PENDING DECISIONS</span>
          <span style={{ fontWeight: 700, color: data.pending_approvals.length ? "var(--amber)" : "var(--text2)" }}>
            {data.pending_approvals.length}
          </span>
        </div>
        <div>
          <span style={{ fontSize: 11, color: "var(--text3)", marginRight: 6 }}>ACTIVE TASKS</span>
          <span style={{ fontWeight: 700, color: pendingTasks.length ? "var(--green)" : "var(--text2)" }}>
            {pendingTasks.length}
          </span>
        </div>
        <div>
          <span style={{ fontSize: 11, color: "var(--text3)", marginRight: 6 }}>REPORTS</span>
          <span style={{ fontWeight: 700 }}>{data.recent_reports.length}</span>
        </div>
      </div>

      <div className="grid2">
        <div className="card">
          <div className="card-title-lg" style={{ color: "#fb923c" }}>Direct Operator Chat</div>
          <div className="muted sm" style={{ marginBottom: 10 }}>
            Talk to Operator directly from the dashboard. Messages stay in the Operator lane instead of going through Roderick.
          </div>
          <div className="dashboard-message" style={{ marginBottom: 12 }}>
            <textarea
              value={chatText}
              onChange={e => setChatText(e.target.value)}
              placeholder="Ask Operator about an initiative, blocker, next move, or execution detail…"
              rows={4}
            />
            <button className="message-send" onClick={() => void sendChat()} disabled={!chatText.trim() || chatState === "sending"}>
              {chatState === "sending" ? "Sending…" : "Send to Operator"}
            </button>
          </div>
          {chatReply && (
            <div className={`operator-chat-reply ${chatState === "failed" ? "failed" : ""}`}>{chatReply}</div>
          )}
          <div className="operator-chat-thread">
            {data.chat_messages.length === 0 ? (
              <div className="empty compact">No Operator dashboard conversation yet.</div>
            ) : data.chat_messages.slice(-10).map(message => (
              <div key={message.id} className={`operator-chat-row ${message.from_agent === "dashboard" ? "outbound" : "inbound"}`}>
                <div className="operator-chat-meta">
                  <AgentPill name={message.from_agent === "dashboard" ? "dashboard" : "operator"} />
                  <span>{formatDateTime(message.created_at)}</span>
                </div>
                <div className="operator-chat-bubble">{message.message}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Initiatives */}
        <div className="card">
          <div className="card-title-lg" style={{ color: "#fb923c" }}>Active Initiatives</div>
          {data.initiatives.length === 0 ? (
            <div className="empty">No initiatives parsed. Check memory/initiatives.md.</div>
          ) : (
            <div className="stack" style={{ gap: 12 }}>
              {uniqueInitiatives.map((ini, i) => (
                <div key={i} style={{ borderLeft: "2px solid #fb923c", paddingLeft: 10 }}>
                  <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 2 }}>{ini.title}</div>
                  <div style={{ display: "flex", gap: 8, marginBottom: 4 }}>
                    {ini.status && (
                      <span className="chip" style={{ color: statusColor(ini.status), borderColor: statusColor(ini.status) }}>
                        {ini.status}
                      </span>
                    )}
                    {ini.priority && (
                      <span className="chip" style={{ fontSize: 10 }}>{ini.priority}</span>
                    )}
                  </div>
                  {ini.blockers && ini.blockers.length > 0 && (
                    <div style={{ fontSize: 11, color: "var(--amber)", marginTop: 2 }}>
                      ⚠ {ini.blockers.slice(0, 2).join(" · ")}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Pending approvals + blocked tasks */}
        <div className="card">
          <div className="card-title-lg">Pending Decisions</div>
          {data.pending_approvals.length === 0 && data.blocked_tasks.length === 0 ? (
            <div className="empty">No pending decisions or blocked tasks.</div>
          ) : (
            <div className="stack" style={{ gap: 8 }}>
              {data.pending_approvals.map(a => (
                <div key={a.id} className="operator-rec warn" style={{ marginBottom: 0 }}>
                  <strong>Approval #{a.id} — {a.request_type}</strong>
                  <p style={{ fontSize: 12, margin: "2px 0" }}>{a.description.slice(0, 200)}</p>
                  <small style={{ color: "var(--text3)" }}>{formatDateTime(a.created_at)}</small>
                </div>
              ))}
              {data.blocked_tasks.map(t => (
                <div key={t.id} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 12, padding: "6px 0", borderBottom: "1px solid var(--border)" }}>
                  <div>
                    <span className="chip">{t.task_type}</span>
                    <span style={{ marginLeft: 6 }}>{t.description.slice(0, 80)}</span>
                  </div>
                  <button
                    className={`btn sm ${actionState[`retry_${t.id}`] === "sent" ? "" : ""}`}
                    disabled={actionState[`retry_${t.id}`] === "working"}
                    onClick={() => void retryTask(t.id)}
                  >
                    {actionState[`retry_${t.id}`] === "working" ? "..." : actionState[`retry_${t.id}`] === "sent" ? "Queued" : "Retry"}
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Recent operator task reports */}
      {data.recent_reports.length > 0 && (
        <div className="card">
          <div className="card-title-lg">Recent Execution Reports</div>
          <div className="stack" style={{ gap: 6 }}>
            {data.recent_reports.map((r, i) => (
              <div key={i} style={{ display: "flex", gap: 12, alignItems: "flex-start", padding: "6px 0", borderBottom: "1px solid var(--border)", fontSize: 12 }}>
                <span className="chip" style={{ flexShrink: 0 }}>{r.task_type || "execution"}</span>
                <div style={{ flex: 1 }}>
                  <div style={{ marginBottom: 2 }}>{r.task_summary || "No summary"}</div>
                  <div style={{ color: "var(--text3)", fontSize: 11 }}>
                    {r.initiative && <span style={{ marginRight: 8 }}>Initiative: {r.initiative}</span>}
                    {r.executed_at && <span>{formatDateTime(r.executed_at)}</span>}
                    {r.approval_required && (
                      <span style={{ color: "var(--amber)", marginLeft: 8 }}>🔐 Approval needed</span>
                    )}
                  </div>
                </div>
                <span className="chip" style={{ color: statusColor(r.status || ""), flexShrink: 0 }}>
                  {r.status || ""}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Operator tasks in queue */}
      {operatorTasks.length > 0 && (
        <div className="card">
          <div className="card-title-lg">Task Queue <span className="muted">({operatorTasks.length})</span></div>
          <div style={{ overflowX: "auto" }}>
            <table className="table">
              <thead>
                <tr>
                  <th>#</th><th>Type</th><th>Description</th><th>Status</th><th>Created</th>
                </tr>
              </thead>
              <tbody>
                {operatorTasks.slice(0, 20).map(t => (
                  <tr key={t.id}>
                    <td style={{ color: "var(--text3)" }}>{t.id}</td>
                    <td><span className="chip">{t.task_type}</span></td>
                    <td style={{ maxWidth: 280 }}>{t.description.slice(0, 100)}</td>
                    <td><span className={`status-dot ${t.status}`}>{t.status}</span></td>
                    <td style={{ color: "var(--text3)", whiteSpace: "nowrap" }}>{formatDateTime(t.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Business ops context (collapsed) */}
      {data.business_ops_summary && (
        <details>
          <summary style={{ cursor: "pointer", padding: "8px 0", color: "var(--text2)", fontSize: 13 }}>
            Business Ops Context (memory/business_ops.md)
          </summary>
          <div className="card" style={{ marginTop: 8 }}>
            <pre className="pre" style={{ maxHeight: 300, overflow: "auto", fontSize: 11 }}>
              {data.business_ops_summary}
            </pre>
          </div>
        </details>
      )}

      {/* Note when dedicated chat not configured */}
      <div className="card" style={{ padding: "8px 16px", fontSize: 11, color: "var(--text3)" }}>
        <strong>Operator chat:</strong> Set OPERATOR_TELEGRAM_BOT_TOKEN and OPERATOR_TELEGRAM_CHAT_ID to enable dedicated Telegram chat.
        Without them, Operator Telegram delivery is skipped; the dashboard chat above still works.
      </div>
    </div>
  );
}

function Opportunities({ data }: { data: OppData }) {
  const oppTitle = (r: Record<string, unknown>, fallback: string) =>
    String(r.opportunity_summary || r.opportunity_name || r.opportunity || r.title || fallback);
  return (
    <div className="stack">
      <div className="grid2">
        <div className="card">
          <div className="card-title-lg">Recent Opportunities <span className="muted">last 30 days</span></div>
          {data.recent.length === 0 ? (
            <div className="empty" style={{ padding: "20px 0" }}>None yet. Venture triggers on opportunity messages or on its 6h schedule.</div>
          ) : (
            <div>
              {data.recent.map((o, i) => (
                <div key={i} className="opp-item">
                  <div className="opp-title">{String(o.title || "Opportunity")}</div>
                  <div className="opp-meta">
                    <span className="chip">{String(o.category || "")}</span>
                    {o.capital ? <span>${Number(o.capital).toLocaleString()}</span> : null}
                    {o.risk ? <span style={{ color: "var(--amber)" }}>{String(o.risk)} risk</span> : null}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="card">
          <div className="card-title-lg">Opportunity Log</div>
          <pre className="pre" style={{ maxHeight: 400, overflow: "auto" }}>{data.log || "No log yet."}</pre>
        </div>
      </div>

      {data.reports.length > 0 && (
        <div className="card">
          <div className="card-title-lg">Full Reports <span className="muted">({data.reports.length})</span></div>
          {data.reports.map((r, i) => (
            <details key={i}>
              <summary>
                <div className="report-summary">{oppTitle(r, `Report ${i + 1}`).slice(0, 100)}</div>
              </summary>
              <div className="report-body">
                {summarizeObject(r as Record<string, unknown>, 10).map(line => <div key={line}>{line}</div>)}
              </div>
            </details>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Shared components ─────────────────────────────────────────────────────────

function Pipeline({ data }: { data: PipelineData | null }) {
  if (!data) {
    return <div className="card"><div className="empty">Pipeline data is not available yet.</div></div>;
  }
  const statuses = Object.entries(data.by_status);
  return (
    <div className="stack">
      <div className="grid3">
        <StatCard label="Active" value={data.active_count} sub="open improvements" />
        <StatCard label="Total" value={data.total_count} sub="tracked improvements" />
        <div className="card">
          <div className="card-title">Generated</div>
          <div className="text2">{formatDateTime(data.generated_at)} local</div>
        </div>
      </div>
      {statuses.length === 0 && (
        <div className="card"><div className="empty">No improvement candidates yet.</div></div>
      )}
      {statuses.map(([status, items]) => (
        <div key={status} className="card">
          <div className="card-title-lg">
            <span className={`badge ${status}`}>{status}</span>
            <span className="muted">{items.length} item{items.length === 1 ? "" : "s"}</span>
          </div>
          <table className="table">
            <thead>
              <tr><th>#</th><th>Title</th><th>Origin</th><th>Priority</th><th>Risk</th><th>Updated</th></tr>
            </thead>
            <tbody>
              {items.map(item => (
                <tr key={item.id}>
                  <td className="muted sm">#{item.id}</td>
                  <td className="truncate">{item.title}</td>
                  <td><AgentPill name={item.origin_agent || "roderick"} /></td>
                  <td><span className="chip">{item.priority}</span></td>
                  <td><span className="chip">{item.risk_level}</span></td>
                  <td className="muted sm">{formatDateTime(item.updated_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}

function ForgeArtifacts({ artifacts, workflow, onControlAction }: {
  artifacts: ForgeArtifact[];
  workflow: ForgeWorkflow | null;
  onControlAction: (action: Record<string, unknown>) => Promise<void>;
}) {
  const [filter, setFilter] = useState("all");
  const [validatingArtifact, setValidatingArtifact] = useState<number | null>(null);
  const types = Array.from(new Set(artifacts.map(a => a.artifact_type))).sort();
  const filtered = artifacts.filter(a => filter === "all" || a.artifact_type === filter);
  const byTask = filtered.reduce<Record<string, ForgeArtifact[]>>((acc, artifact) => {
    const key = String(artifact.task_id);
    if (!acc[key]) acc[key] = [];
    acc[key].push(artifact);
    return acc;
  }, {});

  async function validateArtifact(item: ForgeArtifact) {
    setValidatingArtifact(item.id);
    try {
      await onControlAction({
        action: "validate_forge_artifact",
        artifact_id: item.id,
        task_id: item.task_id,
        reason: `Dashboard operator requested Sentinel validation for Forge artifact ${item.relative_path || item.path}`,
      });
    } finally {
      setValidatingArtifact(null);
    }
  }

  return (
    <div className="stack">
      <div className="workflow-panel">
        <div>
          <div className="card-title-lg">Forge CI/CD Gate</div>
          <p className="text2">
            {workflow?.truthfulness || "Workflow readiness is unknown until the API reports it."}
          </p>
          <div className="workflow-meta">
            <span className={`badge ${workflow?.status || "unknown"}`}>Git {workflow?.status || "unknown"}</span>
            <span className="chip">{workflow?.source_of_truth || "unknown source"}</span>
            {workflow?.github_connected && <span className="chip accent">GitHub connected</span>}
            {workflow?.workflow_configured && <span className="chip accent">Actions gate ready</span>}
          </div>
          {workflow?.github_url && (
            <a className="workflow-link" href={workflow.github_url} target="_blank" rel="noreferrer">
              {workflow.github_repository || workflow.github_url}
            </a>
          )}
        </div>
        <div className="workflow-columns">
          <div>
            <div className="sidebar-label" style={{ paddingLeft: 0 }}>Promotion Flow</div>
            {(workflow?.promotion_flow || []).map((step, i) => <div key={step} className="workflow-step"><span>{i + 1}</span>{step}</div>)}
          </div>
          <div>
            <div className="sidebar-label" style={{ paddingLeft: 0 }}>Sentinel Gate</div>
            {(workflow?.sentinel_gate || []).map(check => <div key={check} className="workflow-check">{check}</div>)}
          </div>
          <div>
            <div className="sidebar-label" style={{ paddingLeft: 0 }}>Phone Approval</div>
            {(workflow?.phone_approval_flow || []).map((step, i) => <div key={step} className="workflow-step"><span>{i + 1}</span>{step}</div>)}
          </div>
        </div>
      </div>

      <div className="card">
        <div className="filter-bar">
          <span style={{ fontWeight: 600 }}>Forge Files <span className="muted sm">({filtered.length})</span></span>
          <select className="select" value={filter} onChange={e => setFilter(e.target.value)}>
            <option value="all">All artifact types</option>
            {types.map(type => <option key={type} value={type}>{type}</option>)}
          </select>
        </div>
        <div className="muted sm">
          Forge-created outputs are staged under the managed local artifact workspace before promotion.
        </div>
      </div>

      {filtered.length === 0 && (
        <div className="card"><div className="empty">No Forge artifacts have been recorded yet.</div></div>
      )}

      {Object.entries(byTask).map(([taskId, items]) => (
        <div key={taskId} className="card">
          <div className="card-title-lg">
            Task #{taskId}
            <span className="muted">{items.length} artifact{items.length === 1 ? "" : "s"}</span>
          </div>
          <table className="table">
            <thead>
              <tr><th>Type</th><th>File</th><th>Origin</th><th>Approval</th><th>Validation</th><th>Action</th><th>Created</th></tr>
            </thead>
            <tbody>
              {items.map(item => (
                <tr key={item.id}>
                  <td><span className="chip">{item.artifact_type}</span></td>
                  <td>
                    <div className="truncate" style={{ maxWidth: 520 }}>{item.relative_path || item.path}</div>
                    {item.summary && <div className="muted sm">{item.summary}</div>}
                  </td>
                  <td>
                    <div className="muted sm">Task #{item.task_id}</div>
                    <div className="muted sm">{artifactOrigin(item)}</div>
                  </td>
                  <td><span className={`badge ${item.approval_state}`}>{item.approval_state}</span></td>
                  <td><span className={`badge ${item.validation_state}`}>{item.validation_state}</span></td>
                  <td>
                    <button className="btn sm" disabled={validatingArtifact === item.id} onClick={() => validateArtifact(item)}>
                      {validatingArtifact === item.id ? "Queuing..." : "Run Sentinel"}
                    </button>
                  </td>
                  <td className="muted sm">{formatDateTime(item.created_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="muted sm" style={{ marginTop: 10, wordBreak: "break-all" }}>
            Root: {items[0]?.artifact_root || "unknown"}
          </div>
        </div>
      ))}
    </div>
  );
}

function artifactOrigin(item: ForgeArtifact): string {
  const metadata = item.metadata || {};
  const candidates = [
    metadata.source,
    metadata.origin,
    metadata.origin_agent,
    metadata.requested_by,
    metadata.requested_path ? `requested path: ${metadata.requested_path}` : "",
  ].filter(Boolean);
  if (candidates.length) return String(candidates[0]);
  if (item.artifact_type.includes("plan")) return "Forge plan artifact";
  if (item.artifact_type === "manifest") return "Forge workspace manifest";
  return "Forge task output";
}

function MemoryGraph({ graph, onControlAction }: {
  graph: MemoryGraphData | null;
  onControlAction: (action: Record<string, unknown>) => Promise<void>;
}) {
  const [selectedType, setSelectedType] = useState("all");
  const [selectedAgent, setSelectedAgent] = useState("all");
  const [selected, setSelected] = useState<GraphNode | null>(null);
  const nodes = graph?.nodes || [];
  const edges = graph?.edges || [];
  const types = Array.from(new Set(nodes.map(n => n.type))).sort();
  const agents = Array.from(new Set(nodes.map(n => n.agent).filter(Boolean) as string[])).sort();
  const filteredNodes = nodes.filter(n =>
    (selectedType === "all" || n.type === selectedType) &&
    (selectedAgent === "all" || n.agent === selectedAgent)
  );
  const visibleIds = new Set(filteredNodes.map(n => n.id));
  const filteredEdges = edges.filter(e => visibleIds.has(e.source) && visibleIds.has(e.target));
  const layout = useMemo(() => graphLayout(filteredNodes), [filteredNodes]);
  const nodeById = useMemo(() => new Map(nodes.map(n => [n.id, n])), [nodes]);
  const selectedTraffic = useMemo<NodeTraffic | null>(() => {
    if (!selected) return null;
    const relatedEdges = edges
      .filter(edge => edge.source === selected.id || edge.target === selected.id)
      .sort((a, b) => (b.timestamp || "").localeCompare(a.timestamp || ""))
      .slice(0, 24);
    const relatedIds = new Set(relatedEdges.map(edge => edge.source === selected.id ? edge.target : edge.source));
    return {
      inbound: relatedEdges.filter(edge => edge.target === selected.id),
      outbound: relatedEdges.filter(edge => edge.source === selected.id),
      related: Array.from(relatedIds).map(id => nodeById.get(id)).filter(Boolean) as GraphNode[],
    };
  }, [edges, nodeById, selected]);

  if (!graph) {
    return <div className="card"><div className="empty">Memory graph is loading from evidence sources.</div></div>;
  }

  return (
    <div className="memory-page">
      <div className="memory-toolbar card">
        <div>
          <div className="card-title-lg">Live Neural Evidence Graph</div>
          <p>Every node and pulse is backed by a task, report, approval, policy, message, artifact, validation, or event.</p>
        </div>
        <div className="memory-stats">
          <span>{graph.stats.node_count} nodes</span>
          <span>{graph.stats.edge_count} edges</span>
          <span>{graph.stats.verified_count} verified</span>
          <span>{graph.stats.unknown_count} unknown</span>
        </div>
        <select className="select" value={selectedType} onChange={e => setSelectedType(e.target.value)}>
          <option value="all">All node types</option>
          {types.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
        <select className="select" value={selectedAgent} onChange={e => setSelectedAgent(e.target.value)}>
          <option value="all">All agents</option>
          {agents.map(a => <option key={a} value={a}>{a}</option>)}
        </select>
        <div className="memory-3d-note">3D orbit • drag to rotate • wheel to zoom • right-drag to pan</div>
      </div>

      <div className="memory-grid">
        <div className="memory-canvas card">
          <div className="memory-hint">Every glow and pulse still maps to real evidence; this is the same graph rendered in 3D.</div>
          <MemoryGraph3D
            graph={{ nodes: filteredNodes, edges: filteredEdges }}
            selectedId={selected?.id || null}
            onSelect={setSelected}
          />
        </div>

        <div className="memory-inspector card">
          <div className="card-title-lg">Proof Inspector</div>
          {selected ? (
            <div className="stack" style={{ gap: 10 }}>
              <div className="memory-node-title">
                <span className={`evidence-pill ${selected.evidence}`}>{selected.evidence}</span>
                <strong>{selected.label}</strong>
              </div>
              <p>{selected.summary || "No summary recorded."}</p>
              <div className="kv-row"><span className="kv-key">Type</span><span className="kv-val">{selected.type}</span></div>
              <div className="kv-row"><span className="kv-key">Agent</span><span className="kv-val">{selected.agent || "none"}</span></div>
              <div className="kv-row"><span className="kv-key">Status</span><span className="kv-val">{selected.status || "unknown"}</span></div>
              <div className="kv-row"><span className="kv-key">Source</span><span className="kv-val">{selected.source || "unknown"}</span></div>
              <div className="kv-row"><span className="kv-key">Time</span><span className="kv-val">{selected.timestamp ? formatDateTime(selected.timestamp) : "unknown"}</span></div>
              {selected.agent && (
                <button className="btn primary" onClick={() => onControlAction({ action: "diagnose_agent", agent: selected.agent, reason: `Memory graph inspection for ${selected.label}` })}>
                  Ask Merlin to inspect this
                </button>
              )}
            </div>
          ) : (
            <div className="empty compact">Click a node to see the evidence behind it.</div>
          )}
          {!!graph.stats.warnings?.length && (
            <div className="memory-warnings">
              {graph.stats.warnings.slice(0, 4).map(w => <span key={w}>{w}</span>)}
            </div>
          )}
        </div>
      </div>
      {selected && selectedTraffic && (
        <MemoryNodeTraffic selected={selected} traffic={selectedTraffic} nodeById={nodeById} />
      )}
    </div>
  );
}

function MemoryNodeTraffic({ selected, traffic, nodeById }: {
  selected: GraphNode;
  traffic: NodeTraffic;
  nodeById: Map<string, GraphNode>;
}) {
  const events = [...traffic.inbound, ...traffic.outbound]
    .sort((a, b) => (b.timestamp || "").localeCompare(a.timestamp || ""))
    .slice(0, 16);
  return (
    <div className="memory-traffic card">
      <div className="memory-traffic-head">
        <div>
          <div className="card-title-lg">Realtime Node Traffic</div>
          <p>Live evidence around <strong>{selected.label}</strong>: messages, task links, policy changes, reports, validations, and artifact flow.</p>
        </div>
        <div className="memory-stats">
          <span>{traffic.inbound.length} inbound</span>
          <span>{traffic.outbound.length} outbound</span>
          <span>{traffic.related.length} related</span>
        </div>
      </div>
      {events.length === 0 ? (
        <div className="empty compact">No recent graph traffic is linked to this node yet.</div>
      ) : (
        <div className="traffic-list">
          {events.map(edge => {
            const source = nodeById.get(edge.source);
            const target = nodeById.get(edge.target);
            const direction = edge.source === selected.id ? "outbound" : "inbound";
            return (
              <div key={edge.id} className={`traffic-row ${edge.evidence}`}>
                <span className={`traffic-direction ${direction}`}>{direction}</span>
                <div>
                  <strong>{source?.label || edge.source}</strong>
                  <span className="muted"> {edge.label || edge.type} </span>
                  <strong>{target?.label || edge.target}</strong>
                  <div className="muted sm">{edge.source_ref || "unknown source"} · {edge.timestamp?.slice(0, 19) || "unknown time"} · {edge.evidence}</div>
                </div>
              </div>
            );
          })}
        </div>
      )}
      {!!traffic.related.length && (
        <div className="related-node-strip">
          {traffic.related.slice(0, 16).map(node => (
            <span key={node.id} className={`evidence-pill ${node.evidence}`}>{node.type}: {node.label.slice(0, 28)}</span>
          ))}
        </div>
      )}
    </div>
  );
}

function graphLayout(nodes: GraphNode[]): Record<string, { x: number; y: number }> {
  const groups = ["user", "agent", "task", "message", "report", "lesson", "artifact", "validation", "approval", "policy", "event", "memory_note", "opportunity", "skill", "improvement", "github_run"];
  const buckets: Record<string, GraphNode[]> = {};
  for (const node of nodes) (buckets[node.type] = buckets[node.type] || []).push(node);
  const result: Record<string, { x: number; y: number }> = {};
  const cx = 550;
  const cy = 330;
  const agentNodes = nodes.filter(n => n.type === "agent" || n.type === "user");
  agentNodes.forEach((node, i) => {
    const angle = (Math.PI * 2 * i) / Math.max(1, agentNodes.length) - Math.PI / 2;
    result[node.id] = { x: cx + Math.cos(angle) * 170, y: cy + Math.sin(angle) * 130 };
  });
  let ring = 0;
  for (const group of groups) {
    if (group === "agent" || group === "user") continue;
    const bucket = buckets[group] || [];
    bucket.forEach((node, i) => {
      const angle = (Math.PI * 2 * (i + ring * 0.37)) / Math.max(1, bucket.length);
      const radiusX = 260 + (ring % 3) * 80;
      const radiusY = 190 + (ring % 3) * 58;
      result[node.id] = { x: cx + Math.cos(angle) * radiusX, y: cy + Math.sin(angle) * radiusY };
    });
    if (bucket.length) ring += 1;
  }
  const unplaced = nodes.filter(node => !result[node.id]);
  unplaced.forEach((node, i) => {
    const angle = (Math.PI * 2 * (i + 0.42)) / Math.max(1, unplaced.length);
    result[node.id] = { x: cx + Math.cos(angle) * 470, y: cy + Math.sin(angle) * 305 };
  });
  return result;
}

function nodeRadius(type: string): number {
  if (type === "agent") return 18;
  if (type === "user") return 20;
  if (type === "approval" || type === "validation") return 15;
  return 11;
}

function isRecent(timestamp?: string): boolean {
  if (!timestamp) return false;
  const ts = new Date(timestamp).getTime();
  return Number.isFinite(ts) && Date.now() - ts < 60 * 60 * 1000;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function StatCard({ label, value, sub, accentColor }: {
  label: string; value: number; sub?: string; accentColor?: string;
}) {
  return (
    <div className="card" style={accentColor ? { borderColor: `${accentColor}55`, background: `color-mix(in srgb, ${accentColor} 4%, var(--surface))` } : {}}>
      <div className="card-title">{label}</div>
      <div className="stat-val" style={accentColor ? { color: accentColor } : {}}>{value}</div>
      {sub && <div className="stat-sub">{sub}</div>}
    </div>
  );
}

function MetricSparkline({ history }: { history: MetricPoint[] }) {
  const points = history.length ? history : [{ ts: "", cpu: 0, memory: 0, disk: 0, gpu: null, vram: null }];
  const series = [
    { key: "cpu", label: "CPU", color: "var(--teal)" },
    { key: "memory", label: "MEM", color: "var(--blue)" },
    { key: "gpu", label: "GPU", color: "var(--amber)" },
  ] as const;
  return (
    <div className="spark-wrap">
      <div className="spark-grid">
        {series.map(s => (
          <div key={s.key} className="spark-row">
            <span>{s.label}</span>
            <div className="spark-bars">
              {points.map((p, i) => {
                const raw = p[s.key];
                const value = typeof raw === "number" ? raw : 0;
                return <i key={`${s.key}-${i}`} style={{ height: `${Math.max(4, Math.min(100, value))}%`, background: s.color, opacity: raw === null ? 0.18 : 0.75 }} />;
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function MetricDial({ label, value, sub }: { label: string; value: number | null; sub?: string }) {
  const display = value === null ? "?" : `${Math.round(value)}%`;
  const cls = value === null ? "unknown" : value > 85 ? "hot" : value > 65 ? "warn" : "ok";
  return (
    <div className={`metric-dial ${cls}`}>
      <div className="dial-ring" style={{ ["--pct" as string]: `${value ?? 0}%` }}>
        <span>{display}</span>
      </div>
      <strong>{label}</strong>
      {sub && <small>{sub}</small>}
    </div>
  );
}

function MetricBar({ label, value, unit, sub }: { label: string; value: number; unit: string; sub?: string }) {
  const cls = value > 85 ? "hot" : value > 65 ? "warn" : "ok";
  return (
    <div>
      <div className="bar-label"><span>{label}</span><span>{value}{unit}</span></div>
      <div className="bar-track"><div className={`bar-fill ${cls}`} style={{ width: `${Math.min(value, 100)}%` }} /></div>
      {sub && <div style={{ fontSize: 11, color: "var(--text3)", marginTop: 3 }}>{sub}</div>}
    </div>
  );
}

function AgentPill({ name }: { name: string }) {
  return <span className={`agent-pill ${name}`}>{name}</span>;
}

// ── Behaviors ─────────────────────────────────────────────────────────────────

function Behaviors({ policies, onAction }: {
  policies: BehaviorPolicy[];
  onAction: (agent: string, key: string, action: "approve" | "reject" | "rollback") => void;
}) {
  const STATUS_COLOR: Record<string, string> = {
    proposed: "var(--amber)", applied: "var(--green)", rejected: "var(--red)",
    rolled_back: "var(--text3)",
  };

  const byAgent: Record<string, BehaviorPolicy[]> = {};
  for (const p of policies) {
    (byAgent[p.agent] = byAgent[p.agent] || []).push(p);
  }

  if (policies.length === 0) {
    return (
      <div className="card">
        <div className="card-title">Agent Behavior Policies</div>
        <div className="empty">No behavior policies yet. Tell Roderick: <code>"Zuko should scan LinkedIn more often"</code></div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div className="card">
        <div className="card-title">Agent Behavior Policies</div>
        <p style={{ fontSize: 12, color: "var(--text2)", marginBottom: 12 }}>
          Tell Roderick to change agent behavior in natural language. Proposed changes appear here for review.
        </p>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {(["proposed", "applied", "rejected", "rolled_back"] as const).map(s => {
            const n = policies.filter(p => p.status === s).length;
            return n > 0 ? <span key={s} style={{ fontSize: 11, padding: "2px 8px", borderRadius: 4, background: `${STATUS_COLOR[s]}22`, color: STATUS_COLOR[s], fontWeight: 600 }}>{s.toUpperCase()} {n}</span> : null;
          })}
        </div>
      </div>

      {Object.entries(byAgent).map(([agent, agentPolicies]) => (
        <div key={agent} className="card">
          <div className="card-title" style={{ color: AGENT_COLORS[agent] ?? "var(--text1)" }}>{agent.charAt(0).toUpperCase() + agent.slice(1)}</div>
          <table className="table">
            <thead>
              <tr>
                <th>Policy</th><th>Value</th><th>Status</th><th>Description</th><th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {agentPolicies.map(p => (
                <tr key={p.policy_key}>
                  <td><code style={{ fontSize: 11 }}>{p.policy_key}</code></td>
                  <td><code style={{ fontSize: 11, color: "var(--teal)" }}>{p.policy_value}</code></td>
                  <td><span style={{ color: STATUS_COLOR[p.status] ?? "var(--text2)", fontWeight: 600, fontSize: 11 }}>{p.status}</span></td>
                  <td style={{ fontSize: 12, color: "var(--text2)", maxWidth: 220 }}>{p.description}</td>
                  <td>
                    <div style={{ display: "flex", gap: 4 }}>
                      {p.status === "proposed" && (
                        <>
                          <button className="btn sm" style={{ background: "var(--green)", color: "#000" }} onClick={() => onAction(p.agent, p.policy_key, "approve")}>✓ Apply</button>
                          <button className="btn sm danger" onClick={() => onAction(p.agent, p.policy_key, "reject")}>✗ Reject</button>
                        </>
                      )}
                      {p.status === "applied" && (
                        <button className="btn sm" onClick={() => onAction(p.agent, p.policy_key, "rollback")}>↩ Rollback</button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}

// ── Jobs ──────────────────────────────────────────────────────────────────────

function Jobs({ data, onScanNow }: { data: JobsData | null; onScanNow: () => void }) {
  const [filter, setFilter] = useState<string>("all");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [scanning, setScanning] = useState(false);

  const STATUS_COLOR: Record<string, string> = {
    pending: "var(--amber)", applied: "var(--green)", skipped: "var(--text3)",
    approved: "var(--teal)", applying: "#60a5fa", failed: "var(--red)", manual: "#818cf8",
  };
  const SOURCE_ICON: Record<string, string> = {
    seek: "🟢", linkedin: "🔵", linkedin_feed: "📢", adzuna: "🟡", unknown: "◌",
  };

  const apps = data?.applications ?? [];
  const stats = data?.stats ?? {};
  const filtered = filter === "all" ? apps : apps.filter(a => a.status === filter);
  const statuses = ["pending", "approved", "applying", "applied", "skipped", "failed", "manual"];

  async function handleScan() {
    setScanning(true);
    try { await onScanNow(); } finally { setScanning(false); }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div className="card" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <div className="card-title">Zuko Job Applications</div>
          <p style={{ fontSize: 12, color: "var(--text2)", marginBottom: 0 }}>
            Live from applications.db — Seek, LinkedIn, LinkedIn Feed.
          </p>
        </div>
        <button className="btn sm primary" onClick={handleScan} disabled={scanning}>
          {scanning ? "Queuing…" : "⟳ Scan Now"}
        </button>
      </div>

      <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
        <div className="card" style={{ flex: "1 1 80px", textAlign: "center", padding: "10px 12px" }}>
          <div style={{ fontSize: 22, fontWeight: 700 }}>{apps.length}</div>
          <div style={{ fontSize: 11, color: "var(--text2)" }}>Total</div>
        </div>
        {statuses.map(s => {
          const n = stats[s] ?? 0;
          if (n === 0) return null;
          return (
            <div key={s} className="card"
              style={{ flex: "1 1 80px", textAlign: "center", padding: "10px 12px", cursor: "pointer", border: filter === s ? `1px solid ${STATUS_COLOR[s] ?? "var(--border)"}` : undefined }}
              onClick={() => setFilter(f => f === s ? "all" : s)}>
              <div style={{ fontSize: 22, fontWeight: 700, color: STATUS_COLOR[s] }}>{n}</div>
              <div style={{ fontSize: 11, color: "var(--text2)", textTransform: "capitalize" }}>{s}</div>
            </div>
          );
        })}
      </div>

      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        {["all", ...statuses.filter(s => (stats[s] ?? 0) > 0)].map(s => (
          <button key={s} className={`btn sm${filter === s ? " primary" : ""}`}
            onClick={() => setFilter(s)} style={{ textTransform: "capitalize" }}>{s}</button>
        ))}
      </div>

      {filtered.length === 0 ? (
        <div className="card">
          <div className="empty">
            {apps.length === 0 ? "No applications yet. Hit Scan Now or send /scan to Zuko." : `No applications with status "${filter}".`}
          </div>
        </div>
      ) : (
        <div className="card" style={{ padding: 0, overflow: "hidden" }}>
          <table className="table">
            <thead>
              <tr><th>Src</th><th>Role</th><th>Company</th><th>Location</th><th>Status</th><th>Date</th><th></th></tr>
            </thead>
            <tbody>
              {filtered.map(app => (
                <Fragment key={app.job_id}>
                  <tr style={{ cursor: app.cover_letter ? "pointer" : undefined }}
                    onClick={() => app.cover_letter && setExpanded(e => e === app.job_id ? null : app.job_id)}>
                    <td style={{ fontSize: 13 }}>{SOURCE_ICON[app.source] ?? "◌"}</td>
                    <td style={{ fontWeight: 500 }}>
                      {app.url
                        ? <a href={app.url} target="_blank" rel="noreferrer" style={{ color: "var(--teal)" }} onClick={e => e.stopPropagation()}>{app.title}</a>
                        : app.title}
                    </td>
                    <td style={{ fontSize: 12, color: "var(--text2)" }}>{app.company}</td>
                    <td style={{ fontSize: 12, color: "var(--text2)" }}>{app.location}</td>
                    <td><span style={{ fontSize: 11, fontWeight: 600, color: STATUS_COLOR[app.status] ?? "var(--text2)", textTransform: "capitalize" }}>{app.status}</span></td>
                    <td style={{ fontSize: 11, color: "var(--text3)" }}>
                      {app.updated_at ? new Date(app.updated_at).toLocaleDateString("en-AU", { day: "2-digit", month: "short" }) : "—"}
                    </td>
                    <td style={{ fontSize: 11, color: "var(--text3)" }}>{app.cover_letter ? (expanded === app.job_id ? "▲" : "▼") : ""}</td>
                  </tr>
                  {expanded === app.job_id && app.cover_letter && (
                    <tr>
                      <td colSpan={7} style={{ background: "var(--surface2)", padding: 16 }}>
                        <div style={{ fontSize: 12, color: "var(--text2)", marginBottom: 8, fontWeight: 600 }}>Cover Letter</div>
                        <pre style={{ fontSize: 12, whiteSpace: "pre-wrap", color: "var(--text1)", margin: 0, lineHeight: 1.6 }}>{app.cover_letter}</pre>
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

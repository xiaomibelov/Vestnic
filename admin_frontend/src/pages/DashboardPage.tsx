import { useEffect, useMemo, useState } from "react";
import { api } from "../lib/api";

type HealthState =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "ok"; latencyMs: number }
  | { kind: "err"; message: string };

export default function DashboardPage() {
  const [health, setHealth] = useState<HealthState>({ kind: "idle" });
  const base = useMemo(() => api.base || "(same origin)", []);

  async function runHealth() {
    const t0 = performance.now();
    setHealth({ kind: "loading" });
    try {
      const data = await api.health();
      const t1 = performance.now();
      if (data?.ok) {
        setHealth({ kind: "ok", latencyMs: Math.round(t1 - t0) });
      } else {
        setHealth({ kind: "err", message: "health returned ok=false" });
      }
    } catch (e: any) {
      setHealth({ kind: "err", message: e?.detail?.message || e?.message || "health failed" });
    }
  }

  useEffect(() => {
    runHealth();
  }, []);

  const healthBadge = (() => {
    if (health.kind === "loading") return <span className="badge">health: …</span>;
    if (health.kind === "ok") return <span className="badge">health: ok · {health.latencyMs}ms</span>;
    if (health.kind === "err") return (
      <span className="badge" style={{ borderColor: "rgba(239,68,68,0.55)", background: "rgba(239,68,68,0.14)" }}>
        health: error
      </span>
    );
    return <span className="badge">health: idle</span>;
  })();

  return (
    <div className="container">
      <div className="col" style={{ gap: 12 }}>
        <div className="row" style={{ justifyContent: "space-between" }}>
          <div>
            <div style={{ fontSize: 22, fontWeight: 700 }}>Dashboard</div>
            <div className="muted">Подключено к FastAPI /health. Дальше — /api/admin/*</div>
          </div>
          <div className="row">
            {healthBadge}
            <button className="btn" onClick={runHealth} disabled={health.kind === "loading"}>Refresh</button>
          </div>
        </div>

        <div className="card">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <div className="col" style={{ gap: 6 }}>
              <div style={{ fontWeight: 700 }}>API</div>
              <div className="muted">VITE_ADMIN_API_BASE: {base}</div>
            </div>
            <span className="badge">/health</span>
          </div>

          {health.kind === "err" ? (
            <div style={{ marginTop: 10 }} className="muted">
              Error: {health.message}
            </div>
          ) : null}
        </div>

        <div className="grid">
          <div className="card">
            <div className="muted">Users</div>
            <div style={{ fontSize: 28, fontWeight: 800, marginTop: 8 }}>—</div>
          </div>
          <div className="card">
            <div className="muted">Channels</div>
            <div style={{ fontSize: 28, fontWeight: 800, marginTop: 8 }}>—</div>
          </div>
          <div className="card">
            <div className="muted">Runs (24h)</div>
            <div style={{ fontSize: 28, fontWeight: 800, marginTop: 8 }}>—</div>
          </div>
        </div>

        <div className="card">
          <div style={{ fontWeight: 700 }}>Next (backend)</div>
          <div className="muted" style={{ marginTop: 6 }}>
            Добавить в FastAPI: /api/admin/auth/login, /api/admin/stats, /api/admin/users, /api/admin/channels, /api/admin/runs, /api/admin/logs
          </div>
        </div>
      </div>
    </div>
  );
}

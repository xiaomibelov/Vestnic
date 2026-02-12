import { useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { login } from "../lib/auth";

export default function LoginPage() {
  const nav = useNavigate();
  const loc = useLocation() as any;
  const from = useMemo(() => loc?.state?.from || "/", [loc]);

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  return (
    <div className="container" style={{ paddingTop: 48 }}>
      <div className="card" style={{ maxWidth: 520, margin: "0 auto" }}>
        <div className="col" style={{ gap: 14 }}>
          <div className="col" style={{ gap: 6 }}>
            <div style={{ fontSize: 22, fontWeight: 700 }}>Admin login</div>
            <div className="muted">Vestnik console</div>
          </div>

          <div className="col">
            <label className="muted">Username</label>
            <input className="input" value={username} onChange={(e) => setUsername(e.target.value)} />
          </div>

          <div className="col">
            <label className="muted">Password</label>
            <input className="input" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
          </div>

          {err ? (
            <div className="badge" style={{ borderColor: "rgba(239,68,68,0.55)", background: "rgba(239,68,68,0.14)" }}>
              {err}
            </div>
          ) : null}

          <div className="row" style={{ justifyContent: "space-between" }}>
            <span className="muted">API base: {(import.meta as any).env?.VITE_ADMIN_API_BASE || "(same origin)"}</span>
            <button
              className="btn primary"
              disabled={busy || !username || !password}
              onClick={async () => {
                setBusy(true);
                setErr(null);
                try {
                  await login(username.trim(), password);
                  nav(from, { replace: true });
                } catch (e: any) {
                  setErr(e?.detail?.message || e?.message || "Login failed");
                } finally {
                  setBusy(false);
                }
              }}
            >
              {busy ? "Signing in..." : "Sign in"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

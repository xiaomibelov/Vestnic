import { NavLink, Outlet, useNavigate } from "react-router-dom";
import { clearToken } from "../lib/storage";

export default function AdminLayout() {
  const nav = useNavigate();

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">Vestnik Admin</div>
        <nav className="nav">
          <NavLink to="/" end className={({ isActive }) => (isActive ? "active" : "")}>Dashboard</NavLink>
          <NavLink to="/users" className={({ isActive }) => (isActive ? "active" : "")}>Users</NavLink>
          <NavLink to="/channels" className={({ isActive }) => (isActive ? "active" : "")}>Channels</NavLink>
          <NavLink to="/runs" className={({ isActive }) => (isActive ? "active" : "")}>Runs</NavLink>
          <NavLink to="/logs" className={({ isActive }) => (isActive ? "active" : "")}>Logs</NavLink>
        </nav>
      </aside>

      <div>
        <header className="topbar">
          <div className="row">
            <span className="badge">admin</span>
            <span className="muted">/</span>
            <span className="muted">console</span>
          </div>
          <div className="row">
            <button
              className="btn"
              onClick={() => {
                clearToken();
                nav("/login", { replace: true });
              }}
            >
              Logout
            </button>
          </div>
        </header>

        <main className="content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

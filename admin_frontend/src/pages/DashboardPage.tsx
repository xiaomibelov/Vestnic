export default function DashboardPage() {
  return (
    <div className="container">
      <div className="col" style={{ gap: 12 }}>
        <div className="row" style={{ justifyContent: "space-between" }}>
          <div>
            <div style={{ fontSize: 22, fontWeight: 700 }}>Dashboard</div>
            <div className="muted">Сводка по системе (пока заглушка под контракты бэка)</div>
          </div>
          <span className="badge">v0</span>
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
          <div style={{ fontWeight: 700 }}>Next</div>
          <div className="muted" style={{ marginTop: 6 }}>
            Подключаем реальные эндпоинты: /users, /channels, /runs, /logs + фильтры/пагинация.
          </div>
        </div>
      </div>
    </div>
  );
}

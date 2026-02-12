export default function StubListPage({ title }: { title: string }) {
  return (
    <div className="container">
      <div className="col" style={{ gap: 12 }}>
        <div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>{title}</div>
          <div className="muted">Заглушка. Подключим к API, когда уточним контракт.</div>
        </div>
        <div className="card">TODO: table + filters + pagination</div>
      </div>
    </div>
  );
}

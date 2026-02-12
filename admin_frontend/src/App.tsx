import { Routes, Route } from "react-router-dom";
import RequireAuth from "./components/RequireAuth";
import AdminLayout from "./components/AdminLayout";
import LoginPage from "./pages/LoginPage";
import DashboardPage from "./pages/DashboardPage";
import StubListPage from "./pages/StubListPage";

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />

      <Route
        path="/"
        element={
          <RequireAuth>
            <AdminLayout />
          </RequireAuth>
        }
      >
        <Route index element={<DashboardPage />} />
        <Route path="users" element={<StubListPage title="Users" />} />
        <Route path="channels" element={<StubListPage title="Channels" />} />
        <Route path="runs" element={<StubListPage title="Runs" />} />
        <Route path="logs" element={<StubListPage title="Logs" />} />
      </Route>
    </Routes>
  );
}

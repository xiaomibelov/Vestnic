import { api } from "./api";
import { setToken } from "./storage";

export type LoginResponse = { token: string };

export async function login(username: string, password: string): Promise<void> {
  // Эндпоинт уточним по бэку. Пока: ожидаем JSON { token: "..." }
  const data = await api.post<LoginResponse>("/api/admin/auth/login", { username, password });
  setToken(data.token);
}

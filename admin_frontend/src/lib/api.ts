import { getToken, clearToken } from "./storage";

const BASE = (import.meta as any).env?.VITE_ADMIN_API_BASE || "";

type ApiError = { status: number; message: string; detail?: any };

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(init?.headers ? (init.headers as any) : {}),
  };

  if (token) headers["Authorization"] = `Bearer ${token}`;

  const res = await fetch(`${BASE}${path}`, { ...init, headers });

  if (res.status === 401) {
    clearToken();
    throw { status: 401, message: "Unauthorized" } as ApiError;
  }

  if (!res.ok) {
    let detail: any = null;
    try { detail = await res.json(); } catch {}
    throw { status: res.status, message: res.statusText || "Request failed", detail } as ApiError;
  }

  if (res.status === 204) return null as any;
  return (await res.json()) as T;
}

export const api = {
  base: BASE,
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: any) =>
    request<T>(path, { method: "POST", body: body ? JSON.stringify(body) : undefined }),

  health: () => request<{ ok: boolean }>("/health"),
};

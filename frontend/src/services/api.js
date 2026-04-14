// Base URL is set in .env as VITE_API_BASE_URL
const BASE = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

async function get(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function post(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail || `${res.status} ${res.statusText}`);
  }
  return res.json();
}

export const api = {
  health: () => get("/health"),
  stats: () => get("/analytics/stats"),
  correlations: () => get("/analytics/correlations"),
  temporal: () => get("/analytics/temporal"),
  regimes: () => get("/analytics/regimes"),
  simulation: () => get("/analytics/simulation"),
  findings: () => get("/analytics/findings"),
  forecastMetrics: () => get("/analytics/forecast-metrics"),
  data: (params = {}) => {
    const q = new URLSearchParams(params).toString();
    return get(`/data${q ? `?${q}` : ""}`);
  },
  predictRegime: (body) => post("/predict/regime", body),
  predictPrice: (body) => post("/predict/price", body),
};
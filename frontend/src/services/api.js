// Use the Vite dev proxy by default to avoid local CORS issues.
const BASE = (import.meta.env.VITE_API_BASE_URL ?? "/api").replace(/\/$/, "");

function buildRequestUrl(path) {
  return `${BASE}${path}`;
}

function formatFetchError(error) {
  if (error instanceof TypeError) {
    return `Unable to reach the backend API. Start the FastAPI server on ${BASE}`;
  }
  return error instanceof Error ? error.message : String(error);
}

function formatApiError(detail, fallback) {
  if (Array.isArray(detail)) {
    return detail
      .map((item) => {
        const location = Array.isArray(item?.loc) ? item.loc.join(".") : "request";
        const message = item?.msg ?? "Invalid value";
        return `${location}: ${message}`;
      })
      .join("; ");
  }

  if (typeof detail === "string" && detail.trim()) {
    return detail;
  }

  return fallback;
}

async function get(path) {
  try {
    const res = await fetch(buildRequestUrl(path));
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return res.json();
  } catch (error) {
    throw new Error(formatFetchError(error));
  }
}

async function post(path, body) {
  try {
    const res = await fetch(buildRequestUrl(path), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(formatApiError(err.detail, `${res.status} ${res.statusText}`));
    }
    return res.json();
  } catch (error) {
    throw new Error(formatFetchError(error));
  }
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

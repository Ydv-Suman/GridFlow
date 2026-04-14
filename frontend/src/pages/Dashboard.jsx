import { useEffect, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { api } from "../services/api.js";

const MONTH_NAMES = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

const REGIME_STYLE = {
  STABLE:   { border: "border-emerald-200", bg: "bg-emerald-50",  badge: "bg-emerald-100 text-emerald-700" },
  VOLATILE: { border: "border-amber-200",   bg: "bg-amber-50",    badge: "bg-amber-100 text-amber-700"   },
  CHAOTIC:  { border: "border-red-200",     bg: "bg-red-50",      badge: "bg-red-100 text-red-700"       },
};

// Human-readable labels for finding evidence keys
const EVIDENCE_LABELS = {
  regime_shares_pct:              "Regime shares (%)",
  price_vs_wti_correlation:       "Correlation: gas price vs WTI",
  price_vs_storage_correlation:   "Correlation: gas price vs storage",
  baseline_cost:                  "Baseline procurement cost ($)",
  optimized_cost:                 "Optimized procurement cost ($)",
  savings_pct:                    "Cost savings (%)",
  price_vs_lag_1m_correlation:    "Correlation: price vs 1-month lag",
  price_vs_lag_12m_correlation:   "Correlation: price vs 12-month lag",
  winter_premium_pct:             "Winter price premium vs non-winter (%)",
  rows:                           "Dataset rows",
  columns:                        "Dataset columns",
  missing_cells:                  "Missing values",
};

function fmtEvidence(key, val) {
  if (typeof val === "object" && val !== null) {
    return Object.entries(val)
      .map(([k, v]) => `${k}: ${typeof v === "number" ? v.toFixed(2) : v}`)
      .join("  ·  ");
  }
  if (typeof val === "number") return val.toFixed(3);
  return String(val);
}

function KpiCard({ label, value, sub, accent }) {
  return (
    <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
      <p className="text-xs text-gray-500 uppercase tracking-wider font-medium">{label}</p>
      <p className={`mt-1 text-2xl font-bold ${accent ?? "text-gray-900"}`}>{value}</p>
      {sub && <p className="mt-1 text-xs text-gray-400">{sub}</p>}
    </div>
  );
}

function RegimeCard({ name, data }) {
  const s = REGIME_STYLE[name] ?? REGIME_STYLE.STABLE;
  return (
    <div className={`border rounded-xl p-5 ${s.border} ${s.bg}`}>
      <span className={`inline-block text-xs font-semibold px-2 py-0.5 rounded-full ${s.badge}`}>
        {name}
      </span>
      <p className="mt-3 text-3xl font-bold text-gray-900">{data.share_pct.toFixed(1)}%</p>
      <p className="mt-1 text-sm text-gray-600">of months</p>
      <div className="mt-3 space-y-1 text-sm text-gray-700">
        <p>Avg gas price: <strong>${data.avg_ng_price_mcf.toFixed(2)}/MCF</strong></p>
        <p>Avg WTI: <strong>${data.avg_wti_price_bbl.toFixed(2)}/bbl</strong></p>
        <p>
          Typical months:{" "}
          <strong>{(data.typical_months ?? []).map((m) => MONTH_NAMES[m]).join(", ") || "—"}</strong>
        </p>
      </div>
    </div>
  );
}

function FindingCard({ id, title, evidence }) {
  return (
    <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
      <span className="text-xs font-semibold text-emerald-600 uppercase tracking-wider">
        {id.replace("finding_", "Finding ")}
      </span>
      <p className="mt-1 text-sm font-semibold text-gray-900">{title}</p>
      <div className="mt-3 space-y-1.5">
        {Object.entries(evidence ?? {}).map(([key, val]) => (
          <div key={key} className="flex flex-col">
            <span className="text-xs text-gray-500">{EVIDENCE_LABELS[key] ?? key}</span>
            <span className="text-sm font-medium text-gray-800">{fmtEvidence(key, val)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function Dashboard() {
  const [stats, setStats] = useState(null);
  const [regimes, setRegimes] = useState(null);
  const [temporal, setTemporal] = useState(null);
  const [simulation, setSimulation] = useState(null);
  const [findings, setFindings] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    Promise.allSettled([
      api.stats(),
      api.regimes(),
      api.temporal(),
      api.simulation(),
      api.findings(),
    ]).then(([statsResult, regimesResult, temporalResult, simulationResult, findingsResult]) => {
      if (statsResult.status === "rejected") {
        const reason = statsResult.reason;
        setError(reason instanceof Error ? reason.message : String(reason));
        return;
      }
      setStats(statsResult.value);
      if (regimesResult.status === "fulfilled") setRegimes(regimesResult.value);
      if (temporalResult.status === "fulfilled") setTemporal(temporalResult.value);
      if (simulationResult.status === "fulfilled") setSimulation(simulationResult.value);
      if (findingsResult.status === "fulfilled") setFindings(findingsResult.value);
    });
  }, []);

  if (error)
    return (
      <div className="text-red-600 bg-red-50 border border-red-200 rounded-xl p-4 text-sm">
        {error} — is the backend running?
      </div>
    );

  if (!stats)
    return <p className="text-gray-400 animate-pulse">Loading dashboard…</p>;

  const monthlyData = temporal?.monthly_averages ?? [];

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <p className="mt-1 text-sm text-gray-500">
          {stats.timestamp_start?.slice(0, 7)} – {stats.timestamp_end?.slice(0, 7)} &middot; {stats.row_count} months
        </p>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Avg Gas Price"
          value={`$${stats.price.mean.toFixed(2)}`}
          sub="$/MCF national avg"
          accent="text-emerald-700"
        />
        <KpiCard
          label="Price Range"
          value={`$${stats.price.min.toFixed(2)} – $${stats.price.max.toFixed(2)}`}
          sub="$/MCF over full period"
        />
        <KpiCard
          label="Procurement Savings"
          value={
            simulation?.savings_pct != null ? `${simulation.savings_pct.toFixed(1)}%` : "—"
          }
          sub={
            simulation?.total_savings != null
              ? `$${simulation.total_savings.toFixed(0)} modeled`
              : "Run exploration/run_analytics.py"
          }
          accent="text-emerald-700"
        />
        <KpiCard
          label="Winter Premium"
          value={
            temporal?.winter_premium_pct != null
              ? `${temporal.winter_premium_pct.toFixed(1)}%`
              : "—"
          }
          sub="vs non-winter avg"
        />
      </div>

      {/* Regimes */}
      {regimes && (
        <div>
          <h2 className="text-lg font-semibold text-gray-900 mb-3">Market Regimes</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {Object.entries(regimes).map(([name, data]) => (
              <RegimeCard key={name} name={name} data={data} />
            ))}
          </div>
        </div>
      )}

      {/* Monthly price chart */}
      <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm">
        <h2 className="text-base font-semibold text-gray-900 mb-4">
          Average Monthly Natural Gas Price ($/MCF)
        </h2>
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={monthlyData} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="month"
              tickFormatter={(m) => MONTH_NAMES[m]}
              tick={{ fill: "#6b7280", fontSize: 12 }}
            />
            <YAxis tick={{ fill: "#6b7280", fontSize: 12 }} width={45} />
            <Tooltip
              contentStyle={{ backgroundColor: "#fff", border: "1px solid #e5e7eb", borderRadius: 8 }}
              labelFormatter={(m) => MONTH_NAMES[m]}
            />
            <Line
              type="monotone"
              dataKey="natural_gas_price_mcf"
              stroke="#059669"
              strokeWidth={2}
              dot={false}
              name="Gas Price ($/MCF)"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Findings */}
      {findings && (
        <div>
          <h2 className="text-lg font-semibold text-gray-900 mb-3">Key Findings</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {Object.entries(findings).map(([id, f]) => (
              <FindingCard key={id} id={id} title={f.title} evidence={f.evidence} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

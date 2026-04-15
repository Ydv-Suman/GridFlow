import { useState } from "react";
import { api } from "../services/api.js";

// -- Regime predictor ---------------------------------------------------------

const REGIME_DEFAULTS = {
  natural_gas_price_mcf: 8.5,
  wti_crude_price_bbl: 75.0,
  renewable_share: 0.22,
  avg_wind_speed_ms: 4.5,
  natural_gas_storage_bcf: 2400,
  ng_price_vs_avg: 0.0,
};

const REGIME_FIELDS = [
  { key: "natural_gas_price_mcf",   label: "Gas Price ($/MCF)", step: "any" },
  { key: "wti_crude_price_bbl",     label: "WTI Crude ($/bbl)", step: "any" },
  { key: "renewable_share",         label: "Renewable Share (0–1)", min: 0, max: 1, step: "0.01" },
  { key: "avg_wind_speed_ms",       label: "Avg Wind Speed (m/s)", step: "any" },
  { key: "natural_gas_storage_bcf", label: "Gas Storage (BCF)", step: "any" },
  { key: "ng_price_vs_avg",         label: "Price vs Historical Avg ($/MCF)", step: "any" },
];

const REGIME_STYLE = {
  STABLE:   { card: "border-emerald-200 bg-emerald-50", badge: "bg-emerald-100 text-emerald-700" },
  VOLATILE: { card: "border-amber-200 bg-amber-50",     badge: "bg-amber-100 text-amber-700"     },
  CHAOTIC:  { card: "border-red-200 bg-red-50",         badge: "bg-red-100 text-red-700"         },
};

function RegimePredictor() {
  const [form, setForm] = useState(REGIME_DEFAULTS);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  function handleChange(key, val) {
    setForm((f) => ({ ...f, [key]: val === "" ? "" : Number(val) }));
  }

  function submit(e) {
    e.preventDefault();
    if (loading) return;
    setLoading(true);
    setError(null);
    api.predictRegime(form)
      .then(setResult)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }

  const s = result ? REGIME_STYLE[result.regime_label] : null;

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm space-y-5">
      <div>
        <h2 className="text-lg font-semibold text-gray-900">Regime Classifier</h2>
        <p className="text-xs text-gray-500 mt-1">
          Classify monthly market conditions as STABLE, VOLATILE, or CHAOTIC.
        </p>
      </div>

      <form onSubmit={submit} className="space-y-3">
        {REGIME_FIELDS.map(({ key, label, min, max, step = "any" }) => (
          <div key={key}>
            <label className="block text-xs text-gray-600 font-medium mb-1">{label}</label>
            <input
              type="number"
              min={min}
              max={max}
              step={step}
              required
              value={form[key]}
              onChange={(e) => handleChange(key, e.target.value)}
              className="w-full bg-gray-50 border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:border-transparent"
            />
          </div>
        ))}
        <button
          type="submit"
          disabled={loading}
          className="w-full py-2.5 bg-emerald-600 hover:bg-emerald-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium transition-colors"
        >
          {loading ? "Classifying…" : "Classify Regime"}
        </button>
      </form>

      {error && (
        <p className="text-red-600 text-sm bg-red-50 border border-red-200 rounded-lg p-3">{error}</p>
      )}

      {result && s && (
        <div className={`border rounded-xl p-4 ${s.card}`}>
          <span className={`inline-block text-xs font-semibold px-2 py-0.5 rounded-full ${s.badge}`}>
            {result.regime_label}
          </span>
          <p className="text-2xl font-bold text-gray-900 mt-2">
            {(result.confidence * 100).toFixed(1)}% confidence
          </p>
          <div className="mt-3 divide-y divide-gray-200">
            {Object.entries(result.distances_to_centroids).map(([regime, dist]) => (
              <div key={regime} className="flex justify-between py-1.5 text-sm text-gray-600">
                <span>{regime}</span>
                <span className="font-medium">dist {dist}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// -- Price forecaster ---------------------------------------------------------

const FORECAST_DEFAULTS = {
  natural_gas_storage_bcf: 2400,
  wti_crude_price_bbl: 75.0,
  renewable_share: 0.22,
  avg_wind_speed_ms: 4.5,
  avg_precipitation_mm: 3.2,
  month: 6,
  ng_price_lag_1m: 8.5,
  ng_price_lag_3m: 8.2,
  ng_price_lag_12m: 7.9,
  ng_price_volatility_3m: 0.4,
  ng_price_change_rate: 0.1,
};

const FORECAST_FIELDS = [
  { key: "natural_gas_storage_bcf",  label: "Gas Storage (BCF)", step: "any" },
  { key: "wti_crude_price_bbl",      label: "WTI Crude ($/bbl)", step: "any" },
  { key: "renewable_share",          label: "Renewable Share (0–1)", min: 0, max: 1, step: "0.01" },
  { key: "avg_wind_speed_ms",        label: "Avg Wind Speed (m/s)", step: "any" },
  { key: "avg_precipitation_mm",     label: "Avg Precipitation (mm)", step: "any" },
  { key: "month",                    label: "Month (1–12)", min: 1, max: 12, step: "1" },
  { key: "ng_price_lag_1m",          label: "Gas Price 1 Month Ago ($/MCF)", step: "any" },
  { key: "ng_price_lag_3m",          label: "Gas Price 3 Months Ago ($/MCF)", step: "any" },
  { key: "ng_price_lag_12m",         label: "Gas Price 12 Months Ago ($/MCF)", step: "any" },
  { key: "ng_price_volatility_3m",   label: "3-Month Price Volatility", step: "any" },
  { key: "ng_price_change_rate",     label: "Month-over-Month Price Change", step: "any" },
];

function PriceForecaster() {
  const [form, setForm] = useState(FORECAST_DEFAULTS);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  function handleChange(key, val) {
    setForm((f) => ({ ...f, [key]: val === "" ? "" : Number(val) }));
  }

  function submit(e) {
    e.preventDefault();
    if (loading) return;
    setLoading(true);
    setError(null);
    api.predictPrice(form)
      .then(setResult)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm space-y-5">
      <div>
        <h2 className="text-lg font-semibold text-gray-900">Price Forecaster</h2>
        <p className="text-xs text-gray-500 mt-1">
          Predict natural gas price ($/MCF) three months ahead.
        </p>
      </div>

      <form onSubmit={submit} className="space-y-3">
        {FORECAST_FIELDS.map(({ key, label, min, max, step = "any" }) => (
          <div key={key}>
            <label className="block text-xs text-gray-600 font-medium mb-1">{label}</label>
            <input
              type="number"
              min={min}
              max={max}
              step={step}
              required
              value={form[key]}
              onChange={(e) => handleChange(key, e.target.value)}
              className="w-full bg-gray-50 border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:border-transparent"
            />
          </div>
        ))}
        <button
          type="submit"
          disabled={loading}
          className="w-full py-2.5 bg-emerald-600 hover:bg-emerald-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium transition-colors"
        >
          {loading ? "Forecasting…" : "Forecast Price"}
        </button>
      </form>

      {error && (
        <p className="text-red-600 text-sm bg-red-50 border border-red-200 rounded-lg p-3">{error}</p>
      )}

      {result && (
        <div className="border border-emerald-200 bg-emerald-50 rounded-xl p-4">
          <p className="text-xs font-semibold text-emerald-600 uppercase tracking-wider">
            3-Month Forecast
          </p>
          <p className="text-3xl font-bold text-gray-900 mt-1">
            ${result.predicted_price_mcf.toFixed(2)}
            <span className="text-sm font-normal text-gray-500 ml-1">/MCF</span>
          </p>
          <p className="text-xs text-gray-500 mt-2">
            Model test MAE: ${result.model_test_mae.toFixed(3)}/MCF
          </p>
        </div>
      )}
    </div>
  );
}

export default function PredictPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Predict</h1>
        <p className="mt-1 text-sm text-gray-500">
          Run the trained models against custom input values.
        </p>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <RegimePredictor />
        <PriceForecaster />
      </div>
    </div>
  );
}

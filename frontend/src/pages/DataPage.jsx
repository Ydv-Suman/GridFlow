import { useEffect, useState } from "react";
import { api } from "../api";

const PAGE_SIZE = 20;

const COLUMNS = [
  { key: "year_month",               label: "Month"           },
  { key: "natural_gas_price_mcf",    label: "Gas $/MCF"       },
  { key: "wti_crude_price_bbl",      label: "WTI $/bbl"       },
  { key: "natural_gas_storage_bcf",  label: "Storage BCF"     },
  { key: "gen_all_fuels_mwh",        label: "Total Gen (MWh)" },
  { key: "gen_natural_gas_mwh",      label: "Gas Gen"         },
  { key: "gen_renewables_mwh",       label: "Renewables"      },
  { key: "avg_wind_speed_ms",        label: "Wind (m/s)"      },
  { key: "avg_precipitation_mm",     label: "Precip (mm)"     },
];

function fmt(val) {
  if (val === null || val === undefined) return "—";
  if (typeof val === "number") return val.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return val;
}

export default function DataPage() {
  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  function load(off = 0) {
    setLoading(true);
    setError(null);
    const params = { limit: PAGE_SIZE, offset: off };
    if (start) params.start = start;
    if (end) params.end = end;
    api
      .data(params)
      .then((d) => { setRows(d.rows); setTotal(d.total); setOffset(off); })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }

  useEffect(() => { load(0); }, []);

  const totalPages = Math.ceil(total / PAGE_SIZE);
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Monthly Dataset</h1>
        <p className="mt-1 text-sm text-gray-500">{total} rows total</p>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-end">
        <div>
          <label className="block text-xs text-gray-500 mb-1 font-medium">Start (YYYY-MM)</label>
          <input
            className="bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm w-32 focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:border-transparent"
            placeholder="2020-01"
            value={start}
            onChange={(e) => setStart(e.target.value)}
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1 font-medium">End (YYYY-MM)</label>
          <input
            className="bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm w-32 focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:border-transparent"
            placeholder="2024-12"
            value={end}
            onChange={(e) => setEnd(e.target.value)}
          />
        </div>
        <button
          onClick={() => load(0)}
          className="px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white rounded-lg text-sm font-medium transition-colors"
        >
          Apply
        </button>
      </div>

      {error && (
        <div className="text-red-600 bg-red-50 border border-red-200 rounded-xl p-3 text-sm">
          {error}
        </div>
      )}

      {/* Table */}
      <div className="bg-white overflow-x-auto rounded-xl border border-gray-200 shadow-sm">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200">
              {COLUMNS.map((c) => (
                <th
                  key={c.key}
                  className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap"
                >
                  {c.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading ? (
              <tr>
                <td colSpan={COLUMNS.length} className="px-4 py-8 text-center text-gray-400">
                  Loading…
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td colSpan={COLUMNS.length} className="px-4 py-8 text-center text-gray-400">
                  No data
                </td>
              </tr>
            ) : (
              rows.map((row, i) => (
                <tr key={i} className="hover:bg-gray-50 transition-colors">
                  {COLUMNS.map((c) => (
                    <td key={c.key} className="px-4 py-2.5 whitespace-nowrap text-gray-700">
                      {fmt(row[c.key])}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between text-sm text-gray-500">
        <span>Page {currentPage} of {totalPages}</span>
        <div className="flex gap-2">
          <button
            disabled={offset === 0}
            onClick={() => load(Math.max(0, offset - PAGE_SIZE))}
            className="px-3 py-1.5 bg-white border border-gray-300 rounded-lg disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            ← Prev
          </button>
          <button
            disabled={offset + PAGE_SIZE >= total}
            onClick={() => load(offset + PAGE_SIZE)}
            className="px-3 py-1.5 bg-white border border-gray-300 rounded-lg disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            Next →
          </button>
        </div>
      </div>
    </div>
  );
}

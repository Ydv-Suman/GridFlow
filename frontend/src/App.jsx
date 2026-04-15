import { useEffect, useState } from "react";

import Dashboard from "./pages/Dashboard.jsx";
import DataPage from "./pages/DataPage.jsx";
import PredictPage from "./pages/PredictPage.jsx";

const PAGES = {
  dashboard: {
    label: "Dashboard",
    component: Dashboard,
  },
  data: {
    label: "Data",
    component: DataPage,
  },
  predict: {
    label: "Predict",
    component: PredictPage,
  },
};

function getPageFromHash() {
  const hash = window.location.hash.replace(/^#/, "");
  return PAGES[hash] ? hash : "dashboard";
}

export default function App() {
  const [activePage, setActivePage] = useState(getPageFromHash);

  useEffect(() => {
    function handleHashChange() {
      setActivePage(getPageFromHash());
    }

    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  function handleNavigate(page) {
    window.location.hash = page;
    setActivePage(page);
  }

  const ActivePage = PAGES[activePage].component;

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <header className="border-b border-gray-200 bg-white">
        <div className="mx-auto flex max-w-6xl flex-col gap-4 px-4 py-4 md:flex-row md:items-center md:justify-between">
          <span className="text-3xl font-semibold tracking-tight text-emerald-800">GridFlow</span>
          <nav className="flex flex-wrap gap-2">
            {Object.entries(PAGES).map(([key, page]) => {
              const isActive = key === activePage;

              return (
                <button
                  key={key}
                  type="button"
                  onClick={() => handleNavigate(key)}
                  className={`rounded-lg px-4 py-2 text-sm font-medium transition-colors ${
                    isActive
                      ? "bg-emerald-600 text-white"
                      : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                  }`}
                >
                  {page.label}
                </button>
              );
            })}
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-4 py-8">
        <ActivePage />
      </main>
    </div>
  );
}

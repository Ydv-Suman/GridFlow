import Dashboard from "./pages/Dashboard.jsx";

export default function App() {
  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <header className="border-b border-gray-200 bg-white">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-4 py-4">
          <span className="text-lg font-semibold tracking-tight text-emerald-800">GridFlow</span>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-4 py-8">
        <Dashboard />
      </main>
    </div>
  );
}

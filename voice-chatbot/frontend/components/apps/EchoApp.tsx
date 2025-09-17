import { useState } from 'react';

export function EchoApp() {
  const [input, setInput] = useState('');
  const [result, setResult] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const apiBase = process.env.NEXT_PUBLIC_API_BASE_URL || '';

  const handleEcho = async () => {
    setLoading(true);
    setResult(null);
    const res = await fetch(`${apiBase}/api/apps/echo`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: input }),
    });
    const data = await res.json();
    setResult(data.echoed);
    setLoading(false);
  };

  return (
    <div className="p-6 bg-white rounded-xl shadow-lg max-w-md mx-auto border border-gray-100">
      <h2 className="text-lg font-bold mb-2">Echo App</h2>
      <input
        className="border rounded px-3 py-2 w-full mb-2"
        value={input}
        onChange={e => setInput(e.target.value)}
        placeholder="Type something to echo..."
      />
      <button
        className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
        onClick={handleEcho}
        disabled={loading || !input}
      >
        {loading ? 'Echoing...' : 'Echo'}
      </button>
      {result && (
        <div className="mt-4 p-3 bg-gray-50 rounded border text-blue-700">
          <strong>Echoed:</strong> {result}
        </div>
      )}
    </div>
  );
} 
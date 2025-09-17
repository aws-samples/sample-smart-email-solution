import { motion } from 'framer-motion';

interface SummaryOutputProps {
  content: {
    title?: string;
    description?: string;
    details?: string | string[];
  };
}

export function SummaryOutput({ content }: SummaryOutputProps) {
  // Accept both string (with newlines) or array for details
  const bullets = Array.isArray(content.details)
    ? content.details
    : typeof content.details === 'string'
      ? content.details.split(/\n|•/).map(b => b.trim()).filter(Boolean)
      : [];

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -20 }}
      className="bg-white rounded-xl shadow-lg p-6 max-w-lg mx-auto border border-gray-100"
    >
      {content.title && (
        <h2 className="text-xl font-bold mb-2 text-blue-700 flex items-center gap-2">
          <span>📝</span> {content.title}
        </h2>
      )}
      {content.description && (
        <p className="text-gray-600 mb-4">{content.description}</p>
      )}
      <ul className="list-disc pl-6 space-y-1 text-gray-800">
        {bullets.map((b, i) => (
          <li key={i}>{b}</li>
        ))}
      </ul>
    </motion.div>
  );
} 
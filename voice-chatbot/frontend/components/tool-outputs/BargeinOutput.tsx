import { useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

const funnyMessages = [
  "Whoa there! I hear you! 🎧",
  "Hold that thought! 🤚",
  "Plot twist! New topic! 🔄",
  "Quick change of plans! 🎯",
  "Switching gears! 🔁",
  "You've got the floor! 🎤",
  "New direction incoming! 🚀",
  "Mid-sentence redirect! ↪️",
  "Conversation hijack successful! 🎉",
  "Smooth interruption! 🌊"
];

export function BargeinOutput() {
  const [message, setMessage] = useState('');

  useEffect(() => {
    // Pick a random message when component mounts
    const randomIndex = Math.floor(Math.random() * funnyMessages.length);
    setMessage(funnyMessages[randomIndex]);
  }, []);

  return (
    <AnimatePresence>
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: -20 }}
        className="flex flex-col items-center justify-center p-6 text-center"
      >
        <motion.div
          animate={{
            scale: [1, 1.2, 1],
            rotate: [0, 5, -5, 0],
          }}
          transition={{
            duration: 0.5,
            ease: "easeInOut",
            times: [0, 0.5, 1],
            repeat: 1
          }}
          className="text-3xl mb-4"
        >
          🎭
        </motion.div>
        <motion.h2
          className="text-xl font-bold mb-2 text-blue-600"
          animate={{
            scale: [1, 1.1, 1],
          }}
          transition={{
            duration: 0.3,
            ease: "easeInOut",
          }}
        >
          {message}
        </motion.h2>
        <p className="text-gray-600 text-sm">
          Switching to your new topic...
        </p>
      </motion.div>
    </AnimatePresence>
  );
} 
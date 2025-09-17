import { motion } from 'framer-motion';
import { ReactNode } from 'react';

interface AnimatedContainerProps {
  children: ReactNode;
  className?: string;
}

const containerVariants = {
  initial: { 
    opacity: 0,
    y: 20,
    scale: 0.98
  },
  animate: { 
    opacity: 1,
    y: 0,
    scale: 1,
    transition: {
      type: "spring",
      stiffness: 200,
      damping: 20,
      staggerChildren: 0.1
    }
  },
  exit: {
    opacity: 0,
    scale: 0.95,
    transition: { duration: 0.2 }
  }
};

export function AnimatedContainer({ children, className = "" }: AnimatedContainerProps) {
  return (
    <motion.div
      variants={containerVariants}
      initial="initial"
      animate="animate"
      exit="exit"
      className={`${className} overflow-hidden`}
      layout
    >
      {children}
    </motion.div>
  );
} 
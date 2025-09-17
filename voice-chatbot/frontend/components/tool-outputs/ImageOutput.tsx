import { ImageToolOutput } from './types';
import { TOOL_OUTPUT_STYLES } from '@/config/tool-outputs';

export const ImageOutput = ({ output }: { output: ImageToolOutput }) => {
  const styles = TOOL_OUTPUT_STYLES.image;

  return (
    <div className={styles.container}>
      {output.content.title && (
        <h3 className={styles.title}>{output.content.title}</h3>
      )}
      <div className={styles.imageWrapper}>
        <img 
          src={output.content.url} 
          alt={output.content.alt || output.content.title || "Tool output image"}
          className={styles.image}
        />
      </div>
      {output.content.description && (
        <p className={styles.description}>{output.content.description}</p>
      )}
    </div>
  );
}; 
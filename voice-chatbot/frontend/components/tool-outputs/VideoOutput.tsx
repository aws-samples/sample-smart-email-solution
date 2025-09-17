import { VideoToolOutput } from './types';
import { TOOL_OUTPUT_STYLES } from '@/config/tool-outputs';

export const VideoOutput = ({ output }: { output: VideoToolOutput }) => {
  const styles = TOOL_OUTPUT_STYLES.video;

  return (
    <div className={styles.container}>
      {output.content.title && (
        <h3 className={styles.title}>{output.content.title}</h3>
      )}
      <div className={styles.videoWrapper}>
        <iframe
          src={output.content.url}
          className={styles.video}
          title={output.content.title || "Video player"}
          allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
          allowFullScreen
        />
      </div>
      {output.content.description && (
        <p className={styles.description}>{output.content.description}</p>
      )}
    </div>
  );
}; 
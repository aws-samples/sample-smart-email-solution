import { PdfToolOutput } from './types';
import { TOOL_OUTPUT_STYLES } from '@/config/tool-outputs';

export const PdfOutput = ({ output }: { output: PdfToolOutput }) => {
  const styles = TOOL_OUTPUT_STYLES.pdf;

  return (
    <div className={styles.container}>
      {output.content.title && (
        <h3 className={styles.title}>{output.content.title}</h3>
      )}
      <div className={styles.pdfWrapper}>
        <iframe
          src={`https://docs.google.com/viewer?embedded=true&url=${encodeURIComponent(output.content.url)}`}
          className={styles.pdf}
          title={output.content.title || "PDF viewer"}
          sandbox="allow-scripts allow-same-origin allow-popups"
          loading="lazy"
          key={output.content.url}
        />
      </div>
      {output.content.description && (
        <p className={styles.description}>{output.content.description}</p>
      )}
      <div className={styles.actionWrapper}>
        <a
          href={output.content.url}
          target="_blank"
          rel="noopener noreferrer"
          className={styles.action}
        >
          Open PDF in new tab ...
        </a>
      </div>
    </div>
  );
}; 
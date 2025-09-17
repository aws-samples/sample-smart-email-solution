import { TextToolOutput } from './types';
import { TOOL_OUTPUT_STYLES } from '@/config/tool-outputs';

export const TextOutput = ({ output }: { output: TextToolOutput }) => {
  const styles = TOOL_OUTPUT_STYLES.text;

  return (
    <div className={styles.container}>
      {output.content.title && (
        <h3 className={styles.title}>{output.content.title}</h3>
      )}
      <div className={styles.content}>
        {Object.entries(output.content).map(([key, value]) => {
          if (key !== 'title' && value) {
            if (Array.isArray(value)) {
              return (
                <div key={key} className="mt-4">
                  {value.map((item: any, index: number) => (
                    <div key={index} className="mb-4 pb-4 border-b border-blue-100 last:border-0">
                      {Object.entries(item).map(([itemKey, itemValue]) => (
                        <p key={itemKey} className={`${itemKey === 'content' ? 'text-sm mb-2' : 'text-xs text-blue-500'}`}>
                          {itemValue as string}
                        </p>
                      ))}
                    </div>
                  ))}
                </div>
              );
            }
            return (
              <p key={key} className={styles.item}>
                {value as string}
              </p>
            );
          }
          return null;
        })}
      </div>
    </div>
  );
}; 
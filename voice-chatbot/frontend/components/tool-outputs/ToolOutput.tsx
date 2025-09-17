import { ToolOutput as ToolOutputType } from './types';
import { TextOutput } from './TextOutput';
import { CardOutput } from './CardOutput';
import { ImageOutput } from './ImageOutput';
import { VideoOutput } from './VideoOutput';
import { PdfOutput } from './PdfOutput';
import { ButtonOutput } from './ButtonOutput';
import { TOOL_OUTPUT_STYLES } from '@/config/tool-outputs';
import { BargeinOutput } from './BargeinOutput';
import { AnimatedContainer } from './AnimatedContainer';
import { AnimatePresence } from 'framer-motion';
import { SummaryOutput } from './SummaryOutput';
import { EchoApp } from '../apps/EchoApp';

const UnsupportedOutput = ({ type }: { type: string }) => (
  <AnimatedContainer className="bg-yellow-50 border border-yellow-100 p-6 rounded-lg">
    <p className="text-yellow-800">Unsupported content type: {type}</p>
  </AnimatedContainer>
);

interface ToolOutputProps {
  output: ToolOutputType;
  websocket?: WebSocket | null;
}

const ToolOutput: React.FC<ToolOutputProps> = ({ output, websocket }) => {
  return (
    <div className="mb-4">
      {output.type === 'text' && output.content && (
        <TextOutput output={output} key="text" />
      )}
      {output.type === 'barge_in' && (
        <BargeinOutput key="barge_in" />
      )}
      {output.type === 'app' && (
        <EchoApp key="app" {...(output.props || {})} />
      )}
      {output.type === 'card' && output.content && (
        <CardOutput output={output} key="card" />
      )}
      {output.type === 'image' && output.content && (
        <ImageOutput output={output} key="image" />
      )}
      {output.type === 'video' && output.content && (
        <VideoOutput output={output} key="video" />
      )}
      {output.type === 'pdf' && output.content && (
        <PdfOutput output={output} key="pdf" />
      )}
      {output.type === 'button' && output.content && (
        <ButtonOutput output={output} websocket={websocket} key="button" />
      )}
      {[ 'barge_in', 'app', 'card', 'text', 'image', 'video', 'pdf', 'button' ].indexOf(output.type) === -1 && (
        <div className="text-gray-500 italic">Unknown output type: {output.type}</div>
      )}
    </div>
  );
};

export { ToolOutput }; 
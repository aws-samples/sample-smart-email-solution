import { useEffect, useRef } from 'react';

interface AudioCaptureMediaRecorderProps {
  websocket: WebSocket | null;
  isCapturing: boolean;
  onError: (error: Error) => void;
  inline?: boolean;
  setIsThinking: (thinking: boolean) => void;
  promptName: string;
  contentName: string;
}

// Utility: Convert Float32Array to Int16 PCM
function floatTo16BitPCM(input: Float32Array): Int16Array {
  const output = new Int16Array(input.length);
  for (let i = 0; i < input.length; i++) {
    const s = Math.max(-1, Math.min(1, input[i]));
    output[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
  }
  return output;
}

// Utility: Encode Int16Array to base64
function int16ToBase64(int16: Int16Array): string {
  let binary = '';
  for (let i = 0; i < int16.length; i++) {
    binary += String.fromCharCode(int16[i] & 0xff, (int16[i] >> 8) & 0xff);
  }
  return btoa(binary);
}

export default function AudioCaptureMediaRecorder({ websocket, isCapturing, onError, inline, setIsThinking, promptName, contentName }: AudioCaptureMediaRecorderProps) {
  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const audioChunksRef = useRef<Blob[]>([]);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animationFrameRef = useRef<number | null>(null);
  const audioContextRef = useRef<AudioContext | null>(null);
  const processorRef = useRef<ScriptProcessorNode | null>(null);
  const analyserRef = useRef<AnalyserNode | null>(null);
  const sourceRef = useRef<MediaStreamAudioSourceNode | null>(null);
  const streamRef = useRef<MediaStream | null>(null);
  const sentAudioRef = useRef<boolean>(false); // Track if any audio was sent

  // Visualize audio waveform
  const visualize = (stream: MediaStream) => {
    if (!canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    const audioContext = new AudioContext();
    audioContextRef.current = audioContext;
    const source = audioContext.createMediaStreamSource(stream);
    sourceRef.current = source;
    const analyser = audioContext.createAnalyser();
    analyser.fftSize = 2048;
    source.connect(analyser);
    analyserRef.current = analyser;
    const bufferLength = analyser.frequencyBinCount;
    const dataArray = new Uint8Array(bufferLength);

    const draw = () => {
      analyser.getByteTimeDomainData(dataArray);
      ctx.fillStyle = '#fff';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.lineWidth = 2;
      ctx.strokeStyle = '#000';
      ctx.beginPath();
      const sliceWidth = canvas.width / bufferLength;
      let x = 0;
      for (let i = 0; i < bufferLength; i++) {
        const v = dataArray[i] / 128.0;
        const y = (v * canvas.height) / 2;
        if (i === 0) {
          ctx.moveTo(x, y);
        } else {
          ctx.lineTo(x, y);
        }
        x += sliceWidth;
      }
      ctx.lineTo(canvas.width, canvas.height / 2);
      ctx.stroke();
      animationFrameRef.current = requestAnimationFrame(draw);
    };
    draw();
  };

  useEffect(() => {
    let stream: MediaStream | null = null;
    let audioContext: AudioContext | null = null;
    let processor: ScriptProcessorNode | null = null;
    sentAudioRef.current = false;
    if (isCapturing) {
      navigator.mediaDevices.getUserMedia({ audio: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true
      } })
        .then((mediaStream) => {
          stream = mediaStream;
          streamRef.current = mediaStream;
          visualize(mediaStream);

          // --- Real-time streaming with ScriptProcessorNode ---
          const AudioCtx = window.AudioContext ? window.AudioContext : (window as any).webkitAudioContext;
          audioContext = new AudioCtx({ latencyHint: 'interactive' });
          if (audioContext) {
            audioContextRef.current = audioContext;
            const source = audioContext.createMediaStreamSource(mediaStream);
            sourceRef.current = source;
            processor = audioContext.createScriptProcessor(512, 1, 1);
            processorRef.current = processor;
            source.connect(processor);
            processor.connect(audioContext.destination);
          }

          if (processor) {
            processor.onaudioprocess = (e) => {
              if (!isCapturing) return;
              const inputBuffer = e.inputBuffer;
              const inputData = inputBuffer.getChannelData(0);
              const pcm = floatTo16BitPCM(inputData);
              const base64 = int16ToBase64(pcm);
              // Send as JSON event (match backend expectation)
              if (websocket && websocket.readyState === WebSocket.OPEN) {
                const eventObj = {
                  event: {
                    audioInput: {
                      promptName,
                      contentName,
                      content: base64
                    }
                  }
                };
                websocket.send(JSON.stringify(eventObj));
                sentAudioRef.current = true;
                console.log('[MediaRecorder] Sent audio chunk', eventObj);
              }
            };
          }

          // --- Session recording with MediaRecorder (optional) ---
          const mediaRecorder = new MediaRecorder(mediaStream, { mimeType: 'audio/webm' });
          mediaRecorderRef.current = mediaRecorder;
          audioChunksRef.current = [];

          mediaRecorder.onstart = () => {
            setIsThinking(true);
            console.log('[MediaRecorder] Recording started');
          };

          mediaRecorder.ondataavailable = (event) => {
            if (event.data.size > 0) {
              audioChunksRef.current.push(event.data);
            }
          };

          mediaRecorder.onstop = () => {
            setIsThinking(false);
            // If no audio was sent, send a short silent buffer
            if (!sentAudioRef.current && websocket && websocket.readyState === WebSocket.OPEN) {
              const silentPCM = new Int16Array(1600); // 100ms of silence at 16kHz
              const base64 = int16ToBase64(silentPCM);
              const eventObj = {
                event: {
                  audioInput: {
                    promptName,
                    contentName,
                    content: base64
                  }
                }
              };
              websocket.send(JSON.stringify(eventObj));
              console.log('[MediaRecorder] Sent fallback silent audio chunk', eventObj);
            }
            audioChunksRef.current = [];
            // Clean up
            if (stream) {
              stream.getTracks().forEach(track => track.stop());
            }
            if (audioContextRef.current) {
              audioContextRef.current.close();
              audioContextRef.current = null;
            }
            if (animationFrameRef.current) {
              cancelAnimationFrame(animationFrameRef.current);
              animationFrameRef.current = null;
            }
            if (analyserRef.current) {
              analyserRef.current.disconnect();
              analyserRef.current = null;
            }
            if (sourceRef.current) {
              sourceRef.current.disconnect();
              sourceRef.current = null;
            }
            if (processorRef.current) {
              processorRef.current.disconnect();
              processorRef.current = null;
            }
            console.log('[MediaRecorder] Recording stopped and cleaned up');
          };

          mediaRecorder.onerror = (e) => {
            onError(e.error || new Error('MediaRecorder error'));
          };

          mediaRecorder.start();
        })
        .catch(onError);
    }
    return () => {
      // Stop recording and clean up
      if (mediaRecorderRef.current && mediaRecorderRef.current.state !== 'inactive') {
        mediaRecorderRef.current.stop();
      }
      if (processorRef.current) {
        processorRef.current.disconnect();
        processorRef.current = null;
      }
      if (streamRef.current) {
        streamRef.current.getTracks().forEach(track => track.stop());
        streamRef.current = null;
      }
      if (audioContextRef.current) {
        audioContextRef.current.close();
        audioContextRef.current = null;
      }
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current);
        animationFrameRef.current = null;
      }
      if (analyserRef.current) {
        analyserRef.current.disconnect();
        analyserRef.current = null;
      }
      if (sourceRef.current) {
        sourceRef.current.disconnect();
        sourceRef.current = null;
      }
    };
  }, [isCapturing, websocket, setIsThinking, onError, promptName, contentName]);

  return (
    <canvas
      ref={canvasRef}
      className={inline ? 'w-full h-6 bg-white' : 'w-full h-24 bg-white rounded'}
      style={inline ? { background: 'transparent', borderRadius: 1, border: '0px solid #eee', width: '50px' } : {}}
      aria-label="Audio Visualizer"
    />
  );
} 
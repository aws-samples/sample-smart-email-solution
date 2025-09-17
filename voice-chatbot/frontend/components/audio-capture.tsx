'use client';

import { useRef, useEffect } from 'react';

interface AudioCaptureProps {
  websocket: WebSocket | null;
  isCapturing: boolean;
  onError: (error: Error) => void;
  inline?: boolean;
  setIsThinking: (thinking: boolean) => void;
}

class RollingBuffer {
  private buffer: Int16Array;
  private writePosition: number = 0;
  private samplesWritten: number = 0;
  private readonly bufferSize: number;

  constructor(bufferSize: number = 3200) { // ~200ms of audio at 16kHz
    this.bufferSize = bufferSize;
    this.buffer = new Int16Array(bufferSize);
  }

  write(newData: Int16Array): Int16Array | null {
    const remaining = this.bufferSize - this.writePosition;
    const newDataLength = newData.length;

    // Write data to buffer
    if (newDataLength >= remaining) {
      // Fill the remaining space
      this.buffer.set(newData.slice(0, remaining), this.writePosition);
      
      // Reset write position
      this.writePosition = 0;
      
      // Write remaining data if any
      if (newDataLength > remaining) {
        this.buffer.set(newData.slice(remaining, newDataLength), 0);
        this.writePosition = newDataLength - remaining;
      }

      this.samplesWritten += newDataLength;
      
      // Return a copy of the filled buffer
      return this.buffer.slice();
    } else {
      // Just write the data and advance the position
      this.buffer.set(newData, this.writePosition);
      this.writePosition += newDataLength;
      this.samplesWritten += newDataLength;
      
      // Return buffer only if it's nearly full (>90%)
      if (this.writePosition > this.bufferSize * 0.9) {
        const filledBuffer = this.buffer.slice(0, this.writePosition);
        this.writePosition = 0;
        return filledBuffer;
      }
    }

    return null;
  }

  clear() {
    this.writePosition = 0;
    this.samplesWritten = 0;
    this.buffer.fill(0);
  }
}

export class AudioCaptureService {
  private mediaStream: MediaStream | null = null;
  private audioContext: AudioContext | null = null;
  private processor: ScriptProcessorNode | null = null;
  private visualizerCtx: CanvasRenderingContext2D | null = null;
  private isCapturing: boolean = false;
  private websocket: WebSocket | null = null;
  private setIsThinking: (thinking: boolean) => void;
  private silenceTimer: NodeJS.Timeout | null = null;
  private rollingBuffer: RollingBuffer;
  private readonly SILENCE_THRESHOLD = 0.01;
  private readonly SILENCE_DURATION = 5000; // Reduced to 500ms for faster response
  private readonly CHUNK_SIZE = 512;
  private consecutiveSilenceChunks: number = 0;
  private readonly MAX_SILENCE_CHUNKS = 3; // Number of chunks that must be silent before considering silence

  constructor(websocket: WebSocket | null, setIsThinking: (thinking: boolean) => void) {
    this.websocket = websocket;
    this.setIsThinking = setIsThinking;
    this.rollingBuffer = new RollingBuffer();
  }

  private detectSilence(audioData: Float32Array): boolean {
    const rms = Math.sqrt(
      audioData.reduce((sum, value) => sum + value * value, 0) / audioData.length
    );
    
    if (rms < this.SILENCE_THRESHOLD) {
      this.consecutiveSilenceChunks++;
    } else {
      this.consecutiveSilenceChunks = 0;
    }
    
    return this.consecutiveSilenceChunks >= this.MAX_SILENCE_CHUNKS;
  }

  private resetSilenceTimer() {
    if (this.silenceTimer) {
      clearTimeout(this.silenceTimer);
      this.silenceTimer = null;
    }
    this.consecutiveSilenceChunks = 0;
  }

  private sendAudioData(audioData: Int16Array) {
    if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) return;
    
    // Send the buffer directly
    this.websocket.send(audioData.buffer);
  }

  async start(canvas: HTMLCanvasElement) {
    if (this.isCapturing) {
      console.log('Already capturing audio');
      return;
    }

    console.log('Starting audio capture...');
    try {
      this.mediaStream = await navigator.mediaDevices.getUserMedia({
        audio: {
          channelCount: 1,
          sampleRate: 16000,
          echoCancellation: true,
          noiseSuppression: true
        }
      });

      this.audioContext = new AudioContext({
        sampleRate: 16000
      });

      const source = this.audioContext.createMediaStreamSource(this.mediaStream);
      this.processor = this.audioContext.createScriptProcessor(this.CHUNK_SIZE, 1, 1);
      this.visualizerCtx = canvas.getContext('2d');
      
      if (!this.visualizerCtx) throw new Error('Could not get canvas context');

      canvas.width = canvas.offsetWidth;
      canvas.height = canvas.offsetHeight;

      this.processor.onaudioprocess = (e) => {
        if (!this.isCapturing) return;
        
        const inputData = e.inputBuffer.getChannelData(0);
        const pcmData = new Int16Array(inputData.length);
        
        // Convert to PCM
        for (let i = 0; i < inputData.length; i++) {
          const s = Math.max(-1, Math.min(1, inputData[i]));
          pcmData[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
        }
        
        this.visualizeAudio(inputData, canvas);
        
        // Write to rolling buffer and check if we have a full buffer to send
        const bufferToSend = this.rollingBuffer.write(pcmData);
        if (bufferToSend) {
          this.sendAudioData(bufferToSend);
        }

        // Check for silence
        if (this.detectSilence(inputData)) {
          if (!this.silenceTimer) {
            this.silenceTimer = setTimeout(() => {
              // Send any remaining data in the rolling buffer
              const finalBuffer = this.rollingBuffer.write(new Int16Array(0));
              if (finalBuffer) {
                this.sendAudioData(finalBuffer);
              }
              this.rollingBuffer.clear();
            }, this.SILENCE_DURATION);
          }
        } else {
          this.resetSilenceTimer();
        }
      };

      source.connect(this.processor);
      this.processor.connect(this.audioContext.destination);
      this.isCapturing = true;
      this.rollingBuffer.clear();
      console.log('Audio capture setup complete');
    } catch (error) {
      console.error('Error setting up audio capture:', error);
      throw error;
    }
  }

  stop() {
    console.log('Stopping audio capture...');
    this.isCapturing = false;
    this.resetSilenceTimer();
    
    // Send any remaining audio data
    const finalBuffer = this.rollingBuffer.write(new Int16Array(0));
    if (finalBuffer) {
      this.sendAudioData(finalBuffer);
    }
    
    if (this.processor) {
      this.processor.disconnect();
      this.processor = null;
    }
    if (this.mediaStream) {
      this.mediaStream.getTracks().forEach(track => track.stop());
      this.mediaStream = null;
    }
    if (this.audioContext) {
      this.audioContext.close();
      this.audioContext = null;
    }
    this.rollingBuffer.clear();
    console.log('Audio capture stopped');
  }

  private visualizeAudio(audioData: Float32Array, canvas: HTMLCanvasElement) {
    if (!this.visualizerCtx) return;

    // Always white background
    this.visualizerCtx.fillStyle = '#fff';
    this.visualizerCtx.fillRect(0, 0, canvas.width, canvas.height);

    this.visualizerCtx.lineWidth = 2;
    this.visualizerCtx.strokeStyle = '#000'; // Black line for wave
    this.visualizerCtx.beginPath();

    const sliceWidth = canvas.width * 1.0 / audioData.length;
    let x = 0;

    for (let i = 0; i < audioData.length; i++) {
      const v = audioData[i] * 0.5;
      const y = (v * canvas.height/2) + canvas.height/2;

      if (i === 0) {
        this.visualizerCtx.moveTo(x, y);
      } else {
        this.visualizerCtx.lineTo(x, y);
      }

      x += sliceWidth;
    }

    this.visualizerCtx.lineTo(canvas.width, canvas.height/2);
    this.visualizerCtx.stroke();
  }
}

export default function AudioCapture({ websocket, isCapturing, onError, inline, setIsThinking }: AudioCaptureProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const captureServiceRef = useRef<AudioCaptureService | null>(null);

  useEffect(() => {
    if (!canvasRef.current) return;
    
    if (!captureServiceRef.current) {
      captureServiceRef.current = new AudioCaptureService(websocket, setIsThinking);
    }

    if (isCapturing) {
      captureServiceRef.current.start(canvasRef.current).catch(onError);
    } else {
      captureServiceRef.current.stop();
    }

    return () => {
      if (captureServiceRef.current) {
        captureServiceRef.current.stop();
      }
    };
  }, [websocket, isCapturing, onError, setIsThinking]);

  return (
    <canvas
      ref={canvasRef}
      className={inline ? "w-full h-6 bg-white" : "w-full h-24 bg-white rounded"}
      style={inline ? { background: 'transparent', borderRadius: 1, border: '0px solid #eee', width: '50px' } : {}}
      aria-label="Audio Visualizer"
    />
  );
} 
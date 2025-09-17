'use client';

import { useEffect, useRef } from 'react';

interface AudioPlaybackProps {
  audioQueue: string[];
  onAudioEnd: (audioUrl: string) => void;
  onAudioStart?: (audioUrl: string) => void;
}

export class AudioPlaybackService {
  private audioContext: AudioContext;
  private gainNode: GainNode;
  private isPlaying: boolean = false;
  private nextStartTime: number = 0;
  private scheduledBuffers: Set<AudioBufferSourceNode> = new Set();

  constructor() {
    this.audioContext = new AudioContext({ sampleRate: 24000 });
    this.gainNode = this.audioContext.createGain();
    this.gainNode.connect(this.audioContext.destination);
  }

  private async decodeBase64PCM(base64String: string): Promise<Float32Array> {
    const byteCharacters = atob(base64String);
    const byteArrays = new Int16Array(byteCharacters.length / 2);
    
    // Convert bytes to Int16 samples
    for (let i = 0; i < byteCharacters.length; i += 2) {
      byteArrays[i/2] = (byteCharacters.charCodeAt(i) | (byteCharacters.charCodeAt(i + 1) << 8));
    }
    
    // Convert Int16 to Float32 (-1.0 to 1.0)
    const floatArray = new Float32Array(byteArrays.length);
    for (let i = 0; i < byteArrays.length; i++) {
      floatArray[i] = byteArrays[i] / 32768.0;
    }
    
    return floatArray;
  }

  async playPCM(pcmBuffer: ArrayBuffer): Promise<void> {
    if (!pcmBuffer || pcmBuffer.byteLength === 0) {
      console.warn('Received empty PCM buffer');
      return;
    }

    try {
      // Convert PCM buffer to base64
      const pcmArray = new Uint8Array(pcmBuffer);
      let binary = '';
      for (let i = 0; i < pcmArray.length; i++) {
        binary += String.fromCharCode(pcmArray[i]);
      }
      const base64String = btoa(binary);

      // Decode the base64 PCM data
      const audioData = await this.decodeBase64PCM(base64String);
      
      // Create audio buffer
      const audioBuffer = this.audioContext.createBuffer(1, audioData.length, 24000);
      audioBuffer.getChannelData(0).set(audioData);

      // Create and configure source node
      const source = this.audioContext.createBufferSource();
      source.buffer = audioBuffer;
      source.connect(this.gainNode);
      
      // Schedule the playback
      const startTime = Math.max(this.audioContext.currentTime, this.nextStartTime);
      source.start(startTime);
      this.nextStartTime = startTime + audioBuffer.duration;
      
      // Track the source node
      this.scheduledBuffers.add(source);
      source.onended = () => {
        this.scheduledBuffers.delete(source);
        if (this.scheduledBuffers.size === 0) {
          this.isPlaying = false;
        }
      };

      this.isPlaying = true;
    } catch (error) {
      console.error('Error processing PCM buffer:', error);
      this.isPlaying = false;
    }
  }

  stop() {
    // Stop all scheduled buffers
    for (const source of this.scheduledBuffers) {
      try {
        source.stop();
      } catch (e) {
        // Ignore errors from already stopped sources
      }
    }
    this.scheduledBuffers.clear();
    this.isPlaying = false;
    this.nextStartTime = 0;

    // Reset audio context
    if (this.audioContext.state !== 'closed') {
      this.audioContext.close().then(() => {
        this.audioContext = new AudioContext({ sampleRate: 24000 });
        this.gainNode = this.audioContext.createGain();
        this.gainNode.connect(this.audioContext.destination);
      });
    }
  }
}

export default function AudioPlayback({ audioQueue, onAudioEnd, onAudioStart }: AudioPlaybackProps) {
  const playbackServiceRef = useRef<AudioPlaybackService | null>(null);
  const currentAudioRef = useRef<HTMLAudioElement | null>(null);

  useEffect(() => {
    if (!playbackServiceRef.current) {
      playbackServiceRef.current = new AudioPlaybackService();
    }

    return () => {
      if (playbackServiceRef.current) {
        playbackServiceRef.current.stop();
      }
    };
  }, []);

  useEffect(() => {
    if (audioQueue.length > 0 && currentAudioRef.current === null) {
      const audioUrl = audioQueue[0];
      console.log('[AUDIOPLAYBACK] Attempting to play', audioUrl);
      const audio = new Audio(audioUrl);
      
      audio.onplay = () => {
        console.log('[AUDIOPLAYBACK] Started', audioUrl);
        onAudioStart?.(audioUrl);
      };

      audio.onended = () => {
        console.log('[AUDIOPLAYBACK] Ended', audioUrl);
        URL.revokeObjectURL(audioUrl);
        currentAudioRef.current = null;
        onAudioEnd(audioUrl);
      };

      audio.onerror = () => {
        console.error('[AUDIOPLAYBACK] Error playing audio', audioUrl);
        URL.revokeObjectURL(audioUrl);
        currentAudioRef.current = null;
        onAudioEnd(audioUrl);
      };

      currentAudioRef.current = audio;
      audio.play().catch(error => {
        console.error('[AUDIOPLAYBACK] Error playing audio:', error);
        URL.revokeObjectURL(audioUrl);
        currentAudioRef.current = null;
        onAudioEnd(audioUrl);
      });
    }
  }, [audioQueue, onAudioEnd, onAudioStart]);

  return null;
} 
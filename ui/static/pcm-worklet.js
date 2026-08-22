/**
 * Microphone capture worklet: Float32 -> mono linear16 at a fixed sample rate.
 *
 * The browser's AudioContext may run at 44.1 or 48 kHz even when 16 kHz is
 * requested, so resampling happens here with linear interpolation rather than
 * being assumed away. Sarvam accepts only 8 or 16 kHz mono linear16.
 *
 * Posts one ArrayBuffer of Int16 samples per block, transferred not copied.
 */
class PCMCaptureProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    const opts = (options && options.processorOptions) || {};
    this.targetSampleRate = opts.targetSampleRate || 16000;
    this.blockSize = opts.blockSize || 512;

    // `sampleRate` is a global in AudioWorkletGlobalScope.
    this.ratio = sampleRate / this.targetSampleRate;
    this.out = new Int16Array(this.blockSize);
    this.filled = 0;
    this.pending = new Float32Array(0);
    this.readIndex = 0;
  }

  flush() {
    const chunk = this.out.slice(0, this.filled);
    this.filled = 0;
    this.port.postMessage(chunk.buffer, [chunk.buffer]);
  }

  process(inputs) {
    const channel = inputs[0] && inputs[0][0];
    if (!channel || channel.length === 0) {
      return true;
    }

    const merged = new Float32Array(this.pending.length + channel.length);
    merged.set(this.pending, 0);
    merged.set(channel, this.pending.length);

    let i = this.readIndex;
    while (i + 1 < merged.length) {
      const base = Math.floor(i);
      const t = i - base;
      const sample = merged[base] * (1 - t) + merged[base + 1] * t;
      const clamped = Math.max(-1, Math.min(1, sample));
      this.out[this.filled++] = clamped < 0 ? clamped * 0x8000 : clamped * 0x7fff;
      if (this.filled === this.blockSize) {
        this.flush();
      }
      i += this.ratio;
    }

    const consumed = Math.floor(i);
    this.pending = merged.slice(consumed);
    this.readIndex = i - consumed;
    return true;
  }
}

registerProcessor("pcm-capture", PCMCaptureProcessor);

import { Rnnoise, DenoiseState } from './rnnoise.js';

class RnnoiseProcessor extends AudioWorkletProcessor {
    static get parameterDescriptors() {
        return [{
            name: 'denoiseStrength',
            defaultValue: 0.99,
            minValue: 0,
            maxValue: 1,
            automationRate: 'k-rate'
        }];
    }

    constructor() {
        super();
        this.frameSize = 480;
        this.inputQueue = [];
        this.outputQueue = [];
        this.denoiseState = null;
        this.ready = false;
        this.frameCount = 0;
        this.prebufferCount = 0;
        this.minPrebuffer = 2; // Number of frames to buffer before starting output

        console.log(`RnnoiseProcessor: initializing... SampleRate=${globalThis.sampleRate}`);

        Rnnoise.load().then(rnnoise => {
            try {
                this.denoiseState = rnnoise.createDenoiseState();
                this.ready = true;
                console.log("rnnoise active");
            } catch (err) {
                console.error("RnnoiseProcessor: Failed to create denoise state", err);
            }
        }).catch(e => console.error("RnnoiseProcessor: RNNoise load failed", e));
    }

    process(inputs, outputs, parameters) {
        const input = inputs[0];
        const output = outputs[0];

        if (!input || !input.length || !output || !output.length) return true;

        const inputData = input[0];
        const outputData = output[0];

        if (!this.ready) {
            outputData.set(inputData);
            for (let i = 1; i < output.length; i++) {
                output[i].set(outputData);
            }
            return true;
        }

        try {
            for (let i = 0; i < inputData.length; i++) {
                this.inputQueue.push(inputData[i]);
            }
            if (this.inputQueue.length > this.frameSize * 5) {
                this.inputQueue.splice(0, this.inputQueue.length - this.frameSize);
            }

            while (this.inputQueue.length >= this.frameSize) {
                const frame = new Float32Array(this.inputQueue.splice(0, this.frameSize));
                const originalFrame = new Float32Array(frame);

                for (let i = 0; i < this.frameSize; i++) {
                    frame[i] *= 32768.0;
                }

                this.denoiseState.processFrame(frame);

                const strength = parameters.denoiseStrength ? parameters.denoiseStrength[0] : 0.8;

                for (let i = 0; i < this.frameSize; i++) {
                    const denoisedSample = frame[i] / 32768.0;
                    this.outputQueue.push(denoisedSample * strength + originalFrame[i] * (1.0 - strength));
                }
            }

            if (this.outputQueue.length >= outputData.length) {
                // If we haven't reached our prebuffer threshold yet, wait
                if (this.prebufferCount < this.minPrebuffer) {
                    this.prebufferCount++;
                    outputData.fill(0);
                } else {
                    for (let i = 0; i < outputData.length; i++) {
                        outputData[i] = this.outputQueue.shift();
                    }
                }
            } else {
                // If we run out of processed data (should be rare with prebuffering),
                // it's better to output silence or partial than to bypass and leak latency.
                outputData.fill(0);
            }

            // Limit queue size to prevent massive buildup if processing falls behind
            if (this.outputQueue.length > this.frameSize * 5) {
                console.warn("RnnoiseProcessor: output queue overflow, draining...");
                this.outputQueue.splice(0, this.outputQueue.length - this.frameSize);
            }

            for (let i = 1; i < output.length; i++) {
                output[i].set(outputData);
            }
        } catch (e) {
            console.error("RnnoiseProcessor: Error during processing", e);
            this.ready = false;
            outputData.set(inputData);
            for (let i = 1; i < output.length; i++) {
                output[i].set(outputData);
            }
        }

        return true;
    }
}

registerProcessor('rnnoise-processor', RnnoiseProcessor);

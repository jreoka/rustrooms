// Browser-side avatar resizing.
// - Oversized GIFs are captured frame-by-frame (canvas + rAF), downscaled and
//   re-encoded with the vendored gifenc encoder to fit the server's 2 MiB
//   avatar cap while keeping the animation.
// - Oversized still images are re-encoded to a smaller JPEG.
// All of this runs client-side and ships inside the server binary (include_str!).
(function () {
    'use strict';

    // Server cap (MAX_AVATAR_DATA_LEN) applies to the data URL string itself;
    // keep a margin for the data:image/gif;base64, prefix and JSON envelope.
    const MAX_AVATAR_DATA_URL_LEN = 2 * 1024 * 1024 - 100 * 1024;
    const GIF_MAX_CAPTURE_DIM = 400;
    // Capture dim for cropped avatars: cropping happens in capture space, so
    // a higher dim keeps zoomed-in crops from getting blurry. Bounded so a
    // pathological GIF can't blow up memory (2.5 MB per frame at 800px).
    const GIF_CROP_CAPTURE_DIM = 800;
    const GIF_MAX_FRAMES = 150;
    const GIF_QUIET_MS = 2000;
    // If the very first frame holds for GIF_QUIET_MS the capture would
    // conclude the GIF is static before the animation has shown a single
    // change (long intro cards, slow-start memes). Only apply the quiet
    // timeout after at least one frame change was observed; for a frame
    // that has never changed, wait up to this longer cap before treating
    // the GIF as truly static.
    const GIF_STATIC_MAX_MS = 8000;
    const MIN_FRAME_DELAY_MS = 20;
    const MAX_FRAME_DELAY_MS = 5000;

    function readFileAsDataUrl(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(reader.result);
            reader.onerror = () => reject(reader.error || new Error('Failed to read file'));
            reader.readAsDataURL(file);
        });
    }

    function loadImage(dataUrl) {
        return new Promise((resolve, reject) => {
            const img = new Image();
            img.onload = () => resolve(img);
            img.onerror = () => reject(new Error('Failed to decode image'));
            img.src = dataUrl;
        });
    }

    function loadImageDims(dataUrl) {
        return loadImage(dataUrl).then(img => ({ width: img.naturalWidth, height: img.naturalHeight }));
    }

    // FNV-1a 32-bit hash over RGBA pixels; also flags any translucent pixel.
    function hashAndCheckAlpha(data) {
        let hash = 0x811c9dc5;
        let hasAlpha = false;
        const n = data.length;
        for (let i = 0; i < n; i++) {
            hash ^= data[i];
            hash = Math.imul(hash, 16777619) >>> 0;
            if ((i & 3) === 3 && data[i] < 255) hasAlpha = true;
        }
        return { hash, hasAlpha };
    }

    // Captures an animated GIF frame-by-frame by rendering it to a canvas on
    // each animation frame and recording distinct frames plus how long each
    // was displayed. Loop duplicates are merged back into their original
    // frame. Stops after one full loop, after GIF_QUIET_MS without a change
    // (static tail / long hold; the first frame gets GIF_STATIC_MAX_MS so
    // slow-starting animations aren't mistaken for static), or at the frame
    // cap.
    function captureGifFramesRaf(dataUrl, maxDim) {
        return new Promise((resolve, reject) => {
            const img = new Image();
            // The browser only runs a GIF's animation while it is painted;
            // attach the image (invisibly) so frames actually advance.
            img.style.position = 'fixed';
            img.style.left = '0';
            img.style.top = '0';
            img.style.width = '1px';
            img.style.height = '1px';
            img.style.opacity = '0.01';
            img.style.pointerEvents = 'none';
            img.style.zIndex = '-1';
            img.alt = '';
            if (document.body) {
                document.body.appendChild(img);
            }
            const cleanup = () => {
                if (img.parentNode) {
                    img.parentNode.removeChild(img);
                }
            };
            img.onerror = () => {
                cleanup();
                reject(new Error('Failed to decode GIF'));
            };
            img.onload = () => {
                const scale = Math.min(maxDim / img.naturalWidth, maxDim / img.naturalHeight, 1);
                const width = Math.max(1, Math.round(img.naturalWidth * scale));
                const height = Math.max(1, Math.round(img.naturalHeight * scale));
                const canvas = document.createElement('canvas');
                canvas.width = width;
                canvas.height = height;
                const ctx = canvas.getContext('2d', { willReadFrequently: true });

                const frames = [];
                const hashes = [];
                let hasAlpha = false;
                let currentIndex = -1;
                let lastChange = 0;

                const finish = (now) => {
                    cleanup();
                    if (currentIndex >= 0) {
                        frames[currentIndex].delay += Math.max(0, now - lastChange);
                    }
                    if (frames.length === 0) {
                        reject(new Error('No frames captured'));
                    } else {
                        resolve({ frames, width, height, hasAlpha });
                    }
                };

                const tick = () => {
                    const now = performance.now();
                    ctx.drawImage(img, 0, 0, width, height);
                    const data = ctx.getImageData(0, 0, width, height).data;
                    const result = hashAndCheckAlpha(data);
                    hasAlpha = hasAlpha || result.hasAlpha;
                    if (result.hash === hashes[currentIndex]) {
                        const hold = now - lastChange;
                        if (
                            (frames.length > 1 && hold >= GIF_QUIET_MS) ||
                            (frames.length === 1 && hold >= GIF_STATIC_MAX_MS) ||
                            frames.length >= GIF_MAX_FRAMES
                        ) {
                            finish(now);
                            return;
                        }
                    } else {
                        if (currentIndex >= 0) {
                            frames[currentIndex].delay += Math.max(0, now - lastChange);
                        }
                        lastChange = now;
                        const dup = hashes.indexOf(result.hash);
                        if (dup === 0) {
                            // Completed one full loop of the animation.
                            currentIndex = dup;
                            finish(now);
                            return;
                        }
                        if (dup >= 0) {
                            currentIndex = dup;
                        } else {
                            currentIndex = frames.length;
                            frames.push({ data: new Uint8ClampedArray(data), delay: 0 });
                            hashes.push(result.hash);
                        }
                    }
                    if (frames.length >= GIF_MAX_FRAMES) {
                        finish(now);
                        return;
                    }
                    requestAnimationFrame(tick);
                };
                requestAnimationFrame(tick);
            };
            img.src = dataUrl;
        });
    }

    // Converts a base64 data URL to a Uint8Array. Must not use fetch():
    // the app's CSP (connect-src 'self' wss: ws:) blocks data: URLs.
    function dataUrlToBytes(dataUrl) {
        const comma = dataUrl.indexOf(',');
        if (comma < 0) {
            throw new Error('Not a data URL');
        }
        const binary = atob(dataUrl.slice(comma + 1));
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i++) {
            bytes[i] = binary.charCodeAt(i);
        }
        return bytes;
    }

    // Decodes every frame of a GIF into canvases (maxDim-capped) plus their
    // GCE delays in ms. Prefers WebCodecs ImageDecoder where it still exists
    // (Chrome < 130); everywhere else a built-in GIF87a/89a parser + LZW
    // decoder is used, so GIF capture, crop and previews keep working with
    // zero dependence on browser GIF playback.
    async function decodeGifFrames(dataUrl, maxDim) {
        if (typeof ImageDecoder !== 'undefined') {
            try {
                if (await ImageDecoder.isTypeSupported('image/gif')) {
                    return await decodeGifFramesWithImageDecoder(dataUrl, maxDim);
                }
            } catch (err) {
                console.warn('ImageDecoder GIF decode failed, using built-in decoder:', err);
            }
        }
        return decodeGifFramesBuiltIn(dataUrl, maxDim);
    }

    // WebCodecs ImageDecoder path (Chrome < 130 only).
    async function decodeGifFramesWithImageDecoder(dataUrl, maxDim) {
        const decoder = new ImageDecoder({ data: dataUrlToBytes(dataUrl), type: 'image/gif' });
        try {
            await decoder.tracks.ready;
            const track = decoder.tracks.selectedTrack;
            const known = track ? track.frameCount : 0;
            // GIF has no frame count field; Chrome may report Infinity for
            // large files, so iterate until decode fails.
            const total = typeof known === 'number' && Number.isFinite(known) && known > 0
                ? Math.min(known, GIF_MAX_FRAMES)
                : GIF_MAX_FRAMES;
            const frames = [];
            let width = 0;
            let height = 0;
            for (let i = 0; i < total; i++) {
                let image = null;
                try {
                    const res = await decoder.decode({ frameIndex: i });
                    image = res.image;
                } catch (err) {
                    if (frames.length === 0) {
                        throw err;
                    }
                    break;
                }
                if (!image) {
                    if (frames.length === 0) {
                        throw new Error('No frames captured');
                    }
                    break;
                }
                try {
                    if (frames.length === 0) {
                        const scale = Math.min(maxDim / image.displayWidth, maxDim / image.displayHeight, 1);
                        width = Math.max(1, Math.round(image.displayWidth * scale));
                        height = Math.max(1, Math.round(image.displayHeight * scale));
                    }
                    // VideoFrame.duration is in microseconds; the last frame
                    // often reports null, so reuse the previous delay.
                    let delay = frames.length > 0 ? frames[frames.length - 1].delay : 100;
                    if (image.duration != null) {
                        delay = Math.max(0, image.duration / 1000);
                    }
                    // Draw to a canvas instead of createImageBitmap: VideoFrame
                    // is directly drawable and this avoids any bitmap API
                    // availability surprises.
                    const canvas = document.createElement('canvas');
                    canvas.width = width;
                    canvas.height = height;
                    const cctx = canvas.getContext('2d');
                    cctx.drawImage(image, 0, 0, width, height);
                    frames.push({ canvas, delay });
                } finally {
                    image.close();
                }
                await yieldToEventLoop();
            }
            if (frames.length === 0) {
                throw new Error('No frames captured');
            }
            return { frames, width, height };
        } finally {
            decoder.close();
        }
    }

    // Built-in GIF parser. WebCodecs ImageDecoder was removed from Chrome
    // 130+ and never shipped in Firefox/Safari, so GIFs are parsed here
    // instead: GCT/LCT palettes, interlace, transparency, disposal methods
    // and GCE delays, composited frame-by-frame.
    function decodeGifFramesBuiltIn(dataUrl, maxDim) {
        return decodeGifBytes(dataUrlToBytes(dataUrl), maxDim);
    }

    async function decodeGifBytes(bytes, maxDim) {
        if (bytes.length < 13) {
            throw new Error('Invalid GIF: too short');
        }
        const signature = String.fromCharCode(bytes[0], bytes[1], bytes[2]);
        const version = String.fromCharCode(bytes[3], bytes[4], bytes[5]);
        if (signature !== 'GIF' || (version !== '87a' && version !== '89a')) {
            throw new Error('Invalid GIF: bad header');
        }
        let pos = 6;
        const logicalWidth = bytes[pos] | (bytes[pos + 1] << 8); pos += 2;
        const logicalHeight = bytes[pos] | (bytes[pos + 1] << 8); pos += 2;
        if (!logicalWidth || !logicalHeight) {
            throw new Error('Invalid GIF: zero dimensions');
        }
        const packed = bytes[pos++];
        const bgColorIndex = bytes[pos++];
        pos++; // pixel aspect ratio
        let palette = null;
        if (packed & 0x80) {
            palette = [];
            const size = 2 << (packed & 0x07);
            for (let i = 0; i < size; i++) {
                palette.push([bytes[pos], bytes[pos + 1], bytes[pos + 2]]);
                pos += 3;
            }
        }

        const dim = maxDim || GIF_MAX_CAPTURE_DIM;
        const scale = Math.min(dim / logicalWidth, dim / logicalHeight, 1);
        const width = Math.max(1, Math.round(logicalWidth * scale));
        const height = Math.max(1, Math.round(logicalHeight * scale));
        const canvas = document.createElement('canvas');
        canvas.width = width;
        canvas.height = height;
        const ctx = canvas.getContext('2d');

        const frames = [];
        let pendingGce = null;
        let prevDisposal = 0;
        let prevRect = null;
        let prevCopy = null;

        while (pos < bytes.length) {
            const blockType = bytes[pos++];
            if (blockType === 0x3b) break; // trailer
            if (blockType === 0x21) {
                const label = bytes[pos++];
                if (label === 0xf9) {
                    // Graphic Control Extension
                    pos++; // block size (4)
                    const flags = bytes[pos++];
                    const delayUnits = bytes[pos] | (bytes[pos + 1] << 8); pos += 2;
                    const transparentIndex = bytes[pos++];
                    pos++; // block terminator
                    pendingGce = {
                        transparency: (flags & 0x01) !== 0,
                        transparentIndex,
                        disposal: (flags & 0x1c) >> 2,
                        // 0 = unspecified; browsers treat it as ~100ms.
                        delay: (delayUnits === 0 ? 100 : delayUnits * 10),
                    };
                } else if (label === 0xff) {
                    // Application extension (NETSCAPE loop count is not needed
                    // here - every frame is emitted once).
                    pos = skipGifSubBlocks(bytes, pos);
                } else {
                    // Comment / plain text / unknown extension.
                    pos = skipGifSubBlocks(bytes, pos);
                }
            } else if (blockType === 0x2c) {
                // Image Descriptor
                const left = bytes[pos] | (bytes[pos + 1] << 8); pos += 2;
                const top = bytes[pos] | (bytes[pos + 1] << 8); pos += 2;
                const frameWidth = bytes[pos] | (bytes[pos + 1] << 8); pos += 2;
                const frameHeight = bytes[pos] | (bytes[pos + 1] << 8); pos += 2;
                const imgPacked = bytes[pos++];
                let framePalette = palette;
                if (imgPacked & 0x80) {
                    framePalette = [];
                    const size = 2 << (imgPacked & 0x07);
                    for (let i = 0; i < size; i++) {
                        framePalette.push([bytes[pos], bytes[pos + 1], bytes[pos + 2]]);
                        pos += 3;
                    }
                }
                if (!framePalette) {
                    throw new Error('Invalid GIF: no color table');
                }
                const interlace = (imgPacked & 0x40) !== 0;
                const minCodeSize = bytes[pos++];
                const lzwBlocks = [];
                while (pos < bytes.length) {
                    const size = bytes[pos++];
                    if (size === 0) break;
                    if (pos + size > bytes.length) break;
                    lzwBlocks.push(bytes.subarray(pos, pos + size));
                    pos += size;
                }
                const indices = lzwDecode(lzwBlocks, minCodeSize, frameWidth * frameHeight);
                const gce = pendingGce || { transparency: false, transparentIndex: 0, disposal: 0, delay: 100 };
                pendingGce = null;

                // Apply the previous frame's disposal before drawing this one.
                if (prevDisposal === 2 && prevRect) {
                    ctx.clearRect(prevRect.x, prevRect.y, prevRect.w, prevRect.h);
                } else if (prevDisposal === 3 && prevCopy) {
                    ctx.drawImage(prevCopy, 0, 0);
                }
                if (gce.disposal === 3) {
                    prevCopy = document.createElement('canvas');
                    prevCopy.width = width;
                    prevCopy.height = height;
                    prevCopy.getContext('2d').drawImage(canvas, 0, 0);
                } else {
                    prevCopy = null;
                }

                drawGifFrame(ctx, framePalette, indices, left, top, frameWidth, frameHeight, interlace, gce, scale);

                prevDisposal = gce.disposal;
                prevRect = {
                    x: Math.round(left * scale),
                    y: Math.round(top * scale),
                    w: Math.max(1, Math.round(frameWidth * scale)),
                    h: Math.max(1, Math.round(frameHeight * scale)),
                };

                const frameCanvas = document.createElement('canvas');
                frameCanvas.width = width;
                frameCanvas.height = height;
                frameCanvas.getContext('2d').drawImage(canvas, 0, 0);
                frames.push({ canvas: frameCanvas, delay: gce.delay });
                await yieldToEventLoop();
            } else {
                throw new Error('Invalid GIF: unexpected block 0x' + blockType.toString(16));
            }
        }
        if (frames.length === 0) {
            throw new Error('No frames captured');
        }
        return { frames, width, height };
    }

    function skipGifSubBlocks(bytes, pos) {
        while (pos < bytes.length) {
            const size = bytes[pos++];
            if (size === 0) return pos;
            pos += size;
        }
        return pos;
    }

    // GIF LZW decompression: LSB-first codes, 2-12 bit code width, clear/end
    // codes and the "KWI" (code == nextCode) case. Returns a Uint8Array of
    // exactly expectedPixels (padded with 0 if the stream is truncated).
    function lzwDecode(blocks, minCodeSize, expectedPixels) {
        if (minCodeSize < 2 || minCodeSize > 8) {
            throw new Error('Invalid GIF: bad LZW min code size');
        }
        const clearCode = 1 << minCodeSize;
        const endCode = clearCode + 1;
        const out = new Uint8Array(expectedPixels);
        const dict = new Array(4096);
        for (let i = 0; i < clearCode; i++) {
            dict[i] = { prefix: -1, suffix: i, first: i };
        }
        let codeSize = minCodeSize + 1;
        let nextCode = endCode + 1;
        let prev = -1;
        let bitBuffer = 0;
        let bitCount = 0;
        let written = 0;
        let done = false;
        const scratch = new Array(4096);
        const writeChain = (code, extra) => {
            let len = 0;
            let cur = code;
            while (cur >= 0 && len < scratch.length) {
                scratch[len++] = dict[cur].suffix;
                cur = dict[cur].prefix;
            }
            for (let i = len - 1; i >= 0 && written < expectedPixels; i--) {
                out[written++] = scratch[i];
            }
            if (extra >= 0 && written < expectedPixels) {
                out[written++] = extra;
            }
        };
        for (let b = 0; b < blocks.length && !done; b++) {
            const block = blocks[b];
            for (let i = 0; i < block.length && !done; i++) {
                bitBuffer |= block[i] << bitCount;
                bitCount += 8;
                while (bitCount >= codeSize && !done) {
                    const code = bitBuffer & ((1 << codeSize) - 1);
                    bitBuffer >>>= codeSize;
                    bitCount -= codeSize;
                    if (code === clearCode) {
                        codeSize = minCodeSize + 1;
                        nextCode = endCode + 1;
                        prev = -1;
                    } else if (code === endCode) {
                        done = true;
                    } else if (prev === -1) {
                        // First code after a clear: single dictionary entry.
                        writeChain(code, -1);
                        prev = code;
                    } else {
                        if (code < nextCode) {
                            writeChain(code, -1);
                            dict[nextCode] = { prefix: prev, suffix: dict[code].first, first: dict[prev].first };
                        } else if (code === nextCode) {
                            // KWI case: sequence of prev + its first byte.
                            writeChain(prev, dict[prev].first);
                            dict[nextCode] = { prefix: prev, suffix: dict[prev].first, first: dict[prev].first };
                        } else {
                            throw new Error('Invalid GIF: bad LZW code');
                        }
                        nextCode++;
                        if (nextCode === (1 << codeSize) && codeSize < 12) {
                            codeSize++;
                        }
                        prev = code;
                    }
                    if (written >= expectedPixels) done = true;
                }
            }
        }
        return out;
    }

    function interlaceRowMap(fh) {
        // maps image row -> data row: the LZW data stores rows in pass order
        // (0,8,16,... then 4,12,... then 2,6,10,... then 1,3,5,...).
        const map = new Uint16Array(fh);
        let row = 0;
        const assign = (i) => { map[i] = row++; };
        for (let i = 0; i < fh; i += 8) assign(i);
        for (let i = 4; i < fh; i += 8) assign(i);
        for (let i = 2; i < fh; i += 4) assign(i);
        for (let i = 1; i < fh; i += 2) assign(i);
        return map;
    }

    // Composites one GIF frame (palette indices, possibly interlaced, with
    // GCE transparency) onto the shared canvas, scaled to the capture dim.
    function drawGifFrame(ctx, palette, indices, left, top, fw, fh, interlace, gce, scale) {
        const sx = Math.round(left * scale);
        const sy = Math.round(top * scale);
        const sw = Math.max(1, Math.round(fw * scale));
        const sh = Math.max(1, Math.round(fh * scale));
        const transIdx = gce.transparency ? gce.transparentIndex : -1;
        const rowMap = interlace ? interlaceRowMap(fh) : null;
        const srcRow = (y) => (rowMap ? rowMap[y] : y);

        if (fw * fh <= 4 * 1024 * 1024) {
            // Full-resolution temp canvas, then drawImage for smooth scaling.
            const temp = document.createElement('canvas');
            temp.width = fw;
            temp.height = fh;
            const tctx = temp.getContext('2d');
            const imgData = tctx.createImageData(fw, fh);
            const data = imgData.data;
            for (let y = 0; y < fh; y++) {
                const rowBase = srcRow(y) * fw;
                for (let x = 0; x < fw; x++) {
                    const idx = indices[rowBase + x];
                    const o = (y * fw + x) * 4;
                    if (idx === transIdx || idx === undefined) {
                        data[o + 3] = 0;
                    } else {
                        const c = palette[idx] || palette[0];
                        data[o] = c[0];
                        data[o + 1] = c[1];
                        data[o + 2] = c[2];
                        data[o + 3] = 255;
                    }
                }
            }
            tctx.putImageData(imgData, 0, 0);
            ctx.drawImage(temp, sx, sy, sw, sh);
        } else {
            // Very large frame: sample directly into the scaled rect so the
            // temp canvas can't balloon into hundreds of MB.
            const img = ctx.createImageData(sw, sh);
            const data = img.data;
            for (let y = 0; y < sh; y++) {
                const srcY = Math.min(fh - 1, Math.floor(y / scale));
                const rowBase = srcRow(srcY) * fw;
                for (let x = 0; x < sw; x++) {
                    const srcX = Math.min(fw - 1, Math.floor(x / scale));
                    const idx = indices[rowBase + srcX];
                    const o = (y * sw + x) * 4;
                    if (idx === transIdx || idx === undefined) {
                        data[o + 3] = 0;
                    } else {
                        const c = palette[idx] || palette[0];
                        data[o] = c[0];
                        data[o + 1] = c[1];
                        data[o + 2] = c[2];
                        data[o + 3] = 255;
                    }
                }
            }
            ctx.putImageData(img, sx, sy);
        }
    }

    // Deterministic capture via WebCodecs ImageDecoder (Chrome): decodes
    // every GIF frame on demand with its exact GCE delay, independent of
    // browser GIF playback. Falls back to canvas capture elsewhere.
    async function captureGifFramesWithDecoder(dataUrl, maxDim) {
        const decoded = await decodeGifFrames(dataUrl, maxDim);
        const canvas = document.createElement('canvas');
        canvas.width = decoded.width;
        canvas.height = decoded.height;
        const ctx = canvas.getContext('2d', { willReadFrequently: true });
        const frames = [];
        const hashes = [];
        let hasAlpha = false;
        for (const frame of decoded.frames) {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.drawImage(frame.canvas, 0, 0, canvas.width, canvas.height);
            const data = ctx.getImageData(0, 0, canvas.width, canvas.height).data;
            const result = hashAndCheckAlpha(data);
            hasAlpha = hasAlpha || result.hasAlpha;
            const dup = hashes.indexOf(result.hash);
            if (dup >= 0) {
                // Identical to an earlier frame: merge the display time back
                // (same loop structure the canvas capture produces) so
                // repeated frames don't bloat the file.
                frames[dup].delay += frame.delay;
            } else {
                frames.push({ data: new Uint8ClampedArray(data), delay: frame.delay });
                hashes.push(result.hash);
            }
            await yieldToEventLoop();
        }
        return { frames, width: decoded.width, height: decoded.height, hasAlpha };
    }

    // Deterministic frame capture via the built-in GIF decoder (or
    // ImageDecoder where available): decodes every frame with its exact GCE
    // delay, independent of browser GIF playback. Falls back to canvas
    // capture on any decoder failure.
    async function captureGifFrames(dataUrl, maxDim) {
        try {
            return await captureGifFramesWithDecoder(dataUrl, maxDim);
        } catch (err) {
            console.warn('GIF frame decode failed, using canvas capture:', err);
        }
        return captureGifFramesRaf(dataUrl, maxDim);
    }

    // Yields to the event loop so WebSocket heartbeat timers keep firing
    // during long processing; the server closes connections that stay
    // silent for 10s ("Inactivity timeout").
    function yieldToEventLoop() {
        return new Promise((resolve) => setTimeout(resolve, 0));
    }

    function clampDelay(ms) {
        return Math.max(MIN_FRAME_DELAY_MS, Math.min(MAX_FRAME_DELAY_MS, ms));
    }

    function bytesToDataUrl(bytes, mime) {
        let binary = '';
        const chunk = 0x8000;
        for (let i = 0; i < bytes.length; i += chunk) {
            binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
        }
        return 'data:' + mime + ';base64,' + btoa(binary);
    }

    async function encodeGifFrames(frames, width, height, hasAlpha) {
        const enc = globalThis.__gifenc;
        const gif = new enc.GIFEncoder();
        for (const frame of frames) {
            // GIF transparency is binary, so semi-transparent pixels were
            // never actually blended by GIF decoders; encoding in rgb565 and
            // forcing fully transparent pixels to a dedicated transparent
            // palette entry is visually equivalent to rgba4444 but ~20x
            // faster (rgba4444 alpha-aware distance is very slow).
            const rgba = new Uint8ClampedArray(frame.data);
            // gifenc's quantize is O(distinct-colors^2) and explodes on
            // photo-like frames (~3s/frame); rounding colors first caps the
            // distinct-color count and makes it fast (~10-250ms/frame).
            // roundRGB must divide 255 (e.g. 15) or values near 255 wrap
            // around to black in gifenc's bit packing.
            enc.prequantize(rgba, { roundRGB: 15, roundAlpha: 255 });
            let palette;
            let index;
            let transparentIndex = -1;
            if (hasAlpha) {
                palette = enc.quantize(rgba, 255, { format: 'rgb565' });
                palette.push([0, 0, 1, 0]);
                transparentIndex = palette.length - 1;
                index = enc.applyPalette(rgba, palette, 'rgb565');
                for (let i = 0; i < frame.data.length; i += 4) {
                    if (frame.data[i + 3] === 0) {
                        index[i >> 2] = transparentIndex;
                    }
                }
            } else {
                palette = enc.quantize(rgba, 256, { format: 'rgb565' });
                index = enc.applyPalette(rgba, palette, 'rgb565');
            }
            const opts = { palette, delay: clampDelay(frame.delay), repeat: 0 };
            if (transparentIndex >= 0) {
                opts.transparent = true;
                opts.transparentIndex = transparentIndex;
            }
            gif.writeFrame(index, width, height, opts);
            await yieldToEventLoop();
        }
        gif.finish();
        return bytesToDataUrl(gif.bytes(), 'image/gif');
    }

    async function downscaleGifFrames(frames, srcW, srcH, dstW, dstH) {
        const src = document.createElement('canvas');
        src.width = srcW;
        src.height = srcH;
        const sctx = src.getContext('2d');
        const dst = document.createElement('canvas');
        dst.width = dstW;
        dst.height = dstH;
        const dctx = dst.getContext('2d');
        const out = [];
        for (const frame of frames) {
            sctx.putImageData(new ImageData(frame.data, srcW, srcH), 0, 0);
            dctx.clearRect(0, 0, dstW, dstH);
            dctx.drawImage(src, 0, 0, dstW, dstH);
            out.push({
                data: new Uint8ClampedArray(dctx.getImageData(0, 0, dstW, dstH).data),
                delay: frame.delay,
            });
            await yieldToEventLoop();
        }
        return out;
    }

    async function resizeGifDataUrlToFit(dataUrl) {
        const captured = await captureGifFrames(dataUrl, GIF_MAX_CAPTURE_DIM);
        // Rough raw-byte budget a data URL of MAX_AVATAR_DATA_URL_LEN fits
        // (base64 is 4/3 of raw). Indexed GIF frames run ~0.8 bytes/pixel
        // after LZW for typical avatar content; skip dims whose total pixel
        // data can't fit so we don't burn an encode pass on them.
        const rawBudget = Math.floor((MAX_AVATAR_DATA_URL_LEN * 3) / 4);
        const dims = [400, 320, 256, 200, 160, 128];
        for (const dim of dims) {
            if (dim > captured.width && dim > captured.height) continue;
            const scale = Math.min(dim / captured.width, dim / captured.height, 1);
            const width = Math.max(1, Math.round(captured.width * scale));
            const height = Math.max(1, Math.round(captured.height * scale));
            if (captured.frames.length * width * height * 0.8 > rawBudget) continue;
            const frames = (width === captured.width && height === captured.height)
                ? captured.frames
                : await downscaleGifFrames(captured.frames, captured.width, captured.height, width, height);
            const out = await encodeGifFrames(frames, width, height, captured.hasAlpha);
            if (out.length <= MAX_AVATAR_DATA_URL_LEN) {
                return { avatar: out, isGif: true };
            }
        }
        // Practically unreachable fallback: first frame as a small JPEG.
        const canvas = document.createElement('canvas');
        canvas.width = captured.width;
        canvas.height = captured.height;
        const ctx = canvas.getContext('2d');
        ctx.putImageData(new ImageData(captured.frames[0].data, captured.width, captured.height), 0, 0);
        let jpeg = canvas.toDataURL('image/jpeg', 0.7);
        if (jpeg.length > MAX_AVATAR_DATA_URL_LEN) {
            jpeg = canvas.toDataURL('image/jpeg', 0.3);
        }
        return { avatar: jpeg, isGif: false };
    }

    async function cropGifFrames(frames, srcW, srcH, cx, cy, cw, ch) {
        const src = document.createElement('canvas');
        src.width = srcW;
        src.height = srcH;
        const sctx = src.getContext('2d');
        const dst = document.createElement('canvas');
        dst.width = cw;
        dst.height = ch;
        const dctx = dst.getContext('2d');
        const out = [];
        for (const frame of frames) {
            sctx.clearRect(0, 0, srcW, srcH);
            sctx.putImageData(new ImageData(frame.data, srcW, srcH), 0, 0);
            dctx.clearRect(0, 0, cw, ch);
            dctx.drawImage(src, cx, cy, cw, ch, 0, 0, cw, ch);
            out.push({
                data: new Uint8ClampedArray(dctx.getImageData(0, 0, cw, ch).data),
                delay: frame.delay,
            });
            await yieldToEventLoop();
        }
        return out;
    }

    // Crops every frame of an animated GIF to the given rectangle and
    // re-encodes it, preserving the animation. cropRect is in the original
    // image's pixel space ({ x, y, width, height }, as returned by
    // Croppie.get()). Downscales to fit the server cap if needed.
    async function cropGifDataUrl(dataUrl, cropRect) {
        const natural = await loadImageDims(dataUrl);
        const captured = await captureGifFrames(dataUrl, GIF_CROP_CAPTURE_DIM);
        const scale = captured.width / natural.width;
        let cx = Math.round(cropRect.x * scale);
        let cy = Math.round(cropRect.y * scale);
        let cw = Math.max(1, Math.round(cropRect.width * scale));
        let ch = Math.max(1, Math.round(cropRect.height * scale));
        cx = Math.max(0, Math.min(cx, captured.width - 1));
        cy = Math.max(0, Math.min(cy, captured.height - 1));
        cw = Math.max(1, Math.min(cw, captured.width - cx));
        ch = Math.max(1, Math.min(ch, captured.height - cy));
        const cropped = await cropGifFrames(captured.frames, captured.width, captured.height, cx, cy, cw, ch);
        const rawBudget = Math.floor((MAX_AVATAR_DATA_URL_LEN * 3) / 4);
        const maxSide = Math.max(cw, ch);
        const sizes = [{ w: cw, h: ch }];
        for (const dim of [400, 320, 256, 200, 160, 128]) {
            if (dim < maxSide) {
                const s = dim / maxSide;
                sizes.push({ w: Math.max(1, Math.round(cw * s)), h: Math.max(1, Math.round(ch * s)) });
            }
        }
        let last = null;
        for (const size of sizes) {
            if (last && last.w === size.w && last.h === size.h) continue;
            last = size;
            if (cropped.length * size.w * size.h * 0.8 > rawBudget) continue;
            const frames = (size.w === cw && size.h === ch)
                ? cropped
                : await downscaleGifFrames(cropped, cw, ch, size.w, size.h);
            const avatar = await encodeGifFrames(frames, size.w, size.h, captured.hasAlpha);
            if (avatar.length <= MAX_AVATAR_DATA_URL_LEN) {
                const staticFrame = await extractGifFirstFrame(avatar);
                return { avatar, staticFrame, isGif: true };
            }
        }
        // Practically unreachable fallback: first frame as a small JPEG.
        const canvas = document.createElement('canvas');
        canvas.width = cw;
        canvas.height = ch;
        const ctx = canvas.getContext('2d');
        ctx.putImageData(new ImageData(cropped[0].data, cw, ch), 0, 0);
        let jpeg = canvas.toDataURL('image/jpeg', 0.7);
        if (jpeg.length > MAX_AVATAR_DATA_URL_LEN) {
            jpeg = canvas.toDataURL('image/jpeg', 0.3);
        }
        const staticFrame = await extractGifFirstFrame(jpeg);
        return { avatar: jpeg, staticFrame, isGif: false };
    }

    // JS-driven GIF player for the speaking animation and avatar/crop
    // previews. Some browsers (enterprise AnimationPolicy, extensions) never
    // advance GIF frames in <img> elements; stepping pre-decoded frames with
    // a timer still works there. Frames come from the built-in GIF decoder
    // (or ImageDecoder where available). Returns null when decoding fails or
    // the GIF has fewer than two frames (static GIFs keep using the plain
    // <img> src swap).
    // maxDim caps the frame capture size (larger = sharper previews when the
    // GIF is displayed larger than GIF_MAX_CAPTURE_DIM).
    async function createGifAnimator(dataUrl, maxDim) {
        let decoded;
        try {
            decoded = await decodeGifFrames(dataUrl, maxDim || GIF_MAX_CAPTURE_DIM);
        } catch (err) {
            console.warn('GIF decode failed:', err);
            return null;
        }
        if (!decoded || decoded.frames.length < 2) {
            return null;
        }
        const frames = decoded.frames;
        let timer = null;
        let index = 0;
        let onFrame = null;
        const tick = () => {
            if (onFrame) {
                onFrame(frames[index].canvas);
            }
            const delay = Math.max(MIN_FRAME_DELAY_MS, Math.min(MAX_FRAME_DELAY_MS, frames[index].delay));
            index = (index + 1) % frames.length;
            timer = setTimeout(tick, delay);
        };
        return {
            start(callback) {
                onFrame = callback;
                index = 0;
                tick();
            },
            stop() {
                if (timer) {
                    clearTimeout(timer);
                    timer = null;
                }
                onFrame = null;
            },
        };
    }

    async function processGifAvatar(file) {
        const dataUrl = await readFileAsDataUrl(file);
        let avatar = dataUrl;
        let isGif = true;
        if (avatar.length > MAX_AVATAR_DATA_URL_LEN) {
            const resized = await resizeGifDataUrlToFit(dataUrl);
            avatar = resized.avatar;
            isGif = resized.isGif;
        }
        const staticFrame = await extractGifFirstFrame(avatar);
        return { avatar, staticFrame, isGif };
    }

    async function fitStaticDataUrl(dataUrl) {
        if (dataUrl.length <= MAX_AVATAR_DATA_URL_LEN) return dataUrl;
        const img = await loadImage(dataUrl);
        const maxDim = 400;
        const scale = Math.min(maxDim / img.naturalWidth, maxDim / img.naturalHeight, 1);
        const width = Math.max(1, Math.round(img.naturalWidth * scale));
        const height = Math.max(1, Math.round(img.naturalHeight * scale));
        const canvas = document.createElement('canvas');
        canvas.width = width;
        canvas.height = height;
        const ctx = canvas.getContext('2d');
        ctx.drawImage(img, 0, 0, width, height);
        for (const quality of [0.7, 0.5, 0.3]) {
            const out = canvas.toDataURL('image/jpeg', quality);
            if (out.length <= MAX_AVATAR_DATA_URL_LEN) return out;
        }
        return canvas.toDataURL('image/jpeg', 0.2);
    }

    globalThis.processGifAvatar = processGifAvatar;
    globalThis.fitStaticDataUrl = fitStaticDataUrl;
    globalThis.createGifAnimator = createGifAnimator;
    globalThis.readAvatarFile = readFileAsDataUrl;
    globalThis.cropGifAvatar = cropGifDataUrl;
    // Test hooks (used by the diagnostic page / node verification).
    globalThis.__captureGifFramesWithDecoder = captureGifFramesWithDecoder;
    globalThis.__decodeGifBytes = decodeGifBytes;
    // Test hooks (used by the node-based verification script).
    globalThis.__avatarTools = {
        encodeGifFrames,
        bytesToDataUrl,
        hashAndCheckAlpha,
        clampDelay,
        MAX_AVATAR_DATA_URL_LEN,
    };
})();

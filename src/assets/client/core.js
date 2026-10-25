
        function decodePathSegment(value) {
            try {
                return decodeURIComponent(value);
            } catch (error) {
                console.warn('Ignoring malformed URL encoding:', error);
                return value;
            }
        }

        let parts = window.location.pathname.split('/').filter(p => p !== '');
        let roomId = decodePathSegment(parts[0] || '');
        let channelId = decodePathSegment(parts[1] || '').trim() || (roomId ? 'General' : '');
        const MAX_AVATAR_UPLOAD_SANITY_BYTES = 30 * 1024 * 1024;
        if (channelId.toLowerCase() === 'general') {
            channelId = 'General';
        }
        if ([...channelId].length > 32) channelId = [...channelId].slice(0, 32).join('');

        // Check if user changed the URL code and hit enter
        const lastRoomId = sessionStorage.getItem('rustrooms_last_room_id');
        const lastChannelId = sessionStorage.getItem('rustrooms_last_channel_id');
        if (lastRoomId && (roomId !== lastRoomId || channelId !== lastChannelId)) {
            sessionStorage.setItem('rustrooms_setup_done', 'false');
            sessionStorage.setItem('rustrooms_welcomed', 'true');
        }

        const initialChannelNameEl = document.getElementById('currentChannelName');
        if (initialChannelNameEl && channelId) {
            initialChannelNameEl.innerText = `# ${channelId}`;
        }

        // Keep the browser tab title in sync with the live channel. Polished
        // product detail: the tab always says where you are (and resets to
        // plain "RustRooms" when you leave).
        function updatePageTitle() {
            const el = document.getElementById('currentChannelName');
            const label = el && el.innerText.trim() ? el.innerText.trim() : '';
            document.title = label ? `RustRooms - ${label}` : 'RustRooms';
        }
        if (roomId) updatePageTitle();

        const currentPath = window.location.pathname;
        const newPath = `/${encodeURIComponent(roomId)}${channelId && channelId.toLowerCase() !== 'general' ? '/' + encodeURIComponent(channelId) : ''}`;
        if (currentPath !== newPath && roomId) {
            window.history.replaceState({ roomId, channelId }, "", newPath);
        }

        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        let wsUrl = roomId ? `${wsProtocol}//${window.location.host}/ws/${encodeURIComponent(roomId)}/${encodeURIComponent(channelId)}` : '';

        const isIOS = (/iPad|iPhone|iPod/.test(navigator.userAgent) && !window.MSStream) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
        const clientUserAgent = navigator.userAgent || navigator.vendor || window.opera || '';
        const isMobileClient = /Mobile|Android|Silk/.test(clientUserAgent) || isIOS;
        let ws;
        let localStream;
        let screenStream;
        let peers = {};
        let peerCamStatus = {};
        let peerScreenStatus = {};
        let peerScreenHasAudio = {};
        let peerMicTrackId = {};
        let peerScreenAudioTrackId = {};
        let peerCamVideoTrackId = {};
        let peerScreenVideoTrackId = {};
        let pendingCandidates = {};
        const MAX_PENDING_ICE_CANDIDATES = 256;
        let userNickname = "Guest";
        // Monotonic revision of the locally persisted profile. Bumped on every
        // savePreferences and sent with join/update-user so the server can tell
        // a genuinely newer local profile from a stale one (e.g. after iOS
        // killed a frozen tab and the localStorage write was lost).
        let profileRev = 0;
        let userAvatar = null;
        let userAvatarIsGif = false;
        if (!sessionStorage.getItem('rustrooms_tab_session_id')) {
            sessionStorage.setItem('rustrooms_tab_session_id', crypto.randomUUID());
        }
        let userAvatarStaticFrame = null;
        let globalRoomList = {};
        let isConfigured = false;
        let audioContext;
        let wakeLock = null;
        let currentAudioOutputId = 'default';
        let currentAudioInputId = null;
        let currentVideoInputId = null;
        let currentFacingMode = 'user';
        let isDeafened = false;
        let roomCreationPassword = sessionStorage.getItem('rustrooms_room_password');
        let workletLoadingPromise = null;
        let isLowBandwidthMode = false;
        let activeSpeakers = {};
        let peerLowBandwidthStatus = {};
        // Avatar payloads are stripped from room-list; this cache restores them
        // in the sidebar for users seen via existing-users / user-joined /
        // identify / user-update.
        let userAvatarCache = {};

        let persistentUserId = localStorage.getItem('rustrooms_user_id');
        if (!persistentUserId) {
            persistentUserId = crypto.randomUUID();
            localStorage.setItem('rustrooms_user_id', persistentUserId);
        }

        let reconnectionAttempts = 0;
        const maxReconnectionAttempts = isIOS ? 50 : 20;
        const baseReconnectionDelay = 1000;
        const maxReconnectionDelay = isIOS ? 15000 : 30000;
        let hasLeftRoom = true;
        let isReconnecting = false;
        let awaitingPassword = false;
        let desktopSlowRetryCount = 0;
        let desktopSlowRetryTimer = null;

        const tabId = crypto.randomUUID();
        let tabHeartbeatInterval = null;
        let activeTabSessionKey = null;
        let isUnloading = false;

        // Intercept getUserMedia to apply appropriate bandwidth constraints
        (function() {
            if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
                const originalGetUserMedia = navigator.mediaDevices.getUserMedia.bind(navigator.mediaDevices);
                navigator.mediaDevices.getUserMedia = function(constraints) {
                    if (constraints && constraints.video) {
                        if (isLowBandwidthMode) {
                            if (typeof constraints.video === 'boolean') {
                                constraints.video = {
                                    width: { max: 320 },
                                    height: { max: 240 },
                                    frameRate: { max: 15 }
                                };
                            } else if (typeof constraints.video === 'object') {
                                constraints.video.width = { max: 320 };
                                constraints.video.height = { max: 240 };
                                constraints.video.frameRate = { max: 15 };
                            }
                        } else {
                            if (typeof constraints.video === 'boolean') {
                                constraints.video = {
                                    width: { ideal: 1280 },
                                    height: { ideal: 720 },
                                    frameRate: { ideal: 30 }
                                };
                            } else if (typeof constraints.video === 'object') {
                                if (constraints.video.width === undefined) {
                                    constraints.video.width = { ideal: 1280 };
                                }
                                if (constraints.video.height === undefined) {
                                    constraints.video.height = { ideal: 720 };
                                }
                                if (constraints.video.frameRate === undefined) {
                                    constraints.video.frameRate = { ideal: 30 };
                                }
                            }
                        }
                    }
                    return originalGetUserMedia(constraints);
                };
            }

            // Intercept RTCPeerConnection.prototype.addTrack to enforce low-bandwidth bitrates
            const originalAddTrack = RTCPeerConnection.prototype.addTrack;
            RTCPeerConnection.prototype.addTrack = function(track, ...streams) {
                const sender = originalAddTrack.apply(this, [track, ...streams]);
                
                // Find the peer user ID associated with this connection
                let targetUserId = null;
                for (const uId in peers) {
                    if (peers[uId] === this) {
                        targetUserId = uId;
                        break;
                    }
                }

                const isRemoteLBM = targetUserId && (peerLowBandwidthStatus[targetUserId] === true);
                if (isLowBandwidthMode || isRemoteLBM) {
                    setTimeout(() => {
                        try {
                            const params = sender.getParameters();
                            if (!params.encodings) params.encodings = [{}];
                            if (track.kind === 'video') {
                                const isScreen = screenStream && screenStream.getVideoTracks().includes(track);
                                params.encodings[0].maxBitrate = isScreen ? 150000 : 80000;
                                params.encodings[0].scaleResolutionDownBy = isScreen ? 1.5 : 2.0;
                            } else if (track.kind === 'audio') {
                                params.encodings[0].maxBitrate = 16000;
                            }
                            sender.setParameters(params).catch(e => console.warn("Failed to set low-bandwidth params on addTrack:", e));
                        } catch (e) {
                            console.warn("Failed to apply track parameters in wrapper:", e);
                        }
                    }, 100);
                }
                return sender;
            };
        })();

        function updateAllSenderBitrates() {
            for (const userId in peers) {
                const pc = peers[userId];
                if (!pc) continue;
                const isRemoteLBM = peerLowBandwidthStatus[userId] === true;
                const shouldLimit = isLowBandwidthMode || isRemoteLBM;
                pc.getSenders().forEach(sender => {
                    if (sender.track) {
                        try {
                            const params = sender.getParameters();
                            if (!params.encodings) params.encodings = [{}];
                            if (sender.track.kind === 'video') {
                                const isScreen = screenStream && screenStream.getVideoTracks().includes(sender.track);
                                if (shouldLimit) {
                                    params.encodings[0].maxBitrate = isScreen ? 150000 : 80000;
                                    params.encodings[0].scaleResolutionDownBy = isScreen ? 1.5 : 2.0;
                                } else {
                                    delete params.encodings[0].maxBitrate;
                                    delete params.encodings[0].scaleResolutionDownBy;
                                }
                            } else if (sender.track.kind === 'audio') {
                                if (shouldLimit) {
                                    params.encodings[0].maxBitrate = 16000;
                                } else {
                                    delete params.encodings[0].maxBitrate;
                                }
                            }
                            sender.setParameters(params).catch(e => console.warn("Failed to dynamically update sender params:", e));
                        } catch (e) {
                            console.warn("Failed to update sender params:", e);
                        }
                    }
                });
            }
        }

        async function updateLocalVideoConstraints() {
            if (localStream) {
                const videoTrack = localStream.getVideoTracks()[0];
                if (videoTrack) {
                    try {
                        if (isLowBandwidthMode) {
                            await videoTrack.applyConstraints({
                                width: { max: 320 },
                                height: { max: 240 },
                                frameRate: { max: 15 }
                            });
                        } else {
                            await videoTrack.applyConstraints({
                                width: { ideal: 1280 },
                                height: { ideal: 720 },
                                frameRate: { ideal: 30 }
                            });
                        }
                    } catch (e) {
                        console.warn("Failed to apply dynamic video constraints:", e);
                    }
                }
            }
        }

        function updateLowBandwidthBadgeVisibility() {
            const lightning = document.getElementById('lowBandwidthLightning');
            if (lightning) {
                if (isLowBandwidthMode) {
                    lightning.classList.remove('hidden');
                } else {
                    lightning.classList.add('hidden');
                }
            }
        }

        async function handleLowBandwidthChange(checked) {
            isLowBandwidthMode = checked;
            const setupLBM = document.getElementById('setupLowBandwidth');
            const settingsLBM = document.getElementById('settingsLowBandwidth');
            if (setupLBM) setupLBM.checked = checked;
            if (settingsLBM) settingsLBM.checked = checked;
            savePreferences();
            updateLowBandwidthBadgeVisibility();
            updateAllSenderBitrates();
            await updateLocalVideoConstraints();
            updateLocalLabel();

            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'update-user',
                    data: { isLowBandwidthMode: checked, profileRev: profileRev }
                }));
            }
        }






        function setActiveTabSession() {
            try {
                if (!activeTabSessionKey) activeTabSessionKey = 'rustrooms_active_tab_' + currentPath;
                localStorage.setItem(activeTabSessionKey, JSON.stringify({ id: tabId, timestamp: Date.now() }));
            } catch(e) {}
        }

        function stopAllMedia(isActualUnload = false) {
            if (isUnloading) return; // Prevent multiple calls
            if (isActualUnload) {
                isUnloading = true;
            }

            // Stop local stream
            if (localStream) {
                localStream.getTracks().forEach(track => {
                    try { 
                        track.enabled = false;
                        track.stop(); 
                    } catch(e) {}
                });
                if (localStream._originalStream) {
                    localStream._originalStream.getTracks().forEach(track => {
                        try { 
                            track.enabled = false;
                            track.stop(); 
                        } catch(e) {}
                    });
                }
                localStream = null;
            }

            // Stop screen stream
            if (screenStream) {
                screenStream.getTracks().forEach(track => {
                    try { 
                        track.enabled = false;
                        track.stop(); 
                    } catch(e) {}
                });
                screenStream = null;
            }

            // Close all peer connections
            if (typeof peers !== 'undefined' && peers) {
                Object.keys(peers).forEach(userId => {
                    try {
                        if (peers[userId]) {
                            peers[userId].getSenders().forEach(sender => {
                                if (sender.track) {
                                    try { 
                                        sender.track.enabled = false;
                                        sender.track.stop(); 
                                    } catch(e) {}
                                }
                            });
                            peers[userId].close();
                        }
                    } catch(e) {}
                });
                peers = {};
            }

            // Close audio context
            if (audioContext) {
                try {
                    audioContext.close().catch(() => {});
                } catch(e) {}
                audioContext = null;
            }

            // Only perform DOM manipulation if we are NOT unloading, as doing so during page tear-down crashes iOS Safari
            if (!isActualUnload) {
                try {
                    const videos = document.querySelectorAll('video');
                    videos.forEach(v => {
                        try {
                            v.pause();
                            v.srcObject = null;
                            v.removeAttribute('src'); // Explicitly remove src
                            v.load();
                        } catch(e) {}
                    });
                } catch(e) {}
            }
        }

        function clearActiveTabSession(isActualUnload = false) {
            stopAllMedia(isActualUnload);
            try {
                if (activeTabSessionKey) {
                    const data = localStorage.getItem(activeTabSessionKey);
                    if (data) {
                        const parsed = JSON.parse(data);
                        if (parsed.id === tabId) {
                            localStorage.removeItem(activeTabSessionKey);
                        }
                    }
                }
            } catch(e) {}
            if (tabHeartbeatInterval) {
                clearInterval(tabHeartbeatInterval);
                tabHeartbeatInterval = null;
            }
        }

        function isAnotherTabActive() {
            try {
                const key = 'rustrooms_active_tab_' + currentPath;
                const data = localStorage.getItem(key);
                if (!data) return false;
                const parsed = JSON.parse(data);
                if (parsed.id === tabId) return false;
                return (Date.now() - parsed.timestamp) < 5000;
            } catch(e) { return false; }
        }

        window.addEventListener('beforeunload', () => clearActiveTabSession(true));
        window.addEventListener('pagehide', () => clearActiveTabSession(true));
        window.addEventListener('unload', () => clearActiveTabSession(true));
        document.addEventListener('visibilitychange', () => {
            if (document.visibilityState === 'hidden') {
                // If we are in the setup screen and not joined yet, 
                // we might want to stop media to be safe if the user switches away/closes
                if (!isConfigured) {
                    // But only if it's not a temporary switch. 
                    // For tab closing, pagehide is usually enough, but visibilitychange 'hidden' is a strong signal.
                }
            }
        });

        let reconnectStatusTimeout = null;
        let reconnectTimer = null;
        let iosSlowRetryTimer = null;
        let wsConnectionId = 0;
        const reconnectDelayMs = 5000;

        let heartbeatInterval = null;
        const heartbeatIntervalMs = isIOS ? 3000 : 2000;
        const heartbeatTimeoutMs = 8000;
        let lastPingSentTime = 0;
        let lastPongTime = Date.now();
        let heartbeatTimeout = null;
        let missedPongCount = 0;

        function getScreenAudioFlag(data) {
            if (!data) return undefined;
            if (data.hasAudio !== undefined) return !!data.hasAudio;
            if (data.screenAudio !== undefined) return !!data.screenAudio;
            return undefined;
        }

        function updatePeerTrackHints(userId, data) {
            if (!data || !userId) return;
            if (data.micTrackId !== undefined) {
                peerMicTrackId[userId] = data.micTrackId || null;
            }
            if (data.screenAudioTrackId !== undefined) {
                peerScreenAudioTrackId[userId] = data.screenAudioTrackId || null;
            }
            if (data.camVideoTrackId !== undefined) {
                peerCamVideoTrackId[userId] = data.camVideoTrackId || null;
            }
            if (data.screenVideoTrackId !== undefined) {
                peerScreenVideoTrackId[userId] = data.screenVideoTrackId || null;
            }
        }

        function ensureScreenAudioUI(userId) {
            if (!peerScreenHasAudio[userId]) return;

            const vid = document.getElementById(`vid-${userId}`);
            const volControls = document.getElementById(`vol-controls-${userId}`);
            if (!vid || !vid.srcObject || !volControls) return;

            if (document.getElementById(`vol-row-screen-${userId}`)) return;

            const savedScreenVol = getVolumeSettings(userId, 'screen');
            const row = document.createElement('div');
            row.className = 'vol-row';
            row.id = `vol-row-screen-${userId}`;
            row.innerHTML = `
                <div class="flex items-center gap-2">
                    <button class="text-white hover:text-blue-400" onclick="toggleMute('${userId}', 'screen')" id="mute-screen-${userId}">
                        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="14" rx="2" ry="2"></rect><line x1="12" y1="22" x2="12" y2="16"></line><path d="M5 12h14"></path><path d="M12 12v4"></path></svg>
                    </button>
                    <input type="range" min="0" max="1" step="0.05" value="${savedScreenVol}" oninput="setVolume('${userId}', 'screen', this.value)">
                </div>
            `;
            volControls.appendChild(row);
        }

        const turnUrl = {{TURN_URL}};
        const rtcConfig = {
            // Browsers reject an ICE server whose URL is empty. Host
            // candidates still work when TURN is intentionally unconfigured.
            iceServers: turnUrl ? [{
                urls: turnUrl,
                username: {{TURN_USERNAME}},
                credential: {{TURN_CREDENTIAL}}
            }] : []
        };

        function getReconnectDelay(attempt) {
            const exponentialDelay = Math.min(
                baseReconnectionDelay * Math.pow(2, attempt),
                maxReconnectionDelay
            );

            const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
            return Math.max(exponentialDelay + jitter, baseReconnectionDelay);
        }

        function updatePingUI(pingMs) {
            const pingContainer = document.getElementById('pingContainer');
            const pingText = document.getElementById('pingText');

            if (pingContainer && pingText) {
                pingContainer.classList.remove('hidden');
                pingText.innerText = `${pingMs}ms`;

                pingContainer.classList.remove('ping-good', 'ping-fair', 'ping-poor');
                if (pingMs < 100) {
                    pingContainer.classList.add('ping-good');
                } else if (pingMs < 250) {
                    pingContainer.classList.add('ping-fair');
                } else {
                    pingContainer.classList.add('ping-poor');
                }
            }
        }

        let statsWindowVisible = false;
        let statsUpdateInterval = null;
        let prevStatsData = {};

        function toggleStatsWindow() {
            const statsWindow = document.getElementById('statsWindow');
            const statusPillWrapper = document.getElementById('statusPillWrapper');
            if (!statsWindow || !statusPillWrapper) return;

            statsWindowVisible = !statsWindowVisible;

            if (statsWindowVisible) {
                const rect = statusPillWrapper.getBoundingClientRect();
                const top = rect.bottom + window.scrollY + 8;
                const right = window.innerWidth - rect.right + window.scrollX;
                statsWindow.style.top = `${top}px`;
                statsWindow.style.right = `${right}px`;
                statsWindow.classList.add('visible');
                prevStatsData = {};
                startStatsUpdate();
            } else {
                statsWindow.classList.remove('visible');
                stopStatsUpdate();
            }
        }

        document.addEventListener('click', (event) => {
            if (statsWindowVisible) {
                const statsWindow = document.getElementById('statsWindow');
                const statusPillWrapper = document.getElementById('statusPillWrapper');
                if (statsWindow && statusPillWrapper &&
                    !statsWindow.contains(event.target) &&
                    !statusPillWrapper.contains(event.target)) {
                    toggleStatsWindow();
                }
            }
        });

        function startStatsUpdate() {
            if (statsUpdateInterval) return;
            updateWebRTCStats();
            statsUpdateInterval = setInterval(updateWebRTCStats, 2000);
        }

        function stopStatsUpdate() {
            if (statsUpdateInterval) {
                clearInterval(statsUpdateInterval);
                statsUpdateInterval = null;
            }
        }

        function calcBitrateKbps(reportId, currentBytes, nowMs) {
            const prev = prevStatsData[reportId];
            if (!prev || !prev.bytes || !prev.timestamp) {
                return 0;
            }
            const deltaBytes = currentBytes - prev.bytes;
            const deltaSec = (nowMs - prev.timestamp) / 1000;
            if (deltaSec <= 0 || deltaBytes <= 0) return 0;
            return Math.round((deltaBytes * 8) / (deltaSec * 1000));
        }

        // Human-friendly bitrate: Mbps with one decimal at 1000 kbps+, else kbps.
        function formatBitrate(kbps) {
            if (kbps >= 1000) {
                return `${(kbps / 1000).toFixed(1).replace(/\.0$/, '')} Mbps`;
            }
            return `${kbps} kbps`;
        }

        async function updateWebRTCStats() {
            const statPing = document.getElementById('statPing');
            const statJitter = document.getElementById('statJitter');
            const statVideoRes = document.getElementById('statVideoRes');
            const statVideoBitrate = document.getElementById('statVideoBitrate');
            const statVideoCodec = document.getElementById('statVideoCodec');
            const statVideoFrames = document.getElementById('statVideoFrames');
            const statAudioBitrate = document.getElementById('statAudioBitrate');
            const statAudioCodec = document.getElementById('statAudioCodec');
            const statPacketsSent = document.getElementById('statPacketsSent');
            const statPacketsReceived = document.getElementById('statPacketsReceived');
            const statPacketsLost = document.getElementById('statPacketsLost');
            const statLowBandwidth = document.getElementById('statLowBandwidth');

            if (statLowBandwidth) {
                if (isLowBandwidthMode) {
                    statLowBandwidth.textContent = 'Enabled';
                    statLowBandwidth.className = 'stats-row-value text-amber-500 font-semibold';
                } else {
                    statLowBandwidth.textContent = 'Disabled';
                    statLowBandwidth.className = 'stats-row-value text-zinc-400 font-normal';
                }
            }

            const pingText = document.getElementById('pingText');
            if (pingText && statPing) {
                statPing.textContent = pingText.textContent;
                statPing.className = 'stat-value ' + (parseInt(pingText.textContent) < 100 ? 'good' : parseInt(pingText.textContent) < 250 ? 'fair' : 'poor');
            }

            let totalPacketsSent = 0;
            let totalPacketsReceived = 0;
            let totalPacketsLost = 0;
            let videoRes = '--';
            let videoBitrate = '--';
            let videoCodec = '--';
            let videoFrames = '--';
            let audioBitrate = '--';
            let audioCodec = '--';
            let jitter = '--';

            const nowMs = Date.now();
            const newStatsData = {};
            const peerValues = Object.values(peers);

            for (const pc of peerValues) {
                try {
                    const stats = await pc.getStats();
                    stats.forEach(report => {
                        if (report.type === 'inbound-rtp' && report.kind === 'video') {
                            const width = report.frameWidth || 0;
                            const height = report.frameHeight || 0;
                            if (width > 0 && height > 0) {
                                videoRes = `${width}x${height}`;
                            }
                            const fps = report.framesPerSecond || 0;
                            if (fps > 0) {
                                videoFrames = `${fps} fps`;
                            }
                            if (report.bytesReceived) {
                                const key = report.id + '_recv';
                                newStatsData[key] = { bytes: report.bytesReceived, timestamp: nowMs };
                                const bitrate = calcBitrateKbps(key, report.bytesReceived, nowMs);
                                if (bitrate > 0) {
                                    videoBitrate = formatBitrate(bitrate);
                                }
                            }
                            totalPacketsReceived += report.packetsReceived || 0;
                            totalPacketsLost += report.packetsLost || 0;
                        } else if (report.type === 'inbound-rtp' && report.kind === 'audio') {
                            if (report.bytesReceived) {
                                const key = report.id + '_recv';
                                newStatsData[key] = { bytes: report.bytesReceived, timestamp: nowMs };
                                const bitrate = calcBitrateKbps(key, report.bytesReceived, nowMs);
                                if (bitrate > 0) {
                                    audioBitrate = formatBitrate(bitrate);
                                }
                            }
                            if (report.jitter && !isNaN(parseFloat(report.jitter))) {
                                jitter = `${Math.round(parseFloat(report.jitter) * 1000)}ms`;
                            }
                            totalPacketsReceived += report.packetsReceived || 0;
                            totalPacketsLost += report.packetsLost || 0;
                        } else if (report.type === 'outbound-rtp' && report.kind === 'video') {
                            const width = report.frameWidth || 0;
                            const height = report.frameHeight || 0;
                            if (width > 0 && height > 0 && videoRes === '--') {
                                videoRes = `${width}x${height}`;
                            }
                            const fps = report.framesPerSecond || 0;
                            if (fps > 0 && videoFrames === '--') {
                                videoFrames = `${fps} fps`;
                            }
                            if (report.bytesSent) {
                                const key = report.id + '_sent';
                                newStatsData[key] = { bytes: report.bytesSent, timestamp: nowMs };
                                const bitrate = calcBitrateKbps(key, report.bytesSent, nowMs);
                                if (bitrate > 0 && videoBitrate === '--') {
                                    videoBitrate = formatBitrate(bitrate);
                                }
                            }
                            totalPacketsSent += report.packetsSent || 0;
                        } else if (report.type === 'outbound-rtp' && report.kind === 'audio') {
                            if (report.bytesSent) {
                                const key = report.id + '_sent';
                                newStatsData[key] = { bytes: report.bytesSent, timestamp: nowMs };
                                const bitrate = calcBitrateKbps(key, report.bytesSent, nowMs);
                                if (bitrate > 0 && audioBitrate === '--') {
                                    audioBitrate = formatBitrate(bitrate);
                                }
                            }
                            totalPacketsSent += report.packetsSent || 0;
                        } else if (report.type === 'codec') {
                            const codecName = report.mimeType || '';
                            if (codecName.includes('video') && videoCodec === '--') {
                                videoCodec = codecName.split('/')[1] || codecName;
                            } else if (codecName.includes('audio') && audioCodec === '--') {
                                audioCodec = codecName.split('/')[1] || codecName;
                            }
                        }
                    });
                } catch (e) {
                    console.warn('Error getting WebRTC stats:', e);
                }
            }

            prevStatsData = newStatsData;

            if (statJitter) statJitter.textContent = jitter;
            if (statVideoRes) statVideoRes.textContent = videoRes;
            if (statVideoBitrate) statVideoBitrate.textContent = videoBitrate;
            if (statVideoCodec) statVideoCodec.textContent = videoCodec;
            if (statVideoFrames) statVideoFrames.textContent = videoFrames;
            if (statAudioBitrate) statAudioBitrate.textContent = audioBitrate;
            if (statAudioCodec) statAudioCodec.textContent = audioCodec;
            if (statPacketsSent) statPacketsSent.textContent = totalPacketsSent.toLocaleString();
            if (statPacketsReceived) statPacketsReceived.textContent = totalPacketsReceived.toLocaleString();
            if (statPacketsLost) statPacketsLost.textContent = totalPacketsLost.toLocaleString();
        }

        let lastVisibilityHidden = 0;

        document.addEventListener('visibilitychange', () => {
            if (document.visibilityState === 'hidden') {
                lastVisibilityHidden = Date.now();
            } else if (document.visibilityState === 'visible') {
                const wasFrozenMs = Date.now() - lastVisibilityHidden;
                if (wasFrozenMs > heartbeatIntervalMs && ws && ws.readyState === WebSocket.OPEN) {
                    console.log(`Tab was hidden for ${Math.round(wasFrozenMs / 1000)}s, restarting heartbeat`);
                    startHeartbeat();
                }
            }
        });

        function escapeHtml(str) {
            if (!str) return '';
            return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
        }

        function sendPing() {
            if (ws && ws.readyState === WebSocket.OPEN) {
                lastPingSentTime = Date.now();
                ws.send(JSON.stringify({ type: 'ping' }));

                if (heartbeatTimeout) clearTimeout(heartbeatTimeout);
                heartbeatTimeout = setTimeout(() => {
                    if (document.visibilityState === 'hidden') {
                        return;
                    }
                    const now = Date.now();
                    const timeSincePong = now - lastPongTime;
                    const timeSinceHidden = now - lastVisibilityHidden;
                    if (timeSinceHidden < heartbeatTimeoutMs * 2) {
                        console.log('Heartbeat timeout skipped - tab was recently hidden, restarting heartbeat');
                        missedPongCount = 0;
                        startHeartbeat();
                        return;
                    }
                    if (timeSincePong > heartbeatIntervalMs + heartbeatTimeoutMs) {
                        missedPongCount++;
                        if (missedPongCount < 2) {
                            console.warn(`Heartbeat: missed pong #${missedPongCount}, sending emergency ping before disconnect`);
                            try { ws.send(JSON.stringify({ type: 'ping' })); } catch(e) {}
                            heartbeatTimeout = setTimeout(() => {
                                const recheckPong = Date.now() - lastPongTime;
                                if (recheckPong > heartbeatIntervalMs + heartbeatTimeoutMs) {
                                    console.warn('Heartbeat timeout - emergency ping also failed, closing connection');
                                    missedPongCount = 0;
                                    ws.close();
                                } else {
                                    console.log('Heartbeat recovered after emergency ping');
                                    missedPongCount = 0;
                                }
                            }, isIOS ? 8000 : 5000);
                        } else {
                            console.warn('Heartbeat timeout - no pong received after retries, closing connection');
                            missedPongCount = 0;
                            ws.close();
                        }
                    }
                }, heartbeatTimeoutMs);
            }
        }

        function startHeartbeat() {
            stopHeartbeat();
            lastPongTime = Date.now();

            sendPing();
            heartbeatInterval = setInterval(sendPing, heartbeatIntervalMs);
        }

        function stopHeartbeat() {
            if (heartbeatInterval) {
                clearInterval(heartbeatInterval);
                heartbeatInterval = null;
            }
            if (heartbeatTimeout) {
                clearTimeout(heartbeatTimeout);
                heartbeatTimeout = null;
            }
            const pingContainer = document.getElementById('pingContainer');
            if (pingContainer) pingContainer.classList.add('hidden');
        }

        function handlePong() {
            lastPongTime = Date.now();
            missedPongCount = 0;
            const pingMs = lastPongTime - lastPingSentTime;

            if (lastPingSentTime > 0) {
                updatePingUI(pingMs);
            }

            if (heartbeatTimeout) {
                clearTimeout(heartbeatTimeout);
                heartbeatTimeout = null;
            }
        }

        const localVideo = document.getElementById('localVideo');
        const previewVideo = document.getElementById('previewVideo');
        const remoteGrid = document.getElementById('remoteGrid');
        const emptyState = document.getElementById('emptyState');
        const connectionDot = document.getElementById('connectionDot');
        const statusText = document.getElementById('statusText');
        const configOverlay = document.getElementById('configOverlay');
        const appLayout = document.getElementById('appLayout');
        const nicknameInput = document.getElementById('nicknameInput');
        const audioSelect = document.getElementById('audioSource');
        const audioOutputSelect = document.getElementById('audioOutputSource');
        const videoSelect = document.getElementById('videoSource');
        const avatarPreview = document.getElementById('avatarPreview');
        const avatarPlaceholder = document.getElementById('avatarPlaceholder');

        if (nicknameInput) {
            nicknameInput.addEventListener('input', () => {
                savePreferences();
            });
        }

        async function initAudioWorklet() {
            if (workletLoadingPromise) return workletLoadingPromise;

            if (!audioContext) {
                audioContext = new (window.AudioContext || window.webkitAudioContext)();
            }

            workletLoadingPromise = (async () => {
                try {
                    await audioContext.audioWorklet.addModule('/rnnoise_processor.js', { type: 'module' });
                    console.log("AudioWorklet loaded");
                    return true;
                } catch (err) {
                    console.error("Failed to load AudioWorklet", err);
                    workletLoadingPromise = null;
                    return false;
                }
            })();

            return workletLoadingPromise;
        }

        async function tryResumeAudioContext(timeoutMs = 300) {
            if (!audioContext) return false;
            if (audioContext.state === 'running') return true;
            if (audioContext.state === 'closed') return false;

            try {
                const resumed = await Promise.race([
                    audioContext.resume().then(() => true).catch(() => false),
                    new Promise(resolve => setTimeout(() => resolve(false), timeoutMs))
                ]);
                return resumed && audioContext.state === 'running';
            } catch (err) {
                console.warn("AudioContext resume failed", err);
                return false;
            }
        }

        let noSleepVideo = null;

        function startNoSleepVideo() {
            if (noSleepVideo) return;
            try {
                noSleepVideo = document.createElement('video');
                noSleepVideo.setAttribute('playsinline', '');
                noSleepVideo.setAttribute('muted', '');
                noSleepVideo.setAttribute('loop', '');
                noSleepVideo.muted = true;
                noSleepVideo.style.position = 'fixed';
                noSleepVideo.style.top = '-1px';
                noSleepVideo.style.left = '-1px';
                noSleepVideo.style.width = '1px';
                noSleepVideo.style.height = '1px';
                noSleepVideo.style.opacity = '0.01';
                noSleepVideo.style.pointerEvents = 'none';
                noSleepVideo.style.zIndex = '-1';
                // Tiny silent MP4 — keeps iOS Safari from throttling/suspending WebSockets
                noSleepVideo.src = 'data:video/mp4;base64,AAAAIGZ0eXBpc29tAAACAGlzb21pc28yYXZjMW1wNDEAAAAIZnJlZQAAA3BtZGF0AAACrwYF//+r3EXpvebZSLeWLNgg2SPu73gyNjQgLSBjb3JlIDE2NCByMzA5NSBiYWVlNDAwIC0gSC4yNjQvTVBFRy00IEFWQyBjb2RlYyAtIENvcHlsZWZ0IDIwMDMtMjAyMiAtIGh0dHA6Ly93d3cudmlkZW9sYW4ub3JnL3gyNjQuaHRtbCAtIG9wdGlvbnM6IGNhYmFjPTEgcmVmPTMgZGVibG9jaz0xOjA6MCBhbmFseXNlPTB4MzoweDExMyBtZT1oZXggc3VibWU9NyBwc3k9MSBwc3lfcmQ9MS4wMDowLjAwIG1peGVkX3JlZj0xIG1lX3JhbmdlPTE2IGNocm9tYV9tZT0xIHRyZWxsaXM9MSA4eDhkY3Q9MSBjcW09MCBkZWFkem9uZT0yMSwxMSBmYXN0X3Bza2lwPTEgY2hyb21hX3FwX29mZnNldD0tMiB0aHJlYWRzPTEgbG9va2FoZWFkX3RocmVhZHM9MSBzbGljZWRfdGhyZWFkcz0wIG5yPTAgZGVjaW1hdGU9MSBpbnRlcmxhY2VkPTAgYmx1cmF5X2NvbXBhdD0wIGNvbnN0cmFpbmVkX2ludHJhPTAgYmZyYW1lcz0zIGJfcHlyYW1pZD0yIGJfYWRhcHQ9MSBiX2JpYXM9MCBkaXJlY3Q9MSB3ZWlnaHRiPTEgb3Blbl9nb3A9MCB3ZWlnaHRwPTIga2V5aW50PTI1MCBrZXlpbnRfbWluPTI1IHNjZW5lY3V0PTQwIGludHJhX3JlZnJlc2g9MCByY19sb29rYWhlYWQ9NDAgcmM9Y3JmIG1idHJlZT0xIGNyZj0yMy4wIHFjb21wPTAuNjAgcXBtaW49MCBxcG1heD02OSBxcHN0ZXA9NCBpcF9yYXRpbz0xLjQwIGFxPTE6MS4wMACAAAAMZWliAAADrfBccwAAAAMAAAMAAAMAIBBgAJQAAAAwAAADAAADAAADAAADAAjUAAADAAADAAADAAADAAADAAADAAADAAADAAADAAADAAADAAAYxgAABwBAAAAGuUGaIAD//vbcvgSuBfAAAAMAAAMAUJgAoEqwEAAAAwAAAwAAAwAADQChIAAAAwAAADAAADAAADAAADAAADAi0AAAAwAAADAAADAAADAAADAAADAAEroAAAAwDMAAABakGaQgwhBAAAAwEC0AAAAwAAAwAA';
                document.body.appendChild(noSleepVideo);
                const playPromise = noSleepVideo.play();
                if (playPromise) playPromise.catch(() => {});
                console.log('NoSleep video started for iOS');
            } catch(e) {
                console.warn('NoSleep video failed:', e);
            }
        }

        function stopNoSleepVideo() {
            if (noSleepVideo) {
                try {
                    noSleepVideo.pause();
                    noSleepVideo.remove();
                } catch(e) {}
                noSleepVideo = null;
            }
        }

        async function requestWakeLock() {
            if (hasLeftRoom) return;
            try {
                if ('wakeLock' in navigator) {
                    if (wakeLock) {
                        try { await wakeLock.release(); } catch(e) {}
                        wakeLock = null;
                    }
                    wakeLock = await navigator.wakeLock.request('screen');
                    wakeLock.addEventListener('release', () => {
                        console.log('Wake Lock released');
                        wakeLock = null;
                    });
                    console.log('Wake Lock active');
                } else if (isIOS) {
                    startNoSleepVideo();
                }
            } catch (err) {
                console.error(`Wake Lock failed: ${err.name}, ${err.message}`);
                if (isIOS) startNoSleepVideo();
            }
        }

        document.addEventListener('visibilitychange', async () => {
            if (document.visibilityState === 'visible') {
                if (!isIOS) {
                    await checkAndRestartLocalStreamIfNeeded();
                }
                if (wakeLock !== null || !hasLeftRoom) {
                    await requestWakeLock();
                }
            }
        });

        ['click', 'touchstart'].forEach(evt => {
            document.addEventListener(evt, () => {
                if (!wakeLock && !hasLeftRoom) {
                    requestWakeLock();
                }
            }, { passive: true });
        });

        // Touch devices (especially iOS Safari) keep the simulated :hover/:active
        // look stuck on the last tapped button until another element is tapped;
        // blurring focus does not reliably clear it. Press feedback on touch is
        // driven by a JS-managed .pressed class (see app.css) whose lifecycle is
        // fully controlled here, so it can never get stuck.
        document.addEventListener('touchstart', (e) => {
            const btn = e.target.closest('.control-btn');
            if (btn && !btn.disabled) btn.classList.add('pressed');
        }, { passive: true });
        ['touchend', 'touchcancel'].forEach(evt => {
            document.addEventListener(evt, () => {
                document.querySelectorAll('.control-btn.pressed')
                    .forEach(b => b.classList.remove('pressed'));
                const el = document.activeElement;
                if (el && el.tagName === 'BUTTON') el.blur();
            }, { passive: true });
        });

        async function loadDevices() {
            const btnJoin = document.getElementById('btnJoin');
            const btnCam = document.getElementById('btnCam');

            isCameraReady = false;
            if (btnCam) btnCam.disabled = true;

            await loadPreferences();
            try {
                try {
                    // Skip initial blind permission probe that forces Bluetooth HFP handsfree telephony mode.
                    // populateDeviceList + startPreview will request the proper media streams directly.
                } catch (e) {
                    console.warn("Permission request failed", e);
                }

                await populateDeviceList();
                await detectCameras();
                navigator.mediaDevices.ondevicechange = populateDeviceList;

                await startPreview();

            } catch (e) {
                console.warn("Device access initialization failed", e);
                updatePreviewButtons();
            }

            if(btnJoin) {
                 btnJoin.disabled = false;
                 btnJoin.innerHTML = "Join Room";
            }

            isCameraReady = true;
            if(btnCam) {
                 btnCam.disabled = false;
                 if (pendingCamToggle) {
                     btnCam.classList.add('active-red');
                     btnCam.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 21l-3.5-3.5m-2-2l-2-2m-2-2l-2-2m-2-2l-3.5-3.5"></path><path d="M15 7h5a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-5"></path><path d="M4 8v8a2 2 0 0 0 2 2h4.5"></path></svg>`;
                 }
            }
        }

        async function populateDeviceList() {
            try {
                const devices = await navigator.mediaDevices.enumerateDevices();

                const audioTrack = localStream ? localStream.getAudioTracks()[0] : null;
                const videoTrack = localStream ? localStream.getVideoTracks()[0] : null;

                const activeAudioId = audioTrack ? audioTrack.getSettings().deviceId : null;
                const activeVideoId = videoTrack ? videoTrack.getSettings().deviceId : null;

                audioSelect.innerHTML = '';
                audioOutputSelect.innerHTML = '';
                videoSelect.innerHTML = '';

                devices.forEach(device => {
                    const option = document.createElement('option');
                    option.value = device.deviceId;
                    option.text = device.label || `${device.kind} (${device.deviceId.slice(0,5)}...)`;
                    if (device.kind === 'audioinput') {
                        audioSelect.appendChild(option);
                    } else if (device.kind === 'audiooutput') {
                        audioOutputSelect.appendChild(option);
                    }
                    else if (device.kind === 'videoinput') videoSelect.appendChild(option);
                });

                const targetAudioId = currentAudioInputId || activeAudioId;
                if (targetAudioId && [...audioSelect.options].some(o => o.value === targetAudioId)) {
                    audioSelect.value = targetAudioId;
                }

                const targetAudioOutputId = currentAudioOutputId || 'default';
                if (targetAudioOutputId && [...audioOutputSelect.options].some(o => o.value === targetAudioOutputId)) {
                    audioOutputSelect.value = targetAudioOutputId;
                }

                const targetVideoId = currentVideoInputId || activeVideoId;
                if (targetVideoId && [...videoSelect.options].some(o => o.value === targetVideoId)) {
                    videoSelect.value = targetVideoId;
                }

                detectCameras();

            } catch(e) {
                console.error("Enumeration error", e);
            }
        }

        async function populateSettingsDeviceList() {
            try {
                const devices = await navigator.mediaDevices.enumerateDevices();
                const settingsAudio = document.getElementById('settingsAudioSource');
                const settingsAudioOutput = document.getElementById('settingsAudioOutputSource');
                const settingsVideo = document.getElementById('settingsVideoSource');

                const audioTrack = localStream ? localStream.getAudioTracks()[0] : null;
                const videoTrack = localStream ? localStream.getVideoTracks()[0] : null;

                const activeAudioId = audioTrack ? audioTrack.getSettings().deviceId : null;
                const activeVideoId = videoTrack ? videoTrack.getSettings().deviceId : null;

                settingsAudio.innerHTML = '';
                settingsAudioOutput.innerHTML = '';
                settingsVideo.innerHTML = '';

                devices.forEach(device => {
                    const option = document.createElement('option');
                    option.value = device.deviceId;
                    option.text = device.label || `${device.kind} (${device.deviceId.slice(0,5)}...)`;
                    if (device.kind === 'audioinput') {
                        settingsAudio.appendChild(option);
                    } else if (device.kind === 'audiooutput') {
                        settingsAudioOutput.appendChild(option);
                    }
                    else if (device.kind === 'videoinput') settingsVideo.appendChild(option);
                });

                const targetAudioId = currentAudioInputId || activeAudioId;
                if (targetAudioId && [...settingsAudio.options].some(o => o.value === targetAudioId)) {
                    settingsAudio.value = targetAudioId;
                }

                const targetAudioOutputId = currentAudioOutputId || 'default';
                if (targetAudioOutputId && [...settingsAudioOutput.options].some(o => o.value === targetAudioOutputId)) {
                    settingsAudioOutput.value = targetAudioOutputId;
                }

                const targetVideoId = currentVideoInputId || activeVideoId;
                if (targetVideoId && [...settingsVideo.options].some(o => o.value === targetVideoId)) {
                    settingsVideo.value = targetVideoId;
                }
            } catch (e) { console.error(e); }
        }

        async function changeAudioOutput(deviceId) {
            currentAudioOutputId = deviceId;
            const elements = document.querySelectorAll('video, audio');
            for (const el of elements) {
                await attachSinkId(el, deviceId);
            }
            savePreferences();
        }

        async function attachSinkId(element, sinkId) {
            if (typeof element.setSinkId === 'function') {
                try {
                    await element.setSinkId(sinkId);
                } catch (e) {
                    console.warn("Failed to set audio output device", e);
                }
            }
        }

        async function switchMediaStream(audioId, videoId) {
             const currentAudioTrack = localStream ? localStream.getAudioTracks()[0] : null;
             const currentVideoTrack = localStream ? localStream.getVideoTracks()[0] : null;
             const currentAudioId = currentAudioTrack ? currentAudioTrack.getSettings().deviceId : "";
             const currentVideoId = currentVideoTrack ? currentVideoTrack.getSettings().deviceId : "";

             const settingsVideoEl = document.getElementById('settingsVideoSource');
             if (videoId && videoId !== currentVideoId && settingsVideoEl) {
                 settingsVideoEl.disabled = true;
             }

             if (audioId && audioId !== currentAudioId) {
                 try {
                     const constraints = {
                        audio: {
                            deviceId: { exact: audioId },
                            echoCancellation: true,
                            noiseSuppression: false,
                            autoGainControl: true,                            sampleRate: 48000
                        }
                    };
                     let stream = await navigator.mediaDevices.getUserMedia(constraints);
                     if (isUnloading) {
                         stream.getTracks().forEach(t => t.stop());
                         return;
                     }

                     if (!audioContext) audioContext = new (window.AudioContext || window.webkitAudioContext)();
                     const workletLoaded = await initAudioWorklet();
                     if (audioContext.state === 'suspended') audioContext.resume().catch(e => {});

                     let newTrack;
                     if (workletLoaded) {
                         const source = audioContext.createMediaStreamSource(stream);
                         const worklet = new AudioWorkletNode(audioContext, 'rnnoise-processor');
                         const dest = audioContext.createMediaStreamDestination();
                         source.connect(worklet);
                         worklet.connect(dest);
                         newTrack = dest.stream.getAudioTracks()[0];
                     } else {
                         newTrack = stream.getAudioTracks()[0];
                     }

                     if (localStream && localStream._originalStream) {
                         localStream._originalStream.getTracks().forEach(t => t.stop());
                     }
                      if (localStream) {
                          if (currentAudioTrack) {
                              currentAudioTrack.stop();
                              localStream.removeTrack(currentAudioTrack);
                          }
                          localStream.addTrack(newTrack);
                      } else {
                          localStream = new MediaStream([newTrack]);
                          if (localVideo) localVideo.srcObject = localStream;
                      }
                      localStream._originalStream = stream;

                      for (const userId in peers) {
                         const pc = peers[userId];
                         const sender = pc.getSenders().find(s => s.track && s.track.kind === 'audio');
                         if (sender) {
                              sender.replaceTrack(newTrack);
                         } else {
                              pc.addTrack(newTrack, localStream);
                              negotiate(userId, pc);
                         }
                      }

                     await setupAudioMonitor(localStream, 'local');
                     await setupVolumeMeter(localStream, 'settingsMicBar');

                 } catch (e) {
                     console.error("Audio switch failed", e);
                     alert("Failed to switch microphone: " + e.message);
                 }
             }

             if (videoId && videoId !== currentVideoId) {
                 try {
                     const constraints = { video: { deviceId: { exact: videoId } } };
                     const newVideoStream = await navigator.mediaDevices.getUserMedia(constraints);
                     if (isUnloading) {
                         newVideoStream.getTracks().forEach(t => t.stop());
                         return;
                     }
                     const newTrack = newVideoStream.getVideoTracks()[0];

                      if (localStream) {
                          localStream.addTrack(newTrack);
                      } else {
                          localStream = new MediaStream([newTrack]);
                          if (localVideo) localVideo.srcObject = localStream;
                      }

                      for (const userId in peers) {
                         const pc = peers[userId];
                         const negotiationNeeded = addCameraTrackToPeer(pc, newTrack);
                         if (negotiationNeeded) negotiate(userId, pc);
                      }

                      if (ws && ws.readyState === WebSocket.OPEN) {
                          ws.send(JSON.stringify({
                              type: 'cam-toggle',
                              data: { enabled: true, camVideoTrackId: newTrack.id }
                          }));
                      }

                      if (currentVideoTrack) {
                          localStream.removeTrack(currentVideoTrack);
                          currentVideoTrack.stop();
                      }

                      currentVideoInputId = videoId;
                      updateLocalMediaLayout();
                      const newFacingMode = newTrack.getSettings().facingMode;
                      if (newFacingMode) {
                          currentFacingMode = newFacingMode;
                      }

                  } catch (e) {
                      console.error("Video switch failed", e);
                  } finally {

                      if (settingsVideoEl) {
                          settingsVideoEl.disabled = false;
                      }
                  }
              }

              updateLocalAvatar();
        }

        let audioMonitorGeneration = {};
        let audioMonitorNodes = {};

        function cleanupAudioMonitor(targetId) {
            if (audioMonitorNodes[targetId]) {
                try { audioMonitorNodes[targetId].source.disconnect(); } catch(e) {}
                try { audioMonitorNodes[targetId].analyser.disconnect(); } catch(e) {}
                delete audioMonitorNodes[targetId];
            }
            if (audioMonitorGeneration[targetId]) {
                audioMonitorGeneration[targetId]++;
            }
        }

        async function setupAudioMonitor(stream, targetId) {
            if (!audioContext) return;
            if (!stream.getAudioTracks().length) return;

            const audioReady = await tryResumeAudioContext();
            if (!audioReady) {
                return;
            }

            cleanupAudioMonitor(targetId);

            if (!audioMonitorGeneration[targetId]) audioMonitorGeneration[targetId] = 0;
            audioMonitorGeneration[targetId]++;
            const myGeneration = audioMonitorGeneration[targetId];

            let source;
            try {
                source = audioContext.createMediaStreamSource(stream);
            } catch (err) {
                console.warn("[setupAudioMonitor] Failed to createMediaStreamSource for", targetId, err);
                return;
            }
            const analyser = audioContext.createAnalyser();
            analyser.fftSize = 256;
            source.connect(analyser);

            audioMonitorNodes[targetId] = { source, analyser };

            const bufferLength = analyser.frequencyBinCount;
            const dataArray = new Uint8Array(bufferLength);

            function checkAudio() {
                if (audioMonitorGeneration[targetId] !== myGeneration) {
                    try { source.disconnect(); } catch(e) {}
                    try { analyser.disconnect(); } catch(e) {}
                    return;
                }
                if (targetId !== 'local' && !document.getElementById(targetId)) {
                    cleanupAudioMonitor(targetId);
                    return;
                }

                analyser.getByteFrequencyData(dataArray);
                let sum = 0;
                for(let i = 0; i < bufferLength; i++) {
                    sum += dataArray[i];
                }
                const average = sum / bufferLength;

                let targetEl;
                let isVideoActive = false;

                if (targetId === 'local') {
                    isVideoActive = localVideo.srcObject && localVideo.srcObject.getVideoTracks().length > 0;
                    targetEl = document.getElementById('localPipWrapper');
                } else {
                    const rawUserId = targetId.startsWith('wrapper-') ? targetId.replace('wrapper-', '') : targetId;
                    const isCamOn = peerCamStatus[rawUserId] !== false;
                    const isScreenOn = peerScreenStatus[rawUserId] === true;

                    if (isCamOn || isScreenOn) {
                        const wrapper = document.getElementById(targetId);
                        if (wrapper) {
                            isVideoActive = wrapper.classList.contains('has-camera') ||
                                wrapper.classList.contains('has-screen');
                        }
                    }

                    const wrapper = document.getElementById(targetId);
                    if (wrapper) {
                        if (isVideoActive) {
                            targetEl = wrapper;
                        } else {
                            targetEl = wrapper.querySelector('.avatar-center');
                        }
                    }
                }

                if (targetEl) {
                    if (average > 10) {
                        targetEl.classList.add('speaking-glow');
                        activeSpeakers[targetId] = average;

                        if (!gifSpeakingState[targetId]) {
                            gifSpeakingState[targetId] = true;
                            toggleGifAnimation(targetId, true);
                        }

                        if (targetId === 'local') {
                            const localSidebarAvatar = document.querySelector(`.room-user-row[data-user-id="${persistentUserId}"] .mini-avatar`);
                            if (localSidebarAvatar) localSidebarAvatar.classList.add('speaking-glow');
                        } else {
                            const rawUserId = targetId.startsWith('wrapper-') ? targetId.replace('wrapper-', '') : targetId;
                            const sidebarAvatar = document.querySelector(`.room-user-row[data-user-id="${rawUserId}"] .mini-avatar`);
                            if (sidebarAvatar) sidebarAvatar.classList.add('speaking-glow');
                        }

                        if (targetId !== 'local' && targetEl.classList.contains('avatar-center')) {
                            const wrapper = document.getElementById(targetId);
                            if (wrapper) wrapper.classList.remove('speaking-glow');
                        }

                        if (targetId !== 'local' && !targetEl.classList.contains('avatar-center')) {
                            const avatar = document.getElementById(targetId)?.querySelector('.avatar-center');
                            if (avatar) avatar.classList.remove('speaking-glow');
                        }
                    } else {
                        targetEl.classList.remove('speaking-glow');
                        delete activeSpeakers[targetId];

                        if (gifSpeakingState[targetId]) {
                            gifSpeakingState[targetId] = false;
                            toggleGifAnimation(targetId, false);
                        }

                        if (targetId === 'local') {
                            const localSidebarAvatar = document.querySelector(`.room-user-row[data-user-id="${persistentUserId}"] .mini-avatar`);
                            if (localSidebarAvatar) localSidebarAvatar.classList.remove('speaking-glow');
                        } else {
                            const wrapper = document.getElementById(targetId);
                            if (wrapper) {
                                wrapper.classList.remove('speaking-glow');
                                const avatar = wrapper.querySelector('.avatar-center');
                                if (avatar) avatar.classList.remove('speaking-glow');
                            }
                            const rawUserId = targetId.startsWith('wrapper-') ? targetId.replace('wrapper-', '') : targetId;
                            const sidebarAvatar = document.querySelector(`.room-user-row[data-user-id="${rawUserId}"] .mini-avatar`);
                            if (sidebarAvatar) sidebarAvatar.classList.remove('speaking-glow');
                        }
                    }
                } else {
                    delete activeSpeakers[targetId];

                    if (targetId !== 'local') {
                        const rawUserId = targetId.startsWith('wrapper-') ? targetId.replace('wrapper-', '') : targetId;
                        const sidebarAvatar = document.querySelector(`.room-user-row[data-user-id="${rawUserId}"] .mini-avatar`);
                        if (sidebarAvatar) sidebarAvatar.classList.remove('speaking-glow');
                    }
                }

                requestAnimationFrame(checkAudio);
            }
            checkAudio();
        }

        const dbName = 'rustrooms_db';
        const storeName = 'avatar_store';

        function initIndexedDB() {
            return new Promise((resolve, reject) => {
                const request = indexedDB.open(dbName, 1);
                request.onupgradeneeded = (event) => {
                    const db = event.target.result;
                    if (!db.objectStoreNames.contains(storeName)) {
                        db.createObjectStore(storeName, { keyPath: 'id' });
                    }
                };
                request.onsuccess = () => resolve(request.result);
                request.onerror = () => reject(request.error);
            });
        }

        async function saveAvatarToDB(avatar, isGif, staticFrame) {
            try {
                const db = await initIndexedDB();
                const transaction = db.transaction(storeName, 'readwrite');
                const store = transaction.objectStore(storeName);
                store.put({ id: 'current_avatar', avatar, isGif, staticFrame });
            } catch (e) {
                console.error("Failed to save avatar to IndexedDB", e);
            }
        }

        async function loadAvatarFromDB() {
            try {
                const db = await initIndexedDB();
                return new Promise((resolve, reject) => {
                    const transaction = db.transaction(storeName, 'readonly');
                    const store = transaction.objectStore(storeName);
                    const request = store.get('current_avatar');
                    request.onsuccess = () => resolve(request.result);
                    request.onerror = () => reject(request.error);
                });
            } catch (e) {
                console.error("Failed to load avatar from IndexedDB", e);
                return null;
            }
        }

        async function loadPreferences() {
            const stored = localStorage.getItem('rustrooms_profile');
            let fallbackAvatar = null;
            let fallbackIsGif = false;
            if (stored) {
                try {
                    const data = JSON.parse(stored);
                    if (typeof data.profileRev === 'number') {
                        profileRev = data.profileRev;
                    }
                    if (data.nickname) {
                        userNickname = data.nickname;
                        if (nicknameInput) nicknameInput.value = userNickname;
                        if (document.getElementById('settingsNicknameInput')) document.getElementById('settingsNicknameInput').value = userNickname;
                    }
                    if (data.avatar) {
                        fallbackAvatar = data.avatar;
                        fallbackIsGif = !!data.isGif;
                    }
                    if (data.audioOutputId) {
                        currentAudioOutputId = data.audioOutputId;
                    }
                    if (data.audioInputId) {
                        currentAudioInputId = data.audioInputId;
                    }
                    if (data.videoInputId) {
                        currentVideoInputId = data.videoInputId;
                    }
                    if (data.isMuted !== undefined) {
                        pendingMicToggle = data.isMuted;
                    }
                    if (data.isCamOff !== undefined) {
                        pendingCamToggle = data.isCamOff;
                    }
                    if (data.isDeafened !== undefined) {
                        isDeafened = data.isDeafened;
                    }
                    if (data.facingMode) {
                        currentFacingMode = data.facingMode;
                    }
                    if (data.isLowBandwidthMode !== undefined) {
                        isLowBandwidthMode = data.isLowBandwidthMode;
                        const setupLBM = document.getElementById('setupLowBandwidth');
                        const settingsLBM = document.getElementById('settingsLowBandwidth');
                        if (setupLBM) setupLBM.checked = isLowBandwidthMode;
                        if (settingsLBM) settingsLBM.checked = isLowBandwidthMode;
                        updateLowBandwidthBadgeVisibility();
                    }
                } catch (e) { console.error("Load pref error", e); }
            }
            try {
                const dbData = await loadAvatarFromDB();
                if (dbData && dbData.avatar) {
                    userAvatar = dbData.avatar;
                    userAvatarIsGif = !!dbData.isGif;
                    userAvatarStaticFrame = dbData.staticFrame || null;
                } else if (fallbackAvatar) {
                    userAvatar = fallbackAvatar;
                    userAvatarIsGif = fallbackIsGif;
                    userAvatarStaticFrame = null;
                }
                if (userAvatar) {
                    const displaySrc = userAvatar;
                    if (avatarPreview) {
                        avatarPreview.src = displaySrc;
                        avatarPreview.classList.remove('hidden');
                        avatarPlaceholder.classList.add('hidden');
                        const removeBtn = document.getElementById('btnRemoveSetupAvatar');
                        if (removeBtn) removeBtn.classList.remove('hidden');
                        if (userAvatarIsGif) {
                            loopGifInElement(avatarPreview, userAvatar);
                        }
                    }
                    if (document.getElementById('settingsAvatarPreview')) {
                        const sap = document.getElementById('settingsAvatarPreview');
                        sap.src = displaySrc;
                        sap.classList.remove('hidden');
                        document.getElementById('settingsAvatarPlaceholder').classList.add('hidden');
                        if (userAvatarIsGif) {
                            loopGifInElement(sap, userAvatar);
                        }
                    }
                    if (userAvatarIsGif && !userAvatarStaticFrame) {
                        extractGifFirstFrame(userAvatar).then(sf => {
                            userAvatarStaticFrame = sf;
                            if (avatarPreview) avatarPreview.src = sf;
                            if (document.getElementById('settingsAvatarPreview')) {
                                document.getElementById('settingsAvatarPreview').src = sf;
                            }
                            saveAvatarToDB(userAvatar, userAvatarIsGif, userAvatarStaticFrame);
                        });
                    } else if (userAvatarIsGif && userAvatarStaticFrame) {
                        if (avatarPreview) avatarPreview.src = userAvatarStaticFrame;
                        if (document.getElementById('settingsAvatarPreview')) {
                            document.getElementById('settingsAvatarPreview').src = userAvatarStaticFrame;
                        }
                    }
                }
            } catch (e) { console.error("DB Avatar error", e); }
        }

        // bumpRev is only true for genuine local saves. Adopting a server
        // echo must persist the server's revision verbatim — bumping it again
        // would make the revision a noisy heartbeat instead of a true "my
        // last save" version, which is what the server's stale-update guard
        // (profileRev) compares against.
        function savePreferences(bumpRev = true) {
            if (bumpRev) profileRev++;
            let audioInputId = currentAudioInputId;
            let videoInputId = currentVideoInputId;
            let audioOutputId = currentAudioOutputId;

            const isSettingsOpen = settingsOverlay && !settingsOverlay.classList.contains('hidden');
            const isConfigOpen = configOverlay && !configOverlay.classList.contains('hidden');

            if (isSettingsOpen) {
                const sAudio = document.getElementById('settingsAudioSource');
                const sVideo = document.getElementById('settingsVideoSource');
                const sAudioOut = document.getElementById('settingsAudioOutputSource');
                const sNickname = document.getElementById('settingsNicknameInput');
                if (sAudio && sAudio.value !== undefined) audioInputId = sAudio.value;
                if (sVideo && sVideo.value !== undefined) videoInputId = sVideo.value;
                if (sAudioOut && sAudioOut.value !== undefined) audioOutputId = sAudioOut.value;
                if (sNickname) userNickname = sNickname.value.trim() || "Guest";
            } else if (isConfigOpen) {
                if (audioSelect) audioInputId = audioSelect.value;
                if (videoSelect) videoInputId = videoSelect.value;
                if (audioOutputSelect) audioOutputId = audioOutputSelect.value;
                const cNickname = document.getElementById('nicknameInput');
                if (cNickname) userNickname = cNickname.value.trim() || "Guest";
            }

            let isMuted = pendingMicToggle;
            let isCamOff = pendingCamToggle;

            if (localStream) {
                const audioTrack = localStream.getAudioTracks()[0];
                const videoTrack = localStream.getVideoTracks()[0];
                if (audioTrack) {
                    isMuted = !audioTrack.enabled;
                } else {
                    isMuted = true;
                }
                if (videoTrack) {
                    isCamOff = !videoTrack.enabled;
                } else {
                    isCamOff = true;
                }
            }

            try {
                localStorage.setItem('rustrooms_profile', JSON.stringify({
                    nickname: userNickname,
                    profileRev: profileRev,
                    audioOutputId: audioOutputId,
                    audioInputId: audioInputId,
                    videoInputId: videoInputId,
                    isMuted: isMuted,
                    isCamOff: isCamOff,
                    isDeafened: isDeafened,
                    facingMode: currentFacingMode,
                    isLowBandwidthMode: isLowBandwidthMode                }));
            } catch(e) {
                console.warn('Could not save preferences to localStorage:', e.message);
            }
            saveAvatarToDB(userAvatar, userAvatarIsGif, userAvatarStaticFrame);

            currentAudioInputId = audioInputId;
            currentVideoInputId = videoInputId;
            currentAudioOutputId = audioOutputId;
        }

        // The server is the authority for the profile it stores (name, avatar,
        // modes, mute state). Whenever it echoes a user-update or sends the
        // existing-users list, this adopts that canonical state into the local
        // variables, the inputs, and localStorage, so a client whose local
        // storage was lost (frozen tab killed) heals instead of drifting.
        function adoptServerProfile(status) {
            if (!status) return;
            if (typeof status.profileRev === 'number') {
                profileRev = status.profileRev;
            }
            if (typeof status.nickname === 'string') {
                userNickname = status.nickname.trim() || 'Guest';
                const activeEl = document.activeElement;
                if (nicknameInput && activeEl !== nicknameInput) nicknameInput.value = userNickname;
                const sNick = document.getElementById('settingsNicknameInput');
                if (sNick && activeEl !== sNick) sNick.value = userNickname;
            }
            if (status.avatar !== undefined) {
                userAvatar = status.avatar || null;
                userAvatarIsGif = !!status.isGif;
                userAvatarStaticFrame = status.staticFrame || null;
                if (userAvatar) {
                    userAvatarCache[persistentUserId] = {
                        avatar: userAvatar,
                        isGif: userAvatarIsGif,
                        staticFrame: userAvatarStaticFrame
                    };
                } else {
                    delete userAvatarCache[persistentUserId];
                }
                if (typeof updateLocalAvatar === 'function') updateLocalAvatar();
            }
            if (status.isMuted !== undefined) {
                pendingMicToggle = !!status.isMuted;
            }
            if (status.isDeafened !== undefined) {
                isDeafened = !!status.isDeafened;
            }
            if (status.isLowBandwidthMode !== undefined) {
                isLowBandwidthMode = !!status.isLowBandwidthMode;
                const setupLBM = document.getElementById('setupLowBandwidth');
                const settingsLBM = document.getElementById('settingsLowBandwidth');
                if (setupLBM) setupLBM.checked = isLowBandwidthMode;
                if (settingsLBM) settingsLBM.checked = isLowBandwidthMode;
                if (typeof updateLowBandwidthBadgeVisibility === 'function') updateLowBandwidthBadgeVisibility();
            }
            if (typeof updateLocalLabel === 'function') updateLocalLabel();
            savePreferences(false);
        }

        async function testSpeaker(selectId) {
            const el = document.getElementById(selectId);
            if (!el) return;
            const deviceId = el.value;

            if (!audioContext) audioContext = new (window.AudioContext || window.webkitAudioContext)();
            if (audioContext.state === 'suspended') await audioContext.resume();

            const osc = audioContext.createOscillator();
            const gain = audioContext.createGain();

            osc.connect(gain);

            const isSetSinkIdSupported = 'setSinkId' in HTMLMediaElement.prototype;
            const isNonDefaultDevice = deviceId && deviceId !== 'default';

            if (isNonDefaultDevice && isSetSinkIdSupported) {
                const dest = audioContext.createMediaStreamDestination();
                gain.connect(dest);

                const audio = new Audio();
                audio.srcObject = dest.stream;

                try {
                    await audio.setSinkId(deviceId);
                } catch(e) {
                    console.warn("setSinkId failed", e);
                }

                audio.play().catch(e => console.warn("Audio play failed", e));
            } else {
                gain.connect(audioContext.destination);
            }

            osc.type = 'sine';
            osc.frequency.setValueAtTime(523.25, audioContext.currentTime);
            osc.frequency.exponentialRampToValueAtTime(1046.5, audioContext.currentTime + 0.1);

            gain.gain.setValueAtTime(0.2, audioContext.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.001, audioContext.currentTime + 0.5);

            osc.start();
            osc.stop(audioContext.currentTime + 0.5);
        }

        let setupMeterFrameId = null;
        let settingsMeterFrameId = null;

        async function setupVolumeMeter(stream, barId) {
            const bar = document.getElementById(barId);
            if (!bar) return;

            if (barId === 'setupMicBar') {
                if (setupMeterFrameId) cancelAnimationFrame(setupMeterFrameId);
            } else if (barId === 'settingsMicBar') {
                if (settingsMeterFrameId) cancelAnimationFrame(settingsMeterFrameId);
            }

            if (bar._audioSource) {
                try { bar._audioSource.disconnect(); } catch(e) {}
                bar._audioSource = null;
            }
            if (bar._analyser) {
                try { bar._analyser.disconnect(); } catch(e) {}
                bar._analyser = null;
            }

            if (!stream || !stream.getAudioTracks().length) {
                bar.style.width = '0%';
                return;
            }

            if (!audioContext) audioContext = new (window.AudioContext || window.webkitAudioContext)();
            const audioReady = await tryResumeAudioContext();
            if (!audioReady) {
                bar.style.width = '0%';
                return;
            }

            const source = audioContext.createMediaStreamSource(stream);
            const analyser = audioContext.createAnalyser();
            analyser.fftSize = 256;
            source.connect(analyser);

            bar._audioSource = source;
            bar._analyser = analyser;

            const dataArray = new Uint8Array(analyser.frequencyBinCount);

            function draw() {
                if (!bar._analyser) return;
                analyser.getByteFrequencyData(dataArray);
                let sum = 0;
                for (let i = 0; i < dataArray.length; i++) {
                    sum += dataArray[i];
                }
                const average = sum / dataArray.length;
                const val = Math.min(100, (average / 60) * 100);
                bar.style.width = val + '%';

                if (barId === 'setupMicBar') {
                    setupMeterFrameId = requestAnimationFrame(draw);
                } else {
                    settingsMeterFrameId = requestAnimationFrame(draw);
                }
            }
            draw();
        }

        function resizeImageForAvatar(file) {
            return new Promise((resolve) => {
                const reader = new FileReader();
                reader.onload = function(e) {
                    const img = new Image();
                    img.onload = function() {
                        const MAX_DIM = 1200;
                        let w = img.naturalWidth;
                        let h = img.naturalHeight;
                        if (w > MAX_DIM || h > MAX_DIM) {
                            if (w > h) { h = Math.round(h * MAX_DIM / w); w = MAX_DIM; }
                            else { w = Math.round(w * MAX_DIM / h); h = MAX_DIM; }
                        }
                        const canvas = document.createElement('canvas');
                        canvas.width = w;
                        canvas.height = h;
                        const ctx = canvas.getContext('2d');
                        ctx.drawImage(img, 0, 0, w, h);
                        resolve(canvas.toDataURL('image/jpeg', 0.8));
                    };
                    img.onerror = function() { resolve(e.target.result); };
                    img.src = e.target.result;
                };
                reader.readAsDataURL(file);
            });
        }

        function handleAvatarUpload(input) {
            const file = input.files[0];
            if (!file) return;

            if (file.size > MAX_AVATAR_UPLOAD_SANITY_BYTES) {
                alert("File is too large! Maximum allowed size is 30MB.");
                input.value = '';
                return;
            }

            const processingEl = document.getElementById('avatarProcessing');
            const showProcessing = () => {
                if (processingEl) {
                    processingEl.classList.remove('hidden');
                    processingEl.classList.add('flex');
                }
            };
            const hideProcessing = () => {
                if (processingEl) {
                    processingEl.classList.add('hidden');
                    processingEl.classList.remove('flex');
                }
            };

            if (file.type === 'image/gif') {
                // GIFs open in the crop modal with the animation looping
                // inside the crop box, just like stills; the crop is applied
                // frame-by-frame (animation preserved) and the result is
                // auto-resized browser-side to fit the server's avatar cap.
                showProcessing();
                globalThis.readAvatarFile(file).then(dataUrl => {
                    hideProcessing();
                    openCropModal(dataUrl, 'setup', dataUrl);
                }).catch(err => {
                    console.error("GIF avatar read failed:", err);
                    hideProcessing();
                    showCustomAlert("Image Error", "Could not process this image. Please try a different file.");
                });
            } else {
                showProcessing();
                resizeImageForAvatar(file).then(dataUrl => {
                    hideProcessing();
                    openCropModal(dataUrl, 'setup');
                });
            }
            input.value = '';
        }

        function removeSetupAvatar() {
            stopGifPreviewLoop(avatarPreview);
            userAvatar = null;
            userAvatarIsGif = false;
            userAvatarStaticFrame = null;
            avatarPreview.src = '';
            avatarPreview.classList.add('hidden');
            avatarPlaceholder.classList.remove('hidden');
            const removeBtn = document.getElementById('btnRemoveSetupAvatar');
            if (removeBtn) removeBtn.classList.add('hidden');
            savePreferences();
        }

        function removeSettingsAvatar() {
            stopGifPreviewLoop(settingsAvatarPreview);
            newAvatarCandidate = null;
            newAvatarCandidateIsGif = false;
            newAvatarCandidateStaticFrame = null;
            settingsAvatarPreview.src = '';
            settingsAvatarPreview.classList.add('hidden');
            settingsAvatarPlaceholder.classList.remove('hidden');
            const removeBtn = document.getElementById('btnRemoveSettingsAvatar');
            if (removeBtn) removeBtn.classList.add('hidden');
            saveSettings();
        }

        function extractGifFirstFrame(gifDataUrl) {
            return new Promise((resolve) => {
                const img = new Image();
                img.onload = function() {
                    const MAX_DIM = 400;
                    let width = img.naturalWidth;
                    let height = img.naturalHeight;
                    if (width > MAX_DIM || height > MAX_DIM) {
                        if (width > height) {
                            height = Math.round(height * MAX_DIM / width);
                            width = MAX_DIM;
                        } else {
                            width = Math.round(width * MAX_DIM / height);
                            height = MAX_DIM;
                        }
                    }
                    const canvas = document.createElement('canvas');
                    canvas.width = width;
                    canvas.height = height;
                    const ctx = canvas.getContext('2d');
                    ctx.drawImage(img, 0, 0, width, height);
                    resolve(canvas.toDataURL('image/jpeg', 0.8));
                };
                img.onerror = function() {
                    resolve(null);
                };
                img.src = gifDataUrl;
            });
        }

        function restartGif(url) {
            if (isIOS) return url;
            return url.split('#')[0] + '#' + Date.now();
        }

        let gifSpeakingState = {};

        // JS-driven GIF playback for the speaking animation: overlays a
        // canvas on the avatar center and steps pre-decoded frames with a
        // timer. Needed for browsers that never advance GIF frames in <img>
        // (enterprise AnimationPolicy etc.); falls back to the plain img src
        // swap when ImageDecoder is unavailable.
        let gifAnimators = {};

        function stopGifFrameAnimation(targetId) {
            const entry = gifAnimators[targetId];
            if (!entry) return;
            if (entry.animator) {
                entry.animator.stop();
            }
            if (entry.canvas && entry.canvas.parentNode) {
                entry.canvas.parentNode.removeChild(entry.canvas);
            }
            if (entry.centerImg) {
                entry.centerImg.style.display = '';
            }
            delete gifAnimators[targetId];
        }

        function drawBitmapCover(ctx, canvas, bitmap) {
            const cw = canvas.clientWidth || bitmap.width;
            const ch = canvas.clientHeight || bitmap.height;
            const scale = Math.max(cw / bitmap.width, ch / bitmap.height);
            const sw = cw / scale;
            const sh = ch / scale;
            const sx = (bitmap.width - sw) / 2;
            const sy = (bitmap.height - sh) / 2;
            ctx.clearRect(0, 0, cw, ch);
            ctx.drawImage(bitmap, sx, sy, sw, sh, 0, 0, cw, ch);
        }

        function startGifFrameAnimation(targetId, avatar, centerEl, centerImg) {
            stopGifFrameAnimation(targetId);
            if (!avatar || !centerEl) return;
            const canvas = document.createElement('canvas');
            canvas.style.cssText = 'position:absolute;inset:0;width:100%;height:100%;';
            centerEl.appendChild(canvas);
            gifAnimators[targetId] = { canvas, centerImg };
            globalThis.createGifAnimator(avatar).then((animator) => {
                const entry = gifAnimators[targetId];
                if (!entry) {
                    // Already stopped while decoding.
                    return;
                }
                if (!animator) {
                    stopGifFrameAnimation(targetId);
                    return;
                }
                // Hide the static img only now, so the avatar never blanks
                // during frame decoding.
                if (centerImg) centerImg.style.display = 'none';
                const ctx = canvas.getContext('2d');
                const sizeCanvas = () => {
                    const cw = centerEl.clientWidth || 120;
                    const ch = centerEl.clientHeight || 120;
                    if (canvas.width !== cw || canvas.height !== ch) {
                        canvas.width = cw;
                        canvas.height = ch;
                    }
                };
                sizeCanvas();
                animator.start((bitmap) => {
                    sizeCanvas();
                    drawBitmapCover(ctx, canvas, bitmap);
                });
                entry.animator = animator;
            }).catch(() => {
                // Decoding failed (e.g. corrupt avatar); restore the static img.
                stopGifFrameAnimation(targetId);
            });
        }

        // The JS frame animator works everywhere (built-in GIF decoder;
        // ImageDecoder was removed from Chrome 130+), so the canvas-based
        // animation path is always available.
        const canUseGifAnimator = () => typeof globalThis.createGifAnimator === 'function';

        // Loops a GIF inside an <img> preview (setup/settings screens) via
        // the JS frame animator, so the animation plays even in browsers
        // that never advance GIF frames in <img> elements.
        let gifPreviewLoops = {};
        let gifPreviewSeq = 0;

        function stopGifPreviewLoop(imgEl) {
            const key = imgEl && imgEl.dataset ? imgEl.dataset.gifLoopKey : null;
            if (!key) return;
            const entry = gifPreviewLoops[key];
            if (!entry) return;
            if (entry.animator) {
                entry.animator.stop();
            }
            if (entry.canvas && entry.canvas.parentNode) {
                entry.canvas.parentNode.removeChild(entry.canvas);
            }
            imgEl.style.display = '';
            delete imgEl.dataset.gifLoopKey;
            delete gifPreviewLoops[key];
        }

        function loopGifInElement(imgEl, gifSrc) {
            if (!imgEl || !gifSrc) return;
            stopGifPreviewLoop(imgEl);
            const container = imgEl.parentElement;
            if (!container) return;
            const key = 'preview-' + (++gifPreviewSeq);
            imgEl.dataset.gifLoopKey = key;
            const canvas = document.createElement('canvas');
            canvas.style.cssText = 'position:absolute;inset:0;width:100%;height:100%;';
            container.appendChild(canvas);
            imgEl.style.display = 'none';
            gifPreviewLoops[key] = { canvas };
            globalThis.createGifAnimator(gifSrc).then((animator) => {
                const entry = gifPreviewLoops[key];
                if (!entry || !canvas.parentNode) {
                    return;
                }
                if (!animator) {
                    stopGifPreviewLoop(imgEl);
                    return;
                }
                const ctx = canvas.getContext('2d');
                const sizeCanvas = () => {
                    const cw = container.clientWidth || 80;
                    const ch = container.clientHeight || 80;
                    if (canvas.width !== cw || canvas.height !== ch) {
                        canvas.width = cw;
                        canvas.height = ch;
                    }
                };
                sizeCanvas();
                animator.start((frame) => {
                    sizeCanvas();
                    drawBitmapCover(ctx, canvas, frame);
                });
                entry.animator = animator;
            });
        }

        function toggleGifAnimation(targetId, isSpeaking) {
            if (targetId === 'local') {
                if (!userAvatarIsGif || !userAvatar) return;
                const centerImg = document.getElementById('localAvatarCenterImg');
                const bgImg = document.getElementById('localAvatarImg');
                const sidebarImg = document.querySelector(`.room-user-row[data-user-id="${persistentUserId}"] .mini-avatar img`);
                const staticSrc = userAvatarStaticFrame || userAvatar;
                if (isSpeaking) {
                    if (canUseGifAnimator()) {
                        const centerEl = centerImg ? centerImg.parentElement : null;
                        startGifFrameAnimation('local', userAvatar, centerEl, centerImg);
                        const sidebarContainer = sidebarImg ? sidebarImg.parentElement : null;
                        startGifFrameAnimation('local-sidebar', userAvatar, sidebarContainer, sidebarImg);
                        if (bgImg) bgImg.src = staticSrc;
                        if (sidebarImg) sidebarImg.src = staticSrc;
                    } else {
                        const animSrc = restartGif(userAvatar);
                        if (centerImg) centerImg.src = animSrc;
                        if (bgImg) bgImg.src = animSrc;
                        if (sidebarImg) sidebarImg.src = animSrc;
                    }
                } else {
                    stopGifFrameAnimation('local');
                    stopGifFrameAnimation('local-sidebar');
                    if (centerImg) centerImg.src = staticSrc;
                    if (bgImg) bgImg.src = staticSrc;
                    if (sidebarImg) sidebarImg.src = staticSrc;
                }
            } else {
                const rawUserId = targetId.startsWith('wrapper-') ? targetId.replace('wrapper-', '') : targetId;
                const wrapper = document.getElementById(targetId);
                if (!wrapper) return;
                const avatarCenter = wrapper.querySelector('.avatar-center');
                if (!avatarCenter) return;
                const imgs = avatarCenter.querySelectorAll('img');
                const firstImg = imgs.length > 0 ? imgs[0] : null;
                const firstGif = firstImg ? firstImg.dataset.gifSrc : null;
                const firstStatic = firstImg ? firstImg.dataset.staticSrc : null;
                const sidebarImg = document.querySelector(`.room-user-row[data-user-id="${rawUserId}"] .mini-avatar img`);
                if (isSpeaking && firstGif && firstStatic && canUseGifAnimator()) {
                    startGifFrameAnimation(targetId, firstGif, avatarCenter, firstImg);
                    const sidebarContainer = sidebarImg ? sidebarImg.parentElement : null;
                    startGifFrameAnimation(targetId + '-sidebar', firstGif, sidebarContainer, sidebarImg);
                    imgs.forEach(img => {
                        if (img.dataset.staticSrc) img.src = img.dataset.staticSrc;
                    });
                } else if (isSpeaking) {
                    imgs.forEach(img => {
                        const gifSrc = img.dataset.gifSrc;
                        const staticSrc = img.dataset.staticSrc;
                        if (gifSrc && staticSrc) {
                            img.src = restartGif(gifSrc);
                        }
                    });
                    if (sidebarImg && sidebarImg.dataset.gifSrc && sidebarImg.dataset.staticSrc) {
                        sidebarImg.src = restartGif(sidebarImg.dataset.gifSrc);
                    }
                } else {
                    stopGifFrameAnimation(targetId);
                    stopGifFrameAnimation(targetId + '-sidebar');
                    imgs.forEach(img => {
                        if (img.dataset.staticSrc) img.src = img.dataset.staticSrc;
                    });
                }
                const bgImg = wrapper.querySelector('.avatar-img');
                if (bgImg && bgImg.dataset.staticSrc) {
                    bgImg.src = bgImg.dataset.staticSrc;
                }
                if (sidebarImg && sidebarImg.dataset.staticSrc) {
                    sidebarImg.src = sidebarImg.dataset.staticSrc;
                }
            }
        }

        let isPreviewStarting = false;
        let pendingCamToggle = false;
        let pendingMicToggle = false;
        let isCameraReady = true;

        async function startPreview() {
            if (isPreviewStarting) {
                return;
            }

            let previousVideoEnabled = true;
            let previousAudioEnabled = true;
            if (localStream) {
                const oldV = localStream.getVideoTracks()[0];
                const oldA = localStream.getAudioTracks()[0];
                if (oldV) previousVideoEnabled = oldV.enabled;
                if (oldA) previousAudioEnabled = oldA.enabled;
            }

            isPreviewStarting = true;

            const btnPreviewCam = document.getElementById('btnPreviewCam');
            const btnPreviewMic = document.getElementById('btnPreviewMic');
            if (btnPreviewCam) {
                btnPreviewCam.disabled = true;
                btnPreviewCam.classList.add('opacity-50', 'cursor-not-allowed');
            }
            if (btnPreviewMic) {
                btnPreviewMic.disabled = true;
                btnPreviewMic.classList.add('opacity-50', 'cursor-not-allowed');
            }

            const videoSelectEl = document.getElementById('videoSource');
            const audioSelectEl = document.getElementById('audioSource');

            const savedAudioValue = audioSelectEl ? audioSelectEl.value : null;
            const savedVideoValue = videoSelectEl ? videoSelectEl.value : null;

            savePreferences();

            const originalVideoSelectContent = videoSelectEl ? videoSelectEl.innerHTML : null;
            const originalAudioSelectContent = audioSelectEl ? audioSelectEl.innerHTML : null;
            if (videoSelectEl) {
                videoSelectEl.innerHTML = '<option value="">Loading...</option>';
                videoSelectEl.disabled = true;
            }
            if (audioSelectEl) {
                audioSelectEl.disabled = true;
            }

            try {
                if (localStream) {
                    localStream.getTracks().forEach(track => track.stop());
                    if (localStream._originalStream) {
                         localStream._originalStream.getTracks().forEach(track => track.stop());
                    }
                    localStream = null;
                }

                const audioSource = savedAudioValue || (audioSelectEl ? audioSelectEl.value : null);
                const videoSource = savedVideoValue || (videoSelectEl ? videoSelectEl.value : null);

                const shouldGetVideo = !pendingCamToggle;

                let videoConstraints = false;
                if (shouldGetVideo) {
                    if (videoSource) {
                        videoConstraints = { deviceId: { exact: videoSource } };
                    } else {
                        videoConstraints = { facingMode: currentFacingMode };
                    }
                }

                const constraints = {
                    audio: {
                        deviceId: audioSource ? { exact: audioSource } : undefined,
                        echoCancellation: true,
                        noiseSuppression: false,
                        autoGainControl: true,                        sampleRate: 48000
                    },
                    video: videoConstraints
                };

                let rawStream = await navigator.mediaDevices.getUserMedia(constraints);
                if (isUnloading) {
                    rawStream.getTracks().forEach(t => t.stop());
                    return;
                }

                const newV = rawStream.getVideoTracks()[0];
                const newA = rawStream.getAudioTracks()[0];
                if (newA) newA.enabled = previousAudioEnabled;

                if (newV) {
                    if (pendingCamToggle) {
                        newV.enabled = false;
                    } else {
                        newV.enabled = previousVideoEnabled;
                    }
                }

                 if (rawStream.getAudioTracks().length > 0) {
                     if (!audioContext) audioContext = new (window.AudioContext || window.webkitAudioContext)();
                     const audioReady = await tryResumeAudioContext();
                     const workletLoaded = audioReady ? await initAudioWorklet() : false;

                     if (workletLoaded) {
                         const source = audioContext.createMediaStreamSource(rawStream);
                         const worklet = new AudioWorkletNode(audioContext, 'rnnoise-processor');
                         const dest = audioContext.createMediaStreamDestination();

                         source.connect(worklet);
                         worklet.connect(dest);

                         const processedAudio = dest.stream.getAudioTracks()[0];
                         if (processedAudio) processedAudio.enabled = previousAudioEnabled;

                         const videoTracks = rawStream.getVideoTracks();

                         localStream = new MediaStream([processedAudio, ...videoTracks]);
                         localStream._originalStream = rawStream;
                     } else {
                         localStream = rawStream;
                     }
                } else {
                    localStream = rawStream;
                }

                await setupVolumeMeter(localStream, 'setupMicBar');

                previewVideo.srcObject = localStream;
                updatePreviewButtons();

                if (ws && ws.readyState === WebSocket.OPEN) {
                    if (document.getElementById('localVideo')) document.getElementById('localVideo').srcObject = localStream;
                    updateLocalLabel();
                    updateLocalAvatar();

                    const btnMic = document.getElementById('btnMic');
                    const btnCam = document.getElementById('btnCam');
                    const micOffSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path><line x1="12" x2="12" y1="19" y2="22"></line></svg>`;
                    const camOffSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 21l-3.5-3.5m-2-2l-2-2m-2-2l-2-2m-2-2l-3.5-3.5"></path><path d="M15 7h5a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-5"></path><path d="M4 8v8a2 2 0 0 0 2 2h4.5"></path></svg>`;
                    const micOnSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3Z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" x2="12" y1="19" y2="22"/></svg>`;
                    const camOnSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 4h-5L7 7H4a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2h-3l-2.5-3z"/><circle cx="12" cy="13" r="3"/></svg>`;

                    const audioTrack = localStream.getAudioTracks()[0];
                    let isMicOn = audioTrack && audioTrack.enabled;
                    if (pendingMicToggle) {
                        isMicOn = !isMicOn;
                    }
                    if (!isMicOn) {
                         if (btnMic) { btnMic.classList.add('active-red'); btnMic.innerHTML = micOffSvg; }
                    } else {
                         if (btnMic) { btnMic.classList.remove('active-red'); btnMic.innerHTML = micOnSvg; }
                    }

                    const videoTrack = localStream.getVideoTracks()[0];
                    let isCamOn = videoTrack && videoTrack.enabled;
                    if (pendingCamToggle) {
                        isCamOn = !isCamOn;
                    }
                    if (!isCamOn) {
                         if (btnCam) { btnCam.classList.add('active-red'); btnCam.innerHTML = camOffSvg; }
                    } else {
                         if (btnCam) { btnCam.classList.remove('active-red'); btnCam.innerHTML = camOnSvg; }
                    }

                    for (const userId in peers) {
                        const pc = peers[userId];
                        let negotiationNeeded = false;

                        if (audioTrack) {
                            const sender = pc.getSenders().find(s => s.track && s.track.kind === 'audio');
                            if (sender) {
                                sender.replaceTrack(audioTrack);
                            } else {
                                pc.addTrack(audioTrack, localStream);
                                negotiationNeeded = true;
                            }
                        }

                        if (videoTrack) {
                            if (addCameraTrackToPeer(pc, videoTrack)) {
                                negotiationNeeded = true;
                            }
                        }

                        if (negotiationNeeded) {
                            negotiate(userId, pc);
                        }
                    }

                    if (videoTrack) {
                        let isCamOn = videoTrack.enabled;
                        if (isPreviewStarting && pendingCamToggle) {
                            isCamOn = !isCamOn;
                        }
                        ws.send(JSON.stringify({
                            type: 'cam-toggle',
                            data: {
                                enabled: isCamOn,
                                camVideoTrackId: isCamOn ? videoTrack.id : null
                            }
                        }));
                    }
                }
            } catch (e) {
                console.error("Preview failed", e);
                document.getElementById('previewPlaceholder').style.display = 'flex';
                 try {
                    let rawStream = await navigator.mediaDevices.getUserMedia({ 
                        audio: {
                            echoCancellation: true,
                            noiseSuppression: false,
                            autoGainControl: true,                        }, 
                        video: false
                    });
                    if (isUnloading) {
                        rawStream.getTracks().forEach(t => t.stop());
                        return;
                    }

                    const newA = rawStream.getAudioTracks()[0];
                    if (newA) newA.enabled = previousAudioEnabled;

                    if (rawStream.getAudioTracks().length > 0) {
                         if (!audioContext) audioContext = new (window.AudioContext || window.webkitAudioContext)();
                         const audioReady = await tryResumeAudioContext();
                         const workletLoaded = audioReady ? await initAudioWorklet() : false;

                         if (workletLoaded) {
                             const source = audioContext.createMediaStreamSource(rawStream);
                             const worklet = new AudioWorkletNode(audioContext, 'rnnoise-processor');
                             const dest = audioContext.createMediaStreamDestination();

                             source.connect(worklet);
                             worklet.connect(dest);

                             const processedAudio = dest.stream.getAudioTracks()[0];
                             if (processedAudio) processedAudio.enabled = previousAudioEnabled;

                             localStream = new MediaStream([processedAudio]);
                             localStream._originalStream = rawStream;
                         } else {
                             localStream = rawStream;
                         }
                    } else {
                        localStream = rawStream;
                    }

                    previewVideo.srcObject = null;
                    await setupVolumeMeter(localStream, 'setupMicBar');
                    updatePreviewButtons();
                } catch(e2) {
                    console.error("Mic fallback start err:", e2);
                    updatePreviewButtons();
                }
            } finally {
                isPreviewStarting = false;

                if (btnPreviewCam) {
                    btnPreviewCam.disabled = false;
                    btnPreviewCam.classList.remove('opacity-50', 'cursor-not-allowed');
                }
                if (btnPreviewMic) {
                    btnPreviewMic.disabled = false;
                    btnPreviewMic.classList.remove('opacity-50', 'cursor-not-allowed');
                }

                if (videoSelectEl && originalVideoSelectContent) {
                    videoSelectEl.innerHTML = originalVideoSelectContent;
                    if (savedVideoValue && [...videoSelectEl.options].some(o => o.value === savedVideoValue)) {
                        videoSelectEl.value = savedVideoValue;
                    }
                    videoSelectEl.disabled = false;
                }
                if (audioSelectEl && originalAudioSelectContent) {
                    audioSelectEl.innerHTML = originalAudioSelectContent;
                    if (savedAudioValue && [...audioSelectEl.options].some(o => o.value === savedAudioValue)) {
                        audioSelectEl.value = savedAudioValue;
                    }
                    audioSelectEl.disabled = false;
                }

                if (localStream) {
                    let needsUpdate = false;
                    if (pendingCamToggle) {
                        const videoTrack = localStream.getVideoTracks()[0];
                        if (videoTrack && videoTrack.enabled) {
                            videoTrack.enabled = false;
                            needsUpdate = true;
                        }
                        pendingCamToggle = false;
                    }
                    if (pendingMicToggle) {
                        const audioTrack = localStream.getAudioTracks()[0];
                        if (audioTrack && audioTrack.enabled) {
                            audioTrack.enabled = false;
                            needsUpdate = true;
                        }
                        pendingMicToggle = false;
                    }
                    if (needsUpdate) {
                        updatePreviewButtons();
                    }
                }
            }
        }

        function updatePreviewButtons() {
             const btnMic = document.getElementById('btnPreviewMic');
             const btnCam = document.getElementById('btnPreviewCam');

             if (!localStream) {
                 btnMic.disabled = true;
                 btnMic.classList.add('opacity-50', 'cursor-not-allowed');
                 btnMic.innerText = "No Mic";

                 btnCam.disabled = true;
                 btnCam.classList.add('opacity-50', 'cursor-not-allowed');
                 btnCam.innerText = "No Cam";
                 document.getElementById('previewPlaceholder').style.display = 'flex';
                 return;
             }

             const audioTrack = localStream.getAudioTracks()[0];
             const videoTrack = localStream.getVideoTracks()[0];

             if (!audioTrack) {
                 btnMic.disabled = true;
                 btnMic.classList.add('opacity-50', 'cursor-not-allowed');
                 btnMic.innerText = "No Mic";
             } else {
                 if (!isPreviewStarting) {
                     btnMic.disabled = false;
                     btnMic.classList.remove('opacity-50', 'cursor-not-allowed');
                 }

                 let isAudioEffectivelyEnabled = audioTrack.enabled;
                 if (pendingMicToggle) {
                     isAudioEffectivelyEnabled = !isAudioEffectivelyEnabled;
                 }

                 if (!isAudioEffectivelyEnabled) {
                     btnMic.classList.add('active-red');
                     btnMic.innerText = "Unmute";
                 } else {
                     btnMic.classList.remove('active-red');
                     btnMic.innerText = "Mute";
                 }
             }

             if (!videoTrack) {

                 if (!isPreviewStarting) {
                     btnCam.disabled = false;
                     btnCam.classList.remove('opacity-50', 'cursor-not-allowed');
                 }
                 btnCam.classList.add('active-red');
                 btnCam.innerText = "Start Cam";
                 document.getElementById('previewPlaceholder').style.display = 'flex';
             } else {

                 if (!isPreviewStarting) {
                     btnCam.disabled = false;
                     btnCam.classList.remove('opacity-50', 'cursor-not-allowed');
                 }

                 let isEffectivelyEnabled = videoTrack.enabled;
                 if (pendingCamToggle) {
                     isEffectivelyEnabled = !isEffectivelyEnabled;
                 }

                 if (!isEffectivelyEnabled) {
                     btnCam.classList.add('active-red');
                     btnCam.innerText = "Start Cam";
                     document.getElementById('previewPlaceholder').style.display = 'flex';
                 } else {
                     btnCam.classList.remove('active-red');
                     btnCam.innerText = "Stop Cam";
                     document.getElementById('previewPlaceholder').style.display = 'none';
                 }
             }
        }

        function togglePreviewMic() {
             if (isPreviewStarting) {
                 pendingMicToggle = !pendingMicToggle;

                 const btnMic = document.getElementById('btnPreviewMic');
                 if (btnMic) {
                    if (btnMic.innerText.includes("Mute") && !btnMic.innerText.includes("Unmute")) {
                        btnMic.classList.add('active-red');
                        btnMic.innerText = "Unmute";
                    } else {
                        btnMic.classList.remove('active-red');
                        btnMic.innerText = "Mute";
                    }
                    btnMic.blur();
                 }
                 savePreferences();
                 return;
             }
             if (!localStream) return;
            const track = localStream.getAudioTracks()[0];
            if (track) {
                track.enabled = !track.enabled;
                if (track.enabled && isDeafened) {
                    isDeafened = false;
                }
                updatePreviewButtons();
                savePreferences();
            }
        }

        function togglePreviewCam() {
             if (isPreviewStarting) {
                 pendingCamToggle = !pendingCamToggle;

                 const btnCam = document.getElementById('btnPreviewCam');
                 const videoTrack = localStream ? localStream.getVideoTracks()[0] : null;
                 const willBeEnabled = videoTrack ? !videoTrack.enabled : !pendingCamToggle;

                 if (btnCam) {
                    if (!willBeEnabled) {
                        btnCam.classList.add('active-red');
                        btnCam.innerText = "Start Cam";
                        document.getElementById('previewPlaceholder').style.display = 'flex';
                    } else {
                        btnCam.classList.remove('active-red');
                        btnCam.innerText = "Stop Cam";
                        document.getElementById('previewPlaceholder').style.display = 'none';
                    }
                    btnCam.blur();
                 }
                 savePreferences();
                 return;
             }
             if (!localStream) return;

             const videoTrack = localStream.getVideoTracks()[0];
             const btnCam = document.getElementById('btnPreviewCam');

             if (videoTrack) {

                 btnCam.disabled = true;
                 btnCam.innerHTML = `<svg class="spinner" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>`;

                 videoTrack.stop();
                 localStream.removeTrack(videoTrack);

                 if (previewVideo) {
                     previewVideo.srcObject = null;
                 }
                 const localVideoEl = document.getElementById('localVideo');
                 if (localVideoEl) {
                     localVideoEl.srcObject = null;
                 }

                 pendingCamToggle = true;

                 btnCam.disabled = false;
                 updatePreviewButtons();
                 savePreferences();
             } else {

                 (async () => {

                     if (btnCam) {
                         btnCam.innerHTML = `<svg class="spinner" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>`;
                         btnCam.disabled = true;
                     }

                     try {
                         const videoSource = videoSelect.value;
                         const constraints = {
                             video: { deviceId: videoSource ? { exact: videoSource } : undefined }
                         };
                         const newStream = await navigator.mediaDevices.getUserMedia(constraints);
                         if (isUnloading) {
                             newStream.getTracks().forEach(t => t.stop());
                             return;
                         }
                         const newTrack = newStream.getVideoTracks()[0];

                         if (!newTrack || newTrack.readyState !== 'live') {
                             console.warn("Camera track not properly initialized, retrying...");
                             newTrack?.stop();
                             if (newTrack && localStream.getVideoTracks().includes(newTrack)) {
                                 localStream.removeTrack(newTrack);
                             }
                             await new Promise(r => setTimeout(r, 100));
                             const retryStream = await navigator.mediaDevices.getUserMedia(constraints);
                             if (isUnloading) {
                                 retryStream.getTracks().forEach(t => t.stop());
                                 return;
                             }
                             const retryTrack = retryStream.getVideoTracks()[0];                             if (retryTrack) {
                                 retryTrack.enabled = true;
                                 localStream.addTrack(retryTrack);
                                 retryStream.getTracks().forEach(t => { if (t !== retryTrack) t.stop(); });
                             }
                         } else {
                             newTrack.enabled = true;
                             localStream.addTrack(newTrack);
                         }

                         pendingCamToggle = false;
                         previewVideo.srcObject = localStream;
                         updatePreviewButtons();
                         savePreferences();
                     } catch (e) {
                         console.error("Could not add camera", e);
                         alert("Could not access camera. Please check permissions.");
                         updatePreviewButtons();
                     }
                 })();
             }
        }

        async function checkAndRestartLocalStreamIfNeeded() {
            if (hasLeftRoom) return;
            const needsRestart = !localStream || localStream.getTracks().some(track => track.readyState === 'ended');
            if (needsRestart) {
                console.log("Local stream tracks are ended/missing. Re-acquiring media...");
                try {
                    await startPreview();
                } catch(e) {
                    console.error("Failed to restart local stream on wakeup:", e);
                }
            }
        }

        async function joinRoom() {

            hasLeftRoom = false;

            if (isAnotherTabActive()) {
                document.getElementById('alertTitle').innerText = 'Already In Call';
                document.getElementById('alertMessage').innerText = 'You already have an active call open in another tab for this room. Please close it first.';

                const alertBtn = document.querySelector('#alertModal button');
                const oldOnClick = alertBtn.onclick;

                alertBtn.onclick = function() {
                    closeCustomAlert();
                    alertBtn.onclick = oldOnClick;
                    sessionStorage.setItem('rustrooms_welcomed', 'false');
                    sessionStorage.setItem('rustrooms_setup_done', 'false');
                    sessionStorage.removeItem('rustrooms_last_room_id');
                    sessionStorage.removeItem('rustrooms_last_channel_id');
                    stopAllMedia(false);
                    roomId = '';
                    channelId = '';
                    history.replaceState(null, '', '/');
                    document.title = 'RustRooms';
                    showWelcome();
                    document.querySelector('main').style.display = 'none';
                    document.querySelector('.taskbar').style.display = 'none';
                    const configOverlay = document.getElementById('configOverlay');
                    if (configOverlay) {
                        configOverlay.classList.add('hidden', 'opacity-0');
                    }
                };

                document.getElementById('alertModal').classList.add('open');
                return;
            }

            proceedJoinRoom();
        }

        async function proceedJoinRoom() {
            userNickname = nicknameInput.value.trim() || "Guest";
            const setupDone = sessionStorage.getItem('rustrooms_setup_done') === 'true';
            if (!setupDone) {
                isDeafened = false;
            }
            savePreferences();

            setActiveTabSession();
            tabHeartbeatInterval = setInterval(setActiveTabSession, 2000);

            if (!audioContext) {
                audioContext = new (window.AudioContext || window.webkitAudioContext)();
            }
            await tryResumeAudioContext(2000);
            await initAudioWorklet();

            previewVideo.srcObject = null;

            if (setupMeterFrameId) cancelAnimationFrame(setupMeterFrameId);
            const configPanel = document.getElementById('configPanel');
            if (configPanel) configPanel.classList.add('panel-pop-away');
            configOverlay.classList.remove('overlay-enter');
            configOverlay.classList.add('opacity-0', 'pointer-events-none', 'overlay-exit');
            setTimeout(() => {
                configOverlay.style.display = 'none';
                if (configPanel) configPanel.classList.remove('panel-pop-away');
                appLayout.classList.remove('hidden');
                appLayout.classList.add('flex');
                document.getElementById('sidebarToggle').classList.remove('hidden');
                applySidebarState(true);
                updatePageTitle();
            }, 300);

            const videoTrack = localStream ? localStream.getVideoTracks()[0] : null;
            if (localVideo) {
                if (videoTrack && videoTrack.enabled) {
                    localVideo.srcObject = localStream;
                } else {
                    localVideo.srcObject = null;
                }
            }

            updateLocalLabel();
            updateLocalAvatar();
            updateLocalMediaLayout();
            const btnMic = document.getElementById('btnMic');
            const btnCam = document.getElementById('btnCam');

            const micOffSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path><line x1="12" x2="12" y1="19" y2="22"></line></svg>`;
            const camOffSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 21l-3.5-3.5m-2-2l-2-2m-2-2l-2-2m-2-2l-3.5-3.5"></path><path d="M15 7h5a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-5"></path><path d="M4 8v8a2 2 0 0 0 2 2h4.5"></path></svg>`;

             if (localStream) {
                const audioTrack = localStream.getAudioTracks()[0];
                const videoTrack = localStream.getVideoTracks()[0];

                const micOnSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3Z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" x2="12" y1="19" y2="22"/></svg>`;
                const camOnSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 4h-5L7 7H4a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2h-3l-2.5-3z"/><circle cx="12" cy="13" r="3"/></svg>`;

                if (!audioTrack || !audioTrack.enabled) {
                     btnMic.classList.add('active-red');
                     btnMic.innerHTML = micOffSvg;
                } else {
                     btnMic.classList.remove('active-red');
                     btnMic.innerHTML = micOnSvg;
                }

                if (!videoTrack || !videoTrack.enabled) {
                     btnCam.classList.add('active-red');
                     btnCam.innerHTML = camOffSvg;
                } else {
                     btnCam.classList.remove('active-red');
                     btnCam.innerHTML = camOnSvg;
                }

                await setupAudioMonitor(localStream, 'local');
            } else {
                 btnMic.classList.add('active-red');
                 btnMic.innerHTML = micOffSvg;
                 btnCam.classList.add('active-red');
                 btnCam.innerHTML = camOffSvg;
            }

            if (isDeafened) {
                const btnDeafen = document.getElementById('btnDeafen');
                const deafenOffSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 14a2 2 0 0 0-2-2h-3a2 2 0 0 0-2 2v3a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2V14z"></path><path d="M3 14a2 2 0 0 1 2-2h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V14z"></path><path d="M20.4 10.4C20.2 6.5 17 3.5 13 3.1"></path><path d="M6.5 5.5A9 9 0 0 0 3 12"></path></svg>`;
                if (btnDeafen) {
                    btnDeafen.classList.add('active-red');
                    btnDeafen.innerHTML = deafenOffSvg;
                }

                if (btnMic) {
                    btnMic.disabled = true;
                }

                document.querySelectorAll('video, audio').forEach(el => {
                    if (el.id !== 'localVideo' && el.id !== 'previewVideo') {
                        el.muted = true;
                    }
                });
            }

            connectWs();

            sessionStorage.setItem('rustrooms_setup_done', 'true');
            sessionStorage.setItem('rustrooms_last_room_id', roomId);
            sessionStorage.setItem('rustrooms_last_channel_id', channelId);
            recordRecentCall();

            await requestWakeLock();
        }


        // ---- Recent calls (landing page list) ----
        // Stored locally, newest first, capped at 5; entries expire
        // after 7 days so the list only ever shows calls you might
        // plausibly rejoin.
        const RECENT_CALLS_KEY = 'rustrooms_recent_calls';
        const RECENT_CALLS_MAX = 5;
        const RECENT_CALLS_TTL_MS = 7 * 24 * 60 * 60 * 1000;

        function loadRecentCalls() {
            try {
                const raw = JSON.parse(localStorage.getItem(RECENT_CALLS_KEY) || '[]');
                if (!Array.isArray(raw)) return [];
                const cutoff = Date.now() - RECENT_CALLS_TTL_MS;
                return raw.filter(e => e && e.room && e.ts > cutoff);
            } catch (e) {
                return [];
            }
        }

        function saveRecentCalls(list) {
            try { localStorage.setItem(RECENT_CALLS_KEY, JSON.stringify(list)); } catch (e) {}
        }

        function recordRecentCall() {
            if (!roomId) return;
            const channel = channelId || 'General';
            const list = loadRecentCalls().filter(e => !(e.room === roomId && e.channel === channel));
            list.unshift({ room: roomId, channel: channel, ts: Date.now(), with: Object.values(recentCallPeers) });
            saveRecentCalls(list.slice(0, RECENT_CALLS_MAX));
            renderRecentCalls();
        }

        // Who else is in the current call, by userId. Fed by the peer
        // lifecycle in connection.js so recent entries can say who a
        // "# General" call was actually with.
        const recentCallPeers = {};

        function noteRecentCallPeer(userId, name) {
            if (!userId || !name || recentCallPeers[userId] === name) return;
            recentCallPeers[userId] = name;
            refreshRecentCallParticipants();
        }

        function forgetRecentCallPeer(userId) {
            // Memory only: the stored entry keeps the last roster it saw,
            // so leaving a call doesn't erase who was in it.
            delete recentCallPeers[userId];
        }

        function refreshRecentCallParticipants() {
            if (!roomId) return;
            const channel = channelId || 'General';
            const list = loadRecentCalls();
            const entry = list.find(e => e.room === roomId && e.channel === channel);
            if (!entry) return;
            entry.with = [...new Set(Object.values(recentCallPeers))];
            saveRecentCalls(list);
        }

        function clearRecentCalls() {
            try { localStorage.removeItem(RECENT_CALLS_KEY); } catch (e) {}
            renderRecentCalls();
        }

        function removeRecentCall(room, channel) {
            saveRecentCalls(loadRecentCalls().filter(e => !(e.room === room && e.channel === channel)));
            renderRecentCalls();
        }

        function recentCallUrl(entry) {
            const base = '/' + encodeURIComponent(entry.room);
            return entry.channel && entry.channel.toLowerCase() !== 'general'
                ? base + '/' + encodeURIComponent(entry.channel)
                : base;
        }

        function formatRecentTime(ts) {
            const mins = Math.floor((Date.now() - ts) / 60000);
            if (mins < 1) return 'just now';
            if (mins < 60) return mins + 'm ago';
            const hours = Math.floor(mins / 60);
            if (hours < 24) return hours + 'h ago';
            const days = Math.floor(hours / 24);
            return days === 1 ? 'yesterday' : days + 'd ago';
        }

        function renderRecentCalls() {
            const wrap = document.getElementById('recentCalls');
            const listEl = document.getElementById('recentCallsList');
            if (!wrap || !listEl) return;
            const list = loadRecentCalls();
            saveRecentCalls(list);
            if (!list.length) {
                wrap.style.display = 'none';
                return;
            }
            wrap.style.display = '';
            listEl.innerHTML = '';
            for (const entry of list) {
                const a = document.createElement('a');
                a.className = 'recent-call';
                a.href = recentCallUrl(entry);
                a.title = new Date(entry.ts).toLocaleString();

                const main = document.createElement('span');
                main.className = 'recent-call-main';

                const name = document.createElement('span');
                name.className = 'recent-call-name';
                name.textContent = '# ' + entry.channel;
                main.appendChild(name);

                if (Array.isArray(entry.with)) {
                    const sub = document.createElement('span');
                    sub.className = 'recent-call-sub';
                    if (entry.with.length === 0) {
                        sub.textContent = 'Just you';
                    } else if (entry.with.length <= 3) {
                        sub.textContent = 'with ' + entry.with.join(', ');
                    } else {
                        sub.textContent = 'with ' + entry.with.slice(0, 2).join(', ') + ' +' + (entry.with.length - 2) + ' more';
                    }
                    main.appendChild(sub);
                }

                const time = document.createElement('span');
                time.className = 'recent-call-time';
                time.textContent = formatRecentTime(entry.ts);

                const remove = document.createElement('button');
                remove.className = 'recent-call-remove';
                remove.type = 'button';
                remove.textContent = '\u00d7';
                remove.title = 'Remove from recent calls';
                remove.setAttribute('aria-label', 'Remove from recent calls');
                remove.addEventListener('click', (ev) => {
                    ev.preventDefault();
                    ev.stopPropagation();
                    removeRecentCall(entry.room, entry.channel);
                });

                a.appendChild(main);
                a.appendChild(time);
                a.appendChild(remove);
                listEl.appendChild(a);
            }
        }

        // ---- Theme: near-black dark (default) / optional light ----
        function currentTheme() {
            return document.documentElement.dataset.theme === 'light' ? 'light' : 'dark';
        }

        function applyTheme(theme) {
            const light = theme === 'light';
            if (light) {
                document.documentElement.dataset.theme = 'light';
            } else {
                delete document.documentElement.dataset.theme;
            }
            const meta = document.querySelector('meta[name="theme-color"]');
            if (meta) meta.setAttribute('content', light ? '#F4F4F6' : '#050506');
            const sun = document.getElementById('iconThemeSun');
            const moon = document.getElementById('iconThemeMoon');
            if (sun) sun.classList.toggle('hidden', light);
            if (moon) moon.classList.toggle('hidden', !light);
            const settingsTheme = document.getElementById('settingsTheme');
            if (settingsTheme) settingsTheme.checked = light;
        }

        function toggleTheme(force) {
            const next = typeof force === 'boolean'
                ? (force ? 'light' : 'dark')
                : (currentTheme() === 'light' ? 'dark' : 'light');
            try { localStorage.setItem('rustrooms_theme', next); } catch (e) {}
            applyTheme(next);
        }

        applyTheme(currentTheme());
        renderRecentCalls();

        // Until the user picks a theme manually (which saves it), keep
        // following the device light/dark setting, including live flips.
        try {
            const osThemeQuery = window.matchMedia('(prefers-color-scheme: light)');
            if (typeof osThemeQuery.addEventListener === 'function') {
                osThemeQuery.addEventListener('change', () => {
                    if (!localStorage.getItem('rustrooms_theme')) {
                        applyTheme(osThemeQuery.matches ? 'light' : 'dark');
                    }
                });
            }
        } catch (e) {}

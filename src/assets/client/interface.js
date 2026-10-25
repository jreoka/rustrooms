        // Global event listeners to handle network and lifecycle events
        window.addEventListener('offline', () => {
            if (hasLeftRoom) return;
            console.warn('Network connection lost (offline)');
            updateStatus('disconnected', 'Network Offline');
            updateConnectionStatus();
        });

        window.addEventListener('online', () => {
            if (hasLeftRoom) return;

            if (isReconnecting) {
                console.log('Already reconnecting, skipping network restore trigger');
                return;
            }

            console.log('Network connection restored (online)');
            updateStatus('connecting', 'Reconnecting...');

            reconnectionAttempts = 0;
            connectWs();
        });

        if (isIOS) {
            document.addEventListener('visibilitychange', async () => {
                if (document.visibilityState === 'visible' && !hasLeftRoom) {
                    stopHeartbeat();
                    
                    // Restart media tracks if they were ended by iOS during lock/background
                    await checkAndRestartLocalStreamIfNeeded();

                    if (!ws || ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
                        console.log('iOS returned from background, WebSocket dead, reconnecting...');
                        isReconnecting = false;
                        reconnectionAttempts = 0;
                        connectWs();
                    } else if (ws.readyState === WebSocket.OPEN) {
                        startHeartbeat();

                        let hasDeadPeer = false;
                        for (const uid in peers) {
                            const peerState = peers[uid].connectionState || peers[uid].iceConnectionState;
                            if (peerState === 'disconnected' || peerState === 'failed' || peerState === 'closed') {
                                hasDeadPeer = true;
                                break;
                            }
                        }
                        if (hasDeadPeer) {
                            console.log('iOS returned from background, dead peers detected, re-establishing...');
                            for (const uid in peers) {
                                removePeer(uid);
                            }
                            peerCamStatus = {};
                            peerScreenStatus = {};
                            peerScreenHasAudio = {};
                            isReconnecting = false;
                            reconnectionAttempts = 0;
                            connectWs();
                        }
                    }
                    if (audioContext && audioContext.state === 'suspended') {
                        audioContext.resume().catch(e => {});
                    }
                }
            });

            // iOS WebSocket watchdog — catches silent WS deaths that don't trigger onclose
            setInterval(() => {
                if (hasLeftRoom) return;
                const now = Date.now();
                const pongAge = now - lastPongTime;
                // If we haven't received a pong in 3x the heartbeat interval, WS is probably dead
                const watchdogThreshold = heartbeatIntervalMs * 3 + heartbeatTimeoutMs;
                if (ws && ws.readyState === WebSocket.OPEN && pongAge > watchdogThreshold) {
                    console.warn(`iOS watchdog: no pong in ${Math.round(pongAge/1000)}s, force-reconnecting`);
                    missedPongCount = 0;
                    ws.close();
                } else if (!ws || ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
                    if (!isReconnecting && !hasLeftRoom) {
                        console.warn('iOS watchdog: WebSocket is dead and no reconnection in progress, reconnecting...');
                        reconnectionAttempts = 0;
                        isReconnecting = false;
                        connectWs();
                    }
                }
            }, 30000);
        } else {
            // Desktop: returning to the tab is the signal to repair a
            // connection the server closed while it was backgrounded
            // (e.g. inactivity timeout code 4001 when timers were throttled).
            document.addEventListener('visibilitychange', () => {
                if (document.visibilityState === 'visible' && !hasLeftRoom) {
                    if (!ws || ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
                        console.log('Returned to tab, WebSocket dead, reconnecting...');
                        isReconnecting = false;
                        reconnectionAttempts = 0;
                        connectWs();
                    }
                }
            });
        }

        // Handle iOS BFCache restoration (back-forward cache)
        window.addEventListener('pageshow', (event) => {
            if (event.persisted && !hasLeftRoom) {
                console.log('Page restored from BFCache, checking WebSocket...');
                if (!ws || ws.readyState !== WebSocket.OPEN) {
                    isReconnecting = false;
                    reconnectionAttempts = 0;
                    connectWs();
                } else {
                    startHeartbeat();
                }
            }
        });

        /* ============================================================
         * Shared transition helpers — polished pop up / pop away.
         * Every overlay/panel in the app funnels through these so the
         * entrances and exits stay consistent.
         * ============================================================ */
        const OVERLAY_MS = 300;
        let _welcomeHideTimer = null;

        // Fade the whole welcome screen in; children (title, subtitle,
        // start button) replay their staggered blur-pop automatically
        // because the overlay goes from display:none to visible.
        function showWelcome() {
            clearTimeout(_welcomeHideTimer);
            const overlay = document.getElementById('welcomeOverlay');
            if (!overlay) return;
            overlay.classList.remove('welcome-exit');
            overlay.style.display = 'flex';
            renderRecentCalls();
            requestAnimationFrame(() => overlay.classList.add('welcome-enter'));
        }

        function hideWelcome() {
            clearTimeout(_welcomeHideTimer);
            const overlay = document.getElementById('welcomeOverlay');
            if (!overlay) return;
            overlay.classList.remove('welcome-enter');
            overlay.classList.add('welcome-exit');
            _welcomeHideTimer = setTimeout(() => {
                overlay.style.display = 'none';
                overlay.classList.remove('welcome-exit');
            }, OVERLAY_MS);
        }

        // Show a full-screen overlay (config / invite / settings / crop).
        // Handles the Tailwind `hidden` + `opacity-0` convention used by
        // the existing overlays.
        const _overlayHideTimers = new WeakMap();
        function showOverlayAnimated(el) {
            if (!el) return;
            const pending = _overlayHideTimers.get(el);
            if (pending) {
                clearTimeout(pending);
                _overlayHideTimers.delete(el);
            }
            el.classList.remove('hidden', 'opacity-0', 'overlay-exit', 'pointer-events-none');
            void el.offsetWidth; // restart any CSS animation on the panel
            el.classList.add('overlay-enter');
        }

        function hideOverlayAnimated(el, done) {
            if (!el) {
                if (done) done();
                return;
            }
            const pending = _overlayHideTimers.get(el);
            if (pending) clearTimeout(pending);
            el.classList.remove('overlay-enter');
            el.classList.add('opacity-0', 'overlay-exit', 'pointer-events-none');
            _overlayHideTimers.set(el, setTimeout(() => {
                _overlayHideTimers.delete(el);
                el.classList.add('hidden');
                if (done) done();
            }, OVERLAY_MS));
        }

        // Animate a modal out (pop-away) before dropping the .open state.
        function animateModalClose(id) {
            const overlay = document.getElementById(id);
            if (!overlay) return;
            overlay.classList.add('closing');
            overlay.classList.remove('open');
            setTimeout(() => overlay.classList.remove('closing'), 280);
        }

        function playNotificationSound(type) {
            if (!audioContext || audioContext.state === 'closed') {
                try {
                    audioContext = new (window.AudioContext || window.webkitAudioContext)();
                } catch (e) {
                    console.warn("Failed to create AudioContext:", e);
                    return;
                }
            }
            if (audioContext.state === 'suspended') {
                audioContext.resume().catch(e => console.warn("Failed to resume AudioContext:", e));
            }

            // Soft modern UI tones: a sine fundamental plus a quiet octave
            // harmonic through a gentle lowpass, shaped by a fast pluck
            // envelope (no bare beeps, no pitch slides). Motifs are two-note
            // shapes from one key family so events feel related: rising =
            // started/joined, falling = stopped/left.
            const t0 = audioContext.currentTime;

            const master = audioContext.createGain();
            master.gain.value = 0.9;
            const lowpass = audioContext.createBiquadFilter();
            lowpass.type = 'lowpass';
            lowpass.frequency.value = 3600;
            lowpass.Q.value = 0.4;
            master.connect(lowpass);
            lowpass.connect(audioContext.destination);

            function tone(freq, at, dur, vol, bendTo) {
                const osc = audioContext.createOscillator();
                osc.type = 'sine';
                osc.frequency.setValueAtTime(freq, t0 + at);
                if (bendTo) osc.frequency.exponentialRampToValueAtTime(bendTo, t0 + at + dur);

                const harm = audioContext.createOscillator();
                harm.type = 'triangle';
                harm.frequency.setValueAtTime(freq * 2, t0 + at);
                if (bendTo) harm.frequency.exponentialRampToValueAtTime(bendTo * 2, t0 + at + dur);

                const g = audioContext.createGain();
                g.gain.setValueAtTime(0.0001, t0 + at);
                g.gain.exponentialRampToValueAtTime(vol, t0 + at + 0.008);
                g.gain.exponentialRampToValueAtTime(0.0001, t0 + at + dur);

                const hg = audioContext.createGain();
                hg.gain.value = 0.18;

                osc.connect(g);
                harm.connect(hg);
                hg.connect(g);
                g.connect(master);

                osc.start(t0 + at);
                harm.start(t0 + at);
                osc.stop(t0 + at + dur + 0.05);
                harm.stop(t0 + at + dur + 0.05);
            }

            switch (type) {
                case 'join':
                    tone(587.33, 0, 0.22, 0.11);
                    tone(880.00, 0.07, 0.30, 0.11);
                    break;
                case 'leave':
                    tone(880.00, 0, 0.20, 0.09);
                    tone(587.33, 0.07, 0.28, 0.09);
                    break;
                case 'disconnect':
                    tone(523.25, 0, 0.22, 0.10);
                    tone(349.23, 0.09, 0.34, 0.10);
                    break;
                case 'mute':
                    tone(392.00, 0, 0.10, 0.10, 349.23);
                    break;
                case 'unmute':
                    tone(659.25, 0, 0.11, 0.10, 739.99);
                    break;
                case 'bandwidth_on':
                    tone(523.25, 0, 0.14, 0.08);
                    tone(783.99, 0.06, 0.20, 0.08);
                    break;
                case 'bandwidth_off':
                    tone(783.99, 0, 0.14, 0.08);
                    tone(523.25, 0.06, 0.20, 0.08);
                    break;
            }
        }

        function updateStatus(state, message) {
            statusText.innerText = message;
            connectionDot.className = 'connection-dot ' + state;
        }

        function showReconnectButtons() {
            const btn = document.getElementById('btnReconnect');
            if (btn) btn.classList.remove('hidden');
        }

        function updateConnectionStatus() {

            const peerIds = Object.keys(peers);
            let hasConnectedPeers = false;
            let hasConnectingPeers = false;

            for (const userId of peerIds) {
                const pc = peers[userId];
                if (pc) {
                    const iceState = pc.iceConnectionState;

                    if (iceState === 'connected' || iceState === 'completed') {
                        hasConnectedPeers = true;
                    } else if (iceState === 'checking' || iceState === 'new') {
                        hasConnectingPeers = true;
                    }
                }
            }

            if (peerIds.length > 0 && !hasConnectedPeers && !hasConnectingPeers) {
                updateStatus('disconnected', 'Connection Lost');
            } else if (hasConnectedPeers) {
                updateStatus('connected', 'Connected');
            }
        }

        function toggleSidebar() {
            const body = document.body;
            const sidebar = document.getElementById('roomSidebar');
            const overlay = document.getElementById('sidebarOverlay');
            const sidebarToggle = document.getElementById('sidebarToggle');
            const isDesktop = window.innerWidth >= 768;
            const wasOpen = sidebar.classList.contains('open');

            const isOpen = !wasOpen;

            if (isOpen) {
                sidebar.classList.add('open');
                overlay.classList.add('open');
                body.classList.add('sidebar-open');
                sidebarToggle.classList.add('hidden');

                if (isDesktop) {
                    const pip = document.getElementById('localPipWrapper');
                    if (pip) {
                        const pipRect = pip.getBoundingClientRect();
                        const sidebarWidth = 340;
                        const margin = 24;

                        if (pipRect.left < sidebarWidth + margin) {
                            const newLeft = sidebarWidth + margin;
                            pip.style.left = newLeft + 'px';
                            pip.style.bottom = '';
                            pip.style.right = '';
                        }
                    }
                }
            } else {
                sidebar.classList.remove('open');
                overlay.classList.remove('open');
                body.classList.remove('sidebar-open');
                sidebarToggle.classList.remove('hidden');

                const pip = document.getElementById('localPipWrapper');
                if (pip) {
                    pip.style.left = '';
                    pip.style.right = '';
                    pip.style.bottom = '';
                }
            }
            localStorage.setItem('rustrooms_sidebar_open', isOpen ? 'true' : 'false');
        }

        let lastViewportWasDesktop = window.innerWidth >= 768;
        window.addEventListener('resize', () => {
            const isDesktop = window.innerWidth >= 768;
            if (isDesktop !== lastViewportWasDesktop) {
                lastViewportWasDesktop = isDesktop;

                const body = document.body;
                const sidebar = document.getElementById('roomSidebar');
                const overlay = document.getElementById('sidebarOverlay');
                const sidebarToggle = document.getElementById('sidebarToggle');
                const isOpen = sidebar.classList.contains('open');

                if (isOpen) {
                    if (isDesktop) {
                        overlay.classList.remove('open');
                        body.classList.add('sidebar-open');
                        sidebarToggle.classList.add('hidden');

                        const pip = document.getElementById('localPipWrapper');
                        if (pip) {
                            const pipRect = pip.getBoundingClientRect();
                            const sidebarWidth = 340;
                            const margin = 24;

                            if (pipRect.left < sidebarWidth + margin) {
                                const newLeft = sidebarWidth + margin;
                                pip.style.left = newLeft + 'px';
                                pip.style.bottom = '';
                                pip.style.right = '';
                            }
                        }
                    } else {
                        overlay.classList.add('open');
                        body.classList.add('sidebar-open');
                        sidebarToggle.classList.add('hidden');
                    }
                } else {
                    overlay.classList.remove('open');
                    body.classList.remove('sidebar-open');
                    sidebarToggle.classList.remove('hidden');
                }
            }
        });

        function applySidebarState(noTransition = false) {
            const savedState = localStorage.getItem('rustrooms_sidebar_open');
            const isOpen = savedState === 'true';
            const isDesktop = window.innerWidth >= 768;
            const sidebarToggle = document.getElementById('sidebarToggle');

            if (isOpen) {
                const body = document.body;
                const sidebar = document.getElementById('roomSidebar');
                const overlay = document.getElementById('sidebarOverlay');

                if (noTransition) {
                    sidebar.style.transition = 'none';
                }
                body.classList.add('sidebar-open');
                sidebar.classList.add('open');
                sidebarToggle.classList.add('hidden');

                if (isDesktop) {
                    overlay.classList.remove('open');

                    const pip = document.getElementById('localPipWrapper');
                    if (pip) {
                        const pipRect = pip.getBoundingClientRect();
                        const sidebarWidth = 340;
                        const margin = 24;

                        if (pipRect.left < sidebarWidth + margin) {
                            const newLeft = sidebarWidth + margin;
                            pip.style.left = newLeft + 'px';
                            pip.style.bottom = '';
                            pip.style.right = '';
                        }
                    }
                } else {
                    overlay.classList.add('open');
                }

                if (noTransition) {

                    sidebar.offsetHeight;

                    setTimeout(() => {
                        sidebar.style.transition = '';
                    }, 50);
                }
            } else {
                sidebarToggle.classList.remove('hidden');
            }
        }

        function showNameModal(title, placeholder, callback) {
            const modal = document.getElementById('nameModal');
            const modalTitle = document.getElementById('modalTitle');
            const modalInput = document.getElementById('modalInput');
            const modalSubmit = document.getElementById('modalSubmit');

            modalTitle.innerText = title;
            modalInput.placeholder = placeholder;
            modalInput.value = '';
            modal.classList.add('open');
            setTimeout(() => modalInput.focus(), 100);

            modalSubmit.onclick = () => {
                const name = modalInput.value.trim();
                callback(name);
                closeNameModal();
            };

            const handleEnter = (e) => {
                if (e.key === 'Enter') {
                    modalSubmit.click();
                    modalInput.removeEventListener('keydown', handleEnter);
                }
            };
            modalInput.addEventListener('keydown', handleEnter);
        }

        function closeNameModal() {
            animateModalClose('nameModal');
        }

        let customAlertCallback = null;
        function showCustomAlert(title, message, onOk) {
            document.getElementById('alertTitle').innerText = title;
            document.getElementById('alertMessage').innerText = message;
            customAlertCallback = typeof onOk === 'function' ? onOk : null;
            document.getElementById('alertModal').classList.add('open');
        }

        function closeCustomAlert() {
            animateModalClose('alertModal');
            if (customAlertCallback) {
                const cb = customAlertCallback;
                customAlertCallback = null;
                cb();
            }
        }

        function showPasswordModal(title, message, callback) {
            const modal = document.getElementById('passwordModal');
            const modalTitle = document.getElementById('passwordModalTitle');
            const modalMessage = document.getElementById('passwordModalMessage');
            const modalInput = document.getElementById('passwordModalInput');
            const modalSubmit = document.getElementById('passwordModalSubmit');

            modalTitle.innerText = title;
            modalMessage.innerText = message || "";
            modalInput.value = '';
            modal.classList.add('open');
            setTimeout(() => modalInput.focus(), 100);

            modalSubmit.onclick = () => {
                const pass = modalInput.value;
                callback(pass);
                closePasswordModal();
            };
        }

        function closePasswordModal() {
            animateModalClose('passwordModal');
        }

        function showCustomConfirm(title, message, onConfirm) {
            document.getElementById('confirmTitle').innerText = title;
            document.getElementById('confirmMessage').innerText = message;
            const modal = document.getElementById('confirmModal');
            const submitBtn = document.getElementById('confirmSubmit');

            const newBtn = submitBtn.cloneNode(true);
            submitBtn.parentNode.replaceChild(newBtn, submitBtn);

            newBtn.onclick = () => {
                onConfirm();
                closeCustomConfirm();
            };

            modal.classList.add('open');
        }

        function closeCustomConfirm() {
            animateModalClose('confirmModal');
        }

        let userClickTracker = {};
        let pendingKickUserId = null;

        function handleUserClick(el) {
            const userId = el.dataset.userId;
            const nickname = el.dataset.userNickname;

            if (!userId || userId === persistentUserId) return;

            const now = Date.now();
            const windowMs = 5000;
            const threshold = 10;

            if (!userClickTracker[userId]) {
                userClickTracker[userId] = [];
            }

            userClickTracker[userId] = userClickTracker[userId].filter(timestamp => now - timestamp < windowMs);
            userClickTracker[userId].push(now);

            if (userClickTracker[userId].length >= threshold) {
                userClickTracker[userId] = [];
                showKickModal(userId, nickname);
            }
        }

        let _uvmLongPressTimer = null;
        let _uvmTouchMoved = false;

        function handleUserContextMenu(e, el) {
            e.preventDefault();
            e.stopPropagation();
            const userId = el.dataset.userId;
            const nickname = el.dataset.userNickname;
            if (!userId || userId === persistentUserId) return;
            showUserVolumeMenu(userId, nickname, e.clientX, e.clientY);
        }

        function handleUserTouchStart(e, el) {
            _uvmTouchMoved = false;
            const touch = e.touches[0];
            const tx = touch.clientX;
            const ty = touch.clientY;
            _uvmLongPressTimer = setTimeout(() => {
                if (_uvmTouchMoved) return;
                const userId = el.dataset.userId;
                const nickname = el.dataset.userNickname;
                if (!userId || userId === persistentUserId) return;
                e.preventDefault();
                showUserVolumeMenu(userId, nickname, tx, ty);
            }, 500);
        }

        function handleUserTouchEnd(e) {
            if (_uvmLongPressTimer) {
                clearTimeout(_uvmLongPressTimer);
                _uvmLongPressTimer = null;
            }
        }

        function handleUserTouchCancel() {
            _uvmTouchMoved = true;
            if (_uvmLongPressTimer) {
                clearTimeout(_uvmLongPressTimer);
                _uvmLongPressTimer = null;
            }
        }

        function showUserVolumeMenu(userId, nickname, x, y) {
            const menu = document.getElementById('userVolumeMenu');
            if (!menu) return;

            const mainVol = getVolumeSettings(userId, 'main');
            const hasScreen = !!peerScreenHasAudio[userId];
            const screenVol = hasScreen ? getVolumeSettings(userId, 'screen') : 1.0;

            const vidEl = document.getElementById(`vid-${userId}`);
            const mainMuted = vidEl ? vidEl.muted : false;
            let screenMuted = false;
            if (hasScreen) {
                const screenAud = document.getElementById(`aud-screen-${userId}`);
                screenMuted = screenAud ? screenAud.muted : false;
            }

            const volSvgOn = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><path d="M19.07 4.93a10 10 0 0 1 0 14.14M15.54 8.46a5 5 0 0 1 0 7.07"></path></svg>`;
            const volSvgOff = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><line x1="23" y1="9" x2="17" y2="15"></line><line x1="17" y1="9" x2="23" y2="15"></line></svg>`;
            const screenSvgOn = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="14" rx="2" ry="2"></rect><line x1="12" y1="22" x2="12" y2="16"></line><path d="M5 12h14"></path><path d="M12 12v4"></path></svg>`;
            const screenSvgOff = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="14" rx="2" ry="2"></rect><line x1="23" y1="9" x2="17" y2="15"></line><line x1="17" y1="9" x2="23" y2="15"></line></svg>`;

            let html = `
                <div class="uvm-header">
                    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--text-muted)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path><circle cx="12" cy="7" r="4"></circle></svg>
                    <span class="uvm-name">${escapeHtml(nickname)}</span>
                    <button class="uvm-close" onclick="closeUserVolumeMenu()">
                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>
                    </button>
                </div>
                <div class="uvm-section">
                    <div class="uvm-label">Mic Volume</div>
                    <div class="uvm-slider-row">
                        <button id="uvm-mute-main" class="${mainMuted ? 'muted' : ''}" onclick="uvmToggleMute('${userId}', 'main')">
                            ${mainMuted ? volSvgOff : volSvgOn}
                        </button>
                        <input type="range" min="0" max="1" step="0.05" value="${mainVol}" id="uvm-slider-main" oninput="uvmSetVolume('${userId}', 'main', this.value)">
                        <span class="uvm-vol-pct" id="uvm-pct-main">${Math.round(mainVol * 100)}%</span>
                    </div>
                </div>
            `;

            if (hasScreen) {
                html += `
                <div class="uvm-section">
                    <div class="uvm-label">Screen Volume</div>
                    <div class="uvm-slider-row">
                        <button id="uvm-mute-screen" class="${screenMuted ? 'muted' : ''}" onclick="uvmToggleMute('${userId}', 'screen')">
                            ${screenMuted ? screenSvgOff : screenSvgOn}
                        </button>
                        <input type="range" min="0" max="1" step="0.05" value="${screenVol}" id="uvm-slider-screen" oninput="uvmSetVolume('${userId}', 'screen', this.value)">
                        <span class="uvm-vol-pct" id="uvm-pct-screen">${Math.round(screenVol * 100)}%</span>
                    </div>
                </div>
                `;
            }

            if (typeof iAmAdmin === 'function' && iAmAdmin() && userId !== persistentUserId) {
                const tCreator = isCreatorId(userId);
                const tAdmin = isAdminId(userId);
                const f = (roomAdminState.forced && roomAdminState.forced[userId]) || {};
                const abtn = (action, label, danger) => `<button class="uvm-admin-btn${danger ? ' danger' : ''}" onclick="adminMenuAction('${action}', '${userId}')">${label}</button>`;
                let adminHtml = '';
                if (tCreator) {
                    adminHtml = `<div class="uvm-admin-note">Room creator · Admin</div>`;
                } else {
                    adminHtml += abtn(tAdmin ? 'demote' : 'promote', tAdmin ? 'Demote admin' : 'Make admin');
                    if (roomAdminState.kind === 'stage' && !tAdmin) {
                        adminHtml += canSpeakId(userId) ? abtn('to-audience', 'Move to audience') : abtn('to-stage', 'Move to stage');
                    }
                    adminHtml += abtn(f.muted ? 'unmute' : 'mute', f.muted ? 'Remove server mute' : 'Server mute');
                    adminHtml += abtn(f.deafened ? 'undeafen' : 'deafen', f.deafened ? 'Remove deafen' : 'Server deafen');
                    adminHtml += abtn(f.camOff ? 'cam-on' : 'cam-off', f.camOff ? 'Allow camera' : 'Turn off camera');
                    adminHtml += abtn(f.shareOff ? 'share-on' : 'share-off', f.shareOff ? 'Allow screen share' : 'Stop screen share');
                    adminHtml += abtn('kick', 'Kick', true);
                    adminHtml += abtn('ban', 'Ban', true);
                }
                html += `
                <div class="uvm-section">
                    <div class="uvm-label">Admin</div>
                    <div class="uvm-admin-grid">${adminHtml}</div>
                </div>`;
            }

            menu.innerHTML = html;
            menu.dataset.userId = userId;

            menu.style.left = '0px';
            menu.style.top = '0px';
            menu.classList.add('open');

            requestAnimationFrame(() => {
                const mw = menu.offsetWidth;
                const mh = menu.offsetHeight;
                const vw = window.innerWidth;
                const vh = window.innerHeight;
                let left = x;
                let top = y;
                if (left + mw > vw - 8) left = vw - mw - 8;
                if (left < 8) left = 8;
                if (top + mh > vh - 8) top = vh - mh - 8;
                if (top < 8) top = 8;
                menu.style.left = left + 'px';
                menu.style.top = top + 'px';
            });
        }

        function closeUserVolumeMenu() {
            const menu = document.getElementById('userVolumeMenu');
            if (menu) {
                menu.classList.remove('open');
                menu.dataset.userId = '';
            }
        }

        window.adminMenuAction = function(action, userId) {
            let nickname = 'this user';
            for (const rid in globalRoomList) {
                const users = (globalRoomList[rid] || {}).users || {};
                if (users[userId]) { nickname = users[userId].nickname || nickname; break; }
            }
            if (action === 'kick') {
                closeUserVolumeMenu();
                showKickModal(userId, nickname);
                return;
            }
            if (action === 'ban') {
                closeUserVolumeMenu();
                showCustomConfirm('Ban user', `Ban "${nickname}" from this room? They won't be able to rejoin until the call fully ends.`, () => sendAdminAction('ban', userId));
                return;
            }
            sendAdminAction(action, userId);
            closeUserVolumeMenu();
        };

        window.uvmToggleMute = function(userId, type) {
            toggleMute(userId, type);

            let el;
            if (type === 'screen') {
                el = document.getElementById(`aud-screen-${userId}`);
                if (!el) el = document.getElementById(`vid-${userId}`);
            } else {
                el = document.getElementById(`vid-${userId}`);
            }
            const isMuted = el ? el.muted : false;

            const btn = document.getElementById(`uvm-mute-${type}`);
            if (btn) {
                if (type === 'screen') {
                    const screenSvgOn = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="14" rx="2" ry="2"></rect><line x1="12" y1="22" x2="12" y2="16"></line><path d="M5 12h14"></path><path d="M12 12v4"></path></svg>`;
                    const screenSvgOff = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="14" rx="2" ry="2"></rect><line x1="23" y1="9" x2="17" y2="15"></line><line x1="17" y1="9" x2="23" y2="15"></line></svg>`;
                    btn.innerHTML = isMuted ? screenSvgOff : screenSvgOn;
                } else {
                    const volSvgOn = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><path d="M19.07 4.93a10 10 0 0 1 0 14.14M15.54 8.46a5 5 0 0 1 0 7.07"></path></svg>`;
                    const volSvgOff = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><line x1="23" y1="9" x2="17" y2="15"></line><line x1="17" y1="9" x2="23" y2="15"></line></svg>`;
                    btn.innerHTML = isMuted ? volSvgOff : volSvgOn;
                }
                btn.classList.toggle('muted', isMuted);
            }
        };

        window.uvmSetVolume = function(userId, type, val) {
            setVolume(userId, type, val);

            const pct = document.getElementById(`uvm-pct-${type}`);
            if (pct) pct.textContent = Math.round(val * 100) + '%';

            const overlaySlider = document.querySelector(`#vol-row-${type}-${userId} input[type=range]`);
            if (overlaySlider) overlaySlider.value = val;
        };

        document.addEventListener('mousedown', function(e) {
            const menu = document.getElementById('userVolumeMenu');
            if (menu && menu.classList.contains('open') && !menu.contains(e.target)) {
                closeUserVolumeMenu();
            }
        });

        document.addEventListener('keydown', function(e) {
            const t = e.target;
            const typing = t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.isContentEditable);

            if (e.key === 'Escape') {
                // Close the top-most thing: popup menu, stats window,
                // settings, then safe modal overlays.
                // (Deliberately no "typing" guard: Escape inside a modal's
                // own text field should still dismiss that modal.)
                closeUserVolumeMenu();

                const statsWindow = document.getElementById('statsWindow');
                if (statsWindow && statsWindow.classList.contains('visible')) {
                    toggleStatsWindow();
                    return;
                }

                const settingsOverlay = document.getElementById('settingsOverlay');
                if (settingsOverlay && !settingsOverlay.classList.contains('hidden')) {
                    closeSettings();
                    return;
                }

                const modalPairs = [
                    ['nameModal', closeNameModal],
                    ['passwordModal', closePasswordModal],
                    ['confirmModal', closeCustomConfirm],
                    ['kickModal', closeKickModal],
                    ['cropModal', closeCropModal]
                ];
                for (const [id, closer] of modalPairs) {
                    const el = document.getElementById(id);
                    if (el && el.classList.contains('open')) {
                        closer();
                        return;
                    }
                }
                return;
            }

            // In-call keyboard shortcuts (M / D / C). Guarded so they never
            // fire while typing, while a modifier is held, or outside a call.
            if (typing || e.ctrlKey || e.metaKey || e.altKey || e.repeat) return;

            const appLayout = document.getElementById('appLayout');
            if (!appLayout || appLayout.classList.contains('hidden')) return;

            switch (e.key.toLowerCase()) {
                case 'm':
                    e.preventDefault();
                    if (typeof toggleMic === 'function') toggleMic();
                    break;
                case 'd':
                    e.preventDefault();
                    if (typeof toggleDeafen === 'function') toggleDeafen();
                    break;
                case 'c': {
                    const camBtn = document.getElementById('btnCam');
                    if (camBtn && !camBtn.disabled) {
                        e.preventDefault();
                        if (typeof toggleCam === 'function') toggleCam();
                    }
                    break;
                }
            }
        });

        function showKickModal(userId, nickname) {
            const modal = document.getElementById('kickModal');
            const title = document.getElementById('kickTitle');
            const message = document.getElementById('kickMessage');
            const submitBtn = document.getElementById('kickSubmit');

            pendingKickUserId = userId;

            title.textContent = 'Kick User';
            message.textContent = `Are you sure you want to kick "${nickname}" from the room?`;

            submitBtn.onclick = () => {
                if (pendingKickUserId && ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'kick-user',
                        data: { userId: pendingKickUserId }
                    }));
                }
                closeKickModal();
            };

            modal.classList.add('open');
        }

        function closeKickModal() {
            animateModalClose('kickModal');
            pendingKickUserId = null;
        }

        let roomDragState = {
            draggedRid: null
        };

        function handleRoomDragStart(e, rid) {
            roomDragState.draggedRid = rid;
            e.dataTransfer.effectAllowed = 'move';
            e.target.closest('.room-item').classList.add('opacity-50');
        }

        function handleRoomDragEnd(e) {
            e.target.closest('.room-item').classList.remove('opacity-50');
            document.querySelectorAll('.room-item').forEach(el => el.classList.remove('border-t-2', 'border-indigo-500'));
        }

        function handleRoomDragOver(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            const roomItem = e.target.closest('.room-item');
            if (roomItem && roomItem.dataset.rid !== roomDragState.draggedRid) {
                roomItem.classList.add('border-t-2', 'border-indigo-500');
            }
        }

        function handleRoomDragLeave(e) {
            const roomItem = e.target.closest('.room-item');
            if (roomItem) {
                roomItem.classList.remove('border-t-2', 'border-indigo-500');
            }
        }

        function handleRoomDrop(e, targetRid) {
            e.preventDefault();
            const draggedRid = roomDragState.draggedRid;
            if (draggedRid === targetRid) return;

            let order = getStoredRoomOrder();
            const currentRids = Object.keys(globalRoomList);
            if (order.length === 0) order = currentRids.sort();

            const fromIndex = order.indexOf(draggedRid);
            const toIndex = order.indexOf(targetRid);

            if (fromIndex !== -1 && toIndex !== -1) {
                order.splice(fromIndex, 1);
                order.splice(toIndex, 0, draggedRid);
                localStorage.setItem('rustrooms_room_order_' + roomId, JSON.stringify(order));
                updateRoomListUI();
            }
        }

        function getStoredRoomOrder() {
            try {
                const value = JSON.parse(localStorage.getItem('rustrooms_room_order_' + roomId) || '[]');
                return Array.isArray(value) ? value.filter(item => typeof item === 'string') : [];
            } catch (error) {
                console.warn('Ignoring invalid saved channel order:', error);
                return [];
            }
        }

        async function createNewChannel() {
            if (typeof roomAdminState !== 'undefined' && roomAdminState.kind === 'stage' && !iAmAdmin()) {
                showCustomAlert("Admins only", "Only admins can create channels in a stage room.");
                return;
            }
            showNameModal("Create New Channel", "Enter channel name", (name) => {
                if (!name) return;
                if ([...name].length > 32) {
                    showCustomAlert("Name Too Long", "Channel names can be at most 32 characters.");
                    return;
                }
                if (/[\\/]/.test(name) || name === '.' || name === '..') {
                    showCustomAlert("Invalid Name", "Channel names cannot contain slashes or be a dot segment.");
                    return;
                }
                performChannelSwitch(roomId, name);
            });
        }

        async function performChannelSwitch(newRoomId, newChannelId) {
            newChannelId = newChannelId.trim();
            if (newChannelId && newChannelId.toLowerCase() === 'general') {
                newChannelId = 'General';
            }
            if ([...newChannelId].length > 32) {
                newChannelId = [...newChannelId].slice(0, 32).join('');
            }
            if (!newChannelId || newChannelId === '.' || newChannelId === '..') {
                showCustomAlert("Invalid Name", "Please choose a valid channel name.");
                return;
            }

            if (ws) {
                ws.onclose = null;
                ws.onerror = null;
                ws.close();

                await new Promise(resolve => setTimeout(resolve, 200));
            }
            stopHeartbeat();

            for (const userId in peers) {
                removePeer(userId);
            }
            peers = {};
            peerCamStatus = {};
            peerScreenStatus = {};
            peerScreenHasAudio = {};
            peerMicTrackId = {};
            peerScreenAudioTrackId = {};
            remoteGrid.innerHTML = '';

            roomId = newRoomId;
            channelId = newChannelId;
            sessionStorage.setItem('rustrooms_last_room_id', roomId);
            sessionStorage.setItem('rustrooms_last_channel_id', channelId);

            const channelNameEl = document.getElementById('currentChannelName');
            if (channelNameEl) {
                channelNameEl.innerText = `# ${channelId}`;
            }
            if (typeof updatePageTitle === 'function') updatePageTitle();

            const newUrl = `/${encodeURIComponent(roomId)}${channelId && channelId.toLowerCase() !== 'general' ? '/' + encodeURIComponent(channelId) : ''}`;
            if (window.location.pathname !== newUrl) {
                history.pushState({ roomId, channelId }, "", newUrl);
            }

            wsUrl = `${wsProtocol}//${window.location.host}/ws/${encodeURIComponent(roomId)}/${encodeURIComponent(channelId)}`;
            updateStatus('connecting', 'Connecting...');

            if (typeof updateRoomListUI === 'function') updateRoomListUI();

            reconnectionAttempts = 0;
            isReconnecting = false;
            connectWs();
            recordRecentCall();
        }

        function switchChannel(newChannelId) {
            if (newChannelId === channelId) return;
            performChannelSwitch(roomId, newChannelId);
        }

        window.onpopstate = function(event) {
            const parts = window.location.pathname.split('/').filter(p => p !== '');
            const newRoomId = decodePathSegment(parts[0] || '');
            const newChannelId = decodePathSegment(parts[1] || '').trim() || (newRoomId ? 'General' : '');

            if (newRoomId && (newRoomId !== roomId || newChannelId !== channelId)) {
                performChannelSwitch(newRoomId, newChannelId);
            } else if (!newRoomId) {
                window.location.reload();
            }
        };

        function renameRoom(targetRoomId, event) {
            if (event) event.stopPropagation();

            if (targetRoomId.toLowerCase() === 'general') {
                showCustomAlert("Action Not Allowed", "Cannot rename the General room.");
                return;
            }

            const roomData = globalRoomList[targetRoomId];
            if (roomData && roomData.users && Object.keys(roomData.users).length > 0) {
                showCustomAlert("Room Not Empty", "You cannot rename a room that still has users in it.");
                return;
            }

            showNameModal("Rename Channel", "Enter new name", (newName) => {
                if (!newName) return;
                if ([...newName].length > 32) {
                    showCustomAlert("Name Too Long", "Channel names can be at most 32 characters.");
                    return;
                }
                if (/[\\/]/.test(newName) || newName === '.' || newName === '..') {
                    showCustomAlert("Invalid Name", "Channel names cannot contain slashes or be a dot segment.");
                    return;
                }
                const normalizedNewName = newName.toLowerCase() === 'general' ? 'General' : newName;
                if (globalRoomList[normalizedNewName]) {
                    showCustomAlert("Channel Exists", `A channel named "${normalizedNewName}" already exists.`);
                    return;
                }
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'rename-channel',
                        data: { channelId: targetRoomId, newName: normalizedNewName }
                    }));
                }
            });
        }

        function deleteRoom(targetRoomId, event) {
            if (event) event.stopPropagation();

            if (targetRoomId.toLowerCase() === 'general') {
                showCustomAlert("Action Not Allowed", "Cannot delete the General room.");
                return;
            }

            const roomData = globalRoomList[targetRoomId];
            if (roomData && roomData.users && Object.keys(roomData.users).length > 0) {
                showCustomAlert("Room Not Empty", "You cannot delete a room that still has users in it.");
                return;
            }

            showCustomConfirm("Delete Channel", `Delete "${targetRoomId}"? This cannot be undone.`, () => {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'delete-channel',
                        data: { channelId: targetRoomId }
                    }));
                }
            });
        }

        function updateRoomListUI() {
            const container = document.getElementById('roomListContainer');
            if (!container) return;

            container.innerHTML = '';

            let order = getStoredRoomOrder();
            const currentRids = Object.keys(globalRoomList);

            order = order.filter(rid => currentRids.includes(rid));
            currentRids.forEach(rid => {
                if (!order.includes(rid)) order.push(rid);
            });

            order.forEach(rid => {
                const roomInfo = globalRoomList[rid];
                if (!roomInfo) return;
                const isActive = (rid === channelId);

                const roomEl = document.createElement('div');
                roomEl.className = `room-item ${isActive ? 'active' : ''}`;
                roomEl.draggable = true;
                roomEl.dataset.rid = rid;

                roomEl.onclick = () => switchChannel(rid);

                roomEl.ondragstart = (e) => handleRoomDragStart(e, rid);
                roomEl.ondragend = (e) => handleRoomDragEnd(e);
                roomEl.ondragover = (e) => handleRoomDragOver(e);
                roomEl.ondragleave = (e) => handleRoomDragLeave(e);
                roomEl.ondrop = (e) => handleRoomDrop(e, rid);

                let stageRowsHtml = '';
                let audienceRowsHtml = '';
                const crownSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M12 3l4.6 6.2L21 5l-1.7 11H4.7L3 5l4.4 4.2z"/><rect x="4.7" y="18" width="14.6" height="2.2" rx="1"/></svg>`;
                const earSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 15v-2a8 8 0 0 1 16 0v2"/><rect x="3" y="14" width="4" height="6" rx="1.5"/><rect x="17" y="14" width="4" height="6" rx="1.5"/></svg>`;
                const users = roomInfo.users || {};
                const userIds = Object.keys(users);

                userIds.forEach(uid => {
                    const u = users[uid];
                    const isMuted = u.isMuted;
                    const isDeafened = u.isDeafened;
                    const isScreenSharing = u.isScreenSharing === true;
                    // room-list no longer carries avatar payloads; fall back to
                    // the cache populated from existing-users / user-joined /
                    // identify / user-update events.
                    const cached = userAvatarCache[uid];
                    let avatar = u.avatar || (cached ? cached.avatar : null) || null;
                    let isGif = !!(u.isGif || (cached ? cached.isGif : false));
                    let staticFrame = u.staticFrame || (cached ? cached.staticFrame : null) || null;
                    // The room-list strips avatar payloads and self is never in
                    // the peer cache (existing-users skips self), so use the
                    // live local profile for the local user's own row.
                    if (uid === persistentUserId) {
                        avatar = userAvatar;
                        isGif = userAvatarIsGif;
                        staticFrame = userAvatarStaticFrame;
                    }

                    const rowHtml = `
                        <div class="room-user-row pointer-events-auto cursor-pointer ${roomAdminState.kind === 'stage' && !canSpeakId(uid) ? 'is-audience' : ''}" data-user-id="${uid}" data-user-nickname="${escapeHtml(u.nickname)}" ${iAmAdmin() ? `draggable="true" onmouseenter="this.closest('.room-item').draggable=false" onmouseleave="this.closest('.room-item').draggable=true" ondragstart="handleUserDragStart(event, '${uid}')" ondragend="handleUserDragEnd(event)"` : ''} onclick="handleUserClick(this)" oncontextmenu="handleUserContextMenu(event, this)" ontouchstart="handleUserTouchStart(event, this)" ontouchend="handleUserTouchEnd(event)" ontouchmove="handleUserTouchCancel()">
                            <div class="user-avatar-wrap">
                                <div class="mini-avatar">
                                    ${avatar ? (isGif && staticFrame ? `<img src="${escapeHtml(staticFrame)}" data-gif-src="${escapeHtml(avatar)}" data-static-src="${escapeHtml(staticFrame)}">` : `<img src="${escapeHtml(avatar)}">`) : `<div class="mini-avatar-placeholder">${escapeHtml(u.nickname.charAt(0).toUpperCase())}</div>`}
                                </div>
                            </div>
                            <span class="room-user-name">${escapeHtml(u.nickname)}</span>
                            ${isCreatorId(uid) ? `<span class="admin-crown creator" title="Room creator · Admin">${crownSvg}</span>` : isAdminId(uid) ? `<span class="admin-crown" title="Admin">${crownSvg}</span>` : ''}
                            ${roomAdminState.kind === 'stage' && !canSpeakId(uid) ? `<span class="audience-badge" title="Audience · listening only">${earSvg}</span>` : ''}
                            ${u.isMobile ? `<span class="device-inline" title="Connected from a mobile device"><svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><rect x="7" y="2" width="10" height="20" rx="2"></rect><line x1="11" y1="18" x2="13" y2="18"></line></svg></span>` : ''}
                            <div class="status-indicators">
                                ${isScreenSharing ? `
                                    <div class="status-icon active" style="color: #10b981;" title="Screen Sharing">
                                        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"></rect><line x1="8" y1="21" x2="16" y2="21"></line><line x1="12" y1="17" x2="12" y2="21"></line></svg>
                                    </div>
                                ` : ''}
                                ${isMuted || isDeafened ? `
                                    <div class="status-icon active" title="${isDeafened ? 'Deafened' : 'Muted'}">
                                        ${isDeafened ? `
                                            <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 14a2 2 0 0 0-2-2h-3a2 2 0 0 0-2 2v3a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2V14z"></path><path d="M3 14a2 2 0 0 1 2-2h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V14z"></path><path d="M20.4 10.4C20.2 6.5 17 3.5 13 3.1"></path><path d="M6.5 5.5A9 9 0 0 0 3 12"></path></svg>
                                        ` : `
                                            <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path></svg>
                                        `}
                                    </div>
                                ` : ''}
                                ${u.isLowBandwidthMode ? `
                                    <div class="status-icon active animate-pulse" style="color: #f59e0b;" title="Low Bandwidth Mode Active">
                                        <svg class="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 24 24"><path d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
                                    </div>
                                ` : ''}
                            </div>
                        </div>
                    `;
                    if (roomAdminState.kind === 'stage' && !canSpeakId(uid)) {
                        audienceRowsHtml += rowHtml;
                    } else {
                        stageRowsHtml += rowHtml;
                    }
                });

                roomEl.innerHTML = `
                    <div class="room-name pointer-events-none">
                        <span class="truncate pr-2">${escapeHtml(roomInfo.name)}</span>
                        <div class="flex items-center gap-2">
                             <span class="channel-timer text-[10px] text-zinc-500 font-medium" data-created-at="${roomInfo.created_at || 0}">
                                ${formatDuration(roomInfo.created_at)}
                             </span>
                             <div class="user-count">${userIds.length}</div>
                             ${rid.toLowerCase() !== 'general' ? `
                                <div class="flex gap-1 pointer-events-auto">
                                    <button onclick="renameRoom(this.closest('.room-item').dataset.rid, event)" class="p-1 text-zinc-500 hover:text-indigo-500 transition-colors" title="Rename Channel">
                                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path></svg>
                                    </button>
                                    <button onclick="deleteRoom(this.closest('.room-item').dataset.rid, event)" class="p-1 text-zinc-500 hover:text-red-500 transition-colors" title="Delete Channel">
                                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path><line x1="10" y1="11" x2="10" y2="17"></line><line x1="14" y1="11" x2="14" y2="17"></line></svg>
                                    </button>
                                </div>
                             ` : ''}
                        </div>
                    </div>
                    <div class="room-users flex flex-col gap-1 mt-2" ondragover="handleChannelDragOver(event)" ondragleave="handleChannelDragLeave(event)" ondrop="handleUserDropOnChannel(event, '${rid}')">
                        ${roomAdminState.kind === 'stage' ? stageSectionsHtml(stageRowsHtml, audienceRowsHtml, rid) : stageRowsHtml}
                        ${userIds.length === 0 && roomAdminState.kind !== 'stage' ? '<span class="text-[10px] text-zinc-600 italic px-2">Empty</span>' : ''}
                    </div>
                `;

                container.appendChild(roomEl);
            });

            // Friendly empty state for a freshly created account with no channels.
            if (container.children.length === 0) {
                const empty = document.createElement('div');
                empty.className = 'sidebar-empty';
                empty.innerHTML = `
                    <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" opacity="0.5"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
                    <span>No channels yet.<br>Create one to start a call.</span>
                `;
                container.appendChild(empty);
            }
        }

        // ---- Drag & drop user moves (admins) ----
        let draggedUserId = null;

        function handleUserDragStart(e, uid) {
            draggedUserId = uid;
            e.stopPropagation();
            // The room item must never compete as a drag source for a user
            // drag (it is draggable itself for channel reordering).
            const item = e.target.closest('.room-item');
            if (item) item.draggable = false;
            if (e.dataTransfer) {
                try { e.dataTransfer.setData('text/plain', uid); } catch (err) {}
                e.dataTransfer.effectAllowed = 'move';
            }
        }

        function handleUserDragEnd() {
            draggedUserId = null;
            document.querySelectorAll('.drop-hover').forEach(el => el.classList.remove('drop-hover'));
            document.querySelectorAll('.room-item').forEach(el => { el.draggable = true; });
        }

        function handleStageDragOver(e) {
            if (!draggedUserId) return;
            e.preventDefault();
            e.stopPropagation();
            e.currentTarget.classList.add('drop-hover');
        }

        function handleStageDragLeave(e) {
            e.currentTarget.classList.remove('drop-hover');
        }

        function handleUserDropOnStage(e, rid) {
            e.preventDefault();
            e.stopPropagation();
            e.currentTarget.classList.remove('drop-hover');
            const uid = draggedUserId;
            draggedUserId = null;
            if (!uid) return;
            if (rid !== channelId) {
                moveUserToChannel(uid, rid);
                return;
            }
            if (e.currentTarget.dataset.drop === 'stage') {
                if (!canSpeakId(uid)) sendAdminAction('to-stage', uid);
            } else if (canSpeakId(uid) && !isAdminId(uid)) {
                sendAdminAction('to-audience', uid);
            }
        }

        function handleChannelDragOver(e) {
            if (!draggedUserId) return;
            e.preventDefault();
            e.stopPropagation();
            const item = e.currentTarget.closest('.room-item');
            if (item) item.classList.add('drop-hover');
        }

        function handleChannelDragLeave(e) {
            const item = e.currentTarget.closest('.room-item');
            if (item) item.classList.remove('drop-hover');
        }

        function handleUserDropOnChannel(e, rid) {
            e.preventDefault();
            e.stopPropagation();
            const item = e.currentTarget.closest('.room-item');
            if (item) item.classList.remove('drop-hover');
            const uid = draggedUserId;
            draggedUserId = null;
            if (uid && rid !== channelId) moveUserToChannel(uid, rid);
        }

        function moveUserToChannel(uid, destChannel) {
            if (uid === persistentUserId) {
                switchChannel(destChannel);
                return;
            }
            sendAdminAction('move', uid, { channelId: destChannel });
        }

        function stageSectionsHtml(stageRows, audienceRows, rid) {
            return `
                <div class="stage-group pointer-events-auto" data-drop="stage" ondragover="handleStageDragOver(event)" ondragleave="handleStageDragLeave(event)" ondrop="handleUserDropOnStage(event, '${rid}')">
                    <div class="stage-group-label">On stage</div>
                    ${stageRows || '<div class="stage-empty">No one on stage</div>'}
                </div>
                <div class="stage-divider"></div>
                <div class="stage-group pointer-events-auto" data-drop="audience" ondragover="handleStageDragOver(event)" ondragleave="handleStageDragLeave(event)" ondrop="handleUserDropOnStage(event, '${rid}')">
                    <div class="stage-group-label">Listeners</div>
                    ${audienceRows || '<div class="stage-empty">No listeners</div>'}
                </div>`;
        }

        async function createRoom() {
            try {
                const res = await fetch('/new');
                if (res.status === 401) {
                    const btn = document.getElementById('btnStartRoom');
                    const pw = document.getElementById('passwordInputContainer');
                    const input = document.getElementById('roomPasswordInput');

                    btn.classList.add('opacity-0', 'pointer-events-none', 'scale-90');
                    pw.classList.remove('opacity-0', 'pointer-events-none', 'translate-y-4');
                    pw.classList.add('translate-y-0');

                    setTimeout(() => input.focus(), 100);
                } else if (res.ok) {
                    sessionStorage.setItem('rustrooms_welcomed', 'true');
                    window.location.href = `/${crypto.randomUUID()}`;
                } else {
                    showCustomAlert("Error", "Error creating room");
                }
            } catch (e) {
                console.error(e);
                showCustomAlert("Error", "Error creating room");
            }
        }

        async function submitPassword() {
            const input = document.getElementById('roomPasswordInput');
            const password = input.value;
            if (!password) return;

            try {
                sessionStorage.setItem('rustrooms_room_password', password);

                const res = await fetch('/new?password=' + encodeURIComponent(password));
                 if (res.ok) {
                     sessionStorage.setItem('rustrooms_welcomed', 'true');
                     window.location.href = `/${crypto.randomUUID()}`;
                 } else if (res.status === 401) {
                     sessionStorage.removeItem('rustrooms_room_password');
                     input.classList.add('ring-2', 'ring-red-500', 'border-red-500', 'shake-x');
                     setTimeout(() => input.classList.remove('ring-2', 'ring-red-500', 'border-red-500', 'shake-x'), 550);
                     input.value = '';
                     input.placeholder = "Incorrect Password";
                 } else {
                     sessionStorage.removeItem('rustrooms_room_password');
                     showCustomAlert("Error", "Error creating room");
                 }
            } catch (e) {
                console.error(e);
                sessionStorage.removeItem('rustrooms_room_password');
                showCustomAlert("Error", "Error creating room");
            }
        }

        function proceedToSetup() {
            sessionStorage.setItem('rustrooms_welcomed', 'true');
            const inviteOverlay = document.getElementById('inviteWelcomeOverlay');
            const invitePanel = inviteOverlay.querySelector('.glass-panel');
            if (invitePanel) invitePanel.classList.add('panel-pop-away');
            inviteOverlay.classList.remove('overlay-enter');
            inviteOverlay.classList.add('opacity-0', 'overlay-exit');
            setTimeout(() => {
                inviteOverlay.classList.add('hidden');
                inviteOverlay.classList.remove('opacity-0', 'overlay-exit');
                if (invitePanel) invitePanel.classList.remove('panel-pop-away');
                showOverlayAnimated(configOverlay);
                initSetupButtonTouchHandlers();
                loadDevices();
            }, OVERLAY_MS);
        }

        async function updateInviteOverlay() {
            if (!roomId || !channelId) return;
            if (sessionStorage.getItem('rustrooms_welcomed') === 'true') return;
            
            try {
                const res = await fetch(`/${encodeURIComponent(roomId)}/${encodeURIComponent(channelId)}/status`);
                if (!res.ok) return;
                
                const data = await res.json();
                
                const uids = Object.keys(data.users);
                if (uids.length === 0) {
                    sessionStorage.setItem('rustrooms_welcomed', 'true');
                    showOverlayAnimated(configOverlay);
                    initSetupButtonTouchHandlers();
                    loadDevices();
                    return;
                }
                
                document.getElementById('inviteChannelName').innerText = `# ${data.name}`;

                const isStage = data.kind === 'stage';
                document.getElementById('inviteKindBadge').innerText = isStage ? 'Live Stage' : 'Live Call';
                document.getElementById('inviteStageNote').classList.toggle('hidden', !isStage);
                document.getElementById('inviteJoinLabel').innerText = isStage ? 'Join as Listener' : 'Join Conversation';
                
                const userList = document.getElementById('inviteUserList');
                userList.innerHTML = '';
                
                uids.forEach(uid => {
                    const u = data.users[uid];
                    const userDiv = document.createElement('div');
                    userDiv.className = 'flex flex-col items-center gap-2 p-3 rounded-2xl bg-zinc-900/50 border border-zinc-800 min-w-[100px]';
                    userDiv.innerHTML = `
                        <div class="w-12 h-12 rounded-xl overflow-hidden bg-zinc-800 border border-zinc-700">
                            ${u.avatar ? `<img src="${escapeHtml(u.staticFrame || u.avatar)}" class="w-full h-full object-cover">` : `<div class="w-full h-full flex items-center justify-center"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" width="20" height="20"><circle cx="12" cy="8" r="4"/><path d="M4 21c0-4 3.6-6.5 8-6.5s8 2.5 8 6.5"/></svg></div>`}
                        </div>
                        <span class="text-xs font-semibold text-zinc-300 truncate max-w-[80px]">${escapeHtml(u.nickname)}</span>
                    `;
                    userList.appendChild(userDiv);
                });
                
                if (data.created_at > 0) {
                    const updateDuration = () => {
                        const el = document.getElementById('inviteCallDuration');
                        if (el) el.innerText = `Running for ${formatDuration(data.created_at)}`;
                    };
                    updateDuration();
                    setInterval(updateDuration, 1000);
                }
                
                const inviteOverlay = document.getElementById('inviteWelcomeOverlay');
                showOverlayAnimated(inviteOverlay);
                
            } catch (e) {
                console.error("Error fetching status", e);
                // Fallback to setup screen
                showOverlayAnimated(configOverlay);
            }
        }

        function formatDuration(createdAt) {
            if (!createdAt) return "0:00";
            const now = Math.floor(Date.now() / 1000);
            const diff = Math.max(0, now - createdAt);
            const h = Math.floor(diff / 3600);
            const m = Math.floor((diff % 3600) / 60);
            const s = diff % 60;
            
            if (h > 0) {
                return `${h}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
            }
            return `${m}:${s.toString().padStart(2, '0')}`;
        }

        setInterval(() => {
            document.querySelectorAll('.channel-timer').forEach(el => {
                const createdAt = parseInt(el.dataset.createdAt);
                if (createdAt) {
                    el.innerText = formatDuration(createdAt);
                }
            });
        }, 1000);

        if (roomId) {
            loadPreferences().then(() => {
                const setupDone = sessionStorage.getItem('rustrooms_setup_done') === 'true';
                const welcomed = sessionStorage.getItem('rustrooms_welcomed') === 'true';

                if (setupDone && roomId) {
                    loadDevices().then(() => joinRoom());
                } else if (welcomed) {
                    showOverlayAnimated(configOverlay);
                    initSetupButtonTouchHandlers();
                    loadDevices();
                } else {
                    updateInviteOverlay();
                }
            });
        } else {
            showWelcome();
        }

        function initSetupButtonTouchHandlers() {
            const btnPreviewMic = document.getElementById('btnPreviewMic');
            const btnPreviewCam = document.getElementById('btnPreviewCam');
            const speakerTestButtons = document.querySelectorAll('.btn-icon-test');

            [btnPreviewMic, btnPreviewCam].forEach(btn => {
                if (btn) {
                    btn.addEventListener('touchstart', function() {
                        this.classList.add('is-pressed');
                    }, { passive: true });
                    btn.addEventListener('touchend', function() {
                        this.classList.remove('is-pressed');
                    }, { passive: true });
                    btn.addEventListener('touchcancel', function() {
                        this.classList.remove('is-pressed');
                    }, { passive: true });
                }
            });

            speakerTestButtons.forEach(btn => {
                btn.addEventListener('touchstart', function() {
                    this.classList.add('is-pressed');
                }, { passive: true });
                btn.addEventListener('touchend', function() {
                    this.classList.remove('is-pressed');
                }, { passive: true });
                btn.addEventListener('touchcancel', function() {
                    this.classList.remove('is-pressed');
                }, { passive: true });
            });
        }


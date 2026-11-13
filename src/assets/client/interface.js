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

        // Fade the whole welcome screen in; children (title, subtitle,
        // start button) replay their staggered blur-pop automatically
        // because the overlay goes from display:none to visible.
        function showWelcome() {
            const overlay = document.getElementById('welcomeOverlay');
            if (!overlay) return;
            overlay.style.display = 'flex';
            renderRecentCalls();
            requestAnimationFrame(() => overlay.classList.add('welcome-enter'));
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

            // The pill reports THIS client's connection (its latency figure
            // is the WebSocket RTT). A peer whose browser died says nothing
            // about our own connection — while the socket is up we are
            // Connected, and the peer's departure is handled by the roster
            // and seat expiry, not by crying "Connection Lost" here.
            const wsUp = typeof ws !== 'undefined' && !!ws && ws.readyState === WebSocket.OPEN;
            if (hasConnectedPeers || (peerIds.length > 0 && wsUp)) {
                updateStatus('connected', 'Connected');
            } else if (peerIds.length > 0 && !hasConnectingPeers) {
                updateStatus('disconnected', 'Connection Lost');
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

        function isMobileSlideOverPortrait() {
            const isSlideOver = window.innerWidth < 768;
            const isPortrait = window.innerHeight >= window.innerWidth ||
                (typeof window.matchMedia === 'function' && window.matchMedia('(orientation: portrait)').matches);
            return isSlideOver && isPortrait;
        }

        function applySidebarState(noTransition = false) {
            const isMobilePortrait = isMobileSlideOverPortrait();
            if (isMobilePortrait) {
                try {
                    localStorage.setItem('rustrooms_sidebar_open', 'false');
                } catch (e) {}
            }

            const savedState = localStorage.getItem('rustrooms_sidebar_open');
            const isOpen = savedState === 'true' && !isMobilePortrait;
            const isDesktop = window.innerWidth >= 768;
            const sidebarToggle = document.getElementById('sidebarToggle');
            const sidebar = document.getElementById('roomSidebar');
            const overlay = document.getElementById('sidebarOverlay');
            const body = document.body;

            if (isOpen) {
                if (noTransition && sidebar) {
                    sidebar.style.transition = 'none';
                }
                body.classList.add('sidebar-open');
                if (sidebar) sidebar.classList.add('open');
                if (sidebarToggle) sidebarToggle.classList.add('hidden');

                if (isDesktop) {
                    if (overlay) overlay.classList.remove('open');

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
                    if (overlay) overlay.classList.add('open');
                }

                if (noTransition && sidebar) {
                    sidebar.offsetHeight;
                    setTimeout(() => {
                        sidebar.style.transition = '';
                    }, 50);
                }
            } else {
                if (noTransition && sidebar) {
                    sidebar.style.transition = 'none';
                }
                if (sidebar) sidebar.classList.remove('open');
                if (overlay) overlay.classList.remove('open');
                body.classList.remove('sidebar-open');
                if (sidebarToggle) sidebarToggle.classList.remove('hidden');

                const pip = document.getElementById('localPipWrapper');
                if (pip) {
                    pip.style.left = '';
                    pip.style.right = '';
                    pip.style.bottom = '';
                }

                if (noTransition && sidebar) {
                    sidebar.offsetHeight;
                    setTimeout(() => {
                        sidebar.style.transition = '';
                    }, 50);
                }
            }
        }

        let currentNameModalKeyHandler = null;
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

            if (currentNameModalKeyHandler) {
                modalInput.removeEventListener('keydown', currentNameModalKeyHandler);
                currentNameModalKeyHandler = null;
            }

            modalSubmit.onclick = () => {
                const name = modalInput.value.trim();
                callback(name);
                closeNameModal();
            };

            currentNameModalKeyHandler = (e) => {
                if (e.key === 'Enter') {
                    modalSubmit.click();
                }
            };
            modalInput.addEventListener('keydown', currentNameModalKeyHandler);
        }

        function closeNameModal() {
            const modalInput = document.getElementById('modalInput');
            if (modalInput && currentNameModalKeyHandler) {
                modalInput.removeEventListener('keydown', currentNameModalKeyHandler);
                currentNameModalKeyHandler = null;
            }
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

        let _userTouchState = {
            active: false,
            el: null,
            userId: null,
            nickname: null,
            sourceRid: null,
            startX: 0,
            startY: 0,
            startTime: 0,
            moved: false,
            isHold: false,
            isDragging: false,
            holdTimer: null,
            lastTouchTime: 0
        };
        let _uvmJustOpenedTime = 0;
        let _dragGhostEl = null;

        function showTouchDragGhost(sourceEl, nickname, x, y) {
            hideTouchDragGhost();
            _dragGhostEl = document.createElement('div');
            _dragGhostEl.id = 'touchDragGhost';
            if (sourceEl) {
                const clone = sourceEl.cloneNode(true);
                clone.removeAttribute('id');
                clone.classList.remove('touch-held', 'touch-drag-active', 'user-drag-active');
                _dragGhostEl.appendChild(clone);
                const rect = sourceEl.getBoundingClientRect();
                if (rect.width > 0) {
                    _dragGhostEl.style.width = Math.round(rect.width) + 'px';
                }
            } else {
                _dragGhostEl.innerHTML = `
                    <div class="room-user-row">
                        <div class="user-avatar-wrap">
                            <div class="mini-avatar"><div class="mini-avatar-placeholder">${escapeHtml((nickname || 'U').charAt(0).toUpperCase())}</div></div>
                        </div>
                        <span class="room-user-name">${escapeHtml(nickname || 'User')}</span>
                    </div>
                `;
            }
            _dragGhostEl.style.left = x + 'px';
            _dragGhostEl.style.top = y + 'px';
            document.body.appendChild(_dragGhostEl);
        }

        function updateTouchDragGhost(x, y) {
            if (_dragGhostEl) {
                _dragGhostEl.style.left = x + 'px';
                _dragGhostEl.style.top = y + 'px';
            }
        }

        function hideTouchDragGhost() {
            if (_dragGhostEl) {
                _dragGhostEl.remove();
                _dragGhostEl = null;
            }
        }

        function handleUserClick(el) {
            // Ignore click if a touch drag or menu hold just occurred
            if (_userTouchState.isHold || _userTouchState.isDragging || (Date.now() - _userTouchState.lastTouchTime < 400)) {
                return;
            }

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

        function handleUserContextMenu(e, el) {
            e.preventDefault();
            e.stopPropagation();

            // On touch, the menu is opened on release after holding. Ignore native touch contextmenu.
            if (_userTouchState.active || (Date.now() - _userTouchState.lastTouchTime < 600) || e.pointerType === 'touch') {
                return;
            }

            const userId = el.dataset.userId;
            const nickname = el.dataset.userNickname;
            if (!userId || userId === persistentUserId) return;

            _uvmJustOpenedTime = Date.now();
            showUserVolumeMenu(userId, nickname, e.clientX, e.clientY);
        }

        function handleGlobalTouchMove(e) {
            if (!_userTouchState.active) return;
            const touch = e.touches[0];
            if (!touch) return;

            const dx = touch.clientX - _userTouchState.startX;
            const dy = touch.clientY - _userTouchState.startY;
            const dist = Math.hypot(dx, dy);

            if (dist > 6) {
                _userTouchState.moved = true;

                // If not holding yet, finger moved quickly -> cancel hold timer (scroll gesture)
                if (!_userTouchState.isHold && _userTouchState.holdTimer) {
                    clearTimeout(_userTouchState.holdTimer);
                    _userTouchState.holdTimer = null;
                }

                // If admin is holding and dragging:
                if (typeof iAmAdmin === 'function' && iAmAdmin() && (_userTouchState.isHold || _userTouchState.isDragging)) {
                    _userTouchState.isDragging = true;
                    draggedUserId = _userTouchState.userId;
                    if (e.cancelable) e.preventDefault(); // Stop sidebar scrolling

                    if (_userTouchState.el) {
                        _userTouchState.el.classList.remove('touch-held');
                        _userTouchState.el.classList.add('touch-drag-active');
                    }
                    if (!_dragGhostEl) {
                        showTouchDragGhost(_userTouchState.el, _userTouchState.nickname, touch.clientX, touch.clientY);
                    } else {
                        updateTouchDragGhost(touch.clientX, touch.clientY);
                    }
                    updateTouchDropHover(touch.clientX, touch.clientY);
                }
            }
        }

        function handleGlobalTouchEnd(e) {
            if (!_userTouchState.active) return;

            document.removeEventListener('touchmove', handleGlobalTouchMove);
            document.removeEventListener('touchend', handleGlobalTouchEnd);
            document.removeEventListener('touchcancel', handleGlobalTouchCancel);

            if (_userTouchState.holdTimer) {
                clearTimeout(_userTouchState.holdTimer);
                _userTouchState.holdTimer = null;
            }

            hideTouchDragGhost();

            _userTouchState.lastTouchTime = Date.now();
            const touch = (e.changedTouches && e.changedTouches[0]) || null;
            const clientX = touch ? touch.clientX : _userTouchState.startX;
            const clientY = touch ? touch.clientY : _userTouchState.startY;

            const wasDragging = _userTouchState.isDragging;
            const wasHold = _userTouchState.isHold;
            const wasMoved = _userTouchState.moved;
            const uid = _userTouchState.userId;
            const nickname = _userTouchState.nickname;

            if (_userTouchState.el) {
                _userTouchState.el.classList.remove('touch-held', 'touch-drag-active');
            }
            document.querySelectorAll('.drop-hover').forEach(h => h.classList.remove('drop-hover'));
            document.querySelectorAll('.room-item').forEach(r => {
                r.classList.remove('opacity-50', 'border-t-2', 'border-indigo-500');
            });

            _userTouchState.active = false;
            _userTouchState.isDragging = false;
            _userTouchState.isHold = false;

            // Case 1: Was dragging to move!
            if (wasDragging && typeof iAmAdmin === 'function' && iAmAdmin() && uid) {
                if (e.cancelable) e.preventDefault();
                draggedUserId = null;
                executeTouchDrop(clientX, clientY, uid);
                return;
            }

            // Case 2: Held down in the same place and released -> trigger right-click menu!
            if (wasHold && !wasMoved) {
                if (e.cancelable) e.preventDefault();
                draggedUserId = null;
                if (uid && uid !== persistentUserId) {
                    _uvmJustOpenedTime = Date.now();
                    showUserVolumeMenu(uid, nickname, clientX, clientY);
                }
                return;
            }

            draggedUserId = null;
        }

        function handleGlobalTouchCancel(e) {
            document.removeEventListener('touchmove', handleGlobalTouchMove);
            document.removeEventListener('touchend', handleGlobalTouchEnd);
            document.removeEventListener('touchcancel', handleGlobalTouchCancel);

            if (_userTouchState.holdTimer) {
                clearTimeout(_userTouchState.holdTimer);
                _userTouchState.holdTimer = null;
            }
            hideTouchDragGhost();

            if (_userTouchState.el) {
                _userTouchState.el.classList.remove('touch-held', 'touch-drag-active');
            }
            document.querySelectorAll('.drop-hover').forEach(h => h.classList.remove('drop-hover'));
            document.querySelectorAll('.room-item').forEach(r => {
                r.classList.remove('opacity-50', 'border-t-2', 'border-indigo-500');
            });

            _userTouchState.active = false;
            _userTouchState.moved = true;
            _userTouchState.isDragging = false;
            _userTouchState.isHold = false;
            _userTouchState.lastTouchTime = Date.now();
            draggedUserId = null;
        }

        function handleUserTouchStart(e, el) {
            if (e.touches.length > 1) return;
            const touch = e.touches[0];
            const userId = el.dataset.userId;
            const nickname = el.dataset.userNickname;
            const sourceRid = el.closest('.room-item')?.dataset.rid || channelId;

            if (_userTouchState.holdTimer) {
                clearTimeout(_userTouchState.holdTimer);
            }

            _userTouchState = {
                active: true,
                el: el,
                userId: userId,
                nickname: nickname,
                sourceRid: sourceRid,
                startX: touch.clientX,
                startY: touch.clientY,
                startTime: Date.now(),
                moved: false,
                isHold: false,
                isDragging: false,
                holdTimer: null,
                lastTouchTime: Date.now()
            };

            // Hold timer: 260ms to register as holding down
            _userTouchState.holdTimer = setTimeout(() => {
                if (!_userTouchState.active || _userTouchState.moved) return;
                _userTouchState.isHold = true;
                if (navigator.vibrate) {
                    try { navigator.vibrate(20); } catch (_) {}
                }
                el.classList.add('touch-held');
            }, 260);

            // Bind document touch listeners for smooth dragging outside the element
            document.removeEventListener('touchmove', handleGlobalTouchMove);
            document.removeEventListener('touchend', handleGlobalTouchEnd);
            document.removeEventListener('touchcancel', handleGlobalTouchCancel);

            document.addEventListener('touchmove', handleGlobalTouchMove, { passive: false });
            document.addEventListener('touchend', handleGlobalTouchEnd, { passive: false });
            document.addEventListener('touchcancel', handleGlobalTouchCancel, { passive: false });
        }

        function handleUserTouchMove(e, el) {
            handleGlobalTouchMove(e);
        }

        function handleUserTouchEnd(e, el) {
            handleGlobalTouchEnd(e);
        }

        function handleUserTouchCancel(e, el) {
            handleGlobalTouchCancel(e);
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
                    <button type="button" class="uvm-close">
                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>
                    </button>
                </div>
                <div class="uvm-section">
                    <div class="uvm-label">Mic Volume</div>
                    <div class="uvm-slider-row">
                        <button type="button" id="uvm-mute-main" data-uvm-mute="main" class="${mainMuted ? 'muted' : ''}">
                            ${mainMuted ? volSvgOff : volSvgOn}
                        </button>
                        <input type="range" min="0" max="1" step="0.05" value="${mainVol}" id="uvm-slider-main" data-uvm-slider="main">
                        <span class="uvm-vol-pct" id="uvm-pct-main">${Math.round(mainVol * 100)}%</span>
                    </div>
                </div>
            `;

            if (hasScreen) {
                html += `
                <div class="uvm-section">
                    <div class="uvm-label">Screen Volume</div>
                    <div class="uvm-slider-row">
                        <button type="button" id="uvm-mute-screen" data-uvm-mute="screen" class="${screenMuted ? 'muted' : ''}">
                            ${screenMuted ? screenSvgOff : screenSvgOn}
                        </button>
                        <input type="range" min="0" max="1" step="0.05" value="${screenVol}" id="uvm-slider-screen" data-uvm-slider="screen">
                        <span class="uvm-vol-pct" id="uvm-pct-screen">${Math.round(screenVol * 100)}%</span>
                    </div>
                </div>
                `;
            }

            if (typeof iAmAdmin === 'function' && iAmAdmin() && userId !== persistentUserId) {
                const tCreator = isCreatorId(userId);
                const tAdmin = isAdminId(userId);
                const f = (roomAdminState.forced && roomAdminState.forced[userId]) || {};
                const icons = {
                    'promote': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path></svg>`,
                    'demote': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path><line x1="9" y1="12" x2="15" y2="12"></line></svg>`,
                    'to-stage': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"></polygon></svg>`,
                    'to-audience': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path></svg>`,
                    'mute': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path><line x1="12" y1="19" x2="12" y2="23"></line><line x1="8" y1="23" x2="16" y2="23"></line></svg>`,
                    'unmute': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"></path><path d="M19 10v2a7 7 0 0 1-14 0v-2"></path><line x1="12" y1="19" x2="12" y2="23"></line><line x1="8" y1="23" x2="16" y2="23"></line></svg>`,
                    'deafen': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M3 18v-6a9 9 0 0 1 14.28-7.2"></path><path d="M21 12v6"></path><path d="M21 19a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3zM3 19a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2H3z"></path></svg>`,
                    'undeafen': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 18v-6a9 9 0 0 1 18 0v6"></path><path d="M21 19a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3zM3 19a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2H3z"></path></svg>`,
                    'cam-off': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 16v1a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h11a2 2 0 0 1 2 2v1"></path><path d="M23 7l-7 5 7 5V7z"></path><line x1="1" y1="1" x2="23" y2="23"></line></svg>`,
                    'cam-on': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="23 7 16 12 23 17 23 7"></polygon><rect x="1" y="5" width="15" height="14" rx="2" ry="2"></rect></svg>`,
                    'share-off': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"></rect><line x1="8" y1="21" x2="16" y2="21"></line><line x1="12" y1="17" x2="12" y2="21"></line><line x1="2" y1="3" x2="22" y2="17"></line></svg>`,
                    'share-on': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"></rect><line x1="8" y1="21" x2="16" y2="21"></line><line x1="12" y1="17" x2="12" y2="21"></line></svg>`,
                    'kick': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path><polyline points="16 17 21 12 16 7"></polyline><line x1="21" y1="12" x2="9" y2="12"></line></svg>`,
                    'ban': `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"></line></svg>`
                };
                const abtn = (action, label, danger) => `<button type="button" class="uvm-admin-btn${danger ? ' danger' : ''}" data-admin-action="${escapeHtml(action)}">${icons[action] || ''}<span>${label}</span></button>`;
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
                    adminHtml += `<div class="uvm-divider"></div>`;
                    adminHtml += abtn('kick', 'Kick', true);
                    adminHtml += abtn('ban', 'Ban', true);
                }
                html += `
                <div class="uvm-section">
                    <div class="uvm-label">Admin</div>
                    <div class="uvm-admin-list uvm-admin-grid">${adminHtml}</div>
                </div>`;
            }

            if (!menu.dataset.uvmDelegated) {
                menu.dataset.uvmDelegated = '1';
                menu.addEventListener('click', (e) => {
                    if (e.target.closest('.uvm-close')) {
                        closeUserVolumeMenu();
                        return;
                    }
                    const muteBtn = e.target.closest('[data-uvm-mute]');
                    if (muteBtn && menu.dataset.userId) {
                        uvmToggleMute(menu.dataset.userId, muteBtn.dataset.uvmMute);
                        return;
                    }
                    const adminBtn = e.target.closest('[data-admin-action]');
                    if (adminBtn && menu.dataset.userId) {
                        adminMenuAction(adminBtn.dataset.adminAction, menu.dataset.userId);
                    }
                });
                menu.addEventListener('input', (e) => {
                    if (e.target.matches('[data-uvm-slider]') && menu.dataset.userId) {
                        uvmSetVolume(menu.dataset.userId, e.target.dataset.uvmSlider, e.target.value);
                    }
                });
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

            const row = document.getElementById(`vol-row-${type}-${userId}`);
            const overlaySlider = row ? row.querySelector('input[type=range]') : null;
            if (overlaySlider) overlaySlider.value = val;
        };

        document.addEventListener('mousedown', function(e) {
            if (Date.now() - _uvmJustOpenedTime < 300) return;
            const menu = document.getElementById('userVolumeMenu');
            if (menu && menu.classList.contains('open') && !menu.contains(e.target)) {
                closeUserVolumeMenu();
            }
        });

        document.addEventListener('touchstart', function(e) {
            if (Date.now() - _uvmJustOpenedTime < 300) return;
            const menu = document.getElementById('userVolumeMenu');
            if (menu && menu.classList.contains('open') && !menu.contains(e.target)) {
                closeUserVolumeMenu();
            }
        }, { passive: true });

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
                    ['alertModal', closeCustomAlert],
                    ['createRoomModal', closeCreateRoomMenu],
                    ['nameModal', closeNameModal],
                    ['passwordModal', closePasswordModal],
                    ['confirmModal', closeCustomConfirm],
                    ['kickModal', closeKickModal],
                    ['cropModal', closeCropModal]
                ];
                for (const [id, closer] of modalPairs) {
                    const el = document.getElementById(id);
                    if (el && (id === 'cropModal' ? !el.classList.contains('hidden') : el.classList.contains('open'))) {
                        closer();
                        return;
                    }
                }

                const configOverlay = document.getElementById('configOverlay');
                if (configOverlay && !configOverlay.classList.contains('hidden')) {
                    goBackFromSetup();
                    return;
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
            // Never drag room on mobile touch or if dragging/touching inside user rows or buttons
            if (isMobileClient || _userTouchState.active || e.target.closest('.room-users') || e.target.closest('.room-user-row') || e.target.closest('button')) {
                if (e.preventDefault) e.preventDefault();
                return;
            }
            roomDragState.draggedRid = rid;
            e.dataTransfer.effectAllowed = 'move';
            const item = e.target.closest('.room-item');
            if (item) item.classList.add('opacity-50');
        }

        function handleRoomDragEnd(e) {
            document.querySelectorAll('.room-item').forEach(el => {
                el.classList.remove('opacity-50');
                el.classList.remove('border-t-2', 'border-indigo-500');
            });
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

        let isSwitchingChannel = false;
        async function performChannelSwitch(newRoomId, newChannelId) {
            if (isSwitchingChannel) return;
            isSwitchingChannel = true;
            try {
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

                if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
                if (iosSlowRetryTimer) { clearTimeout(iosSlowRetryTimer); iosSlowRetryTimer = null; }
                if (desktopSlowRetryTimer) { clearTimeout(desktopSlowRetryTimer); desktopSlowRetryTimer = null; }
                if (reconnectStatusTimeout) { clearTimeout(reconnectStatusTimeout); reconnectStatusTimeout = null; }

                if (ws) {
                    if (ws.readyState === WebSocket.OPEN) {
                        try { ws.send(JSON.stringify({ type: 'expedite-departure' })); } catch (e) {}
                    }
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
                peerCamVideoTrackId = {};
                peerScreenVideoTrackId = {};
                pendingCandidates = {};
                activeSpeakers = {};
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
            } finally {
                isSwitchingChannel = false;
            }
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
                roomEl.draggable = !isMobileClient;
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
                    let avatar = safeAvatarSrc(u.avatar || (cached ? cached.avatar : null));
                    let isGif = !!(avatar && (u.isGif || (cached ? cached.isGif : false)));
                    let staticFrame = avatar ? safeAvatarSrc(u.staticFrame || (cached ? cached.staticFrame : null)) : null;
                    // The room-list strips avatar payloads and self is never in
                    // the peer cache (existing-users skips self), so use the
                    // live local profile for the local user's own row.
                    if (uid === persistentUserId) {
                        avatar = safeAvatarSrc(userAvatar);
                        isGif = !!(avatar && userAvatarIsGif);
                        staticFrame = avatar ? safeAvatarSrc(userAvatarStaticFrame) : null;
                    }

                    const safeUid = escapeHtml(uid);
                    const rowHtml = `
                        <div class="room-user-row pointer-events-auto cursor-pointer ${roomAdminState.kind === 'stage' && !canSpeakId(uid) ? 'is-audience' : ''}" data-user-id="${safeUid}" data-user-nickname="${escapeHtml(u.nickname)}" ${iAmAdmin() ? `draggable="true" onmouseenter="this.closest('.room-item').draggable=false" onmouseleave="this.closest('.room-item').draggable=true" ondragstart="handleUserDragStart(event, this.dataset.userId)" ondragend="handleUserDragEnd(event)"` : ''} onclick="handleUserClick(this)" oncontextmenu="handleUserContextMenu(event, this)" ontouchstart="handleUserTouchStart(event, this)" ontouchmove="handleUserTouchMove(event, this)" ontouchend="handleUserTouchEnd(event, this)" ontouchcancel="handleUserTouchCancel(event, this)">
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
                    <div class="room-users flex flex-col gap-1 mt-2" ondragover="handleChannelDragOver(event)" ondragleave="handleChannelDragLeave(event)" ondrop="handleUserDropOnChannel(event, this.closest('.room-item').dataset.rid)">
                        ${roomAdminState.kind === 'stage' ? stageSectionsHtml(stageRowsHtml, audienceRowsHtml) : stageRowsHtml}
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

        function getUserCurrentRoom(uid) {
            for (const r in globalRoomList) {
                if (globalRoomList[r] && globalRoomList[r].users && globalRoomList[r].users[uid]) {
                    return r;
                }
            }
            return channelId;
        }

        function updateTouchDropHover(clientX, clientY) {
            document.querySelectorAll('.drop-hover').forEach(el => el.classList.remove('drop-hover'));
            const elUnder = document.elementFromPoint(clientX, clientY);
            if (!elUnder) return;

            const stageGroup = elUnder.closest('.stage-group');
            if (stageGroup) {
                stageGroup.classList.add('drop-hover');
                return;
            }

            const roomItem = elUnder.closest('.room-item');
            if (roomItem) {
                const targetRid = roomItem.dataset.rid;
                const sourceRid = (_userTouchState && _userTouchState.sourceRid) || getUserCurrentRoom(_userTouchState && _userTouchState.userId);
                if (targetRid && targetRid !== sourceRid) {
                    roomItem.classList.add('drop-hover');
                }
            }
        }

        function executeTouchDrop(clientX, clientY, uid) {
            document.querySelectorAll('.drop-hover').forEach(el => el.classList.remove('drop-hover'));
            if (!uid) return;

            const elUnder = document.elementFromPoint(clientX, clientY);
            if (!elUnder) return;

            const sourceRid = (_userTouchState && _userTouchState.sourceRid) || getUserCurrentRoom(uid);

            const stageGroup = elUnder.closest('.stage-group');
            if (stageGroup) {
                const targetRid = stageGroup.closest('.room-item')?.dataset.rid || channelId;
                if (targetRid !== sourceRid) {
                    moveUserToChannel(uid, targetRid);
                } else if (stageGroup.dataset.drop === 'stage') {
                    if (!canSpeakId(uid)) sendAdminAction('to-stage', uid);
                } else if (canSpeakId(uid)) {
                    sendAdminAction('to-audience', uid);
                }
                return;
            }

            const roomItem = elUnder.closest('.room-item');
            if (roomItem) {
                const targetRid = roomItem.dataset.rid;
                if (targetRid && targetRid !== sourceRid) {
                    moveUserToChannel(uid, targetRid);
                }
            }
        }

        function handleUserDragStart(e, uid) {
            // Never trigger native HTML5 drag on mobile or if touch is active without movement
            if (isMobileClient || (_userTouchState.active && !_userTouchState.moved)) {
                if (e.preventDefault) e.preventDefault();
                return;
            }

            if (_userTouchState.holdTimer) {
                clearTimeout(_userTouchState.holdTimer);
                _userTouchState.holdTimer = null;
            }

            draggedUserId = uid;
            e.stopPropagation();
            // The room item must never compete as a drag source for a user
            // drag (it is draggable itself for channel reordering).
            const item = e.target.closest('.room-item');
            if (item) item.draggable = false;
            if (e.dataTransfer) {
                try { e.dataTransfer.setData('text/plain', uid); } catch (err) {}
                e.dataTransfer.effectAllowed = 'move';

                // Give desktop drag preview the identical bluepurple outline and styling as mobile
                if (typeof e.dataTransfer.setDragImage === 'function') {
                    const targetRow = e.target.closest('.room-user-row') || e.target;
                    const rect = targetRow.getBoundingClientRect();
                    const clone = targetRow.cloneNode(true);
                    clone.id = 'desktopDragGhost';
                    clone.style.position = 'fixed';
                    clone.style.left = '-9999px';
                    clone.style.top = '-9999px';
                    clone.style.width = Math.round(rect.width > 0 ? rect.width : targetRow.offsetWidth) + 'px';
                    clone.style.background = 'var(--md-surface-container-high, #1e1e24)';
                    clone.style.border = '1.5px solid var(--md-primary, #6366f1)';
                    clone.style.borderRadius = 'var(--radius-md, 8px)';
                    clone.style.boxShadow = '0 12px 30px rgba(0, 0, 0, 0.6), 0 0 14px rgba(99, 102, 241, 0.35)';
                    clone.style.boxSizing = 'border-box';
                    clone.style.pointerEvents = 'none';
                    clone.style.zIndex = '9999';
                    document.body.appendChild(clone);

                    const offsetX = (e.clientX && rect.left) ? Math.max(10, Math.min(e.clientX - rect.left, rect.width - 10)) : 20;
                    const offsetY = (e.clientY && rect.top) ? Math.max(8, Math.min(e.clientY - rect.top, rect.height - 8)) : 16;
                    try {
                        e.dataTransfer.setDragImage(clone, offsetX, offsetY);
                    } catch (err) {}
                    setTimeout(() => {
                        if (clone.parentNode) clone.parentNode.removeChild(clone);
                    }, 0);
                }
            }

            const targetRow = e.target.closest('.room-user-row') || e.target;
            if (targetRow && targetRow.classList) {
                targetRow.classList.add('user-drag-active');
            }
        }

        function handleUserDragEnd() {
            draggedUserId = null;
            hideTouchDragGhost();
            if (_userTouchState.el) {
                _userTouchState.el.classList.remove('touch-held', 'touch-drag-active');
            }
            document.querySelectorAll('.room-user-row.user-drag-active').forEach(el => el.classList.remove('user-drag-active'));
            document.querySelectorAll('.drop-hover').forEach(el => el.classList.remove('drop-hover'));
            document.querySelectorAll('.room-item').forEach(el => {
                el.classList.remove('opacity-50', 'border-t-2', 'border-indigo-500');
                el.draggable = !isMobileClient;
            });
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
            const sourceRid = getUserCurrentRoom(uid);
            if (rid !== sourceRid) {
                moveUserToChannel(uid, rid);
                return;
            }
            if (e.currentTarget.dataset.drop === 'stage') {
                if (!canSpeakId(uid)) sendAdminAction('to-stage', uid);
            } else if (canSpeakId(uid)) {
                sendAdminAction('to-audience', uid);
            }
        }

        function handleChannelDragOver(e) {
            if (!draggedUserId) return;
            e.preventDefault();
            e.stopPropagation();
            const item = e.currentTarget.closest('.room-item');
            if (item) {
                const targetRid = item.dataset.rid;
                const sourceRid = getUserCurrentRoom(draggedUserId);
                if (targetRid && targetRid !== sourceRid) {
                    item.classList.add('drop-hover');
                }
            }
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
            const sourceRid = getUserCurrentRoom(uid);
            if (uid && rid && rid !== sourceRid) moveUserToChannel(uid, rid);
        }

        function moveUserToChannel(uid, destChannel) {
            if (uid === persistentUserId) {
                switchChannel(destChannel);
                return;
            }
            sendAdminAction('move', uid, { channelId: destChannel });
        }

        function stageSectionsHtml(stageRows, audienceRows) {
            return `
                <div class="stage-group pointer-events-auto" data-drop="stage" ondragover="handleStageDragOver(event)" ondragleave="handleStageDragLeave(event)" ondrop="handleUserDropOnStage(event, this.closest('.room-item').dataset.rid)">
                    <div class="stage-group-label">On stage</div>
                    ${stageRows || '<div class="stage-empty">Nobody is on the stage right now</div>'}
                </div>
                <div class="stage-divider"></div>
                <div class="stage-group pointer-events-auto" data-drop="audience" ondragover="handleStageDragOver(event)" ondragleave="handleStageDragLeave(event)" ondrop="handleUserDropOnStage(event, this.closest('.room-item').dataset.rid)">
                    <div class="stage-group-label">Listeners</div>
                    ${audienceRows || '<div class="stage-empty">No listeners right now</div>'}
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
                    if (res.redirected && res.url) {
                        window.location.href = res.url;
                    } else {
                        window.location.href = `/${crypto.randomUUID()}`;
                    }
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
                     if (res.redirected && res.url) {
                         window.location.href = res.url;
                     } else {
                         window.location.href = `/${crypto.randomUUID()}`;
                     }
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

        let inviteDurationTimer = null;
        let cameFromInviteOverlay = false;

        function proceedToSetup() {
            cameFromInviteOverlay = true;
            if (inviteDurationTimer) {
                clearInterval(inviteDurationTimer);
                inviteDurationTimer = null;
            }
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

        function goBackFromSetup() {
            if (typeof previewVideo !== 'undefined' && previewVideo) {
                previewVideo.srcObject = null;
            }
            if (typeof setupMeterFrameId !== 'undefined' && setupMeterFrameId) {
                cancelAnimationFrame(setupMeterFrameId);
                setupMeterFrameId = null;
            }
            if (typeof localStream !== 'undefined' && localStream) {
                localStream.getTracks().forEach(track => track.stop());
                localStream = null;
            }

            const inviteOverlay = document.getElementById('inviteWelcomeOverlay');
            const userList = document.getElementById('inviteUserList');
            const stageList = document.getElementById('inviteStageList');
            const hasInviteUsers = (userList && userList.children.length > 0) || (stageList && stageList.children.length > 0);

            if (cameFromInviteOverlay || hasInviteUsers) {
                sessionStorage.removeItem('rustrooms_welcomed');
                cameFromInviteOverlay = false;
                const configPanel = document.getElementById('configPanel');
                if (configPanel) configPanel.classList.add('panel-pop-away');
                configOverlay.classList.remove('overlay-enter');
                configOverlay.classList.add('opacity-0', 'overlay-exit');
                setTimeout(() => {
                    configOverlay.classList.add('hidden');
                    configOverlay.classList.remove('opacity-0', 'overlay-exit');
                    if (configPanel) configPanel.classList.remove('panel-pop-away');
                    showOverlayAnimated(inviteOverlay);
                    if (typeof updateInviteOverlay === 'function') {
                        updateInviteOverlay();
                    }
                }, OVERLAY_MS);
            } else {
                if (window.history.length > 1 && document.referrer && document.referrer.includes(window.location.host) && !document.referrer.includes(window.location.pathname)) {
                    window.history.back();
                } else {
                    window.location.href = '/';
                }
            }
        }
        window.goBackFromSetup = goBackFromSetup;

        async function updateInviteOverlay() {
            if (!roomId || !channelId) return;
            if (sessionStorage.getItem('rustrooms_welcomed') === 'true') return;
            
            try {
                const res = await fetch(`/${encodeURIComponent(roomId)}/${encodeURIComponent(channelId)}/status`);
                if (!res.ok) return;
                
                const data = await res.json();
                
                const uids = Object.keys(data.users);
                if (uids.length === 0) {
                    if (typeof getCreateOpts === 'function' && getCreateOpts()) {
                        sessionStorage.setItem('rustrooms_welcomed', 'true');
                        showOverlayAnimated(configOverlay);
                        initSetupButtonTouchHandlers();
                        loadDevices();
                    } else if (typeof openCreateRoomMenu === 'function') {
                        openCreateRoomMenu();
                    }
                    return;
                }
                
                document.getElementById('inviteChannelName').innerText = `# ${data.name}`;

                const isStage = data.kind === 'stage';
                const isLobby = !!data.lobby;
                document.getElementById('inviteKindBadge').innerText = isStage ? 'Live Stage' : 'Live Call';
                document.getElementById('inviteStageNote').classList.toggle('hidden', !isStage);
                const inviteLobbyBadge = document.getElementById('inviteLobbyBadge');
                if (inviteLobbyBadge) {
                    inviteLobbyBadge.style.display = isLobby ? 'inline-flex' : 'none';
                }
                const inviteLobbyNote = document.getElementById('inviteLobbyNote');
                if (inviteLobbyNote) {
                    inviteLobbyNote.classList.toggle('hidden', !isLobby);
                }
                document.getElementById('inviteJoinLabel').innerText = isLobby ? 'Request to Join' : (isStage ? 'Join as Listener' : 'Join Conversation');
                
                const buildInviteUserCard = (u) => {
                    const userDiv = document.createElement('div');
                    userDiv.className = 'flex flex-col items-center gap-2 p-3 rounded-2xl bg-zinc-900/50 border border-zinc-800 min-w-[100px]';
                    userDiv.innerHTML = `
                        <div class="w-12 h-12 rounded-xl overflow-hidden bg-zinc-800 border border-zinc-700">
                            ${u.avatar ? `<img src="${escapeHtml(u.staticFrame || u.avatar)}" class="w-full h-full object-cover">` : `<div class="w-full h-full flex items-center justify-center"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" width="20" height="20"><circle cx="12" cy="8" r="4"/><path d="M4 21c0-4 3.6-6.5 8-6.5s8 2.5 8 6.5"/></svg></div>`}
                        </div>
                        <span class="text-xs font-semibold text-zinc-300 truncate max-w-[80px]">${escapeHtml(u.nickname)}</span>
                    `;
                    return userDiv;
                };

                document.getElementById('inviteFlatBlock').classList.toggle('hidden', isStage);
                document.getElementById('inviteStageBlock').classList.toggle('hidden', !isStage);
                const userList = document.getElementById('inviteUserList');
                const stageList = document.getElementById('inviteStageList');
                const audienceList = document.getElementById('inviteAudienceList');
                userList.innerHTML = '';
                stageList.innerHTML = '';
                audienceList.innerHTML = '';
                const speakerSet = new Set(data.speakers || []);
                let stageCount = 0;
                let audienceCount = 0;

                uids.forEach(uid => {
                    const card = buildInviteUserCard(data.users[uid]);
                    if (!isStage) {
                        userList.appendChild(card);
                    } else if (speakerSet.has(uid)) {
                        stageList.appendChild(card);
                        stageCount++;
                    } else {
                        audienceList.appendChild(card);
                        audienceCount++;
                    }
                });
                if (isStage) {
                    document.getElementById('inviteStageEmpty').classList.toggle('hidden', stageCount > 0);
                    document.getElementById('inviteAudienceEmpty').classList.toggle('hidden', audienceCount > 0);
                }
                
                if (inviteDurationTimer) {
                    clearInterval(inviteDurationTimer);
                    inviteDurationTimer = null;
                }
                if (data.created_at > 0) {
                    const updateDuration = () => {
                        const el = document.getElementById('inviteCallDuration');
                        if (el) el.innerText = `Running for ${formatDuration(data.created_at)}`;
                    };
                    updateDuration();
                    inviteDurationTimer = setInterval(updateDuration, 1000);
                }
                
                const inviteOverlay = document.getElementById('inviteWelcomeOverlay');
                showOverlayAnimated(inviteOverlay);
                
            } catch (e) {
                console.error("Error fetching status", e);
                // Fallback to setup screen or create modal if no create opts
                if (typeof getCreateOpts === 'function' && getCreateOpts()) {
                    showOverlayAnimated(configOverlay);
                    initSetupButtonTouchHandlers();
                    loadDevices();
                } else if (typeof openCreateRoomMenu === 'function') {
                    openCreateRoomMenu();
                }
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
            const buttons = document.querySelectorAll('#configPanel button, #settingsPanel button, .btn-secondary, .btn-primary, .btn-icon-test, .setup-back-btn');

            buttons.forEach(btn => {
                if (btn && !btn.dataset.touchInit) {
                    btn.dataset.touchInit = '1';
                    btn.addEventListener('touchstart', function() {
                        this.classList.add('is-pressed');
                    }, { passive: true });
                    btn.addEventListener('touchend', function() {
                        this.classList.remove('is-pressed');
                        this.blur();
                    }, { passive: true });
                    btn.addEventListener('touchcancel', function() {
                        this.classList.remove('is-pressed');
                        this.blur();
                    }, { passive: true });
                }
            });
        }


        function connectWs() {
            // Close any existing WebSocket to prevent ghost connections
            wsConnectionId++;
            const thisConnectionId = wsConnectionId;

            if (ws) {
                const oldWs = ws;
                oldWs.onclose = null;
                oldWs.onerror = null;
                if (oldWs.readyState === WebSocket.OPEN || oldWs.readyState === WebSocket.CONNECTING) {
                    oldWs.close();
                }
            }

            // Cancel any pending timers from previous connection attempts
            if (reconnectTimer) {
                clearTimeout(reconnectTimer);
                reconnectTimer = null;
            }
            if (iosSlowRetryTimer) {
                clearTimeout(iosSlowRetryTimer);
                iosSlowRetryTimer = null;
            }
            if (desktopSlowRetryTimer) {
                clearTimeout(desktopSlowRetryTimer);
                desktopSlowRetryTimer = null;
            }
            if (reconnectStatusTimeout) {
                clearTimeout(reconnectStatusTimeout);
                reconnectStatusTimeout = null;
            }

            stopHeartbeat();
            isReconnecting = false;
            updateStatus('connecting', 'Connecting...');

            Object.keys(peers).forEach(uid => {
                removePeer(uid);
            });
            peerCamStatus = {};
            peerScreenStatus = {};
            peerScreenHasAudio = {};
            peerMicTrackId = {};
            peerScreenAudioTrackId = {};
            peerCamVideoTrackId = {};
            peerScreenVideoTrackId = {};
            pendingCandidates = {};
            userAvatarCache = {};

            ws = new WebSocket(wsUrl);

                        ws.onopen = () => {
                            if (wsConnectionId !== thisConnectionId) return; // stale connection

                            if (reconnectStatusTimeout) {
                                clearTimeout(reconnectStatusTimeout);
                                reconnectStatusTimeout = null;
                            }
                            if (iosSlowRetryTimer) {
                                clearTimeout(iosSlowRetryTimer);
                                iosSlowRetryTimer = null;
                            }
                            if (desktopSlowRetryTimer) {
                                clearTimeout(desktopSlowRetryTimer);
                                desktopSlowRetryTimer = null;
                            }
                            if (reconnectTimer) {
                                clearTimeout(reconnectTimer);
                                reconnectTimer = null;
                            }

                            playNotificationSound('join');
                            reconnectionAttempts = 0;
                            desktopSlowRetryCount = 0;
                            isReconnecting = false;
                            updateStatus('connected', 'Connected');
                            startHeartbeat();
                            const camEnabled = localStream && localStream.getVideoTracks()[0] && localStream.getVideoTracks()[0].enabled;
                            const screenEnabled = !!screenStream;
                            const screenHasAudio = screenStream && screenStream.getAudioTracks().length > 0;
                            const audioTrack = localStream && localStream.getAudioTracks()[0];
                            const isMuted = !audioTrack || !audioTrack.enabled;

                                ws.send(JSON.stringify({
                                type: "join",
                                data: {
                                    userId: persistentUserId,
                                    nickname: userNickname,
                                    profileRev: profileRev,
                                    avatar: userAvatar,
                                    isGif: userAvatarIsGif,
                                    staticFrame: userAvatarStaticFrame,
                                    camEnabled: camEnabled,
                                    screenEnabled: screenEnabled,
                                    screenAudio: screenHasAudio,
                                    micTrackId: audioTrack ? audioTrack.id : null,
                                    camVideoTrackId: localStream ? (localStream.getVideoTracks()[0]?.id || null) : null,
                                    screenVideoTrackId: screenStream ? (screenStream.getVideoTracks()[0]?.id || null) : null,
                                    screenAudioTrackId: screenStream ? (screenStream.getAudioTracks()[0]?.id || null) : null,
                                    isMuted: isMuted,
                                    isDeafened: isDeafened,
                                    isLowBandwidthMode: isLowBandwidthMode,
                                    isOnTheGoMode: isOnTheGoMode,
                                    isMobile: isMobileClient,
                                    password: roomCreationPassword
                                }
                            }));
                            checkEmpty();
                        };

                        ws.onmessage = async (event) => {
                            if (wsConnectionId !== thisConnectionId) return; // stale connection
                            let msg;
                            try {
                                msg = JSON.parse(event.data);
                            } catch (error) {
                                console.warn('Ignoring malformed WebSocket message:', error);
                                return;
                            }

                            switch (msg.type) {
                                case 'joined':
                                    if (msg.userId) {
                                        persistentUserId = msg.userId;
                                        localStorage.setItem('rustrooms_user_id', persistentUserId);
                                    }
                                    break;
                                case 'error':
                                    if (msg.data && msg.data.code === 'PASSWORD_REQUIRED') {

                                        awaitingPassword = true;
                                        hasLeftRoom = true;
                                        isReconnecting = false;
                                        if (reconnectStatusTimeout) {
                                            clearTimeout(reconnectStatusTimeout);
                                            reconnectStatusTimeout = null;
                                        }

                                        const modal = document.getElementById('passwordModal');
                                        if (modal && !modal.classList.contains('open')) {
                                            showPasswordModal("Room Creation Password", msg.data.message || "Password required to create this room:", (pass) => {
                                                if (pass) {
                                                    roomCreationPassword = pass;
                                                    sessionStorage.setItem('rustrooms_room_password', pass);
                                                    awaitingPassword = false;
                                                    hasLeftRoom = false;
                                                    reconnectionAttempts = 0;
                                                    isReconnecting = false;
                                                    connectWs();
                                                } else {
                                                    hasLeftRoom = true;
                                                    window.location.href = "/";
                                                }
                                            });
                                        }
                                    } else {
                                        showCustomAlert("Error", msg.data.message || "An error occurred.");
                                    }
                                    break;
                                case 'room-list':
                                    try {
                                        globalRoomList = msg.data;
                                        if (typeof updateRoomListUI === 'function') updateRoomListUI();
                                    } catch (e) { console.error("Error updating room-list UI:", e); }
                                    break;
                                case 'existing-users':
                                    try {
                                        if (msg.data && Array.isArray(msg.data.users)) {
                                            msg.data.users.forEach(user => {
                                                if (user.id === persistentUserId) {
                                                    // The server's canonical copy of our own
                                                    // profile: adopt it so a client that lost
                                                    // its local storage (frozen tab killed by
                                                    // iOS) heals instead of reverting to Guest.
                                                    adoptServerProfile(user.status);
                                                }
                                                userAvatarCache[user.id] = {
                                                    avatar: user.status.avatar,
                                                    isGif: user.status.isGif,
                                                    staticFrame: user.status.staticFrame
                                                };
                                                if (user.status.isScreenSharing !== undefined) {
                                                    peerScreenStatus[user.id] = user.status.isScreenSharing;
                                                }
                                                if (user.status.isLowBandwidthMode !== undefined) {
                                                    peerLowBandwidthStatus[user.id] = user.status.isLowBandwidthMode;
                                                }
                                                if (user.status.isOnTheGoMode !== undefined) {
                                                    peerOnTheGoStatus[user.id] = user.status.isOnTheGoMode;
                                                }
                                                if (peers[user.id]) {
                                                    updatePeerInfo(user.id, user.status.nickname, user.status.avatar, user.status.isMuted, user.status.isDeafened, user.status.isGif, user.status.staticFrame);
                                                } else {
                                                    initPeer(user.id, false, user.status.nickname, user.status.avatar, user.status.isMuted, user.status.isDeafened, user.status.isGif, user.status.staticFrame);
                                                }
                                            });
                                            updateAllSenderBitrates();
                                        }
                                    } catch (e) { console.error("Error processing existing-users:", e); }
                                    break;
                                case 'user-joined':
                                    try {
                                        playNotificationSound('join');
                                        const joinedScreenAudio = getScreenAudioFlag(msg.data);
                                        updatePeerTrackHints(msg.userId, msg.data);
                                        if (msg.data) {
                                            userAvatarCache[msg.userId] = {
                                                avatar: msg.data.avatar,
                                                isGif: msg.data.isGif,
                                                staticFrame: msg.data.staticFrame
                                            };
                                        }
                                        if (msg.data.camEnabled !== undefined) {
                                            peerCamStatus[msg.userId] = msg.data.camEnabled;
                                        }
                                        if (msg.data.screenEnabled !== undefined) {
                                            peerScreenStatus[msg.userId] = msg.data.screenEnabled;
                                        }
                                        if (msg.data.isLowBandwidthMode !== undefined) {
                                            peerLowBandwidthStatus[msg.userId] = msg.data.isLowBandwidthMode;
                                            updateAllSenderBitrates();
                                        }
                                        if (msg.data.isOnTheGoMode !== undefined) {
                                            peerOnTheGoStatus[msg.userId] = msg.data.isOnTheGoMode;
                                        }

                                        if (peers[msg.userId]) {
                                            updatePeerInfo(msg.userId, msg.data?.nickname, msg.data?.avatar, msg.data?.isMuted, msg.data?.isDeafened, msg.data?.isGif, msg.data?.staticFrame);
                                            if (joinedScreenAudio !== undefined) {
                                                peerScreenHasAudio[msg.userId] = joinedScreenAudio;
                                            }
                                        } else {
                                            if (joinedScreenAudio !== undefined) {
                                                peerScreenHasAudio[msg.userId] = joinedScreenAudio;
                                            }
                                            initPeer(msg.userId, true, msg.data?.nickname, msg.data?.avatar, msg.data?.isMuted, msg.data?.isDeafened, msg.data?.isGif, msg.data?.staticFrame);
                                            if (peerScreenStatus[msg.userId] === true && joinedScreenAudio === true) {
                                                ensureScreenAudioUI(msg.userId);
                                            }
                                        }

                                        const myAudioTrack = localStream && localStream.getAudioTracks()[0];
                                        const myMuted = !myAudioTrack || !myAudioTrack.enabled;
                                        const myCamEnabled = localStream && localStream.getVideoTracks()[0] && localStream.getVideoTracks()[0].enabled;
                                        const myScreenEnabled = !!screenStream;
                                        const myScreenHasAudio = screenStream && screenStream.getAudioTracks().length > 0;

                                        ws.send(JSON.stringify({
                                            type: 'identify',
                                            target: msg.userId,
                                            data: {
                                                userId: persistentUserId,
                                                nickname: userNickname,
                                                avatar: userAvatar,
                                                isGif: userAvatarIsGif,
                                                staticFrame: userAvatarStaticFrame,
                                                camEnabled: myCamEnabled,
                                                screenEnabled: myScreenEnabled,
                                                screenAudio: myScreenHasAudio,
                                                micTrackId: myAudioTrack ? myAudioTrack.id : null,
                                                camVideoTrackId: localStream ? (localStream.getVideoTracks()[0]?.id || null) : null,
                                                screenVideoTrackId: screenStream ? (screenStream.getVideoTracks()[0]?.id || null) : null,
                                                screenAudioTrackId: screenStream ? (screenStream.getAudioTracks()[0]?.id || null) : null,
                                                isMuted: myMuted,
                                                isDeafened: isDeafened,
                                                isLowBandwidthMode: isLowBandwidthMode,
                                                isOnTheGoMode: isOnTheGoMode,
                                                isMobile: isMobileClient
                                            }
                                        }));
                                    } catch (e) { console.error("Error processing user-joined:", e); }
                                    break;
                                case 'user-left':

                                    delete userAvatarCache[msg.userId];
                                    if (msg.userId !== persistentUserId) {
                                        playNotificationSound('leave');
                                        removePeer(msg.userId);
                                        delete peerCamStatus[msg.userId];
                                        delete peerScreenStatus[msg.userId];
                                        delete peerScreenHasAudio[msg.userId];
                                        delete peerMicTrackId[msg.userId];
                                        delete peerScreenAudioTrackId[msg.userId];
                                        delete peerLowBandwidthStatus[msg.userId];
                                        delete peerOnTheGoStatus[msg.userId];
                                    }
                                    break;
                                case 'user-kicked':
                                    delete userAvatarCache[msg.userId];
                                    if (msg.userId === persistentUserId) {
                                        hasLeftRoom = true;
                                        alert("You have been kicked from the room.");
                                        sessionStorage.removeItem('rustrooms_setup_done');
                                        sessionStorage.removeItem('rustrooms_last_room_id');
                                        sessionStorage.removeItem('rustrooms_last_channel_id');
                                        window.location.href = "/";
                                    } else {
                                        playNotificationSound('leave');
                                        removePeer(msg.userId);
                                        delete peerCamStatus[msg.userId];
                                        delete peerScreenStatus[msg.userId];
                                        delete peerScreenHasAudio[msg.userId];
                                        delete peerMicTrackId[msg.userId];
                                        delete peerScreenAudioTrackId[msg.userId];
                                        delete peerLowBandwidthStatus[msg.userId];
                                        delete peerOnTheGoStatus[msg.userId];
                                        updateRoomListUI();
                                    }
                                    break;
                                case 'user-update':
                                     if (msg.userId === persistentUserId) {
                                         // Authoritative echo of our own update: adopt it
                                         // so local state can never diverge from what the
                                         // server (and everyone else) sees.
                                         if (msg.data) adoptServerProfile(msg.data);
                                         break;
                                     }
                                     if (!msg.data) break;
                                     updatePeerTrackHints(msg.userId, msg.data);
                                     userAvatarCache[msg.userId] = {
                                         avatar: msg.data.avatar,
                                         isGif: msg.data.isGif,
                                         staticFrame: msg.data.staticFrame
                                     };
                                     if (msg.data.isLowBandwidthMode !== undefined) {
                                         peerLowBandwidthStatus[msg.userId] = msg.data.isLowBandwidthMode;
                                         updateAllSenderBitrates();
                                     }
                                     if (msg.data.isOnTheGoMode !== undefined) {
                                         peerOnTheGoStatus[msg.userId] = msg.data.isOnTheGoMode;
                                     }
                                     updatePeerInfo(msg.userId, msg.data.nickname, msg.data.avatar, msg.data.isMuted, msg.data.isDeafened, msg.data.isGif, msg.data.staticFrame);
                                    break;
                                case 'cam-toggle':
                                    if (msg.data && msg.data.enabled !== undefined) {
                                        updatePeerTrackHints(msg.userId, msg.data);
                                        peerCamStatus[msg.userId] = msg.data.enabled;
                                        updatePeerVideoLayout(msg.userId);
                                    }
                                    break;
                                case 'screen-toggle':
                                    if (msg.data && msg.data.enabled !== undefined) {
                                        updatePeerTrackHints(msg.userId, msg.data);
                                        peerScreenStatus[msg.userId] = msg.data.enabled;
                                        if (msg.data.hasAudio !== undefined) {
                                            peerScreenHasAudio[msg.userId] = msg.data.hasAudio;
                                        }
                                        if (msg.data.enabled && msg.data.hasAudio === true) {
                                            ensureScreenAudioUI(msg.userId);
                                        }
                                        updatePeerVideoLayout(msg.userId);

                                        if (!msg.data.enabled || msg.data.hasAudio === false) {
                                            const row = document.getElementById(`vol-row-screen-${msg.userId}`);
                                            if (row) row.remove();
                                            const aud = document.getElementById(`aud-screen-${msg.userId}`);
                                            if (aud) aud.remove();
                                        }

                                        const wrapper = document.getElementById(`wrapper-${msg.userId}`);
                                        if (wrapper) {
                                            const vid = document.getElementById(`vid-${msg.userId}`);
                                            if (vid && vid.srcObject && vid.srcObject.getAudioTracks().length > 0) {
                                                (async () => { await setupAudioMonitor(vid.srcObject, `wrapper-${msg.userId}`); })();
                                            }
                                        }
                                    }
                                    break;
                                case 'identify':
                                    try {
                                        const identifiedScreenAudio = getScreenAudioFlag(msg.data);
                                        updatePeerTrackHints(msg.userId, msg.data);
                                        if (msg.data) {
                                            userAvatarCache[msg.userId] = {
                                                avatar: msg.data.avatar,
                                                isGif: msg.data.isGif,
                                                staticFrame: msg.data.staticFrame
                                            };
                                        }
                                        if (msg.data.camEnabled !== undefined) {
                                            peerCamStatus[msg.userId] = msg.data.camEnabled;
                                        }
                                        if (msg.data.screenEnabled !== undefined) {
                                            peerScreenStatus[msg.userId] = msg.data.screenEnabled;
                                        }
                                        if (msg.data.isLowBandwidthMode !== undefined) {
                                            peerLowBandwidthStatus[msg.userId] = msg.data.isLowBandwidthMode;
                                            updateAllSenderBitrates();
                                        }
                                        if (msg.data.isOnTheGoMode !== undefined) {
                                            peerOnTheGoStatus[msg.userId] = msg.data.isOnTheGoMode;
                                        }
                                        if (identifiedScreenAudio !== undefined) {
                                            peerScreenHasAudio[msg.userId] = identifiedScreenAudio;
                                        }
                                        if (peers[msg.userId]) {
                                            updatePeerInfo(msg.userId, msg.data.nickname, msg.data.avatar, msg.data.isMuted, msg.data.isDeafened, msg.data.isGif, msg.data.staticFrame);
                                        } else {
                                            initPeer(msg.userId, false, msg.data.nickname, msg.data.avatar, msg.data.isMuted, msg.data.isDeafened, msg.data.isGif, msg.data.staticFrame);
                                        }
                                        if (peerScreenStatus[msg.userId] === true && identifiedScreenAudio === true) {
                                            ensureScreenAudioUI(msg.userId);
                                        }
                                    } catch (e) { console.error("Error processing identify:", e); }
                                    break;
                                case 'rename-channel':
                                    if (msg.data && roomId === msg.data.roomId && channelId === msg.data.oldName) {
                                        performChannelSwitch(roomId, msg.data.newName);
                                    }
                                    break;
                                case 'signal':
                                    handleSignal(msg.userId, msg.data);
                                    break;
                                case 'keepalive':
                                    // Server keepalive — ignore (not a pong response to our ping)
                                    break;
                                case 'pong':
                                    handlePong();
                                    break;
                            }
                        };

                        ws.onclose = (event) => {
                            if (wsConnectionId !== thisConnectionId) return; // stale connection

                            // Code 4001 = server inactivity timeout — skip reconnect, show disconnected
                            if (event.code === 4001) {
                                stopHeartbeat();
                                updateStatus('disconnected', 'Disconnected (inactive)');
                                showReconnectButtons();
                                isReconnecting = false;
                                return;
                            }

                            stopHeartbeat();

                            if (reconnectStatusTimeout) {
                                clearTimeout(reconnectStatusTimeout);
                                reconnectStatusTimeout = null;
                            }

                            if (hasLeftRoom) {
                                console.log('User left the room, not reconnecting');
                                isReconnecting = false;
                                return;
                            }

                            if (isReconnecting) {
                                console.log('Reconnection already in progress, skipping duplicate onclose');
                                return;
                            }

                            isReconnecting = true;
                            reconnectionAttempts++;
                            if (reconnectionAttempts >= maxReconnectionAttempts) {
                                if (isIOS) {
                                    // On iOS, never fully give up — fall back to slow periodic retries
                                    console.warn(`iOS: exhausted ${maxReconnectionAttempts} fast retries, switching to slow retry every 30s`);
                                    updateStatus('connecting', 'Connection lost — retrying...');
                                    showReconnectButtons();
                                    isReconnecting = false;
                                    iosSlowRetryTimer = setTimeout(() => {
                                        iosSlowRetryTimer = null;
                                        if (!hasLeftRoom && (!ws || ws.readyState !== WebSocket.OPEN)) {
                                            reconnectionAttempts = Math.floor(maxReconnectionAttempts * 0.75);
                                            isReconnecting = false;
                                            connectWs();
                                        }
                                    }, 30000);
                                } else {
                                    // Desktop: fall back to slow periodic retries (every 60s, up to 5 times)
                                    // before giving up entirely, so transient outages don't require manual action
                                    console.warn(`Desktop: exhausted ${maxReconnectionAttempts} fast retries, switching to slow retry every 60s`);
                                    updateStatus('connecting', 'Connection lost — retrying...');
                                    showReconnectButtons();
                                    isReconnecting = false;
                                    desktopSlowRetryCount = (desktopSlowRetryCount || 0) + 1;
                                    if (desktopSlowRetryCount <= 5) {
                                        desktopSlowRetryTimer = setTimeout(() => {
                                            desktopSlowRetryTimer = null;
                                            if (!hasLeftRoom && (!ws || ws.readyState !== WebSocket.OPEN)) {
                                                reconnectionAttempts = Math.floor(maxReconnectionAttempts * 0.75);
                                                isReconnecting = false;
                                                connectWs();
                                            }
                                        }, 60000);
                                    } else {
                                        updateStatus('disconnected', 'Disconnected');
                                        console.error('WebSocket disconnected after multiple retries. No further attempts will be made.');
                                        stopHeartbeat();
                                    }
                                }
                            } else {
                                const delay = getReconnectDelay(reconnectionAttempts);

                                reconnectStatusTimeout = setTimeout(() => {

                                    if (isReconnecting && (!ws || ws.readyState !== WebSocket.OPEN)) {
                                        updateStatus('connecting', `Reconnecting... (Attempt ${reconnectionAttempts}/${maxReconnectionAttempts})`);
                                    }
                                }, reconnectDelayMs);

                                console.log(`Reconnecting in ${Math.round(delay)}ms...`);
                                reconnectTimer = setTimeout(() => {
                                    reconnectTimer = null;
                                    if (reconnectStatusTimeout) {
                                        clearTimeout(reconnectStatusTimeout);
                                        reconnectStatusTimeout = null;
                                    }
                                    isReconnecting = false;
                                    connectWs();
                                }, delay);
                            }
                        };

                        ws.onerror = (error) => {
                            console.error('WebSocket Error:', error);
                            // onerror is usually followed by onclose which handles reconnection,
                            // but on some mobile browsers onclose may not fire after onerror.
                            // Kick off a fallback reconnect after a short delay if onclose doesn't fire.
                            const errConnectionId = thisConnectionId;
                            setTimeout(() => {
                                if (wsConnectionId !== errConnectionId) return; // stale
                                if (hasLeftRoom) return;
                                if (ws && ws.readyState === WebSocket.CONNECTING) return; // still connecting
                                if (isReconnecting) return;
                                // If the socket is closed/closing and onclose never fired, trigger reconnect
                                if (!ws || ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
                                    console.warn('onerror fallback: socket appears dead without onclose, triggering reconnect');
                                    reconnectionAttempts++;
                                    isReconnecting = true;
                                    const delay = getReconnectDelay(reconnectionAttempts);
                                    reconnectTimer = setTimeout(() => {
                                        reconnectTimer = null;
                                        isReconnecting = false;
                                        connectWs();
                                    }, delay);
                                }
                            }, 2000);
                        };
                    }

        function retryConnection() {
            // Cancel any pending timers from previous connection attempts
            if (reconnectTimer) {
                clearTimeout(reconnectTimer);
                reconnectTimer = null;
            }
            if (iosSlowRetryTimer) {
                clearTimeout(iosSlowRetryTimer);
                iosSlowRetryTimer = null;
            }
            if (desktopSlowRetryTimer) {
                clearTimeout(desktopSlowRetryTimer);
                desktopSlowRetryTimer = null;
            }
            if (reconnectStatusTimeout) {
                clearTimeout(reconnectStatusTimeout);
                reconnectStatusTimeout = null;
            }

            const btns = [
                document.getElementById('btnReconnect'),
                document.getElementById('onTheGoBtnReconnect')
            ];
            btns.forEach(btn => {
                if (btn) {
                    btn.classList.add('text-green-500', 'bg-green-500/10');
                    btn.classList.remove('text-slate-400', 'hover:text-white', 'hover:bg-slate-700');
                }
            });

            setTimeout(() => {
                btns.forEach(btn => {
                    if (btn) {
                        btn.classList.add('hidden');
                        btn.classList.remove('text-green-500', 'bg-green-500/10');
                        btn.classList.add('text-slate-400', 'hover:text-white', 'hover:bg-slate-700');
                    }
                });

                hasLeftRoom = false;
                isReconnecting = false;
                reconnectionAttempts = 0;
                desktopSlowRetryCount = 0;
                connectWs();
            }, 300);
        }

        function setAvatar(layer, avatar, isGif, staticFrame) {
            layer.innerHTML = '';
            if (avatar) {
               const displaySrc = isGif && staticFrame ? staticFrame : avatar;
               const bgImg = document.createElement('img');
               bgImg.src = displaySrc;
               bgImg.className = 'avatar-img';
               bgImg.draggable = false;
               if (isGif && staticFrame) {
                   bgImg.dataset.gifSrc = avatar;
                   bgImg.dataset.staticSrc = staticFrame;
               }

               const centerDiv = document.createElement('div');
               centerDiv.className = 'avatar-center';

               const centerImg = document.createElement('img');
               centerImg.src = displaySrc;
               centerImg.draggable = false;
               if (isGif && staticFrame) {
                   centerImg.dataset.gifSrc = avatar;
                   centerImg.dataset.staticSrc = staticFrame;
               }

               centerDiv.appendChild(centerImg);
               layer.appendChild(bgImg);
               layer.appendChild(centerDiv);
           } else {
               const centerDiv = document.createElement('div');
               centerDiv.className = 'avatar-center avatar-empty';

               const text = document.createElement('div');
               text.style.display = 'flex';
               text.style.alignItems = 'center';
               text.style.justifyContent = 'center';
               text.style.width = '100%';
               text.style.height = '100%';
               text.style.margin = '0';
               text.style.color = 'var(--text-muted)';
               text.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" width="60" height="60"><circle cx="12" cy="8" r="4"/><path d="M4 21c0-4 3.6-6.5 8-6.5s8 2.5 8 6.5"/></svg>';

               centerDiv.appendChild(text);
               layer.appendChild(centerDiv);
           }
        }

        function updatePeerInfo(userId, nickname, avatar, isMuted, isDeafened, isGif, staticFrame) {
            const wrapper = document.getElementById(`wrapper-${userId}`);
            if (wrapper) {
                const nameSpan = wrapper.querySelector('.peer-name');
                // Update even for falsy nicknames: the server normalizes
                // empty names to "Guest", and skipping them would leave the
                // stale name visible here while the owner shows "Guest".
                if (nameSpan) nameSpan.innerText = nickname || 'Guest';

                const statusContainer = wrapper.querySelector('.peer-status-icons');
                if (statusContainer) {
                    statusContainer.innerHTML = '';
                    let hasIcons = false;
                    let iconsHTML = '';

                    if (isDeafened) {
                        hasIcons = true;
                        iconsHTML += `<span class="text-red-500" title="Deafened"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 14a2 2 0 0 0-2-2h-3a2 2 0 0 0-2 2v3a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2V14z"></path><path d="M3 14a2 2 0 0 1 2-2h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V14z"></path><path d="M20.4 10.4C20.2 6.5 17 3.5 13 3.1"></path><path d="M6.5 5.5A9 9 0 0 0 3 12"></path></svg></span>`;
                    } else if (isMuted) {
                        hasIcons = true;
                        iconsHTML += `<span class="text-red-500" title="Muted"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path></svg></span>`;
                    }

                    const isPeerLBM = (userId === 'local') ? isLowBandwidthMode : (peerLowBandwidthStatus[userId] === true);
                    if (isPeerLBM) {
                        hasIcons = true;
                        iconsHTML += `
                            <span class="text-amber-500 animate-pulse" title="Low Bandwidth Mode Active">
                                <svg class="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 24 24"><path d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
                            </span>
                        `;
                    }

                    const isPeerOTG = (userId === 'local') ? isOnTheGoMode : (peerOnTheGoStatus[userId] === true);
                    if (isPeerOTG) {
                        hasIcons = true;
                        iconsHTML += `
                            <span class="text-blue-400" title="On-the-go Mode Active">
                                <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="2" width="14" height="20" rx="2" ry="2"></rect><line x1="12" x2="12.01" y1="18" y2="18"></line></svg>
                            </span>
                        `;
                    }

                    if (hasIcons) {
                        statusContainer.classList.remove('hidden');
                        statusContainer.classList.add('flex', 'items-center', 'gap-1.5');
                        statusContainer.innerHTML = iconsHTML;
                    } else {
                        statusContainer.classList.add('hidden');
                        statusContainer.classList.remove('flex', 'items-center', 'gap-1.5');
                    }
                }

                const avatarLayer = wrapper.querySelector('.avatar-layer');
                if (avatarLayer) {
                     setAvatar(avatarLayer, avatar, isGif, staticFrame);
                }
            }
        }

        let dragState = {
            isDragging: false,
            draggedEl: null,
            placeholder: null,
            startX: 0,
            startY: 0,
            initialRect: null,
            allTiles: [],
            currentIndex: 0,
            tilePositions: null,
            // Autoscroll fields
            scrollSpeed: 0,
            scrollInterval: null,
            lastClientX: 0,
            lastClientY: 0
        };

        function startAutoScroll(speed) {
            dragState.scrollSpeed = speed;
            if (!dragState.scrollInterval) {
                const scrollContainer = remoteGrid.parentElement;
                const scrollLoop = () => {
                    if (!dragState.isDragging || !dragState.scrollSpeed) {
                        dragState.scrollInterval = null;
                        return;
                    }
                    
                    scrollContainer.scrollTop += dragState.scrollSpeed;
                    
                    // Update positions of remaining tiles relative to the scrolled viewport
                    updateTilePositions();
                    
                    // Trigger intersection & reordering checks at the updated positions
                    checkIntersectionAndReorder(dragState.lastClientX, dragState.lastClientY);
                    
                    dragState.scrollInterval = requestAnimationFrame(scrollLoop);
                };
                dragState.scrollInterval = requestAnimationFrame(scrollLoop);
            }
        }

        function stopAutoScroll() {
            dragState.scrollSpeed = 0;
            if (dragState.scrollInterval) {
                cancelAnimationFrame(dragState.scrollInterval);
                dragState.scrollInterval = null;
            }
        }

        function updateTilePositions() {
            if (!dragState.isDragging) return;
            dragState.tilePositions = dragState.allTiles.map(tile => {
                if (tile === dragState.draggedEl) return null;
                const rect = tile.getBoundingClientRect();
                return {
                    tile,
                    rect,
                    centerX: rect.left + rect.width / 2,
                    centerY: rect.top + rect.height / 2,
                    minDimension: Math.min(rect.width, rect.height)
                };
            }).filter(p => p !== null);
        }

        function checkIntersectionAndReorder(clientX, clientY) {
            const offsetX = clientX - dragState.startX;
            const offsetY = clientY - dragState.startY;

            dragState.draggedEl.style.transform = `scale(1.05) translate3d(${offsetX}px, ${offsetY}px, 0)`;

            const draggedCenterX = dragState.initialRect.left + dragState.initialRect.width / 2 + offsetX;
            const draggedCenterY = dragState.initialRect.top + dragState.initialRect.height / 2 + offsetY;

            let newDropIndex = -1;

            for (let i = 0; i < dragState.tilePositions.length; i++) {
                const pos = dragState.tilePositions[i];

                const distance = Math.hypot(draggedCenterX - pos.centerX, draggedCenterY - pos.centerY);

                if (distance < pos.minDimension * 0.6) {
                    newDropIndex = dragState.allTiles.indexOf(pos.tile);
                    break;
                }
            }

            if (newDropIndex !== -1 && newDropIndex !== dragState.currentIndex) {
                const placeholderArray = [...remoteGrid.querySelectorAll('.video-container')];

                if (newDropIndex > dragState.currentIndex) {
                    const targetTile = dragState.allTiles[newDropIndex];
                    const targetIndex = placeholderArray.indexOf(targetTile);
                    if (targetIndex !== -1) {
                        targetTile.after(dragState.placeholder);
                    }
                } else {
                    const targetTile = dragState.allTiles[newDropIndex];
                    const targetIndex = placeholderArray.indexOf(targetTile);
                    if (targetIndex !== -1) {
                        targetTile.before(dragState.placeholder);
                    }
                }

                dragState.currentIndex = newDropIndex;
            }
        }

        function setupSmoothDragAndDrop(container) {
            container.addEventListener('mousedown', handleDragStart);
            container.addEventListener('touchstart', handleDragStart, { passive: false });
        }

        function handleDragStart(e) {
            if (e.target.closest('button') || e.target.closest('input') || e.target.closest('a')) {
                return;
            }

            const isTouch = e.type === 'touchstart';
            const clientX = isTouch ? e.touches[0].clientX : e.clientX;
            const clientY = isTouch ? e.touches[0].clientY : e.clientY;

            dragState.isDragging = false;
            dragState.draggedEl = this;
            dragState.startX = clientX;
            dragState.startY = clientY;
            dragState.initialRect = this.getBoundingClientRect();

            if (isTouch) {
                document.addEventListener('touchmove', handleDragMove, { passive: false });
                document.addEventListener('touchend', handleDragEnd);
                document.addEventListener('touchcancel', handleDragEnd);
            } else {
                document.addEventListener('mousemove', handleDragMove);
                document.addEventListener('mouseup', handleDragEnd);
            }
        }

        function handleDragMove(e) {
            if (!dragState.draggedEl) return;

            const isTouch = e.type === 'touchmove';
            const clientX = isTouch ? e.touches[0].clientX : e.clientX;
            const clientY = isTouch ? e.touches[0].clientY : e.clientY;

            dragState.lastClientX = clientX;
            dragState.lastClientY = clientY;

            if (!dragState.isDragging) {
                const deltaX = Math.abs(clientX - dragState.startX);
                const deltaY = Math.abs(clientY - dragState.startY);
                if (deltaX < 5 && deltaY < 5) return;

                dragState.isDragging = true;
                dragState.allTiles = [...remoteGrid.querySelectorAll('.video-container')];
                dragState.currentIndex = dragState.allTiles.indexOf(dragState.draggedEl);

                updateTilePositions();

                dragState.placeholder = dragState.draggedEl.cloneNode(true);
                dragState.placeholder.classList.add('drag-placeholder');
                dragState.placeholder.classList.remove('is-dragging');
                dragState.placeholder.style.pointerEvents = 'none';

                dragState.draggedEl.classList.add('is-dragging');
                dragState.draggedEl.style.transition = 'none';
                dragState.draggedEl.style.width = dragState.initialRect.width + 'px';
                dragState.draggedEl.style.height = dragState.initialRect.height + 'px';
                dragState.draggedEl.style.left = dragState.initialRect.left + 'px';
                dragState.draggedEl.style.top = dragState.initialRect.top + 'px';

                dragState.draggedEl.parentNode.insertBefore(dragState.placeholder, dragState.draggedEl);

                dragState.allTiles.forEach(tile => {
                    if (tile !== dragState.draggedEl && tile !== dragState.placeholder) {
                        tile.classList.add('is-shifting');
                    }
                });

                e.preventDefault();
            }

            checkIntersectionAndReorder(clientX, clientY);

            // AUTO-SCROLL LOGIC
            const scrollContainer = remoteGrid.parentElement;
            const containerRect = scrollContainer.getBoundingClientRect();
            
            const threshold = 60;
            const distFromTop = clientY - containerRect.top;
            const distFromBottom = containerRect.bottom - clientY;

            if (distFromTop < threshold) {
                const speed = -Math.max(2, (1 - distFromTop / threshold) * 15);
                startAutoScroll(speed);
            } else if (distFromBottom < threshold) {
                const speed = Math.max(2, (1 - distFromBottom / threshold) * 15);
                startAutoScroll(speed);
            } else {
                stopAutoScroll();
            }

            if (isTouch) {
                e.preventDefault();
            }
        }

        function handleDragEnd(e) {
            stopAutoScroll();

            if (!dragState.draggedEl) return;

            const wasDragging = dragState.isDragging;

            dragState.draggedEl.classList.remove('is-dragging');
            dragState.draggedEl.style.position = '';
            dragState.draggedEl.style.zIndex = '';
            dragState.draggedEl.style.boxShadow = '';
            dragState.draggedEl.style.transform = '';
            dragState.draggedEl.style.transition = '';
            dragState.draggedEl.style.pointerEvents = '';
            dragState.draggedEl.style.opacity = '';
            dragState.draggedEl.style.width = '';
            dragState.draggedEl.style.height = '';
            dragState.draggedEl.style.left = '';
            dragState.draggedEl.style.top = '';

            document.querySelectorAll('.video-container.is-shifting').forEach(tile => {
                tile.classList.remove('is-shifting');
            });

            if (dragState.placeholder && dragState.placeholder.parentNode) {
                dragState.placeholder.parentNode.insertBefore(dragState.draggedEl, dragState.placeholder);
                dragState.placeholder.remove();
            }

            if (wasDragging) {
                saveTileOrder();
            }

            document.removeEventListener('mousemove', handleDragMove);
            document.removeEventListener('mouseup', handleDragEnd);
            document.removeEventListener('touchmove', handleDragMove);
            document.removeEventListener('touchend', handleDragEnd);
            document.removeEventListener('touchcancel', handleDragEnd);

            dragState.isDragging = false;
            dragState.draggedEl = null;
            dragState.placeholder = null;
            dragState.tilePositions = null;
        }

        function saveTileOrder() {
            const order = [...remoteGrid.querySelectorAll('.video-container')]
                .map(el => el.dataset.userId)
                .filter(id => id);
            localStorage.setItem('tileOrder', JSON.stringify(order));
        }

        function loadTileOrder() {
            try {
                const saved = localStorage.getItem('tileOrder');
                if (!saved) return;
                const order = JSON.parse(saved);
                const containers = {};

                [...remoteGrid.querySelectorAll('.video-container')].forEach(el => {
                    const userId = el.dataset.userId;
                    if (userId) {
                        containers[userId] = el;
                    }
                });

                order.forEach(userId => {
                    if (containers[userId]) {
                        remoteGrid.appendChild(containers[userId]);
                    }
                });
            } catch (e) {
                console.warn('Failed to load tile order:', e);
            }
        }

        function checkEmpty() {
            const count = Object.keys(peers).length;
            if (count === 0) {
                emptyState.style.display = 'block';
            } else {
                emptyState.style.display = 'none';
                loadTileOrder();
            }
            updateGridLayout(count);
        }

        function updateGridLayout(count) {
            remoteGrid.className = 'grid gap-2 md:gap-4 w-full h-full max-w-[1600px] transition-all duration-500 grid-expand my-auto';
            // Clear layouts installed for the previous peer count; otherwise
            // an inline auto-fit template overrides the new utility classes.
            remoteGrid.style.gridTemplateColumns = '';
            remoteGrid.style.justifyContent = '';

            if (count === 0) return;

            if (count === 1) {
                remoteGrid.classList.add('grid-cols-1');
            } else if (count === 2) {
                remoteGrid.classList.add('grid-cols-1', 'md:grid-cols-2');
            } else if (count === 3) {
                remoteGrid.classList.add('grid-cols-1');
                remoteGrid.style.gridTemplateColumns = 'repeat(auto-fit, minmax(min(100%, 400px), 1fr))';
                remoteGrid.style.justifyContent = 'center';
            } else if (count === 4) {
                remoteGrid.classList.add('grid-cols-2');
                remoteGrid.style.gridTemplateColumns = '';
            } else if (count === 5) {
                remoteGrid.style.gridTemplateColumns = 'repeat(auto-fit, minmax(min(100%, 350px), 1fr))';
            } else if (count === 6) {
                remoteGrid.classList.add('grid-cols-2', 'md:grid-cols-3');
                remoteGrid.style.gridTemplateColumns = '';
            } else if (count === 7) {
                remoteGrid.style.gridTemplateColumns = 'repeat(auto-fit, minmax(min(100%, 320px), 1fr))';
            } else if (count === 8) {
                remoteGrid.classList.add('grid-cols-2', 'md:grid-cols-4');
                remoteGrid.style.gridTemplateColumns = '';
            } else if (count === 9) {
                remoteGrid.classList.add('grid-cols-3');
                remoteGrid.style.gridTemplateColumns = '';
            } else {
                remoteGrid.classList.add('grid-cols-3', 'md:grid-cols-4');
                remoteGrid.style.gridTemplateColumns = '';
            }
        }

        function forceStereoAudio(sdp) {
            let sdpLines = sdp.split('\r\n');
            let opusPayload = -1;
            let rtpmapLineIndex = -1;

            for (let i = 0; i < sdpLines.length; i++) {
                if (sdpLines[i].startsWith('a=rtpmap:')) {
                    if (sdpLines[i].includes('opus/48000')) {
                        opusPayload = sdpLines[i].split(':')[1].split(' ')[0];
                        rtpmapLineIndex = i;
                        break;
                    }
                }
            }

            if (opusPayload === -1) return sdp;

            let fmtpLineIndex = -1;
            for (let i = 0; i < sdpLines.length; i++) {
                if (sdpLines[i].startsWith('a=fmtp:' + opusPayload)) {
                    fmtpLineIndex = i;
                    break;
                }
            }

            if (fmtpLineIndex === -1) {
                sdpLines.splice(rtpmapLineIndex + 1, 0, 'a=fmtp:' + opusPayload + ' stereo=1;sprop-stereo=1;maxaveragebitrate=510000;useinbandfec=1;cbr=1;usedtx=0');
            } else {
                let fmtpLine = sdpLines[fmtpLineIndex];
                if (!fmtpLine.includes('stereo=1')) {
                    sdpLines[fmtpLineIndex] = fmtpLine + ';stereo=1;sprop-stereo=1;maxaveragebitrate=510000;useinbandfec=1;cbr=1;usedtx=0';
                }
            }
            return sdpLines.join('\r\n');
        }

        function negotiate(userId, pc, isIceRestart = false) {
            if (!peers[userId] || peers[userId] !== pc) return; // peer was removed
            if (pc.connectionState === 'closed' || pc.connectionState === 'failed') return; // peer is dead
            const options = isIceRestart ? { iceRestart: true } : {};
            pc.createOffer(options)
                .then(offer => {
                    if (!peers[userId] || peers[userId] !== pc) return; // peer was removed during async op
                    offer.sdp = forceStereoAudio(offer.sdp);
                    return pc.setLocalDescription(offer);
                })
                .then(() => {
                    if (!peers[userId] || peers[userId] !== pc) return; // peer was removed during async op
                    sendSignal(userId, { type: 'offer', sdp: pc.localDescription });
                })
                .catch(e => console.error("Negotiation error", e));
        }

        function getLiveVideoTrack(video) {
            if (!video || !video.srcObject) return null;
            return video.srcObject.getVideoTracks().find(track => track.readyState === 'live') || null;
        }

        function updatePeerVideoLayout(userId) {
            const container = document.getElementById(`wrapper-${userId}`);
            if (!container) return;

            const cameraVideo = document.getElementById(`vid-${userId}`);
            const screenVideo = document.getElementById(`screen-${userId}`);
            const cameraTrack = getLiveVideoTrack(cameraVideo);
            const screenTrack = getLiveVideoTrack(screenVideo);
            const hasCamera = peerCamStatus[userId] !== false && !!cameraTrack;
            const hasScreen = peerScreenStatus[userId] === true && !!screenTrack;

            // Status messages arrive before renegotiation. Clear the old frame
            // immediately so a removed sender can never leave a frozen screen.
            if (peerCamStatus[userId] === false && cameraVideo?.srcObject) {
                cameraVideo.srcObject.getVideoTracks().forEach(track => cameraVideo.srcObject.removeTrack(track));
            }
            if (peerScreenStatus[userId] === false && screenVideo?.srcObject) {
                screenVideo.srcObject.getVideoTracks().forEach(track => screenVideo.srcObject.removeTrack(track));
            }

            container.classList.toggle('has-camera', hasCamera);
            container.classList.toggle('has-screen', hasScreen);
            cameraVideo?.classList.toggle('active', hasCamera);
            screenVideo?.classList.toggle('active', hasScreen);
        }

        function addCameraTrackToPeer(pc, track) {
            if (!track) return false;
            if (pc._cameraVideoSender && pc.getSenders().includes(pc._cameraVideoSender)) {
                pc._cameraVideoSender.replaceTrack(track).catch(error => console.warn('Camera replaceTrack failed:', error));
                return false;
            }
            pc._cameraVideoSender = pc.addTrack(track, localStream || new MediaStream([track]));
            return true;
        }

        function addScreenTrackToPeer(pc, track) {
            if (!track) return false;
            if (pc._screenVideoSender && pc.getSenders().includes(pc._screenVideoSender)) {
                pc._screenVideoSender.replaceTrack(track).catch(error => console.warn('Screen replaceTrack failed:', error));
                return false;
            }
            pc._screenVideoSender = pc.addTrack(track, screenStream || new MediaStream([track]));
            return true;
        }

        function removePeerSender(pc, propertyName) {
            const sender = pc[propertyName];
            if (!sender || !pc.getSenders().includes(sender)) return false;
            pc.removeTrack(sender);
            pc[propertyName] = null;
            return true;
        }

        function updateLocalMediaLayout() {
            const wrapper = document.getElementById('localPipWrapper');
            const cameraVideo = document.getElementById('localVideo');
            const screenVideo = document.getElementById('localScreenVideo');
            if (!wrapper || !cameraVideo || !screenVideo) return;

            const cameraTrack = localStream?.getVideoTracks()[0];
            const screenTrack = screenStream?.getVideoTracks()[0];
            const hasCamera = !!cameraTrack && cameraTrack.enabled && cameraTrack.readyState === 'live';
            const hasScreen = !!screenTrack && screenTrack.readyState === 'live';

            cameraVideo.srcObject = hasCamera ? localStream : null;
            screenVideo.srcObject = hasScreen ? screenStream : null;
            wrapper.classList.toggle('has-camera', hasCamera);
            wrapper.classList.toggle('has-screen', hasScreen);

            const avatar = document.getElementById('localAvatarLayer');
            if (avatar && (hasCamera || hasScreen)) avatar.style.display = 'none';
        }

        function createPeerUI(userId, displayName, avatarUrl, remoteIsDeafened, remoteIsMuted, isGif, staticFrame) {

            if (document.getElementById(`wrapper-${userId}`)) {
                return;
            }

            const container = document.createElement('div');
            container.id = `wrapper-${userId}`;
            container.className = 'video-container group bg-slate-800 border border-slate-700';

            const vid = document.createElement('video');
            vid.id = `vid-${userId}`;
            vid.className = 'camera-video';
            vid.autoplay = true;
            vid.playsInline = true;
            attachSinkId(vid, currentAudioOutputId);

            const screenVid = document.createElement('video');
            screenVid.id = `screen-${userId}`;
            screenVid.className = 'screen-video';
            screenVid.autoplay = true;
            screenVid.playsInline = true;
            screenVid.muted = true;
            screenVid.srcObject = new MediaStream();

            const savedVol = getVolumeSettings(userId, 'main');
            vid.volume = savedVol;

            vid.srcObject = new MediaStream();
            if (isDeafened) vid.muted = true;

            const avatarLayer = document.createElement('div');
            avatarLayer.className = 'avatar-layer';

            setAvatar(avatarLayer, avatarUrl, isGif, staticFrame);

            const label = document.createElement('div');
            label.className = 'name-tag absolute bottom-3 left-3 bg-black/45 backdrop-blur-xl px-3 py-1.5 rounded-lg text-sm text-white z-30 flex items-center gap-1.5';

            const nameSpan = document.createElement('span');
            nameSpan.className = 'peer-name';
            nameSpan.innerText = displayName;

            const statusContainer = document.createElement('div');
            statusContainer.className = 'peer-status-icons items-center' + (remoteIsDeafened || remoteIsMuted ? ' flex' : ' hidden');

            if (remoteIsDeafened) {
                statusContainer.innerHTML = `<span class="text-red-500"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 14a2 2 0 0 0-2-2h-3a2 2 0 0 0-2 2v3a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2V14z"></path><path d="M3 14a2 2 0 0 1 2-2h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V14z"></path><path d="M20.4 10.4C20.2 6.5 17 3.5 13 3.1"></path><path d="M6.5 5.5A9 9 0 0 0 3 12"></path></svg></span>`;
            } else if (remoteIsMuted) {
                statusContainer.innerHTML = `<span class="text-red-500"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path></svg></span>`;
            }

            label.appendChild(nameSpan);
            label.appendChild(statusContainer);

            const volControls = document.createElement('div');
            volControls.id = `vol-controls-${userId}`;
            volControls.className = 'volume-controls z-30';

            const mainVolRow = document.createElement('div');
            mainVolRow.className = 'vol-row';
            mainVolRow.id = `vol-row-main-${userId}`;
            mainVolRow.innerHTML = `
                <button class="text-white hover:text-blue-400" onclick="toggleMute('${userId}', 'main')" id="mute-main-${userId}">
                    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${savedVol === 0 ? '<polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><line x1="23" y1="9" x2="17" y2="15"></line><line x1="17" y1="9" x2="23" y2="15"></line>' : '<polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><path d="M19.07 4.93a10 10 0 0 1 0 14.14M15.54 8.46a5 5 0 0 1 0 7.07"></path>'}</svg>
                </button>
                <input type="range" min="0" max="1" step="0.05" value="${savedVol}" oninput="setVolume('${userId}', 'main', this.value)">
            `;
            if (savedVol === 0) {
                const btn = mainVolRow.querySelector("button");
                if (btn) btn.classList.add("text-red-500");
            }
            volControls.appendChild(mainVolRow);

            const fsBtn = document.createElement('button');
            fsBtn.className = 'fs-btn absolute top-3 right-3 p-2 rounded-lg bg-black/40 hover:bg-black/60 text-white backdrop-blur-md transition-all opacity-0 group-hover:opacity-100 scale-95 hover:scale-100 z-30';
            fsBtn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/></svg>';
            fsBtn.onclick = () => toggleFullscreen(userId);
            fsBtn.title = "Toggle Fullscreen";

            const onFullscreenChange = () => {
                if (document.fullscreenElement === container) {
                    fsBtn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3v3a2 2 0 0 1-2 2H3m18 0h-3a2 2 0 0 1-2-2V3m0 18v-3a2 2 0 0 1 2-2h3"/></svg>';
                    fsBtn.classList.add('bg-indigo-600');
                } else {
                    fsBtn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/></svg>';
                    fsBtn.classList.remove('bg-indigo-600');
                }
            };
            // fullscreenchange is dispatched on Document, not the button.
            document.addEventListener('fullscreenchange', onFullscreenChange);
            container._fullscreenChangeHandler = onFullscreenChange;

            container.dataset.userId = userId;

            setupSmoothDragAndDrop(container);

            container.appendChild(vid);
            container.appendChild(screenVid);
            container.appendChild(avatarLayer);
            container.appendChild(label);
            container.appendChild(volControls);
            container.appendChild(fsBtn);

            const remoteGrid = document.getElementById('remoteGrid');
            if (remoteGrid) {
                remoteGrid.appendChild(container);
                checkEmpty();
            } else {
                console.error('remoteGrid not found!');
            }

        }

        function initPeer(userId, initiator, nickname, avatarUrl, isMuted, remoteIsDeafened, isGif, staticFrame) {
            if (peers[userId]) return;

            // Same fallback the owner's own UI uses: an unset nickname is
            // "Guest" for everyone, never a name only one side shows.
            const displayName = nickname || 'Guest';

            const pc = new RTCPeerConnection(rtcConfig);
            peers[userId] = pc;

            if (localStream) {
                localStream.getAudioTracks().forEach(track => {
                    pc._micAudioSender = pc.addTrack(track, localStream);
                });
                const cameraTrack = localStream.getVideoTracks()[0];
                if (cameraTrack) addCameraTrackToPeer(pc, cameraTrack);
            }

            if (screenStream) {
                const screenTrack = screenStream.getVideoTracks()[0];
                if (screenTrack) addScreenTrackToPeer(pc, screenTrack);

                const screenAudioTrack = screenStream.getAudioTracks()[0];
                if (screenAudioTrack) {
                    const sender = pc.addTrack(screenAudioTrack, screenStream);
                    pc._screenAudioSender = sender;
                    const params = sender.getParameters();
                    if (!params.encodings) params.encodings = [{}];
                    params.encodings[0].maxBitrate = 512000;
                    sender.setParameters(params).catch(e => console.warn(e));
                }
            }

            if ((!localStream || localStream.getVideoTracks().length === 0) && !screenStream) {
                 pc.addTransceiver('video', { direction: 'recvonly' });
            }

            if (!localStream || localStream.getAudioTracks().length === 0 || isDeafened) {
                 pc.addTransceiver('audio', { direction: 'recvonly' });
            }

            createPeerUI(userId, displayName, avatarUrl, remoteIsDeafened, isMuted, isGif, staticFrame);
            updatePeerInfo(userId, displayName, avatarUrl, isMuted, remoteIsDeafened, isGif, staticFrame);

            pc.ontrack = (event) => {

                if (peers[userId] !== pc) {
                    return;
                }

                let container = document.getElementById(`wrapper-${userId}`);
                let vid = document.getElementById(`vid-${userId}`);
                let screenVid = document.getElementById(`screen-${userId}`);

                if (!container || !vid || !screenVid) {
                    createPeerUI(userId, displayName, avatarUrl, remoteIsDeafened, isMuted, isGif, staticFrame);
                    container = document.getElementById(`wrapper-${userId}`);
                    vid = document.getElementById(`vid-${userId}`);
                    screenVid = document.getElementById(`screen-${userId}`);
                }

                if (!vid || !vid.srcObject || !screenVid || !screenVid.srcObject) {
                    console.error('[ontrack] Video element or srcObject is null for', userId);
                    return;
                }

                const volControls = document.getElementById(`vol-controls-${userId}`);
                const mainStream = vid.srcObject;

                if (event.track.kind === 'video') {
                     const hintedScreenId = peerScreenVideoTrackId[userId];
                     const hintedCameraId = peerCamVideoTrackId[userId];
                     const cameraAlreadyAttached = !!getLiveVideoTrack(vid);
                     const matchesCameraHint = !!hintedCameraId && event.track.id === hintedCameraId;
                     const isScreenTrack = (!!hintedScreenId && event.track.id === hintedScreenId) ||
                         (!matchesCameraHint && peerScreenStatus[userId] === true &&
                          (peerCamStatus[userId] !== true || cameraAlreadyAttached));
                     const targetVideo = isScreenTrack ? screenVid : vid;
                     const targetStream = targetVideo.srcObject;

                     targetStream.getVideoTracks().forEach(track => targetStream.removeTrack(track));
                     targetStream.addTrack(event.track);
                     targetVideo.play().then(() => {
                         if (!isScreenTrack) {
                             const savedVolume = getVolumeSettings(userId, 'main');
                             if (vid.volume !== savedVolume) vid.volume = savedVolume;
                         }
                     }).catch(error => console.error('Remote play err', error));

                     const refreshVideoLayout = () => updatePeerVideoLayout(userId);
                     event.track.onmute = refreshVideoLayout;
                     event.track.onunmute = refreshVideoLayout;
                     event.track.onended = () => {
                         if (targetVideo.srcObject) targetVideo.srcObject.removeTrack(event.track);
                         updatePeerVideoLayout(userId);
                     };
                     updatePeerVideoLayout(userId);
                }

                if (event.track.kind === 'audio') {

                    const existingTracks = mainStream.getAudioTracks();
                    const trackAlreadyExists = existingTracks.some(t => t.id === event.track.id);

                    if (trackAlreadyExists) {

                        return;
                    }

                    const hintedMicTrackId = peerMicTrackId[userId];
                    const hintedScreenTrackId = peerScreenAudioTrackId[userId];
                    const isHintedScreenTrack = !!hintedScreenTrackId && event.track.id === hintedScreenTrackId;
                    const isHintedMicTrack = !!hintedMicTrackId && event.track.id === hintedMicTrackId;

                    if (isHintedScreenTrack && !isHintedMicTrack) {
                        peerScreenHasAudio[userId] = true;
                        const savedScreenVol = getVolumeSettings(userId, 'screen');

                        let screenAud = document.getElementById(`aud-screen-${userId}`);
                        if (!screenAud) {
                            screenAud = document.createElement('audio');
                            screenAud.id = `aud-screen-${userId}`;
                            screenAud.autoplay = true;
                            attachSinkId(screenAud, currentAudioOutputId);
                            screenAud.volume = savedScreenVol;
                            container.appendChild(screenAud);
                        }

                        const screenStream = new MediaStream([event.track]);
                        screenAud.srcObject = screenStream;
                        if (isDeafened) screenAud.muted = true;

                        if (!document.getElementById(`vol-row-screen-${userId}`)) {
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

                        (async () => { await setupAudioMonitor(screenStream, `wrapper-${userId}`); })();
                        event.track.onended = () => {
                            const row = document.getElementById(`vol-row-screen-${userId}`);
                            if (row) row.remove();
                            const aud = document.getElementById(`aud-screen-${userId}`);
                            if (aud) aud.remove();
                        };
                        return;
                    }

                    if (mainStream.getAudioTracks().length === 0 || isHintedMicTrack) {
                        mainStream.addTrack(event.track);
                        const sv = getVolumeSettings(userId, 'main');
                        if (vid.volume !== sv) vid.volume = sv;
                        (async () => { await setupAudioMonitor(mainStream, `wrapper-${userId}`); })();

                    } else {

                        peerScreenHasAudio[userId] = true;

                        const savedScreenVol = getVolumeSettings(userId, 'screen');

                        let screenAud = document.getElementById(`aud-screen-${userId}`);
                        if (!screenAud) {
                            screenAud = document.createElement('audio');
                            screenAud.id = `aud-screen-${userId}`;
                            screenAud.autoplay = true;
                            attachSinkId(screenAud, currentAudioOutputId);
                            screenAud.volume = savedScreenVol;
                            container.appendChild(screenAud);
                        }

                        const screenStream = new MediaStream([event.track]);
                        screenAud.srcObject = screenStream;
                        if (isDeafened) screenAud.muted = true;

                        if (!document.getElementById(`vol-row-screen-${userId}`)) {
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

                        (async () => { await setupAudioMonitor(screenStream, `wrapper-${userId}`); })();

                        event.track.onended = () => {
                            screenAud.remove();
                            const row = document.getElementById(`vol-row-screen-${userId}`);
                            if (row) row.remove();
                        };
                    }
                }

                if (event.track.kind === 'video') {
                     vid.onloadedmetadata = () => updatePeerVideoLayout(userId);
                     screenVid.onloadedmetadata = () => updatePeerVideoLayout(userId);
                }

                if (!container.dataset.interval) {
                    const intId = setInterval(() => updatePeerVideoLayout(userId), 1000);
                    container.dataset.interval = intId;
                }
            };

            pc.onicecandidate = (event) => {
                if (event.candidate) {
                    sendSignal(userId, { type: 'candidate', candidate: event.candidate });
                }
            };

            pc.oniceconnectionstatechange = () => {
                const state = pc.iceConnectionState;
                console.log(`ICE connection state for ${userId.substr(0,4)}: ${state}`);

                if (state === 'failed' || state === 'disconnected' || state === 'closed') {
                    console.warn(`Peer ${userId.substr(0,4)} connection ${state}`);

                    updateConnectionStatus();
                } else if (state === 'connected') {
                    updateConnectionStatus();
                }
            };

            pc.onconnectionstatechange = () => {
                const state = pc.connectionState;
                console.log(`Connection state for ${userId.substr(0,4)}: ${state}`);

                if (state === 'disconnected') {

                    console.warn(`Peer ${userId.substr(0,4)} temporarily disconnected, waiting for recovery...`);
                    updateConnectionStatus();

                    if (initiator) {
                        setTimeout(() => {
                            if (peers[userId] === pc && pc.connectionState === 'disconnected') {
                                console.log(`Triggering ICE restart for ${userId.substr(0,4)}`);
                                negotiate(userId, pc, true);
                            }
                        }, 2000);
                    }

                    if (!pc._disconnectTimeout) {
                        pc._disconnectTimeout = setTimeout(() => {
                            if (peers[userId] === pc && pc.connectionState === 'disconnected') {
                                console.warn(`Peer ${userId.substr(0,4)} did not recover, removing...`);
                                removePeer(userId);
                            }
                            pc._disconnectTimeout = null;
                        }, 15000);
                    }
                } else if (state === 'failed' || state === 'closed') {

                    if (pc._disconnectTimeout) {
                        clearTimeout(pc._disconnectTimeout);
                        pc._disconnectTimeout = null;
                    }
                    console.warn(`Peer ${userId.substr(0,4)} connection ${state}, removing...`);
                    removePeer(userId);
                } else if (state === 'connected') {

                    if (pc._disconnectTimeout) {
                        clearTimeout(pc._disconnectTimeout);
                        pc._disconnectTimeout = null;
                        console.log(`Peer ${userId.substr(0,4)} reconnected successfully`);
                    }
                    const _vid = document.getElementById(`vid-${userId}`);
                    if (_vid) {
                        const sv = getVolumeSettings(userId, 'main');
                        if (_vid.volume !== sv) _vid.volume = sv;
                    }
                    const _screenAud = document.getElementById(`aud-screen-${userId}`);
                    if (_screenAud) {
                        const ssv = getVolumeSettings(userId, 'screen');
                        if (_screenAud.volume !== ssv) _screenAud.volume = ssv;
                    }
                    updateConnectionStatus();
                }
            };

            if (initiator) {
                negotiate(userId, pc);
            }
        }

        async function flushPendingCandidates(userId, pc) {
            if (!pendingCandidates[userId] || pendingCandidates[userId].length === 0) return;
            const candidates = pendingCandidates[userId].splice(0);
            for (const candidate of candidates) {
                try {
                    await pc.addIceCandidate(new RTCIceCandidate(candidate));
                } catch (e) {
                    console.warn("Failed to flush buffered ICE candidate for", userId.substr(0,4), e);
                }
            }
        }

        async function handleSignal(userId, data) {
            if (!peers[userId]) initPeer(userId, false, undefined, null);
            const pc = peers[userId];
            if (!pc || pc.connectionState === 'closed' || pc.connectionState === 'failed') return; // peer is dead

            try {
                if (data.type === 'offer') {
                    if (pc.signalingState !== 'stable' && pc.signalingState !== 'have-local-offer') {
                        console.warn(`Ignoring offer from ${userId.substr(0,4)} in state ${pc.signalingState}`);
                        return;
                    }
                    pendingCandidates[userId] = [];
                    await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
                    if (!peers[userId] || peers[userId] !== pc) return; // peer was removed during async op
                    await flushPendingCandidates(userId, pc);
                    if (!peers[userId] || peers[userId] !== pc) return;
                    const answer = await pc.createAnswer();
                    answer.sdp = forceStereoAudio(answer.sdp);
                    await pc.setLocalDescription(answer);
                    sendSignal(userId, { type: 'answer', sdp: answer });
                } else if (data.type === 'answer') {
                    if (pc.signalingState !== 'have-local-offer') {
                        console.warn(`Ignoring answer from ${userId.substr(0,4)} in state ${pc.signalingState}`);
                        return;
                    }
                    await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
                    if (!peers[userId] || peers[userId] !== pc) return; // peer was removed during async op
                    await flushPendingCandidates(userId, pc);
                } else if (data.type === 'candidate') {
                    if (!pc.remoteDescription || !pc.remoteDescription.type) {
                        if (!pendingCandidates[userId]) pendingCandidates[userId] = [];
                        if (pendingCandidates[userId].length < MAX_PENDING_ICE_CANDIDATES) {
                            pendingCandidates[userId].push(data.candidate);
                        } else {
                            console.warn('Dropping excessive buffered ICE candidate for', userId.substr(0, 4));
                        }
                        return;
                    }
                    await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
                }
            } catch (e) {
                console.error("Signaling error", e);
            }
        }

        function removePeer(userId) {
            cleanupAudioMonitor(`wrapper-${userId}`);
            if (typeof stopGifFrameAnimation === 'function') {
                stopGifFrameAnimation(`wrapper-${userId}`);
                stopGifFrameAnimation(`wrapper-${userId}-sidebar`);
            }

            if (peers[userId]) {
                try {
                    peers[userId].getReceivers().forEach(receiver => {
                        if (receiver.track) {
                            receiver.track.onmute = null;
                            receiver.track.onunmute = null;
                            receiver.track.onended = null;
                        }
                    });
                } catch(e) {}

                if (peers[userId]._disconnectTimeout) {
                    clearTimeout(peers[userId]._disconnectTimeout);
                    peers[userId]._disconnectTimeout = null;
                }
                peers[userId].close();
                delete peers[userId];
            }

            const vid = document.getElementById(`vid-${userId}`);
            const screenVid = document.getElementById(`screen-${userId}`);
            [vid, screenVid].forEach(video => {
                if (!video) return;
                video.pause();
                if (video.srcObject) {
                    try {
                        video.srcObject.getTracks().forEach(track => track.stop());
                    } catch(e) {}
                    video.srcObject = null;
                }
            });

            const el = document.getElementById(`wrapper-${userId}`);
            if (el) {
                const intervalId = Number(el.dataset.interval);
                if (Number.isFinite(intervalId) && intervalId > 0) {
                    clearInterval(intervalId);
                }
                if (el._fullscreenChangeHandler) {
                    document.removeEventListener('fullscreenchange', el._fullscreenChangeHandler);
                }
                el.remove();
            }

            const screenAud = document.getElementById(`aud-screen-${userId}`);
            if (screenAud) {
                screenAud.pause();
                screenAud.srcObject = null;
                screenAud.remove();
            }
            const volRow = document.getElementById(`vol-row-screen-${userId}`);
            if (volRow) volRow.remove();

            const sidebarAvatar = document.querySelector(`.room-user-row[data-user-id="${userId}"] .mini-avatar`);
            if (sidebarAvatar) sidebarAvatar.classList.remove('speaking-glow');

            delete peerMicTrackId[userId];
            delete peerScreenAudioTrackId[userId];
            delete peerCamVideoTrackId[userId];
            delete peerScreenVideoTrackId[userId];
            delete pendingCandidates[userId];
            checkEmpty();
        }

        function sendSignal(toId, data) {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'signal', target: toId, data: data }));
            }
        }

        window.toggleFullscreen = function(userId) {
            const el = document.getElementById(`wrapper-${userId}`);
            if (!el) return;

            const isFullscreen = document.fullscreenElement || document.webkitFullscreenElement || document.mozFullScreenElement || document.msFullscreenElement;

            if (!isFullscreen) {
                const vid = document.getElementById(`vid-${userId}`);

                if (el.requestFullscreen) {
                    el.requestFullscreen().catch(err => {
                        console.error(`Error attempting to enable fullscreen: ${err.message}`);
                    });
                } else if (el.webkitRequestFullscreen) {
                    el.webkitRequestFullscreen();
                } else if (el.mozRequestFullScreen) {
                    el.mozRequestFullScreen();
                } else if (el.msRequestFullscreen) {
                    el.msRequestFullscreen();
                } else if (vid && vid.webkitEnterFullscreen) {
                    vid.webkitEnterFullscreen();
                }
            } else {
                if (document.exitFullscreen) {
                    document.exitFullscreen();
                } else if (document.webkitExitFullscreen) {
                    document.webkitExitFullscreen();
                } else if (document.mozCancelFullScreen) {
                    document.mozCancelFullScreen();
                } else if (document.msExitFullscreen) {
                    document.msExitFullscreen();
                }
            }
        };

        window.toggleMute = function(userId, type) {
            let el;
            let btn;

            if (type === 'screen') {
                el = document.getElementById(`aud-screen-${userId}`);
                if (!el) {
                    el = document.getElementById(`vid-${userId}`);
                }
                btn = document.getElementById(`mute-screen-${userId}`);
            } else {
                el = document.getElementById(`vid-${userId}`);
                btn = document.getElementById(`mute-main-${userId}`);
            }

            if (el) {
                el.muted = !el.muted;
                const isMuted = el.muted;

                if (type === 'screen') {
                     if (isMuted) {
                        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="14" rx="2" ry="2"></rect><line x1="23" y1="9" x2="17" y2="15"></line><line x1="17" y1="9" x2="23" y2="15"></line></svg>`;
                        btn.classList.add('text-red-500');
                    } else {
                        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="2" width="16" height="14" rx="2" ry="2"></rect><line x1="12" y1="22" x2="12" y2="16"></line><path d="M5 12h14"></path><path d="M12 12v4"></path></svg>`;
                        btn.classList.remove('text-red-500');
                    }
                } else {
                    if (isMuted) {
                        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><line x1="23" y1="9" x2="17" y2="15"></line><line x1="17" y1="9" x2="23" y2="15"></line></svg>`;
                        btn.classList.add('text-red-500');
                    } else {
                        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon><path d="M19.07 4.93a10 10 0 0 1 0 14.14M15.54 8.46a5 5 0 0 1 0 7.07"></path></svg>`;
                        btn.classList.remove('text-red-500');
                    }
                }
            }
        }

        window.setVolume = function(userId, type, val) {
             let el;
            if (type === 'screen') {
                el = document.getElementById(`aud-screen-${userId}`);
                if (!el) {
                    el = document.getElementById(`vid-${userId}`);
                }
            } else {
                el = document.getElementById(`vid-${userId}`);
            }
            if (el) {
                el.volume = val;
                saveVolumeSettings(userId, type, val);
            }
        }

        function saveVolumeSettings(userId, type, val) {
            sessionStorage.setItem(`rustrooms_vol_${userId}_${type}`, val);
        }

        function getVolumeSettings(userId, type) {
            const val = sessionStorage.getItem(`rustrooms_vol_${userId}_${type}`);
            return val ? parseFloat(val) : 1.0;
        }

        function leaveRoom() {

            hasLeftRoom = true;

            // Reset the tab title when the call ends.
            document.title = 'RustRooms';

            // Release wake lock and stop no sleep video
            if (wakeLock) {
                try {
                    wakeLock.release();
                } catch(e) {}
                wakeLock = null;
            }
            stopNoSleepVideo();

            clearActiveTabSession(false);

            if (statsWindowVisible) {
                toggleStatsWindow();
            }

            playNotificationSound('leave');

            if (localStream) {

                localStream.getVideoTracks().forEach(track => track.stop());
                if (localStream._originalStream) {
                    localStream._originalStream.getVideoTracks().forEach(track => track.stop());
                }

                const aTracks = localStream.getAudioTracks();
                aTracks.forEach(t => t.enabled = false);
                const origATracks = localStream._originalStream ? localStream._originalStream.getAudioTracks() : [];
                origATracks.forEach(t => t.enabled = false);

                setTimeout(() => {
                    aTracks.forEach(track => track.stop());
                    origATracks.forEach(track => track.stop());
                }, 800);

                localStream = null;
            }

            if (screenStream) {
                const sTracks = screenStream.getTracks();
                setTimeout(() => {
                    sTracks.forEach(track => track.stop());
                }, 800);
                screenStream = null;
            }

            Object.keys(peers).forEach(userId => {
                if (peers[userId]) {
                    peers[userId].close();
                    delete peers[userId];
                }

                const vid = document.getElementById(`vid-${userId}`);
                if (vid) {
                    vid.pause();
                    vid.srcObject = null;
                }

                const screenAud = document.getElementById(`aud-screen-${userId}`);
                if (screenAud) {
                    screenAud.pause();
                    screenAud.srcObject = null;
                    screenAud.remove();
                }

                const volRowScreen = document.getElementById(`vol-row-screen-${userId}`);
                if (volRowScreen) volRowScreen.remove();

                const el = document.getElementById(`wrapper-${userId}`);
                if (el) el.remove();
            });

            if (ws) {
                ws.onclose = null;
                ws.onerror = null;
                ws.close();
                ws = null;
            }

            if (audioContext && audioContext.state !== 'closed') {
                setTimeout(() => {
                    if (audioContext && audioContext.state !== 'closed') {
                        audioContext.close().catch(e => console.error('Error closing audio context:', e));
                        audioContext = null;
                    }
                }, 800);
            }

            const mainApp = document.querySelector('main');
            const taskbar = document.querySelector('.taskbar');
            const sidebar = document.getElementById('roomSidebar');
            const overlay = document.getElementById('sidebarOverlay');

            if (sidebar) {
                sidebar.style.transition = 'none';
                sidebar.classList.remove('open');
            }
            if (overlay) overlay.classList.remove('open');
            document.body.classList.remove('sidebar-open');

            sessionStorage.setItem('rustrooms_welcomed', 'false');
            sessionStorage.setItem('rustrooms_setup_done', 'false');
            sessionStorage.removeItem('rustrooms_last_room_id');
            sessionStorage.removeItem('rustrooms_last_channel_id');

            roomId = '';
            channelId = '';
            if (window.location.pathname !== '/') {
                history.pushState(null, "", "/");
            }

            const inviteOverlay = document.getElementById('inviteWelcomeOverlay');

            if (roomId) {
                updateInviteOverlay();
                hideWelcome();
            } else {
                showWelcome();
                if (inviteOverlay) {
                    inviteOverlay.classList.add('hidden', 'opacity-0');
                }
            }
            if (mainApp) mainApp.style.display = 'none';
            if (taskbar) taskbar.style.display = 'none';

            // Hide the On-the-go overlay visually, but preserve the setting
            const otgOverlay = document.getElementById('onTheGoOverlay');
            if (otgOverlay) {
                otgOverlay.classList.add('hidden');
                // Unlock screen orientation
                if (screen.orientation && screen.orientation.unlock) {
                    try {
                        screen.orientation.unlock();
                    } catch(e) {}
                }
            }

            // Reset AudioWorklet cached promise to support new AudioContext
            workletLoadingPromise = null;

            // Reset reconnection counters
            reconnectionAttempts = 0;
            desktopSlowRetryCount = 0;

            // Reset speaker tracking
            activeSpeakers = {};
            peerLowBandwidthStatus = {};
            peerOnTheGoStatus = {};

            // Hide/reset status pill elements to pristine states
            const reconnectBtn = document.getElementById('btnReconnect');
            if (reconnectBtn) reconnectBtn.classList.add('hidden');
            const otgReconnectBtn = document.getElementById('onTheGoBtnReconnect');
            if (otgReconnectBtn) otgReconnectBtn.classList.add('hidden');

            const pingContainer = document.getElementById('pingContainer');
            if (pingContainer) pingContainer.classList.add('hidden');
            const otgPingContainer = document.getElementById('onTheGoPingContainer');
            if (otgPingContainer) otgPingContainer.classList.add('hidden');

            const lightning = document.getElementById('lowBandwidthLightning');
            if (lightning) lightning.classList.add('hidden');
            const otgLightning = document.getElementById('onTheGoLowBandwidthLightning');
            if (otgLightning) otgLightning.classList.add('hidden');

            statusText.innerText = 'Waiting...';
            connectionDot.className = 'connection-dot';
            const otgStatusText = document.getElementById('onTheGoStatusText');
            if (otgStatusText) otgStatusText.innerText = 'Connected';
            const otgDot = document.getElementById('onTheGoConnectionDot');
            if (otgDot) otgDot.className = 'connection-dot';

            sessionStorage.removeItem('rustrooms_setup_done');
            sessionStorage.removeItem('rustrooms_last_room_id');
            sessionStorage.removeItem('rustrooms_last_channel_id');
            history.replaceState(null, '', '/');
        }

        function toggleMic() {
            if (!localStream) return;
            const tracks = localStream.getAudioTracks();
            if (tracks.length > 0) {
                const track = tracks[0];

                if (isDeafened) {

                    return;
                }

                track.enabled = !track.enabled;
                const btn = document.getElementById('btnMic');
                if (!track.enabled) {
                    playNotificationSound('mute');
                    btn.classList.add('active-red');
                    btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path><line x1="12" x2="12" y1="19" y2="22"></line></svg>`;
                } else {
                    playNotificationSound('unmute');
                    btn.classList.remove('active-red');
                    btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3Z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" x2="12" y1="19" y2="22"/></svg>`;
                }
                updateLocalLabel();
                updateOnTheGoButtons();

                if (track.enabled) {
                    const screenAudioTrack = screenStream?.getAudioTracks()[0];
                    for (const userId in peers) {
                        const pc = peers[userId];
                        const senders = pc.getSenders();
                        let micSender = null;

                        for (const sender of senders) {
                            if (sender.track && sender.track.kind === 'audio') {
                                const isScreenAudio = screenAudioTrack && sender.track.id === screenAudioTrack.id;
                                if (!isScreenAudio) {
                                    micSender = sender;
                                    break;
                                }
                            }
                        }

                        if (micSender) {
                            micSender.replaceTrack(track).catch(() => {});
                            if (micSender.transceiver && (micSender.transceiver.direction === 'recvonly' || micSender.transceiver.direction === 'inactive')) {
                                micSender.transceiver.direction = 'sendrecv';
                            }
                        } else {
                            let attachedToNullSender = false;
                            for (const sender of senders) {
                                if (!sender.track || sender.track === null) {
                                    sender.replaceTrack(track).catch(() => {});
                                    if (sender.transceiver && (sender.transceiver.direction === 'recvonly' || sender.transceiver.direction === 'inactive')) {
                                        sender.transceiver.direction = 'sendrecv';
                                    }
                                    attachedToNullSender = true;
                                    break;
                                }
                            }
                            if (!attachedToNullSender) {
                                pc.addTrack(track, localStream);
                            }
                        }

                        negotiate(userId, pc);
                    }
                }

                savePreferences();

                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'update-user',
                        data: {
                            isMuted: !track.enabled,
                            isDeafened: isDeafened,
                            profileRev: profileRev,
                            micTrackId: track ? track.id : null,
                            screenAudioTrackId: screenStream ? (screenStream.getAudioTracks()[0]?.id || null) : null
                        }
                    }));
                }
            }
        }

        function toggleDeafen() {
            isDeafened = !isDeafened;
            const btn = document.getElementById('btnDeafen');
            const btnMic = document.getElementById('btnMic');

            const deafenOnSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 18v-6a9 9 0 0 1 18 0v6"></path><path d="M21 19a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3zM3 19a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2H3z"></path></svg>`;
            const deafenOffSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 14a2 2 0 0 0-2-2h-3a2 2 0 0 0-2 2v3a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2V14z"></path><path d="M3 14a2 2 0 0 1 2-2h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V14z"></path><path d="M20.4 10.4C20.2 6.5 17 3.5 13 3.1"></path><path d="M6.5 5.5A9 9 0 0 0 3 12"></path></svg>`;

            const micAudioTrack = localStream?.getAudioTracks()[0];
            const screenAudioTrack = screenStream?.getAudioTracks()[0];

            if (isDeafened) {
                playNotificationSound('mute');
                btn.classList.add('active-red');
                btn.innerHTML = deafenOffSvg;

                if (micAudioTrack && micAudioTrack.enabled) {
                    btn.dataset.micWasEnabled = 'true';
                }

                if (btnMic) {
                    btnMic.disabled = true;

                    if (micAudioTrack && micAudioTrack.enabled) {
                        btnMic.classList.add('active-red');
                        btnMic.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path><line x1="12" x2="12" y1="19" y2="22"></line></svg>`;
                    }
                }

                if (micAudioTrack) {
                    micAudioTrack.enabled = false;
                }

                document.querySelectorAll('video, audio').forEach(el => {
                    if (el.id !== 'localVideo' && el.id !== 'previewVideo') {
                        el.dataset.wasMuted = el.muted;
                        el.muted = true;
                    }
                });
            } else {
                playNotificationSound('unmute');
                btn.classList.remove('active-red');
                btn.innerHTML = deafenOnSvg;

                if (btnMic) {
                    btnMic.disabled = false;
                }

                const shouldEnableMic = btn.dataset.micWasEnabled === 'true';

                if (micAudioTrack && shouldEnableMic) {
                    micAudioTrack.enabled = true;

                    if (btnMic) {
                        btnMic.classList.remove('active-red');
                        btnMic.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3Z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" x2="12" y1="19" y2="22"/></svg>`;
                    }
                    delete btn.dataset.micWasEnabled;
                }

                if (micAudioTrack) {
                    for (const userId in peers) {
                        const pc = peers[userId];
                        const senders = pc.getSenders();
                        let changed = false;
                        let micSender = null;
                        for (const s of senders) {
                            if (s.track && s.track.kind === 'audio') {
                                const isScreenAudio = screenAudioTrack && s.track.id === screenAudioTrack.id;
                                if (!isScreenAudio) {
                                    micSender = s;
                                    break;
                                }
                            }
                        }

                        if (micSender) {
                            if (micSender.track !== micAudioTrack) {
                                micSender.replaceTrack(micAudioTrack).catch(() => {});
                                changed = true;
                            }
                        } else {
                            let nullSenderFound = false;
                            for (const s of senders) {
                                if (!s.track || s.track === null) {
                                    s.replaceTrack(micAudioTrack).catch(() => {});
                                    nullSenderFound = true;
                                    changed = true;
                                    break;
                                }
                            }

                            if (!nullSenderFound) {
                                pc.addTrack(micAudioTrack, localStream);
                                changed = true;
                            }
                        }

                        if (changed) {
                            negotiate(userId, pc);
                        }
                    }
                }

                document.querySelectorAll('video, audio').forEach(el => {
                    if (el.id !== 'localVideo' && el.id !== 'previewVideo') {
                        el.muted = el.dataset.wasMuted === 'true';
                    }
                });
            }

            updateLocalLabel();

            savePreferences();

            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'update-user',
                    data: {
                        isMuted: isDeafened || !micAudioTrack || !micAudioTrack.enabled,
                        isDeafened: isDeafened,
                        profileRev: profileRev,
                        micTrackId: micAudioTrack ? micAudioTrack.id : null,
                        screenAudioTrackId: screenAudioTrack ? screenAudioTrack.id : null
                    }
                }));
            }
            updateOnTheGoButtons();
        }

        let camToggleInProgress = false;

        async function toggleCam() {
            if (camToggleInProgress || !isCameraReady) return;

            const btn = document.getElementById('btnCam');
            if (!localStream) return;

            camToggleInProgress = true;
            isCameraReady = false;
            btn.disabled = true;

            try {
                let tracks = localStream.getVideoTracks();

                let trackIsBroken = false;
                if (tracks.length > 0) {
                    const track = tracks[0];
                    if (track.readyState === 'ended' || track.muted) {
                        trackIsBroken = true;
                        console.warn("Camera track is broken, cleaning up");
                        track.stop();
                        localStream.removeTrack(track);
                        tracks = [];
                    }
                }

                if (tracks.length === 0 || trackIsBroken) {

                    btn.innerHTML = `<svg class="spinner" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>`;

                    try {
                        const camVideoConstraints = currentVideoInputId ? { deviceId: { exact: currentVideoInputId } } : { facingMode: currentFacingMode };
                        const newStream = await navigator.mediaDevices.getUserMedia({ video: camVideoConstraints });
                        if (isUnloading) {
                            newStream.getTracks().forEach(t => t.stop());
                            return;
                        }
                        const newTrack = newStream.getVideoTracks()[0];

                        if (!newTrack || newTrack.readyState !== 'live') {
                            console.warn("Camera track not properly initialized, retrying...");
                            newTrack?.stop();
                            await new Promise(r => setTimeout(r, 100));
                            const retryStream = await navigator.mediaDevices.getUserMedia({ video: camVideoConstraints });
                            if (isUnloading) {
                                retryStream.getTracks().forEach(t => t.stop());
                                return;
                            }
                            const retryTrack = retryStream.getVideoTracks()[0];
                            if (retryTrack) {                                retryTrack.enabled = true;
                                localStream.addTrack(retryTrack);
                                retryStream.getTracks().forEach(t => { if (t !== retryTrack) t.stop(); });
                            }
                        } else {
                            newTrack.enabled = true;
                            localStream.addTrack(newTrack);
                        }

                        tracks = localStream.getVideoTracks();
                        const activeCameraTrack = tracks[0];
                        if (!activeCameraTrack) throw new Error('Could not initialize camera track');

                        for (const userId in peers) {
                            const pc = peers[userId];
                            const negotiationNeeded = addCameraTrackToPeer(pc, activeCameraTrack);
                            if (negotiationNeeded) negotiate(userId, pc);
                        }

                        btn.classList.remove('active-red');
                        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 4h-5L7 7H4a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2h-3l-2.5-3z"/><circle cx="12" cy="13" r="3"/></svg>`;

                        const localVideo = document.getElementById('localVideo');
                        if (localVideo) {
                            localVideo.srcObject = null;
                            localVideo.srcObject = localStream;
                        }

                        if (ws && ws.readyState === WebSocket.OPEN) {
                            ws.send(JSON.stringify({
                                type: 'cam-toggle',
                                data: { enabled: true, camVideoTrackId: activeCameraTrack.id }
                            }));
                        }

                        pendingCamToggle = false;

                        updateLocalAvatar();
                        updateLocalMediaLayout();
                        savePreferences();
                        return;
                    } catch (e) {
                        console.error("Could not add camera", e);
                        alert("Could not access camera. Please check permissions.");
                        btn.classList.add('active-red');
                        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 4h-5L7 7H4a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2h-3l-2.5-3z"/><circle cx="12" cy="13" r="3"/></svg>`;
                        updateLocalAvatar();
                        return;
                    }
                }

                const track = tracks[0];

                if (track.enabled) {

                    for (const userId in peers) {
                        const pc = peers[userId];
                        if (removePeerSender(pc, '_cameraVideoSender')) {
                            negotiate(userId, pc);
                        }
                    }

                    track.stop();
                    localStream.removeTrack(track);

                    const localVideo = document.getElementById('localVideo');
                    if (localVideo) {
                        localVideo.srcObject = null;
                    }
                    if (previewVideo) {
                        previewVideo.srcObject = null;
                    }

                    btn.classList.add('active-red');
                    btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 21l-3.5-3.5m-2-2l-2-2m-2-2l-2-2m-2-2l-3.5-3.5"></path><path d="M15 7h5a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-5"></path><path d="M4 8v8a2 2 0 0 0 2 2h4.5"></path></svg>`;

                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({
                            type: 'cam-toggle',
                            data: { enabled: false, camVideoTrackId: null }
                        }));
                    }

                    pendingCamToggle = true;
                    updateLocalMediaLayout();
                } else {

                    btn.classList.remove('active-red');
                    btn.innerHTML = `<svg class="spinner" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>`;

                    try {
                        const oldTrack = localStream.getVideoTracks()[0];
                        if (oldTrack) {
                            oldTrack.stop();
                            localStream.removeTrack(oldTrack);
                        }

                        const camVideoConstraints = currentVideoInputId ? { deviceId: { exact: currentVideoInputId } } : { facingMode: currentFacingMode };
                        const newStream = await navigator.mediaDevices.getUserMedia({ video: camVideoConstraints });
                        if (isUnloading) {
                            newStream.getTracks().forEach(t => t.stop());
                            return;
                        }
                        const newTrack = newStream.getVideoTracks()[0];

                        if (!newTrack || newTrack.readyState !== 'live') {
                            console.warn("Camera track not properly initialized, retrying...");
                            newTrack?.stop();
                            await new Promise(r => setTimeout(r, 100));
                            const retryStream = await navigator.mediaDevices.getUserMedia({ video: camVideoConstraints });
                            if (isUnloading) {
                                retryStream.getTracks().forEach(t => t.stop());
                                return;
                            }
                            const retryTrack = retryStream.getVideoTracks()[0];
                            if (retryTrack) {                                retryTrack.enabled = true;
                                localStream.addTrack(retryTrack);
                                retryStream.getTracks().forEach(t => { if (t !== retryTrack) t.stop(); });
                            }
                        } else {
                            newTrack.enabled = true;
                            localStream.addTrack(newTrack);
                        }

                        const activeCameraTrack = localStream.getVideoTracks()[0];
                        if (!activeCameraTrack) throw new Error('Could not initialize camera track');
                        for (const userId in peers) {
                            const pc = peers[userId];
                            const negotiationNeeded = addCameraTrackToPeer(pc, activeCameraTrack);
                            if (negotiationNeeded) negotiate(userId, pc);
                        }

                        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 4h-5L7 7H4a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2h-3l-2.5-3z"/><circle cx="12" cy="13" r="3"/></svg>`;

                        const localVideo = document.getElementById('localVideo');
                        if (localVideo) {
                            localVideo.srcObject = null;
                            localVideo.srcObject = localStream;
                        }

                        if (ws && ws.readyState === WebSocket.OPEN) {
                            ws.send(JSON.stringify({
                                type: 'cam-toggle',
                                data: { enabled: true, camVideoTrackId: activeCameraTrack.id }
                            }));
                        }

                        pendingCamToggle = false;
                        updateLocalMediaLayout();
                    } catch (e) {
                        console.error("Could not re-add camera", e);
                        alert("Could not access camera. Please check permissions.");
                        btn.classList.add('active-red');
                        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 21l-3.5-3.5m-2-2l-2-2m-2-2l-2-2m-2-2l-3.5-3.5"></path><path d="M15 7h5a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-5"></path><path d="M4 8v8a2 2 0 0 0 2 2h4.5"></path></svg>`;
                    }
                }

                updateLocalAvatar();
                savePreferences();
            } finally {
                camToggleInProgress = false;
                isCameraReady = true;
                btn.disabled = false;
            }
        }

        let switchCamInProgress = false;

        async function switchCamera() {
            if (switchCamInProgress || !isCameraReady) return;
            if (!localStream) return;

            const videoTrack = localStream.getVideoTracks()[0];
            if (!videoTrack) return;

            switchCamInProgress = true;
            const btn = document.getElementById('btnSwitchCam');
            if (btn) btn.disabled = true;

            try {
                const trackSettings = videoTrack.getSettings();
                const actualFacing = trackSettings.facingMode || currentFacingMode;
                const newFacingMode = actualFacing === 'user' ? 'environment' : 'user';

                const newStream = await navigator.mediaDevices.getUserMedia({
                    video: { facingMode: { exact: newFacingMode } }
                });

                if (isUnloading) {
                    newStream.getTracks().forEach(t => t.stop());
                    return;
                }

                const newTrack = newStream.getVideoTracks()[0];
                if (!newTrack || newTrack.readyState !== 'live') {
                    newTrack?.stop();
                    throw new Error('Could not get camera track');
                }

                localStream.removeTrack(videoTrack);
                videoTrack.stop();
                localStream.addTrack(newTrack);

                for (const userId in peers) {
                    const pc = peers[userId];
                    const negotiationNeeded = addCameraTrackToPeer(pc, newTrack);
                    if (negotiationNeeded) negotiate(userId, pc);
                }

                const localVideoEl = document.getElementById('localVideo');
                if (localVideoEl) {
                    localVideoEl.srcObject = null;
                    localVideoEl.srcObject = localStream;
                }

                currentFacingMode = newTrack.getSettings().facingMode || newFacingMode;
                currentVideoInputId = newTrack.getSettings().deviceId || null;

                const settingsVideo = document.getElementById('settingsVideoSource');
                if (settingsVideo && currentVideoInputId) {
                    if ([...settingsVideo.options].some(o => o.value === currentVideoInputId)) {
                        settingsVideo.value = currentVideoInputId;
                    }
                }

                const setupVideo = document.getElementById('videoSource');
                if (setupVideo && currentVideoInputId) {
                    if ([...setupVideo.options].some(o => o.value === currentVideoInputId)) {
                        setupVideo.value = currentVideoInputId;
                    }
                }

                updateLocalMediaLayout();
                savePreferences();
            } catch (e) {
                console.error("Camera switch failed", e);
            } finally {
                switchCamInProgress = false;
                if (btn) {
                    btn.disabled = false;
                    // Clear focus so the pressed (:active) style doesn't stick
                    // on mobile after the tap.
                    btn.blur();
                }
            }
        }

        async function detectCameras() {
            try {
                const devices = await navigator.mediaDevices.enumerateDevices();
                const videoDevices = devices.filter(d => d.kind === 'videoinput');
                const btnSwitchCam = document.getElementById('btnSwitchCam');
                if (btnSwitchCam && videoDevices.length > 1) {
                    btnSwitchCam.classList.remove('hidden');
                }
            } catch (e) {
                console.warn('Could not enumerate devices for camera detection:', e);
            }
        }

        function isMobileDevice() {
            return isMobileClient;
        }

        async function toggleScreen() {

            if (isMobileDevice()) {
                alert('Screen sharing is not supported on mobile devices.');
                return;
            }

            const btn = document.getElementById('btnShare');

            if (screenStream) {
                const endingScreenStream = screenStream;
                const screenTrack = endingScreenStream.getVideoTracks()[0];
                const screenAudioTrack = endingScreenStream.getAudioTracks()[0];

                // Clear state and UI before stopping tracks. Some browsers fire
                // `ended` synchronously and would otherwise run this path twice.
                screenStream = null;
                endingScreenStream.getTracks().forEach(track => {
                    track.onended = null;
                    track.stop();
                });
                btn.classList.remove('active-green');
                updateLocalMediaLayout();

                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'screen-toggle',
                        data: {
                            enabled: false,
                            hasAudio: false,
                            screenVideoTrackId: null,
                            screenAudioTrackId: null
                        }
                    }));
                }

                for (const userId in peers) {
                    const pc = peers[userId];
                    let shouldNegotiate = removePeerSender(pc, '_screenVideoSender');

                    // Fallback for a connection created before sender roles were
                    // recorded (for example, during a hot reload/reconnect).
                    if (!shouldNegotiate && screenTrack) {
                        const sender = pc.getSenders().find(item => item.track === screenTrack);
                        if (sender) {
                            pc.removeTrack(sender);
                            shouldNegotiate = true;
                        }
                    }

                    if (removePeerSender(pc, '_screenAudioSender')) {
                        shouldNegotiate = true;
                    } else if (screenAudioTrack) {
                        const sender = pc.getSenders().find(item => item.track === screenAudioTrack);
                        if (sender) {
                            pc.removeTrack(sender);
                            shouldNegotiate = true;
                        }
                    }

                    if (shouldNegotiate) {
                        negotiate(userId, pc);
                    }
                }

                updateLocalAvatar();

                if (localStream && localStream.getAudioTracks().length > 0) {
                    await setupAudioMonitor(localStream, 'local');
                }

            } else {
                try {
                    screenStream = await navigator.mediaDevices.getDisplayMedia({
                        video: { cursor: true },
                        systemAudio: "include",
                        audio: {
                            echoCancellation: false,
                            noiseSuppression: false,
                            autoGainControl: false,
                            restrictOwnAudio: true,
                            channelCount: 2,
                            sampleRate: 48000,
                            sampleSize: 16
                        }
                    });
                    if (isUnloading) {
                        screenStream.getTracks().forEach(t => t.stop());
                        return;
                    }
                    const screenTrack = screenStream.getVideoTracks()[0];
                    const screenAudioTrack = screenStream.getAudioTracks()[0];
                    if (!screenTrack) {
                        screenStream.getTracks().forEach(track => track.stop());
                        screenStream = null;
                        throw new Error('The selected source did not provide a screen video track');
                    }

                    if (screenAudioTrack) {
                        screenAudioTrack.contentHint = "music";
                    }

                    updateLocalAvatar();
                    updateLocalMediaLayout();

                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({
                            type: 'screen-toggle',
                            data: {
                                enabled: true,
                                hasAudio: !!screenAudioTrack,
                                screenVideoTrackId: screenTrack.id,
                                screenAudioTrackId: screenAudioTrack ? screenAudioTrack.id : null
                            }
                        }));
                    }

                    for (const userId in peers) {
                        const pc = peers[userId];
                        let shouldNegotiate = addScreenTrackToPeer(pc, screenTrack);

                        if (screenAudioTrack) {
                            let sender = pc.addTrack(screenAudioTrack, screenStream);
                            pc._screenAudioSender = sender;

                            const params = sender.getParameters();
                            if (!params.encodings) params.encodings = [{}];
                            params.encodings[0].maxBitrate = 512000;
                            sender.setParameters(params).catch(e => console.warn(e));

                            shouldNegotiate = true;
                        }

                        if (shouldNegotiate) {
                            negotiate(userId, pc);
                        }
                    }

                    screenTrack.onended = () => {
                        if (screenStream && screenStream.getVideoTracks().includes(screenTrack)) {
                            toggleScreen();
                        }
                    };
                    btn.classList.add('active-green');

                    if (localStream && localStream.getAudioTracks().length > 0) {
                        await setupAudioMonitor(localStream, 'local');
                    }
                } catch (e) {
                    console.error("Screen share failed", e);
                }
            }
        }

        function updateLocalLabel() {
            const label = document.getElementById('localLabel');
            if (!label) return;

            let statusIcons = '';
            if (isDeafened) {
                statusIcons = `<span class="ml-1.5 inline-flex items-center text-red-500" title="Deafened"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 14a2 2 0 0 0-2-2h-3a2 2 0 0 0-2 2v3a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2V14z"></path><path d="M3 14a2 2 0 0 1 2-2h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V14z"></path><path d="M20.4 10.4C20.2 6.5 17 3.5 13 3.1"></path><path d="M6.5 5.5A9 9 0 0 0 3 12"></path></svg></span>`;
            } else {
                const audioTrack = localStream ? localStream.getAudioTracks()[0] : null;
                if (!audioTrack || !audioTrack.enabled) {
                    statusIcons = `<span class="ml-1.5 inline-flex items-center text-red-500" title="Muted"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path></svg></span>`;
                }
            }

            if (isLowBandwidthMode) {
                statusIcons += `
                    <span class="ml-1.5 inline-flex items-center text-amber-500 animate-pulse" title="Low Bandwidth Mode Active">
                        <svg class="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 24 24"><path d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
                    </span>
                `;
            }

            if (isOnTheGoMode) {
                statusIcons += `
                    <span class="ml-1.5 inline-flex items-center text-blue-400" title="On-the-go Mode Active">
                        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="2" width="14" height="20" rx="2" ry="2"></rect><line x1="12" x2="12.01" y1="18" y2="18"></line></svg>
                    </span>
                `;
            }

            label.innerHTML = `<span class="flex items-center">${escapeHtml(userNickname)} (You)${statusIcons}</span>`;
        }

        function fallbackCopyLink() {
            const ta = document.createElement('textarea');
            ta.value = window.location.href;
            ta.style.position = 'fixed';
            ta.style.opacity = '0';
            document.body.appendChild(ta);
            ta.select();
            try { document.execCommand('copy'); } catch (e) { /* clipboard unavailable */ }
            document.body.removeChild(ta);
        }

        function copyLink() {
            // The async clipboard API only exists in secure contexts; on a
            // plain-http LAN address it is undefined and the old call threw,
            // killing the "Copied!" feedback below.
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(window.location.href).catch(() => fallbackCopyLink());
            } else {
                fallbackCopyLink();
            }

            const btn = document.getElementById('btnCopy');
            const otgBtn = document.getElementById('btnOnTheGoCopy');

            if (btn && !btn.classList.contains('copy-btn-copied')) {
                const originalHTML = btn.innerHTML;
                const originalClass = btn.className;

                btn.innerHTML = `<span class="text-xs md:text-sm font-medium text-white">Copied!</span><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>`;
                btn.classList.add('copy-btn-copied');
                btn.classList.remove('hover:bg-slate-700/50');

                setTimeout(() => {
                    btn.innerHTML = originalHTML;
                    btn.className = originalClass;
                }, 2000);
            }

            if (otgBtn && !otgBtn.classList.contains('copy-btn-copied')) {
                const originalHTML = otgBtn.innerHTML;
                const originalClass = otgBtn.className;

                otgBtn.innerHTML = `
                    <div id="onTheGoCopyIconWrapper">
                        <svg class="w-7 h-7" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
                    </div>
                    <span id="onTheGoCopyText">Copied!</span>
                `;
                otgBtn.classList.add('copy-btn-copied');
                otgBtn.classList.remove('bg-zinc-800', 'hover:bg-zinc-700', 'border-zinc-700');

                setTimeout(() => {
                    otgBtn.innerHTML = originalHTML;
                    otgBtn.className = originalClass;
                }, 2000);
            }
        }


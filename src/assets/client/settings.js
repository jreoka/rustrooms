        const settingsOverlay = document.getElementById('settingsOverlay');
        const settingsNicknameInput = document.getElementById('settingsNicknameInput');
        const settingsAvatarInput = document.getElementById('settingsAvatarInput');
        const settingsAvatarPreview = document.getElementById('settingsAvatarPreview');
        const settingsAvatarPlaceholder = document.getElementById('settingsAvatarPlaceholder');
        let newAvatarCandidate = null;
        let newAvatarCandidateIsGif = false;
        let newAvatarCandidateStaticFrame = null;
        let settingsNicknameDebounce = null;

        function handleSettingsNicknameInput() {
            userNickname = settingsNicknameInput.value.trim() || "Guest";
            savePreferences();
            updateLocalLabel();
            if (settingsNicknameDebounce) clearTimeout(settingsNicknameDebounce);
            settingsNicknameDebounce = setTimeout(() => {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: "update-user",
                        data: { nickname: userNickname, profileRev: profileRev }
                    }));
                }
            }, 500);
        }

        async function handleSettingsMicChange(value) {
            currentAudioInputId = value;
            const currentVideoTrack = localStream ? localStream.getVideoTracks()[0] : null;
            const currentVideoId = currentVideoTrack ? currentVideoTrack.getSettings().deviceId : null;
            await switchMediaStream(value, currentVideoId);
            savePreferences();
        }

        async function handleSettingsCamChange(value) {
            currentVideoInputId = value;
            const currentAudioTrack = localStream ? localStream.getAudioTracks()[0] : null;
            const currentAudioId = currentAudioTrack ? currentAudioTrack.getSettings().deviceId : null;
            await switchMediaStream(currentAudioId, value);
            savePreferences();
        }

        async function openSettings() {
            settingsNicknameInput.value = userNickname;
            newAvatarCandidate = userAvatar;
            newAvatarCandidateIsGif = userAvatarIsGif;
            newAvatarCandidateStaticFrame = userAvatarStaticFrame;

            const settingsLBM = document.getElementById('settingsLowBandwidth');
            if (settingsLBM) settingsLBM.checked = isLowBandwidthMode;

            const removeBtn = document.getElementById('btnRemoveSettingsAvatar');
            if (userAvatar) {
                const displaySrc = userAvatarIsGif && userAvatarStaticFrame ? userAvatarStaticFrame : userAvatar;
                settingsAvatarPreview.src = displaySrc;
                settingsAvatarPreview.classList.remove('hidden');
                settingsAvatarPlaceholder.classList.add('hidden');
                if (removeBtn) removeBtn.classList.remove('hidden');
                if (userAvatarIsGif) {
                    loopGifInElement(settingsAvatarPreview, userAvatar);
                }
            } else {
                stopGifPreviewLoop(settingsAvatarPreview);
                settingsAvatarPreview.classList.add('hidden');
                settingsAvatarPlaceholder.classList.remove('hidden');
                if (removeBtn) removeBtn.classList.add('hidden');
            }

            await populateSettingsDeviceList();
            showOverlayAnimated(settingsOverlay);
            initSetupButtonTouchHandlers();
            if (localStream) {
                await setupVolumeMeter(localStream, 'settingsMicBar');
            }
        }

        function closeSettings() {
            hideOverlayAnimated(settingsOverlay);
            if (settingsMeterFrameId) cancelAnimationFrame(settingsMeterFrameId);
        }

        function handleSettingsAvatarUpload(input) {
            const file = input.files[0];
            if (!file) return;

            if (file.size > MAX_AVATAR_UPLOAD_SANITY_BYTES) {
                alert("File is too large! Maximum allowed size is 30MB.");
                input.value = '';
                return;
            }

            const processingEl = document.getElementById('settingsAvatarProcessing');
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
                    openCropModal(dataUrl, 'settings', dataUrl);
                }).catch(err => {
                    console.error("GIF avatar read failed:", err);
                    hideProcessing();
                    showCustomAlert("Image Error", "Could not process this image. Please try a different file.");
                });
            } else {
                showProcessing();
                resizeImageForAvatar(file).then(dataUrl => {
                    hideProcessing();
                    openCropModal(dataUrl, 'settings');
                });
            }
            input.value = '';
        }

        async function saveSettings() {
            userAvatar = newAvatarCandidate;
            userAvatarIsGif = newAvatarCandidateIsGif;
            userAvatarStaticFrame = newAvatarCandidateStaticFrame;
            userAvatarCache[persistentUserId] = {
                avatar: userAvatar,
                isGif: userAvatarIsGif,
                staticFrame: userAvatarStaticFrame
            };
            savePreferences();

            updateLocalAvatar();

            if (ws && ws.readyState === WebSocket.OPEN) {
                 ws.send(JSON.stringify({
                    type: "update-user",
                    data: {
                        nickname: userNickname,
                        profileRev: profileRev,
                        avatar: userAvatar,
                        isGif: userAvatarIsGif,
                        staticFrame: userAvatarStaticFrame
                    }
                }));
            }

        }

        function updateLocalAvatar() {
             const layer = document.getElementById('localAvatarLayer');
             const img = document.getElementById('localAvatarImg');
             const centerImg = document.getElementById('localAvatarCenterImg');
             const placeholder = document.getElementById('localAvatarPlaceholder');

             let camEnabled = false;
             if (localStream) {
                 const videoTrack = localStream.getVideoTracks()[0];
                 if (videoTrack && videoTrack.enabled) camEnabled = true;
             }

             if (screenStream || camEnabled) {
                 if (screenStream) {
                     layer.style.display = 'none';
                 } else {
                    layer.style.display = 'none';
                 }
             } else {
                 layer.style.display = 'flex';
                 if (userAvatar) {
                     const displaySrc = userAvatarIsGif && userAvatarStaticFrame ? userAvatarStaticFrame : userAvatar;
                     img.src = displaySrc;
                     img.classList.remove('hidden');

                     centerImg.src = displaySrc;
                     centerImg.classList.remove('hidden');
                     placeholder.classList.add('hidden');
                 } else {
                     img.classList.add('hidden');
                     centerImg.classList.add('hidden');
                     placeholder.classList.remove('hidden');
                 }
             }
        }

        (function() {
            const pip = document.getElementById('localPipWrapper');
            const topbar = document.querySelector('.app-topbar');
            const taskbar = document.querySelector('.taskbar');
            const connectionDot = document.getElementById('connectionDot');
            const btnCopy = document.getElementById('btnCopy');
            const sidebar = document.getElementById('roomSidebar');

            let isDragging = false;
            let dragOffset = { x: 0, y: 0 };
            let dragBounds = null;
            let pendingFrame = false;
            let collisionRects = null;
            let lastX = 0;
            let lastY = 0;

            function startDrag(clientX, clientY) {
                isDragging = true;
                pip.style.cursor = 'grabbing';
                pip.style.transition = 'none';

                const rect = pip.getBoundingClientRect();
                const topbarRect = topbar.getBoundingClientRect();
                const taskbarRect = taskbar.getBoundingClientRect();
                const sidebarRect = sidebar && sidebar.classList.contains('open') ? sidebar.getBoundingClientRect() : null;

                pip.style.bottom = 'auto';
                pip.style.right = 'auto';
                pip.style.left = rect.left + 'px';
                pip.style.top = rect.top + 'px';

                dragOffset.x = clientX - rect.left;
                dragOffset.y = clientY - rect.top;

                lastX = clientX;
                lastY = clientY;

                let minX = 16;
                let maxX = window.innerWidth - rect.width - 16;
                if (sidebarRect) {
                    minX = sidebarRect.right + 16;
                }

                dragBounds = {
                    minX: minX,
                    maxX: maxX,
                    minY: topbarRect.bottom + 16,
                    maxY: taskbarRect.top - rect.height - 16
                };

                const margin = 16;
                collisionRects = {
                    statusRect: connectionDot && connectionDot.parentElement ? connectionDot.parentElement.getBoundingClientRect() : null,
                    copyRect: btnCopy ? btnCopy.getBoundingClientRect() : null,
                    sidebarRect: sidebarRect,
                    margin: margin,
                    pipWidth: rect.width
                };
            }

            function onMouseDown(e) {
                if (e.target.closest('button') || e.target.closest('input')) return;

                e.preventDefault();

                startDrag(e.clientX, e.clientY);
                document.addEventListener('mousemove', onMouseMove);
                document.addEventListener('mouseup', onMouseUp);
            }

            function onTouchStart(e) {
                if (e.target.closest('button') || e.target.closest('input')) return;

                const touch = e.touches[0];
                startDrag(touch.clientX, touch.clientY);

                document.addEventListener('touchmove', onTouchMove, { passive: false });
                document.addEventListener('touchend', onTouchEnd);
                document.addEventListener('touchcancel', onTouchEnd);
            }

            function handleMove(clientX, clientY) {
                lastX = clientX;
                lastY = clientY;

                if (!isDragging || pendingFrame) return;

                pendingFrame = true;

                requestAnimationFrame(() => {
                    if (!isDragging) {
                        pendingFrame = false;
                        return;
                    }

                    let newX = lastX - dragOffset.x;
                    let newY = lastY - dragOffset.y;

                    if (dragBounds) {
                        newX = Math.max(dragBounds.minX, Math.min(newX, dragBounds.maxX));
                        newY = Math.max(dragBounds.minY, Math.min(newY, dragBounds.maxY));
                    }

                    if (collisionRects) {
                        const { statusRect, copyRect, sidebarRect, margin, pipWidth } = collisionRects;

                        if (statusRect) {
                            const dangerRight = statusRect.right + margin;
                            const dangerBottom = statusRect.bottom + margin;

                            if (newX < dangerRight && newY < dangerBottom) {
                                const distToRight = dangerRight - newX;
                                const distToBottom = dangerBottom - newY;
                                if (distToRight < distToBottom) newX = dangerRight;
                                else newY = dangerBottom;
                            }
                        }

                        if (copyRect) {
                            const dangerLeft = copyRect.left - margin - pipWidth;
                            const dangerBottom = copyRect.bottom + margin;

                            if (newX > dangerLeft && newY < dangerBottom) {
                                const distToLeft = newX - dangerLeft;
                                const distToBottom = dangerBottom - newY;
                                if (distToLeft < distToBottom) newX = dangerLeft;
                                else newY = dangerBottom;
                            }
                        }

                        if (sidebarRect) {
                            const dangerRight = sidebarRect.right + margin;
                            const dangerBottom = sidebarRect.bottom + margin;

                            if (newX < dangerRight && newY < dangerBottom) {
                                const distToRight = dangerRight - newX;
                                const distToBottom = dangerBottom - newY;
                                if (distToRight < distToBottom) newX = dangerRight;
                                else newY = dangerBottom;
                            }
                        }
                    }

                    pip.style.left = newX + 'px';
                    pip.style.top = newY + 'px';
                    pendingFrame = false;
                });
            }

            function onMouseMove(e) {
                handleMove(e.clientX, e.clientY);
            }

            function onTouchMove(e) {
                if (e.cancelable) e.preventDefault();
                const touch = e.touches[0];
                handleMove(touch.clientX, touch.clientY);
            }

            function onMouseUp() {
                isDragging = false;
                pip.style.cursor = 'grab';
                pip.style.transition = '';
                document.removeEventListener('mousemove', onMouseMove);
                document.removeEventListener('mouseup', onMouseUp);
            }

            function onTouchEnd() {
                isDragging = false;
                pip.style.cursor = 'grab';
                pip.style.transition = '';
                document.removeEventListener('touchmove', onTouchMove);
                document.removeEventListener('touchend', onTouchEnd);
                document.removeEventListener('touchcancel', onTouchEnd);
            }

            pip.addEventListener('mousedown', onMouseDown);
            pip.addEventListener('touchstart', onTouchStart, { passive: false });

            let lastOrientation = window.innerWidth > window.innerHeight ? 'landscape' : 'portrait';
            let resizeTimeoutId = null;
            window.addEventListener('resize', () => {
                if (resizeTimeoutId) clearTimeout(resizeTimeoutId);

                resizeTimeoutId = setTimeout(() => {
                    const currentOrientation = window.innerWidth > window.innerHeight ? 'landscape' : 'portrait';
                    const isScreenFlip = currentOrientation !== lastOrientation;
                    lastOrientation = currentOrientation;

                    pip.style.left = '';
                    pip.style.top = '';
                    pip.style.bottom = '';
                    pip.style.right = '';

                    if (isScreenFlip) {
                        return;
                    }

                }, 250);
            });
        })();

        let idleTimer = null;
        document.addEventListener('mousemove', () => {
            if (document.fullscreenElement && document.fullscreenElement.classList.contains('video-container')) {
                document.fullscreenElement.classList.remove('idle-fullscreen');
                clearTimeout(idleTimer);
                idleTimer = setTimeout(() => {
                    if (document.fullscreenElement && document.fullscreenElement.classList.contains('video-container')) {
                        document.fullscreenElement.classList.add('idle-fullscreen');
                    }
                }, 2500);
            }
        });

        document.addEventListener('fullscreenchange', () => {
            if (!document.fullscreenElement) {
                clearTimeout(idleTimer);
                document.querySelectorAll('.video-container.idle-fullscreen').forEach(el => el.classList.remove('idle-fullscreen'));
            }
        });

        let currentCroppie = null;
        let currentCropTarget = null;
        let currentCropIsGif = false;
        let currentCropGifDataUrl = null;
        let cropGifOverlay = null;
        let cropGifOverlaySeq = 0;

        // Croppie measures the viewport/boundary (via getBoundingClientRect)
        // once, at bind time, and derives the min zoom and centering from
        // that. The crop panel pops in with a scale(0.97)->scale(1)
        // animation, and getBoundingClientRect includes ancestor transforms,
        // so binding mid-pop measures a shrunken viewport: the zoomed-out
        // image then never fills the square and sits off-center. Wait for the
        // pop to finish before binding (a spinner shows meanwhile).
        const CROP_POP_TIMEOUT_MS = 500;
        let cropBindSeq = 0;
        function waitForCropPanelPop(panel) {
            return new Promise(resolve => {
                let settled = false;
                const finish = () => { if (!settled) { settled = true; resolve(); } };
                const anims = (panel.getAnimations ? panel.getAnimations() : [])
                    .filter(a => a.animationName === 'panel-pop');
                if (!anims.length) { finish(); return; }
                const failSafe = setTimeout(finish, CROP_POP_TIMEOUT_MS);
                Promise.all(anims.map(a => a.finished.catch(() => {})))
                    .then(() => { clearTimeout(failSafe); finish(); });
            });
        }

        function openCropModal(imageUrl, target, gifDataUrl) {
            currentCropTarget = target;
            currentCropIsGif = !!gifDataUrl;
            currentCropGifDataUrl = gifDataUrl || null;
            stopCropGifOverlay();
            const modal = document.getElementById('cropModal');
            const wrapper = document.getElementById('cropWrapper');
            wrapper.innerHTML = '';
            showOverlayAnimated(modal);
            const panel = modal.querySelector('.glass-panel');
            const seq = ++cropBindSeq;
            wrapper.innerHTML = '<div class="flex items-center justify-center" style="height:250px;">' +
                '<svg class="spinner" xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color: var(--accent);"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>' +
                '</div>';
            waitForCropPanelPop(panel).then(() => {
                // Guard against the modal being closed or reopened while we
                // waited for the pop animation to finish.
                if (seq !== cropBindSeq) return;
                wrapper.innerHTML = '';
                currentCroppie = new Croppie(wrapper, {
                    viewport: { width: 200, height: 200, type: 'square' },
                    boundary: { width: '100%', height: 250 },
                    showZoomer: true,
                    // GIFs have no EXIF orientation to honor; keep the plain
                    // <img> preview so the looping overlay below can mirror it.
                    enableOrientation: !currentCropIsGif
                });
                currentCroppie.bind({ url: imageUrl, zoom: 0 }).then(() => {
                    // Guard against a stale bind resolving after the modal was
                    // closed or reopened with a different image.
                    if (currentCropIsGif && currentCropGifDataUrl === gifDataUrl) {
                        loopGifInCroppie(currentCropGifDataUrl);
                    }
                }).catch(err => {
                    console.error("Crop preview failed:", err);
                    closeCropModal();
                    showCustomAlert("Image Error", "Could not process this image. Please try a different file.");
                });
            });
        }

        function closeCropModal() {
            cropBindSeq++; // cancel any pending delayed bind
            hideOverlayAnimated(document.getElementById('cropModal'));
            if (currentCroppie) {
                currentCroppie.destroy();
                currentCroppie = null;
            }
            stopCropGifOverlay();
            currentCropIsGif = false;
            currentCropGifDataUrl = null;
            currentCropTarget = null;
        }

        // Overlays a JS-driven GIF loop on the Croppie preview: Croppie only
        // ever displays the first GIF frame, so a canvas that mirrors the
        // preview's transform AND transform-origin (pan/zoom) plays the
        // animation on top. The preview img keeps the original GIF source (so
        // its layout size, and with it Croppie.get()'s coordinates, stay in
        // the original image's pixel space) and the opaque overlay covers it.
        function loopGifInCroppie(gifDataUrl) {
            stopCropGifOverlay();
            const wrapper = document.getElementById('cropWrapper');
            const preview = wrapper.querySelector('.cr-image');
            if (!preview) return;
            const seq = ++cropGifOverlaySeq;
            globalThis.createGifAnimator(gifDataUrl, 800).then(animator => {
                if (seq !== cropGifOverlaySeq || !preview.isConnected) return;
                if (!animator) return;
                const canvas = document.createElement('canvas');
                canvas.style.cssText = 'position:absolute;top:0;left:0;pointer-events:none;z-index:-1;';
                canvas.width = 1;
                canvas.height = 1;
                const boundary = preview.parentElement;
                boundary.appendChild(canvas);
                const ctx = canvas.getContext('2d');
                let lastTransform = '';
                let lastOrigin = '';
                cropGifOverlay = { canvas, animator, preview };
                const sync = () => {
                    if (!canvas.parentNode) return;
                    const cs = getComputedStyle(preview);
                    if (cs.transform !== lastTransform) {
                        lastTransform = cs.transform;
                        canvas.style.transform = cs.transform;
                    }
                    if (cs.transformOrigin !== lastOrigin) {
                        lastOrigin = cs.transformOrigin;
                        canvas.style.transformOrigin = cs.transformOrigin;
                    }
                    const w = preview.offsetWidth;
                    const h = preview.offsetHeight;
                    if (w > 0 && (canvas.style.width !== w + 'px' || canvas.style.height !== h + 'px')) {
                        canvas.style.width = w + 'px';
                        canvas.style.height = h + 'px';
                    }
                    cropGifOverlay.raf = requestAnimationFrame(sync);
                };
                sync();
                animator.start(frame => {
                    if (!canvas.parentNode) return;
                    if (canvas.width !== frame.width || canvas.height !== frame.height) {
                        canvas.width = frame.width;
                        canvas.height = frame.height;
                    }
                    ctx.drawImage(frame, 0, 0);
                });
            });
        }

        function stopCropGifOverlay() {
            cropGifOverlaySeq++;
            if (!cropGifOverlay) return;
            if (cropGifOverlay.raf) cancelAnimationFrame(cropGifOverlay.raf);
            if (cropGifOverlay.animator) cropGifOverlay.animator.stop();
            if (cropGifOverlay.canvas && cropGifOverlay.canvas.parentNode) {
                cropGifOverlay.canvas.parentNode.removeChild(cropGifOverlay.canvas);
            }
            cropGifOverlay = null;
        }

        function setCropProcessing(active) {
            const btn = document.querySelector('#cropModal .btn-primary');
            if (!btn) return;
            btn.disabled = active;
            btn.classList.toggle('opacity-60', active);
            btn.textContent = active ? 'Processing...' : 'Save Avatar';
        }

        // Applies the cropped result (still or animated GIF) to whichever
        // target opened the modal and persists it.
        function commitCropResult(result) {
            if (currentCropTarget === 'setup') {
                stopGifPreviewLoop(avatarPreview);
                userAvatar = result.avatar;
                userAvatarIsGif = result.isGif;
                userAvatarStaticFrame = result.staticFrame;
                userAvatarCache[persistentUserId] = {
                    avatar: userAvatar,
                    isGif: userAvatarIsGif,
                    staticFrame: userAvatarStaticFrame
                };
                avatarPreview.src = result.staticFrame || result.avatar;
                avatarPreview.classList.remove('hidden');
                avatarPlaceholder.classList.add('hidden');
                if (result.isGif) {
                    loopGifInElement(avatarPreview, result.avatar);
                }
                const removeBtn = document.getElementById('btnRemoveSetupAvatar');
                if (removeBtn) removeBtn.classList.remove('hidden');
                savePreferences();
            } else if (currentCropTarget === 'settings') {
                stopGifPreviewLoop(settingsAvatarPreview);
                newAvatarCandidate = result.avatar;
                newAvatarCandidateIsGif = result.isGif;
                newAvatarCandidateStaticFrame = result.staticFrame;
                settingsAvatarPreview.src = result.staticFrame || result.avatar;
                settingsAvatarPreview.classList.remove('hidden');
                settingsAvatarPlaceholder.classList.add('hidden');
                if (result.isGif) {
                    loopGifInElement(settingsAvatarPreview, result.avatar);
                }
                const removeBtn = document.getElementById('btnRemoveSettingsAvatar');
                if (removeBtn) removeBtn.classList.remove('hidden');
                saveSettings();
            }
        }

        function applyGifCrop() {
            if (!currentCroppie) return;
            const points = currentCroppie.get().points;
            stopCropGifOverlay();
            setCropProcessing(true);
            globalThis.cropGifAvatar(currentCropGifDataUrl, {
                x: points[0],
                y: points[1],
                width: points[2] - points[0],
                height: points[3] - points[1]
            }).then(result => {
                setCropProcessing(false);
                commitCropResult(result);
                closeCropModal();
            }).catch(err => {
                console.error("GIF crop failed:", err);
                setCropProcessing(false);
                showCustomAlert("Image Error", "Could not process this GIF. Please try a different file.");
            });
        }

        function applyCrop() {
            if (!currentCroppie) return;
            if (currentCropIsGif && currentCropGifDataUrl) {
                applyGifCrop();
                return;
            }
            currentCroppie.result({
                type: 'base64',
                size: { width: 400, height: 400 },
                format: 'jpeg',
                quality: 0.8
            }).then(function(base64) {
                return fitStaticDataUrl(base64).then(function(fitBase64) {
                    commitCropResult({
                        avatar: fitBase64,
                        isGif: false,
                        staticFrame: null
                    });
                    closeCropModal();
                });
            });
        }

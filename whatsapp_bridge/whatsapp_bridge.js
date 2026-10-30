// Global process handlers: prevent unhandled Puppeteer / whatsapp-web.js rejections from crashing the container
process.on('unhandledRejection', (reason, promise) => {
    console.error('⚠️ Unhandled Rejection at:', promise, 'reason:', reason);
    const reasonStr = String((reason && reason.message) ? reason.message : reason || '').toLowerCase();
    if (
        typeof isClientReady !== 'undefined' && !isClientReady &&
        (reasonStr.includes('auth') ||
         reasonStr.includes('timeout') ||
         reasonStr.includes('qrcode') ||
         reasonStr.includes('navigation') ||
         reasonStr.includes('protocolerror') ||
         reasonStr.includes('target closed') ||
         reasonStr.includes('execution context') ||
         reasonStr.includes('session closed'))
    ) {
        latestQr = 'AUTH_TIMEOUT_RETRYING';
        if (typeof scheduleClientRetry === 'function') {
            scheduleClientRetry('unhandledRejection', 8000);
        }
    }
});

process.on('uncaughtException', (err) => {
    console.error('⚠️ Uncaught Exception in WhatsApp Bridge (process kept alive):', (err && err.message) ? err.message : err);
});

const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const axios = require('axios');
const FormData = require('form-data');
const express = require('express');

const app = express();
app.use(express.json());
const BRIDGE_PORT = 3000; // Hardcoded internal bridge port (NEVER process.env.PORT)
const PYTHON_PORT = Number(process.env.PORT) || 8000;
let latestQr = null;
let isClientReady = false;
let client = null;
let isInitializing = false;
let retryTimer = null;
let authWatchdogTimer = null;

function clearAuthWatchdog() {
    if (authWatchdogTimer) {
        clearTimeout(authWatchdogTimer);
        authWatchdogTimer = null;
    }
}

function resetAuthWatchdog(timeoutMs = 180000) {
    clearAuthWatchdog();
    authWatchdogTimer = setTimeout(() => {
        if (!isClientReady) {
            console.warn('⚠️ [WhatsApp Bridge] Client auth/QR watchdog timed out. Updating QR status to AUTH_TIMEOUT_RETRYING...');
            latestQr = 'AUTH_TIMEOUT_RETRYING';
            scheduleClientRetry('auth_watchdog_timeout', 5000);
        }
    }, timeoutMs);
}

function scheduleClientRetry(reason = 'retry', delayMs = 8000) {
    if (retryTimer || isClientReady) {
        return;
    }
    console.log(`🔄 [WhatsApp Bridge] Scheduling client re-initialization in ${delayMs}ms (reason: ${reason})...`);
    retryTimer = setTimeout(async () => {
        retryTimer = null;
        await safeInitializeClient(reason);
    }, delayMs);
}

function attachClientListeners(waClient) {
    waClient.on('qr', (qr) => {
        try {
            latestQr = qr;
            resetAuthWatchdog(180000);
            console.log('\n======================================================');
            console.log('🔗 [WhatsApp Bridge] SCAN THIS QR CODE');
            qrcode.generate(qr, { small: true });
            console.log('======================================================\n');
        } catch (err) {
            console.error('⚠️ [WhatsApp Bridge] Error in qr listener:', err.message || err);
        }
    });

    waClient.on('ready', async () => {
        try {
            clearAuthWatchdog();
            console.log('✅ [WhatsApp Bridge] Client authenticated. Live listener active.');
            isClientReady = true;
            latestQr = null;
        } catch (err) {
            console.error('⚠️ [WhatsApp Bridge] Error in ready listener:', err.message || err);
        }
    });

    waClient.on('authenticated', () => {
        try {
            clearAuthWatchdog();
            console.log('🔑 [WhatsApp Bridge] Session authenticated.');
            latestQr = null;
        } catch (err) {
            console.error('⚠️ [WhatsApp Bridge] Error in authenticated listener:', err.message || err);
        }
    });

    waClient.on('auth_failure', (msg) => {
        try {
            clearAuthWatchdog();
            console.error('❌ [WhatsApp Bridge] Auth failure:', msg);
            isClientReady = false;
            latestQr = 'AUTH_TIMEOUT_RETRYING';
            scheduleClientRetry('auth_failure', 6000);
        } catch (err) {
            console.error('⚠️ [WhatsApp Bridge] Error in auth_failure listener:', err.message || err);
        }
    });

    waClient.on('disconnected', (reason) => {
        try {
            clearAuthWatchdog();
            console.warn('⚠️ [WhatsApp Bridge] Client disconnected:', reason);
            isClientReady = false;
            latestQr = 'AUTH_TIMEOUT_RETRYING';
            scheduleClientRetry(`disconnected:${reason}`, 6000);
        } catch (err) {
            console.error('⚠️ [WhatsApp Bridge] Error in disconnected listener:', err.message || err);
        }
    });

    waClient.on('code', (code) => {
        try {
            console.log('\n======================================================');
            console.log('🔗 [WhatsApp Bridge] LINK WITH PHONE NUMBER');
            console.log(`Type this Pairing Code into your WhatsApp app: ${code}`);
            console.log('======================================================\n');
        } catch (err) {
            console.error('⚠️ [WhatsApp Bridge] Error in code listener:', err.message || err);
        }
    });

    // Live Event Listener strictly gated to ALLOWED_KTRADE_CHATS
    waClient.on('message_create', async (msg) => {
        try {
            if (!msg) return;
            const fromId = getSerializedId(msg.from);
            const toId = getSerializedId(msg.to);
            const chatId = msg.fromMe ? toId : fromId;

            // STRICT GATE: Never process non-whitelisted chats
            if (!ALLOWED_KTRADE_CHATS.has(chatId)) {
                return;
            }
            if (msg.isStatus) {
                return;
            }

            const allowedTypes = ['chat', 'image', 'document', 'audio', 'ptt', 'video', 'voice'];
            if (!allowedTypes.includes(msg.type) && !msg.hasMedia) {
                return;
            }

            let chatName = CHAT_NAMES[chatId] || 'KTrade';
            try {
                const chat = await msg.getChat();
                if (chat && chat.name) chatName = chat.name;
            } catch (_) {
                // Keep default chatName from CHAT_NAMES map
            }

            console.log(`[Diagnostic] Live KTrade Message! Chat: "${chatName}", ID: ${chatId}, Type: ${msg.type}, Media: ${msg.hasMedia}`);
            await forwardMessageToPython(msg, chatName, chatId);
        } catch (err) {
            console.warn(`⚠️ [WhatsApp Bridge] Live event skipped: ${(err && err.message) ? err.message : err}`);
        }
    });
}

function createWhatsAppClient() {
    const waClient = new Client({
        authStrategy: new LocalAuth({
            dataPath: './.wwebjs_auth'
        }),
        qrMaxRetries: 10,
        authTimeoutMs: 180000,
        puppeteer: {
            headless: true, // Silent background operation
            executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--single-process',
                '--renderer-process-limit=1',
                '--js-flags=--max-old-space-size=96'
            ],
            protocolTimeout: 300000 // Forces browser to wait for WhatsApp to load
        }
    });
    attachClientListeners(waClient);
    return waClient;
}

// STRICT WHITELIST: Only the two authentic KTrade group IDs
const ALLOWED_KTRADE_CHATS = new Set([
    '120363405626670375@g.us', // KTrade - E Trader Assistance 6
    '120363404462896702@g.us'  // KTrade - E Trader Assistance 6 (Secondary)
]);

const CHAT_NAMES = {
    '120363405626670375@g.us': 'KTrade - E Trader Assistance 6',
    '120363404462896702@g.us': 'KTrade - E Trader Assistance 6 (Secondary)'
};

function getSerializedId(raw) {
    if (!raw) return '';
    if (typeof raw === 'string') return raw;
    if (raw._serialized) return raw._serialized;
    return String(raw);
}

// Unified forwarder for multimodal media (images, PDFs, voice notes) and text intelligence
async function forwardMessageToPython(msg, chatName, chatId) {
    try {
        const targetChatId = chatId || getSerializedId(msg.from) || '';
        
        // STRICT GATE: Never forward non-whitelisted chats under any circumstances
        if (!ALLOWED_KTRADE_CHATS.has(targetChatId)) {
            return;
        }

        const sender = msg.author || msg.from || 'KTrade';
        const timestamp = msg.timestamp || Math.floor(Date.now() / 1000);

        if (msg.hasMedia) {
            let media = null;
            try {
                media = await msg.downloadMedia();
            } catch (dlErr) {
                console.warn(`⚠️ [WhatsApp Bridge] Attachment download attempt 1 threw for msg ${msg.id?.id || 'unknown'} (${dlErr.message || dlErr}). Waiting 1500ms to hydrate...`);
                await new Promise(resolve => setTimeout(resolve, 1500));
                try {
                    media = await msg.downloadMedia();
                } catch (retryErr) {
                    console.warn(`⚠️ [WhatsApp Bridge] Attachment download retry failed: ${retryErr.message || retryErr}`);
                }
            }

            if (media && media.data) {
                const mime = (media.mimetype || '').toLowerCase();
                const isImage = mime.startsWith('image/');
                const isPdf = mime === 'application/pdf';
                const isAudio = mime.startsWith('audio/') || mime.includes('ogg') || mime.includes('mpeg') || mime.includes('mp4') || mime.includes('aac') || mime.includes('wav');

                if (isImage || isPdf || isAudio) {
                    const sizeBytes = media.filesize || Buffer.byteLength(media.data, 'base64');
                    // Accept media up to 15 MB
                    if (sizeBytes <= 15 * 1024 * 1024) {
                        const fallbackName = isPdf ? `doc_${Date.now()}.pdf` : (isAudio ? `voice_${Date.now()}.ogg` : `chart_${Date.now()}.png`);
                        const filename = media.filename || fallbackName;

                        console.log(`🚀 [WhatsApp Bridge] Forwarding multimodal media: "${filename}" (${mime}, ${(sizeBytes/1024).toFixed(1)} KB) from "${chatName}"...`);

                        const payload = {
                            chat_id: targetChatId,
                            sender: sender,
                            body: msg.body || '',
                            caption: msg.body || '',
                            source: chatName || 'KTrade',
                            timestamp: timestamp,
                            is_group: true,
                            has_media: true,
                            media_base64: media.data,
                            mime_type: media.mimetype,
                            filename: filename
                        };

                        await axios.post(`http://127.0.0.1:${PYTHON_PORT}/api/ingest-whatsapp-message`, payload, {
                            maxContentLength: 50 * 1024 * 1024,
                            maxBodyLength: 50 * 1024 * 1024,
                            timeout: 15000
                        });
                        return;
                    } else {
                        console.warn(`⚠️ [WhatsApp Bridge] Media exceeds 15MB limit (${(sizeBytes / (1024*1024)).toFixed(1)} MB). Falling back to text if available.`);
                    }
                } else {
                    console.log(`ℹ️ [WhatsApp Bridge] Skipping unsupported media type (${mime})`);
                }
            } else {
                if (!msg.body || msg.body.trim().length === 0) {
                    console.log(`ℹ️ [WhatsApp Bridge] Media unavailable and message body empty. Cleanly skipping msg ${msg.id?.id || 'unknown'}.`);
                    return;
                }
            }
        }

        // If no media (or media was skipped/failed) and message body exists, forward text intel
        if (msg.body && msg.body.trim().length > 0) {
            console.log(`🚀 [WhatsApp Bridge] Forwarding text intel (${msg.body.trim().length} chars) from "${chatName}"...`);
            const payload = {
                chat_id: targetChatId,
                sender: sender,
                body: msg.body.trim(),
                text: msg.body.trim(),
                source: chatName || 'KTrade',
                timestamp: timestamp,
                is_group: true,
                has_media: false
            };
            await axios.post(`http://127.0.0.1:${PYTHON_PORT}/api/ingest-whatsapp-message`, payload, {
                timeout: 15000
            });
        }
    } catch (err) {
        console.error(`❌ [WhatsApp Bridge] Forwarding failed for message ID ${msg.id?.id || 'unknown'}:`, err.message);
    }
}

// Health check endpoint
app.get('/ping', (req, res) => {
    if (isClientReady) {
        res.json({ status: 'pong', message: 'WhatsApp Bridge is live and ready.' });
    } else {
        res.status(503).json({ status: 'starting', message: 'Chromium is still booting...' });
    }
});

// Live QR code status endpoint for cloud browser rendering
app.get('/qr', (req, res) => {
    try {
        if (latestQr === 'AUTH_TIMEOUT_RETRYING' && !isInitializing && !isClientReady) {
            scheduleClientRetry('qr_endpoint_reload', 500);
        }
        const status = isClientReady
            ? 'READY'
            : (latestQr === 'AUTH_TIMEOUT_RETRYING'
                ? 'AUTH_TIMEOUT_RETRYING'
                : (latestQr ? 'QR_READY' : 'INITIALIZING'));
        res.json({
            status: status,
            qr: latestQr,
            monitored_chats: Array.from(ALLOWED_KTRADE_CHATS)
        });
    } catch (err) {
        res.json({
            status: 'AUTH_TIMEOUT_RETRYING',
            qr: 'AUTH_TIMEOUT_RETRYING',
            monitored_chats: Array.from(ALLOWED_KTRADE_CHATS)
        });
    }
});

// List all group chats with their serialized @g.us IDs (diagnostic)
app.get('/chats', async (req, res) => {
    try {
        if (!client || !isClientReady) {
            return res.status(503).json({ status: 'starting', message: 'WhatsApp client is not ready yet.' });
        }
        
        let groups = [];
        try {
            const chats = await client.getChats();
            groups = chats
                .filter(c => c.id && c.id._serialized && c.id._serialized.endsWith('@g.us'))
                .map(c => ({
                    name: c.name || 'Unnamed Group',
                    id: c.id._serialized,
                    unreadCount: c.unreadCount || 0,
                    isWhitelisted: ALLOWED_KTRADE_CHATS.has(c.id._serialized)
                }));
        } catch (getChatsErr) {
            console.warn(`⚠️ [WhatsApp Bridge] Standard client.getChats() threw (${getChatsErr.message || getChatsErr}). Using direct DOM evaluation fallback...`);
            
            groups = await client.pupPage.evaluate(() => {
                let chatModels = [];
                try {
                    const coll = window.require('WAWebCollections');
                    if (coll && coll.Chat) {
                        chatModels = coll.Chat.getModelsArray ? coll.Chat.getModelsArray() : (coll.Chat.models || []);
                    }
                } catch (e) {}

                if (!chatModels || chatModels.length === 0) {
                    try {
                        const store = window.Store || window.require('WAWebStore');
                        if (store && store.Chat) {
                            chatModels = store.Chat.models || (store.Chat.getModelsArray ? store.Chat.getModelsArray() : []);
                        }
                    } catch (e) {}
                }

                return (chatModels || [])
                    .filter(c => {
                        const id = (c && c.id && (c.id._serialized || c.id)) || '';
                        return typeof id === 'string' && id.endsWith('@g.us');
                    })
                    .map(c => ({
                        name: c.name || c.formattedTitle || 'Unnamed Group',
                        id: (c && c.id && (c.id._serialized || c.id)) || '',
                        unreadCount: c.unreadCount || 0
                    }));
            });
        }
        
        res.json({ status: 'ok', count: groups.length, groups: groups });
    } catch (err) {
        console.error('❌ [WhatsApp Bridge] Error fetching chats:', err);
        res.status(500).json({ error: err.message || String(err) });
    }
});

// Start local API sweep endpoint (strictly whitelisted to KTrade)
app.get('/api/trigger-sweep', async (req, res) => {
    console.log('🔄 [WhatsApp Bridge] Python requested startup catch-up sweep...');
    try {
        if (!client || !client.pupPage) {
            return res.status(200).json({ status: 'skipped', message: 'Client not ready' });
        }

        const chats = await client.getChats().catch(() => []);
        const ktradeChats = chats.filter(c => {
            const id = c.id?._serialized || '';
            return ALLOWED_KTRADE_CHATS.has(id);
        });

        if (ktradeChats.length === 0) {
            return res.status(200).json({ status: 'skipped', message: 'KTrade chats not found.' });
        }

        for (const ktradeChat of ktradeChats) {
            const chatId = ktradeChat.id?._serialized;
            const chatName = CHAT_NAMES[chatId] || ktradeChat.name || 'KTrade';
            const messages = await ktradeChat.fetchMessages({ limit: 50 }).catch(() => []);
            for (const msg of messages) {
                try {
                    await forwardMessageToPython(msg, chatName, chatId);
                } catch (err) {
                    console.warn(`⚠️ [WhatsApp Bridge] Sweep skipped message: ${err.message}`);
                }
            }
        }

        res.status(200).json({ status: 'success', message: 'Sweep completed.' });
    } catch (error) {
        console.error('⚠️ [WhatsApp Bridge] Handled Sweep Exception:', error.message || error);
        res.status(200).json({ status: 'partial_failure', error: String(error) });
    }
});

// Hardened catchup endpoint with Store fallback and multimodal media replay
app.post('/api/catchup', async (req, res) => {
    const { since_timestamp, target_chat_ids } = req.body;
    const rawIds = (target_chat_ids && target_chat_ids.length > 0) ? target_chat_ids : Array.from(ALLOWED_KTRADE_CHATS);
    // STRICT GATE: Only allow whitelisted KTrade IDs
    const chatIdsToScan = rawIds.filter(id => ALLOWED_KTRADE_CHATS.has(id));

    console.log(`🔄 [WhatsApp Bridge] Catching up on messages since timestamp: ${since_timestamp || 0} for ${chatIdsToScan.length} whitelisted KTrade chats...`);

    let processedCount = 0;

    for (const chatId of chatIdsToScan) {
        try {
            let handled = false;
            let chatName = CHAT_NAMES[chatId] || 'KTrade';

            // Attempt 1: Standard client.getChatById
            try {
                const chat = await client.getChatById(chatId);
                if (chat) {
                    chatName = chat.name || chatName;
                    const messages = await chat.fetchMessages({ limit: 500 });
                    const unanalyzed = messages
                        .filter(m => (m.timestamp || 0) > (since_timestamp || 0))
                        .sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));

                    for (const msg of unanalyzed) {
                        await forwardMessageToPython(msg, chatName, chatId);
                        processedCount++;
                    }
                    handled = true;
                }
            } catch (standardErr) {
                console.warn(`⚠️ [WhatsApp Bridge] Standard getChatById failed for ${chatId} (${standardErr.message || standardErr}). Using Store/DOM fallback...`);
            }

            // Attempt 2: Direct Store/DOM fallback via client.pupPage.evaluate
            if (!handled && client.pupPage) {
                const domResult = await client.pupPage.evaluate((targetId, since) => {
                    try {
                        let chatObj = null;
                        try {
                            const coll = window.require('WAWebCollections');
                            if (coll && coll.Chat) {
                                chatObj = coll.Chat.get(targetId);
                            }
                        } catch (e) {}

                        if (!chatObj) {
                            try {
                                const store = window.Store || window.require('WAWebStore');
                                if (store && store.Chat) {
                                    chatObj = store.Chat.get(targetId);
                                }
                            } catch (e) {}
                        }

                        if (!chatObj) return { error: `Chat ${targetId} not found in DOM Store` };

                        const name = chatObj.name || chatObj.formattedTitle || 'KTrade';
                        let msgs = [];
                        if (chatObj.msgs) {
                            msgs = chatObj.msgs.getModelsArray ? chatObj.msgs.getModelsArray() : (chatObj.msgs.models || []);
                        }

                        const unanalyzed = msgs
                            .filter(m => {
                                const t = m.t || m.timestamp || 0;
                                return t > (since || 0);
                            })
                            .sort((a, b) => (a.t || a.timestamp || 0) - (b.t || b.timestamp || 0))
                            .map(m => ({
                                id: (m.id && (m.id._serialized || m.id.id)) || '',
                                body: m.body || m.caption || '',
                                timestamp: m.t || m.timestamp || Math.floor(Date.now() / 1000),
                                type: m.type || 'chat',
                                hasMedia: Boolean(m.isMedia || ['image', 'document', 'audio', 'ptt', 'video'].includes(m.type) || m.mediaData)
                            }));

                        return { name, messages: unanalyzed };
                    } catch (e) {
                        return { error: e.message || String(e) };
                    }
                }, chatId, since_timestamp);

                if (domResult && domResult.messages && domResult.messages.length > 0) {
                    chatName = domResult.name || chatName;
                    console.log(`📱 [WhatsApp Bridge] DOM fallback found ${domResult.messages.length} messages in "${chatName}" (${chatId})`);

                    for (const m of domResult.messages) {
                        let dispatched = false;
                        if (m.hasMedia && m.id) {
                            try {
                                const msgObj = await client.getMessageById(m.id);
                                if (msgObj) {
                                    await forwardMessageToPython(msgObj, chatName, chatId);
                                    dispatched = true;
                                }
                            } catch (msgErr) {
                                console.warn(`⚠️ [WhatsApp Bridge] Could not resolve full message object for ${m.id}: ${msgErr.message}`);
                            }
                        }

                        // If not dispatched as media but text exists, forward as text
                        if (!dispatched && m.body && m.body.trim().length > 0) {
                            await forwardMessageToPython({
                                id: { id: m.id },
                                body: m.body,
                                timestamp: m.timestamp,
                                from: chatId,
                                hasMedia: false
                            }, chatName, chatId);
                        }
                        processedCount++;
                    }
                } else if (domResult && domResult.error) {
                    console.warn(`⚠️ [WhatsApp Bridge] DOM fallback for ${chatId} returned: ${domResult.error}`);
                }
            }
        } catch (err) {
            console.error(`❌ Failed catch-up for chat ${chatId}:`, err.message);
        }
    }

    console.log(`✅ Catch-up complete. Replayed and analyzed ${processedCount} missed messages.`);
    res.json({ status: 'ok', replayed: processedCount });
});

// START EXPRESS STRICTLY ON LOOPBACK (127.0.0.1:3000) SO CLOUD PORT SCANNERS IGNORE IT
app.listen(BRIDGE_PORT, '127.0.0.1', () => {
    console.log(`⚙️  [WhatsApp Bridge] Local API listening on 127.0.0.1:${BRIDGE_PORT}. Waiting for Chrome...`);
});

async function safeInitializeClient(reason = 'startup') {
    if (isInitializing || isClientReady) {
        return;
    }
    isInitializing = true;
    try {
        if (client) {
            try {
                await client.destroy();
            } catch (_) {
                // Ignore cleanup errors on stale Puppeteer browser
            }
            client = null;
        }
        console.log(`🚀 [WhatsApp Bridge] Initializing Puppeteer WhatsApp client (reason: ${reason})...`);
        client = createWhatsAppClient();
        resetAuthWatchdog(180000);
        await client.initialize();
    } catch (initErr) {
        clearAuthWatchdog();
        console.error(`⚠️ [WhatsApp Bridge] Client initialization/auth error (${reason}):`, (initErr && initErr.message) ? initErr.message : initErr);
        isClientReady = false;
        latestQr = 'AUTH_TIMEOUT_RETRYING';
        scheduleClientRetry('initialize_error', 8000);
    } finally {
        isInitializing = false;
    }
}

safeInitializeClient('startup').catch((err) => {
    console.error('⚠️ [WhatsApp Bridge] Startup init wrapper caught error:', (err && err.message) ? err.message : err);
    latestQr = 'AUTH_TIMEOUT_RETRYING';
    scheduleClientRetry('startup_wrapper', 8000);
});

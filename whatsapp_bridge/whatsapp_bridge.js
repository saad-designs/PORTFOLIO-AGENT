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
const fs = require('fs');
const path = require('path');
const https = require('https');
const { execSync } = require('child_process');
const os = require('os');

// Recursive cleanup of stale Chromium lock files (.wwebjs_auth and /tmp)
// Uses shell find on Linux to delete broken symlinks where fs.existsSync returns false
function cleanStaleChromiumLocks(dirPath, depth = 0) {
    if (depth > 5) return;
    if (process.platform !== 'win32' && depth === 0) {
        try {
            execSync('find /app -name "Singleton*" -delete 2>/dev/null || true', { stdio: 'ignore' });
            execSync('find /tmp -name "Singleton*" -delete 2>/dev/null || true', { stdio: 'ignore' });
            execSync('find /app -name "lockfile" -delete 2>/dev/null || true', { stdio: 'ignore' });
            execSync('find /tmp -name "psx_chrome_profile" -exec rm -rf {} + 2>/dev/null || true', { stdio: 'ignore' });
        } catch (_) {}
    }
    if (!dirPath) return;
    try {
        let entries = [];
        try {
            entries = fs.readdirSync(dirPath, { withFileTypes: true });
        } catch (_) {
            return;
        }
        for (const entry of entries) {
            const fullPath = path.join(dirPath, entry.name);
            const name = entry.name.toLowerCase();
            if (name.startsWith('singleton') || name === 'lockfile') {
                try {
                    fs.rmSync(fullPath, { force: true, recursive: true });
                    console.log(`🧹 [Chromium Lock Cleaner] Removed stale lock/symlink: ${fullPath}`);
                } catch (e) {
                    try { fs.unlinkSync(fullPath); } catch (_) {}
                }
            } else if (entry.isDirectory() && !name.startsWith('.') && name !== 'node_modules') {
                cleanStaleChromiumLocks(fullPath, depth + 1);
            }
        }
    } catch (err) {
        // Ignore errors during directory traversal
    }
}

// Clean stale locks immediately on process launch
try {
    cleanStaleChromiumLocks(path.resolve(__dirname, '.wwebjs_auth'));
    if (process.platform !== 'win32') {
        cleanStaleChromiumLocks('/tmp');
    }
} catch (_) {}

function getChromeExecutable() {
    if (process.env.PUPPETEER_EXECUTABLE_PATH && fs.existsSync(process.env.PUPPETEER_EXECUTABLE_PATH)) {
        return process.env.PUPPETEER_EXECUTABLE_PATH;
    }
    const candidates = [
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
        'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
        process.env.LOCALAPPDATA ? path.join(process.env.LOCALAPPDATA, 'Google', 'Chrome', 'Application', 'chrome.exe') : null
    ];
    for (const p of candidates) {
        if (p && fs.existsSync(p)) return p;
    }
    return undefined;
}
let puppeteer = null;
try {
    puppeteer = require('puppeteer');
} catch (e) {
    try {
        puppeteer = require('whatsapp-web.js/node_modules/puppeteer');
    } catch (_) {}
}

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
            executablePath: getChromeExecutable() || undefined,
            args: [
                '--headless=new',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--no-zygote',
                '--ozone-platform=headless'
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
    res.status(200).json({
        status: isClientReady ? 'pong' : 'starting',
        ready: isClientReady,
        message: isClientReady ? 'WhatsApp Bridge is live and ready.' : 'Bridge is running, client initializing...',
        uptime_s: Math.round(process.uptime())
    });
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
            monitored_chats: Array.from(ALLOWED_KTRADE_CHATS),
            memory_mb: Math.round(process.memoryUsage().rss / 1048576),
            uptime_s: Math.round(process.uptime())
        });
    } catch (err) {
        res.json({
            status: 'AUTH_TIMEOUT_RETRYING',
            qr: 'AUTH_TIMEOUT_RETRYING',
            monitored_chats: Array.from(ALLOWED_KTRADE_CHATS),
            memory_mb: Math.round(process.memoryUsage().rss / 1048576),
            uptime_s: Math.round(process.uptime())
        });
    }
});

// --- PSX LIVE CHROMIUM ENDPOINTS ---
app.get(['/psx-live-quotes', '/api/psx/live-quotes'], (req, res) => {
    res.json({
        status: 'ok',
        source: 'chromium_psx_live',
        count: Object.keys(latestPsxQuotes).length,
        timestamp: latestPsxTimestamp,
        quotes: latestPsxQuotes
    });
});

app.get('/api/psx/status', (req, res) => {
    const quoteCount = Object.keys(latestPsxQuotes).length;
    const isHealthy = Boolean(latestPsxTimestamp && (Date.now() - latestPsxTimestamp < 120000) && quoteCount > 0);
    res.status(200).json({
        status: isHealthy ? 'healthy' : (quoteCount > 0 ? 'stale' : 'booting'),
        healthy: isHealthy,
        source: 'chromium_psx_live',
        count: quoteCount,
        last_update_ms_ago: latestPsxTimestamp ? (Date.now() - latestPsxTimestamp) : null,
        error: isHealthy ? null : psxMonitorError
    });
});

app.get('/api/psx/timeseries/:ticker', async (req, res) => {
    const ticker = (req.params.ticker || '').toUpperCase();
    if (!ticker) return res.status(400).json({ error: 'Ticker required' });
    if (!psxPage || psxPage.isClosed()) {
        return res.status(503).json({ error: 'PSX Chromium page not ready' });
    }
    try {
        const data = await psxPage.evaluate(async (sym) => {
            try {
                const r = await fetch(`https://dps.psx.com.pk/timeseries/int/${sym}`, { cache: 'no-store' });
                return await r.json();
            } catch (e) {
                return { error: e.message };
            }
        }, ticker);
        return res.json(data);
    } catch (err) {
        return res.status(500).json({ error: err.message || String(err) });
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

// --- PERSISTENT 1-TAB HEADLESS CHROMIUM PSX LIVE MONITOR ---
let psxBrowser = null;
let psxPage = null;
let latestPsxQuotes = {};
let latestPsxTimestamp = 0;
let isPsxMonitorBooting = false;
let psxMonitorError = null;
let psxPollTimer = null;
let psxCycleCounter = 0;
let isExtractingPsx = false;

const SCRIP_TO_SYMBOL = {
    'Systems Limited': 'SYS',
    'Fauji Fert': 'FFC',
    'Fauji Fertilizer': 'FFC',
    'Fauji FertXD': 'FFC',
    'Oil & Gas Dev': 'OGDC',
    'Oil & Gas DevXD': 'OGDC',
    'Oil and Gas Development': 'OGDC',
    'Mari Energies Ltd.': 'MARI',
    'Mari Petroleum': 'MARI',
    'Hub Power Co.': 'HUBC',
    'Hub Power Co.XD': 'HUBC',
    'Hub Power': 'HUBC',
    'Lucky Cement': 'LUCK',
    'Engro FertertXD': 'EFERT',
    'Engro Fertert': 'EFERT',
    'Engro Fertilizer': 'EFERT',
    'Engro Fert': 'EFERT',
    'Meezan Bank Ltd': 'MEBL',
    'Meezan Bank LtdXD': 'MEBL',
    'Meezan Bank': 'MEBL',
    'Faysal Bank': 'FABL',
    'Faysal BankXD': 'FABL',
    'Faysal Bank Ltd': 'FABL',
    'Pakistan Petroleum': 'PPL',
    'Pak PetroleumXD': 'PPL',
    'P.P.L.': 'PPL',
    'P.P.L.XD': 'PPL',
    'Pak Oilfields': 'POL',
    'Pakistan State Oil': 'PSO',
    'P.S.O.': 'PSO',
    'P.S.O.XD': 'PSO',
    'United Bank': 'UBL',
    'MCB Bank': 'MCB',
    'Habib Bank': 'HBL',
    'Bank AL Habib': 'BAHL',
    'TRG Pakistan': 'TRG',
    'Engro Corp': 'ENGRO',
    'Engro Corporation': 'ENGRO',
    'D.G. Khan Cement': 'DGKC',
    'Maple Leaf': 'MLCF',
    'Cherat Cement': 'CHCC',
    'Fauji Cement': 'FCCL',
    'Unity Foods': 'UNITY',
    'Pakistan Refinery': 'PRL',
    'Attock Refinery': 'ATRL',
    'National Refinery': 'NRL',
    'Pak Elektron': 'PAEL',
    'WorldCall Telecom': 'WTL',
    'Telecard': 'TELE',
    'Lotte Chemical': 'LOTCHEM',
    'Cnergyico PK': 'CNERGY'
};

function parseMarketSummaryHtml(html) {
    if (!html || typeof html !== 'string') return { bySymbol: {}, count: 0 };
    const rows = html.match(/<tr[\s\S]*?<\/tr>/gi) || [];
    const bySymbol = {};
    let count = 0;

    for (const r of rows) {
        let tds = (r.match(/<t[hd][\s\S]*?<\/t[hd]>/gi) || []).map(t => t.replace(/<[^>]+>/g, '').trim());
        if (tds.length < 7) continue;

        // Strip leading empty or numeric sequence cell if present
        if (tds[0] === '' || /^\d+$/.test(tds[0])) {
            tds = tds.slice(1);
        }
        if (tds.length < 7) continue;
        const scrip = tds[0];
        if (!scrip || scrip.toUpperCase() === 'SCRIP' || scrip.toUpperCase() === 'SYMBOL') continue;

        const cleanNum = (str) => {
            if (!str) return 0;
            const clean = str.replace(/,/g, '').replace(/[^\d.-]/g, '');
            const val = parseFloat(clean);
            return isNaN(val) ? 0 : val;
        };

        const ldcp = cleanNum(tds[1]);
        const open = cleanNum(tds[2]);
        const high = cleanNum(tds[3]);
        const low = cleanNum(tds[4]);
        const current = cleanNum(tds[5]);
        const change = cleanNum(tds[6]);
        const volume = parseInt((tds[7] || '0').replace(/,/g, '')) || 0;

        if (current > 0) {
            count++;
            const change_pct = ldcp > 0 ? Math.round(((change / ldcp) * 100) * 100) / 100 : 0;
            const quote = {
                scrip,
                price: Math.round(current * 100) / 100,
                ldcp: Math.round(ldcp * 100) / 100,
                prev_close: Math.round(ldcp * 100) / 100,
                open: Math.round(open * 100) / 100,
                high: Math.round(high * 100) / 100,
                low: Math.round(low * 100) / 100,
                change: Math.round(change * 100) / 100,
                change_abs: Math.round(change * 100) / 100,
                change_pct: change_pct,
                volume: volume,
                source: 'chromium_psx_live',
                timestamp: Date.now() / 1000
            };

            const cleanScrip = scrip
                .replace(/(\.|\s*)(XD|NC|XB|XR|WU|SPOT|PRO)$/i, '')
                .replace(/\./g, '')
                .trim();

            const sym = SCRIP_TO_SYMBOL[scrip] || SCRIP_TO_SYMBOL[cleanScrip] || cleanScrip.toUpperCase();
            bySymbol[sym] = quote;
            bySymbol[scrip.toUpperCase()] = quote;

            const parts = scrip.split('-');
            if (parts.length === 2 && ['NOV', 'NOVB', 'NOVC', 'DEC', 'DECB', 'JAN', 'FEB'].includes(parts[1])) {
                const baseSym = parts[0].toUpperCase();
                if (!bySymbol[baseSym]) bySymbol[baseSym] = quote;
            }
        }
    }
    return { bySymbol, count };
}

function fetchPsxMarketSummaryDirect() {
    return new Promise((resolve) => {
        const options = {
            hostname: 'www.psx.com.pk',
            port: 443,
            path: '/market-summary/',
            method: 'GET',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            },
            timeout: 8000
        };
        const req = https.request(options, (res) => {
            if (res.statusCode !== 200) {
                res.resume();
                return resolve(false);
            }
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    const { bySymbol, count } = parseMarketSummaryHtml(data);
                    if (count > 0) {
                        latestPsxQuotes = Object.assign(latestPsxQuotes, bySymbol);
                        latestPsxTimestamp = Date.now();
                        psxMonitorError = null;
                        return resolve(true);
                    }
                } catch (e) {
                    if (Object.keys(latestPsxQuotes).length === 0) {
                        psxMonitorError = e.message;
                    }
                }
                resolve(false);
            });
        });
        req.on('error', (err) => {
            if (Object.keys(latestPsxQuotes).length === 0) {
                psxMonitorError = err.message;
            }
            resolve(false);
        });
        req.end();
    });
}

function parseDpsMarketWatchHtml(html) {
    if (!html || typeof html !== 'string') return { bySymbol: {}, count: 0 };
    const rows = html.match(/<tr[\s\S]*?<\/tr>/gi) || [];
    const bySymbol = {};
    let count = 0;

    for (const r of rows) {
        const mSym = r.match(/<td[^>]*data-search=[\'"]([^\'"]+)[\'"]/i) || r.match(/<td[^>]*data-order=[\'"]([A-Z0-9_\-]+)[\'"]/i);
        if (!mSym) continue;
        const sym = mSym[1].toUpperCase().trim();
        if (!sym || sym === 'SCRIP' || sym === 'SYMBOL' || /^\d+$/.test(sym)) continue;

        const rightTds = r.match(/<td[^>]*class=[\'"][^\'"]*right[^\'"]*[\'"][^>]*>[\s\S]*?<\/td>/gi) || [];
        if (rightTds.length < 5) continue;

        const getVal = (tdStr) => {
            const ord = tdStr.match(/data-order=[\'"]([^\'"]+)[\'"]/i);
            if (ord) {
                const v = parseFloat(ord[1]);
                if (!isNaN(v)) return v;
            }
            const clean = tdStr.replace(/<[^>]+>/g, '').replace(/,/g, '').replace(/[^\d.-]/g, '').trim();
            const v = parseFloat(clean);
            return isNaN(v) ? 0 : v;
        };

        const ldcp = getVal(rightTds[0]);
        const open = getVal(rightTds[1]);
        const high = getVal(rightTds[2]);
        const low = getVal(rightTds[3]);
        const current = getVal(rightTds[4]);
        const change = rightTds.length > 5 ? getVal(rightTds[5]) : (current - ldcp);
        const change_pct = rightTds.length > 6 ? getVal(rightTds[6]) : (ldcp > 0 ? (change / ldcp) * 100 : 0);
        const volume = rightTds.length > 7 ? parseInt(getVal(rightTds[7])) : 0;

        if (current > 0) {
            count++;
            bySymbol[sym] = {
                scrip: sym,
                price: Math.round(current * 100) / 100,
                ldcp: Math.round(ldcp * 100) / 100,
                prev_close: Math.round(ldcp * 100) / 100,
                open: Math.round(open * 100) / 100,
                high: Math.round(high * 100) / 100,
                low: Math.round(low * 100) / 100,
                change: Math.round(change * 100) / 100,
                change_abs: Math.round(change * 100) / 100,
                change_pct: Math.round(change_pct * 100) / 100,
                volume: volume,
                source: 'chromium_psx_live',
                timestamp: Date.now() / 1000
            };
        }
    }
    return { bySymbol, count };
}

async function extractPsxMarketQuotes() {
    if (isExtractingPsx) return;
    isExtractingPsx = true;
    try {
        psxCycleCounter++;

        // 1. Direct Node fetch for market-summary (590+ stocks fallback)
        await fetchPsxMarketSummaryDirect();

        // 2. Headless Chromium live extraction from dps.psx.com.pk/market-watch
        if (psxPage && !psxPage.isClosed()) {
            try {
                const domQuotes = await psxPage.evaluate(() => {
                    const out = {};
                    const rows = document.querySelectorAll('tr');
                    for (const tr of rows) {
                        const tdSym = tr.querySelector('td[data-search]');
                        if (!tdSym) continue;
                        const sym = (tdSym.getAttribute('data-search') || tdSym.getAttribute('data-order') || '').toUpperCase().trim();
                        if (!sym || sym === 'SCRIP' || sym === 'SYMBOL' || /^\d+$/.test(sym)) continue;

                        const rightTds = tr.querySelectorAll('td.right');
                        if (rightTds.length < 5) continue;

                        const getVal = (el) => {
                            if (!el) return 0;
                            const ord = el.getAttribute('data-order');
                            if (ord !== null && ord !== '') {
                                const v = parseFloat(ord);
                                if (!isNaN(v)) return v;
                            }
                            const clean = el.textContent.replace(/,/g, '').replace(/[^\d.-]/g, '');
                            const v = parseFloat(clean);
                            return isNaN(v) ? 0 : v;
                        };

                        const ldcp = getVal(rightTds[0]);
                        const open = getVal(rightTds[1]);
                        const high = getVal(rightTds[2]);
                        const low = getVal(rightTds[3]);
                        const current = getVal(rightTds[4]);
                        const change = rightTds.length > 5 ? getVal(rightTds[5]) : (current - ldcp);
                        const change_pct = rightTds.length > 6 ? getVal(rightTds[6]) : (ldcp > 0 ? (change / ldcp) * 100 : 0);
                        const volume = rightTds.length > 7 ? parseInt(rightTds[7].getAttribute('data-order') || rightTds[7].textContent.replace(/,/g, '')) || 0 : 0;

                        if (current > 0) {
                            out[sym] = {
                                scrip: sym,
                                price: Math.round(current * 100) / 100,
                                ldcp: Math.round(ldcp * 100) / 100,
                                prev_close: Math.round(ldcp * 100) / 100,
                                open: Math.round(open * 100) / 100,
                                high: Math.round(high * 100) / 100,
                                low: Math.round(low * 100) / 100,
                                change: Math.round(change * 100) / 100,
                                change_abs: Math.round(change * 100) / 100,
                                change_pct: Math.round(change_pct * 100) / 100,
                                volume: volume,
                                source: 'chromium_psx_live',
                                timestamp: Date.now() / 1000
                            };
                        }
                    }
                    return out;
                });

                if (domQuotes && Object.keys(domQuotes).length > 0) {
                    latestPsxQuotes = Object.assign(latestPsxQuotes, domQuotes);
                    latestPsxTimestamp = Date.now();
                    psxMonitorError = null;
                } else {
                    const html = await psxPage.content();
                    const { bySymbol, count } = parseDpsMarketWatchHtml(html);
                    if (count > 0) {
                        latestPsxQuotes = Object.assign(latestPsxQuotes, bySymbol);
                        latestPsxTimestamp = Date.now();
                        psxMonitorError = null;
                    }
                }
            } catch (pageErr) {
                const errMsg = pageErr && pageErr.message ? pageErr.message : String(pageErr);
                if (!errMsg.includes('Execution context was destroyed') && !errMsg.includes('Navigation')) {
                    if (Object.keys(latestPsxQuotes).length === 0) {
                        psxMonitorError = errMsg;
                    }
                }
            }
        }
    } catch (err) {
        try {
            await fetchPsxMarketSummaryDirect();
        } catch (_) {}
    } finally {
        isExtractingPsx = false;
    }
}

async function initPsxChromiumMonitor() {
    if (isPsxMonitorBooting || (psxPage && !psxPage.isClosed())) {
        return;
    }
    if (!puppeteer) {
        console.warn('⚠️ [PSX Monitor] Puppeteer is not available; continuing with direct network feeds.');
        return;
    }
    isPsxMonitorBooting = true;
    try {
        const psxProfileDir = process.platform === 'win32'
            ? path.join(os.tmpdir(), 'psx_chrome_profile')
            : '/tmp/psx_chrome_profile';

        try {
            fs.rmSync(psxProfileDir, { recursive: true, force: true });
        } catch (_) {}
        try {
            fs.mkdirSync(psxProfileDir, { recursive: true });
        } catch (_) {}

        cleanStaleChromiumLocks(path.resolve(__dirname, '.wwebjs_auth'));
        cleanStaleChromiumLocks(psxProfileDir);

        const execPath = getChromeExecutable();
        console.log(`🌐 [PSX Monitor] Launching persistent 1-Tab Headless Chromium in separate profile (${psxProfileDir})...`);
        psxBrowser = await puppeteer.launch({
            executablePath: execPath,
            headless: true,
            userDataDir: psxProfileDir,
            args: [
                '--headless=new',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--no-zygote',
                '--ozone-platform=headless'
            ]
        });

        psxPage = await psxBrowser.newPage();
        await psxPage.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');
        await psxPage.setViewport({ width: 1280, height: 800 });

        console.log('🌐 [PSX Monitor] Navigating to PSX market stream (https://dps.psx.com.pk/market-watch)...');
        await psxPage.goto('https://dps.psx.com.pk/market-watch', {
            waitUntil: 'domcontentloaded',
            timeout: 30000
        });

        await extractPsxMarketQuotes();
        console.log(`✅ [PSX Monitor] Connected! Loaded ${Object.keys(latestPsxQuotes).length} stock quotes.`);
    } catch (err) {
        psxMonitorError = err.message || String(err);
        console.warn(`⚠️ [PSX Monitor] Boot notice: ${psxMonitorError}. Active fallback operational.`);
    } finally {
        isPsxMonitorBooting = false;
    }
}

const cleanupPsx = async () => {
    if (psxPollTimer) clearInterval(psxPollTimer);
    if (psxBrowser) {
        try { await psxBrowser.close(); } catch (_) {}
    }
    try {
        const psxProfileDir = process.platform === 'win32'
            ? path.join(os.tmpdir(), 'psx_chrome_profile')
            : '/tmp/psx_chrome_profile';
        fs.rmSync(psxProfileDir, { recursive: true, force: true });
    } catch (_) {}
};
process.on('exit', cleanupPsx);
process.on('SIGINT', cleanupPsx);
process.on('SIGTERM', cleanupPsx);

// START EXPRESS IMMEDIATELY ON 0.0.0.0:3000
app.listen(BRIDGE_PORT, '0.0.0.0', () => {
    console.log(`⚙️  [WhatsApp Bridge] Local API listening on 0.0.0.0:${BRIDGE_PORT} instantly on startup.`);

    // 1. Immediately fetch direct market summary (<1s) so /psx-live-quotes has all 580+ quotes right away
    fetchPsxMarketSummaryDirect().then((ok) => {
        if (ok) {
            console.log(`✅ [PSX Fast Feed] Pre-populated ${Object.keys(latestPsxQuotes).length} stocks directly in <1s.`);
        }
    }).catch(() => {});

    // 2. Poll every 2 seconds
    if (psxPollTimer) clearInterval(psxPollTimer);
    psxPollTimer = setInterval(extractPsxMarketQuotes, 2000);

    // 3. Launch Chromium monitor asynchronously without blocking Express
    initPsxChromiumMonitor().catch((err) => {
        console.warn('⚠️ [PSX Monitor] Initial launch notice:', err.message || err);
    });

    // 4. Initialize WhatsApp client asynchronously
    safeInitializeClient('startup').catch((err) => {
        console.error('⚠️ [WhatsApp Bridge] Startup init error:', err.message || err);
    });
});

async function safeInitializeClient(reason = 'startup') {
    if (isInitializing || isClientReady) {
        return;
    }
    isInitializing = true;
    try {
        cleanStaleChromiumLocks(path.resolve(__dirname, '.wwebjs_auth'));
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

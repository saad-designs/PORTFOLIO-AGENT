const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const axios = require('axios');
const FormData = require('form-data');
const express = require('express');

const app = express();
app.use(express.json());
const client = new Client({
    authStrategy: new LocalAuth({
        dataPath: './.wwebjs_auth'
    }),
    puppeteer: {
        headless: true, // Reverted to true for silent background operation
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
        protocolTimeout: 300000 // Forces the browser to wait for WhatsApp to load
    }
});

client.on('qr', (qr) => {
    console.log('\n======================================================');
    console.log('🔗 [WhatsApp Bridge] SCAN THIS QR CODE');
    qrcode.generate(qr, {small: true});
    console.log('======================================================\n');
});

client.on('code', (code) => {
    console.log('\n======================================================');
    console.log('🔗 [WhatsApp Bridge] LINK WITH PHONE NUMBER');
    console.log(`Type this Pairing Code into your WhatsApp app: ${code}`);
    console.log('======================================================\n');
});

// Monitored KTrade Group IDs
const KTRADE_CHAT_IDS = [
    '120363405626670375@g.us', // KTrade - E Trader Assistance 6
    '120363404462896702@g.us'  // KTrade - E Trader Assistance 6 (Secondary)
];

// Unified forwarder for multimodal media (images, PDFs, voice notes) and text intelligence
async function forwardMessageToPython(msg, chatName, chatId) {
    try {
        const targetChatId = chatId || (msg.from ? (msg.from._serialized || msg.from) : '') || '120363405626670375@g.us';
        const sender = msg.author || msg.from || 'KTrade';
        const timestamp = msg.timestamp || Math.floor(Date.now() / 1000);

        if (msg.hasMedia) {
            let media = null;
            try {
                media = await msg.downloadMedia();
            } catch (dlErr) {
                console.warn(`⚠️ [WhatsApp Bridge] Attachment download threw for msg ${msg.id?.id || 'unknown'}: ${dlErr.message}`);
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

                        await axios.post('http://127.0.0.1:8000/api/ingest-whatsapp-message', payload, {
                            maxContentLength: 50 * 1024 * 1024,
                            maxBodyLength: 50 * 1024 * 1024,
                            timeout: 120000
                        });
                        return;
                    } else {
                        console.warn(`⚠️ [WhatsApp Bridge] Media exceeds 15MB limit (${(sizeBytes / (1024*1024)).toFixed(1)} MB). Falling back to text if available.`);
                    }
                } else {
                    console.log(`ℹ️ [WhatsApp Bridge] Skipping unsupported media type (${mime})`);
                }
            } else {
                console.warn(`⚠️ [WhatsApp Bridge] Media data empty for message ID: ${msg.id?.id || 'unknown'}`);
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
            await axios.post('http://127.0.0.1:8000/api/ingest-whatsapp-message', payload, {
                timeout: 30000
            });
        }
    } catch (err) {
        console.error(`❌ [WhatsApp Bridge] Forwarding failed for message ID ${msg.id?.id || 'unknown'}:`, err.message);
    }
}

let isClientReady = false;

client.on('ready', async () => {
    console.log('✅ [WhatsApp Bridge] Client authenticated. Live listener active.');
    isClientReady = true;
});

// Health check endpoint
app.get('/ping', (req, res) => {
    if (isClientReady) {
        res.json({ status: 'pong', message: 'WhatsApp Bridge is live and ready.' });
    } else {
        res.status(503).json({ status: 'starting', message: 'Chromium is still booting...' });
    }
});

// List all group chats with their serialized @g.us IDs
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
                    unreadCount: c.unreadCount || 0
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

// Start local API sweep endpoint
app.get('/api/trigger-sweep', async (req, res) => {
    console.log('🔄 [WhatsApp Bridge] Python requested startup catch-up sweep...');
    try {
        if (!client || !client.pupPage) {
            return res.status(200).json({ status: 'skipped', message: 'Client not ready' });
        }

        const chats = await client.getChats().catch(() => []);
        const ktradeChats = chats.filter(c => {
            const id = c.id?._serialized || '';
            const name = c.name || '';
            return KTRADE_CHAT_IDS.includes(id) || name.includes('KTrade');
        });

        if (ktradeChats.length === 0) {
            return res.status(200).json({ status: 'skipped', message: 'KTrade chats not found.' });
        }

        for (const ktradeChat of ktradeChats) {
            const messages = await ktradeChat.fetchMessages({ limit: 50 }).catch(() => []);
            for (const msg of messages) {
                try {
                    await forwardMessageToPython(msg, ktradeChat.name || 'KTrade', ktradeChat.id?._serialized);
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
    const chatIdsToScan = (target_chat_ids && target_chat_ids.length > 0) ? target_chat_ids : KTRADE_CHAT_IDS;
    console.log(`🔄 [WhatsApp Bridge] Catching up on messages since timestamp: ${since_timestamp || 0} for ${chatIdsToScan.length} chats...`);

    let processedCount = 0;

    for (const chatId of chatIdsToScan) {
        try {
            let handled = false;
            let chatName = 'KTrade - E Trader Assistance 6';

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

// START EXPRESS IMMEDIATELY
app.listen(3000, () => console.log('⚙️  [WhatsApp Bridge] Local API listening on port 3000. Waiting for Chrome...'));

// Live Event Listener (harden to accept audio, ptt, images, and match KTrade group IDs directly)
client.on('message_create', async (msg) => {
    const allowedTypes = ['chat', 'image', 'document', 'audio', 'ptt', 'video', 'voice'];
    if (!allowedTypes.includes(msg.type) && !msg.hasMedia) {
        return;
    }
    if (msg.isStatus) {
        return;
    }

    try {
        const fromId = msg.from ? (msg.from._serialized || msg.from) : '';
        const toId = msg.to ? (msg.to._serialized || msg.to) : '';

        // Check if message is from/to a monitored KTrade chat ID directly
        const isDirectMatch = KTRADE_CHAT_IDS.includes(fromId) || KTRADE_CHAT_IDS.includes(toId);

        let chat = null;
        let chatName = 'KTrade - E Trader Assistance 6';
        try {
            chat = await msg.getChat();
            if (chat) chatName = chat.name || chatName;
        } catch (getChatErr) {
            // Suppress expected getChat error when Store changes, fall back to direct ID match
        }

        const isNameMatch = chatName && chatName.includes('KTrade');
        if (!isDirectMatch && !isNameMatch) {
            return;
        }

        const effectiveChatId = isDirectMatch ? (KTRADE_CHAT_IDS.includes(fromId) ? fromId : toId) : (chat?.id?._serialized || fromId);
        console.log(`[Diagnostic] Live KTrade Message! Chat: "${chatName}", ID: ${effectiveChatId}, Type: ${msg.type}, Media: ${msg.hasMedia}`);

        await forwardMessageToPython(msg, chatName, effectiveChatId);
    } catch (err) {
        const errorMsg = err.message || err;
        if (errorMsg !== 'r') {
            console.warn(`⚠️ [WhatsApp Bridge] Live event skipped: ${errorMsg}`);
        }
    }
});

client.initialize();


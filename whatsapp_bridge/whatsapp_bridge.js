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

// Unified forwarder for both binary attachments and raw text/links
async function forwardMessageToPython(msg, chatName) {
    try {
        if (msg.hasMedia) {
            const media = await msg.downloadMedia();
            if (!media || !media.data) {
                console.warn(`⚠️ [WhatsApp Bridge] Attachment download failed for message ID: ${msg.id.id}`);
                return;
            }

            const isImage = media.mimetype.startsWith('image/');
            const isPdf = media.mimetype === 'application/pdf';
            if (!isImage && !isPdf) return;

            const fileBuffer = Buffer.from(media.data, 'base64');
            const filename = media.filename || (isPdf ? `doc_${Date.now()}.pdf` : `img_${Date.now()}.png`);

            const formData = new FormData();
            formData.append('file', fileBuffer, { filename: filename, contentType: media.mimetype });
            formData.append('caption', msg.body || '');
            formData.append('source', chatName);

            console.log(`🚀 [WhatsApp Bridge] Forwarding ${filename} to Python backend...`);
            await axios.post('http://127.0.0.1:8000/api/ingest-whatsapp-media', formData, {
                headers: formData.getHeaders(),
                maxContentLength: Infinity,
                maxBodyLength: Infinity,
                timeout: 120000
            });

        } else if (msg.body && msg.body.trim().length > 10) {
            // Forward raw text, market commentary, and research links
            console.log(`🚀 [WhatsApp Bridge] Forwarding text intel (${msg.body.trim().length} chars) from "${chatName}"...`);
            await axios.post('http://127.0.0.1:8000/api/ingest-whatsapp-text', {
                text: msg.body.trim(),
                source: chatName,
                timestamp: msg.timestamp || Math.floor(Date.now() / 1000)
            }, {
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

// Health check endpoint (Now checks the boolean state)
app.get('/ping', (req, res) => {
    if (isClientReady) {
        res.json({ status: 'pong', message: 'WhatsApp Bridge is live and ready.' });
    } else {
        res.status(503).json({ status: 'starting', message: 'Chromium is still booting...' });
    }
});

// Start local API so Python can trigger historical sweeps
app.get('/api/trigger-sweep', async (req, res) => {
    console.log('🔄 [WhatsApp Bridge] Python requested startup catch-up sweep...');
    try {
        if (!client || !client.pupPage) {
            return res.status(200).json({ status: 'skipped', message: 'Client not ready' });
        }

        const chats = await client.getChats().catch(() => []);
        const ktradeChat = chats.find(c => c.name && c.name.includes('KTrade'));

        if (!ktradeChat) {
            return res.status(200).json({ status: 'skipped', message: 'KTrade chat not found.' });
        }

        const messages = await ktradeChat.fetchMessages({ limit: 50 }).catch(() => []);
        
        for (const msg of messages) {
            try {
                const chat = await msg.getChat();
                if (chat && chat.name && chat.name.includes('KTrade')) {
                    await forwardMessageToPython(msg, chat.name);
                }
            } catch (err) {
                console.warn(`⚠️ [WhatsApp Bridge] Sweep evaluation skipped an unresolvable message: ${err.message}`);
            }
        }

        res.status(200).json({ status: 'success', message: 'Sweep completed.' });
    } catch (error) {
        console.error('⚠️ [WhatsApp Bridge] Handled Sweep Exception:', error.message || error);
        // Return 200 with skipped flag to prevent Python from repeating the request endlessly
        res.status(200).json({ status: 'partial_failure', error: String(error) });
    }
});

app.post('/api/catchup', async (req, res) => {
    const { since_timestamp, target_chat_ids } = req.body;
    console.log(`🔄 [WhatsApp Bridge] Catching up on messages since timestamp: ${since_timestamp}...`);

    let processedCount = 0;

    for (const chatId of target_chat_ids) {
        try {
            const chat = await client.getChatById(chatId);
            // Fetch the last 500 messages from the chat (increased for robust fault-tolerance)
            const messages = await chat.fetchMessages({ limit: 500 });

            // Filter for messages that arrived AFTER our last checkpoint
            const unanalyzed = messages
                .filter(m => m.timestamp > since_timestamp)
                .sort((a, b) => a.timestamp - b.timestamp); // Chronological order

            for (const msg of unanalyzed) {
                // Forward through standard pipeline to Python
                await forwardMessageToPython(msg, chat.name);
                processedCount++;
            }
        } catch (err) {
            console.error(`❌ Failed catch-up for chat ${chatId}:`, err.message);
        }
    }

    console.log(`✅ Catch-up complete. Replayed and analyzed ${processedCount} missed messages.`);
    res.json({ status: 'ok', replayed: processedCount });
});

// START EXPRESS IMMEDIATELY, OUTSIDE THE READY EVENT
app.listen(3000, () => console.log('⚙️  [WhatsApp Bridge] Local API listening on port 3000. Waiting for Chrome...'));

// Live Event Listener
client.on('message_create', async (msg) => {
    // Drop system events, reactions, and broadcasts BEFORE touching Puppeteer DOM
    const allowedTypes = ['chat', 'image', 'document'];
    if (!allowedTypes.includes(msg.type) || msg.isStatus) {
        return;
    }

    try {
        const chat = await msg.getChat();
        if (chat) {
            console.log(`[Diagnostic] Live Message Received! Name: "${chat.name}", ID: ${chat.id._serialized}`);
            if (chat.name && chat.name.includes('KTrade')) {
                await forwardMessageToPython(msg, chat.name);
            }
        }
    } catch (err) {
        // Ignore minified 'r' errors from unresolvable background DOM elements
        const errorMsg = err.message || err;
        if (errorMsg !== 'r') {
            console.warn(`⚠️ [WhatsApp Bridge] Live event skipped: ${errorMsg}`);
        }
    }
});

client.initialize();

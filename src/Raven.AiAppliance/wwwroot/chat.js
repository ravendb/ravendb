(function () {
    'use strict';

    const els = {
        agentId:     document.getElementById('agent-id'),
        connectBtn:  document.getElementById('connect-btn'),
        newchatBtn:  document.getElementById('newchat-btn'),
        prompt:      document.getElementById('prompt'),
        sendBtn:     document.getElementById('send-btn'),
        feed:        document.getElementById('feed'),
        healthDot:   document.getElementById('health-dot'),
        healthLabel: document.getElementById('health-label'),
        convId:      document.getElementById('conv-id'),
    };

    const state = {
        agentId:        null,
        conversationId: null,
        inflight:       false,
    };

    async function probeHealth() {
        try {
            const r = await fetch('/healthz', { cache: 'no-store' });
            if (!r.ok) throw new Error('http ' + r.status);
            els.healthDot.className = 'dot dot-ok';
            els.healthLabel.textContent = 'appliance ready';
        } catch (e) {
            els.healthDot.className = 'dot dot-bad';
            els.healthLabel.textContent = 'appliance not ready';
        }
    }
    probeHealth();
    setInterval(probeHealth, 15000);

    els.connectBtn.addEventListener('click', onConnect);
    els.newchatBtn.addEventListener('click', onNewChat);
    els.sendBtn.addEventListener('click', onSend);
    els.prompt.addEventListener('keydown', e => {
        if (e.key === 'Enter' && !e.shiftKey && !state.inflight) {
            e.preventDefault();
            onSend();
        }
    });
    els.agentId.addEventListener('keydown', e => {
        if (e.key === 'Enter') { e.preventDefault(); onConnect(); }
    });

    function onConnect() {
        const aid = els.agentId.value.trim();
        if (!aid) {
            appendSystem('Enter an agent id (e.g. demo-agent).');
            return;
        }
        state.agentId = aid;
        state.conversationId = null;
        updateConvIdLabel();

        els.agentId.disabled = true;
        els.connectBtn.disabled = true;
        els.newchatBtn.disabled = false;
        els.prompt.disabled = false;
        els.sendBtn.disabled = false;
        els.prompt.focus();

        clearHint();
        appendSystem(`Connected to agent "${aid}". Until T-3's wizard provisions it server-side, the stream will return an error.`);
    }

    function onNewChat() {
        state.agentId = null;
        state.conversationId = null;
        updateConvIdLabel();

        els.agentId.disabled = false;
        els.connectBtn.disabled = false;
        els.newchatBtn.disabled = true;
        els.prompt.disabled = true;
        els.sendBtn.disabled = true;
        els.prompt.value = '';
        els.agentId.focus();

        els.feed.innerHTML = '';
        appendSystem('Conversation cleared.');
    }

    async function onSend() {
        if (state.inflight) return;
        const prompt = els.prompt.value.trim();
        if (!prompt) return;
        if (!state.agentId) {
            appendSystem('Connect to an agent first.');
            return;
        }

        appendUser(prompt);
        els.prompt.value = '';

        const botRow = appendBotShell();
        const bubble = botRow.querySelector('.bubble');
        bubble.classList.add('streaming');

        state.inflight = true;
        els.sendBtn.disabled = true;
        els.prompt.disabled = true;

        try {
            const res = await fetch('/api/chat/stream', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    agentId:        state.agentId,
                    prompt:         prompt,
                    conversationId: state.conversationId,
                }),
            });

            if (!res.ok) {
                const errText = await safeText(res);
                throw new Error(`HTTP ${res.status}: ${errText || res.statusText}`);
            }

            await consumeNdjson(res, ev => {
                if (ev.type === 'chunk') {
                    bubble.textContent = (bubble.textContent || '') + (ev.text || '');
                    scrollToEnd();
                } else if (ev.type === 'done') {
                    bubble.classList.remove('streaming');
                    state.conversationId = ev.conversationId || state.conversationId;
                    updateConvIdLabel();
                } else if (ev.type === 'error') {
                    botRow.remove();
                    appendError(ev.message || 'agent returned an error');
                }
            });

            bubble.classList.remove('streaming');
            if (!bubble.textContent) {
                botRow.remove();
                appendError('agent returned an empty response');
            }
        } catch (e) {
            botRow.remove();
            appendError(e.message || String(e));
        } finally {
            state.inflight = false;
            els.sendBtn.disabled = false;
            els.prompt.disabled = false;
            els.prompt.focus();
        }
    }

    async function consumeNdjson(res, onEvent) {
        const reader  = res.body.getReader();
        const decoder = new TextDecoder('utf-8');
        let buf = '';
        while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            buf += decoder.decode(value, { stream: true });
            let nl;
            while ((nl = buf.indexOf('\n')) >= 0) {
                const line = buf.slice(0, nl);
                buf = buf.slice(nl + 1);
                if (!line.trim()) continue;
                try {
                    onEvent(JSON.parse(line));
                } catch (e) {
                    onEvent({ type: 'error', message: 'malformed event: ' + line });
                }
            }
        }
        if (buf.trim()) {
            try { onEvent(JSON.parse(buf)); } catch { /* tolerate */ }
        }
    }

    async function safeText(res) {
        try { return await res.text(); } catch { return ''; }
    }

    function clearHint() {
        const hint = els.feed.querySelector('.hint');
        if (hint) hint.remove();
    }

    function appendUser(text) {
        const row = document.createElement('div');
        row.className = 'row you';
        row.innerHTML = '<div class="who">you</div><div class="bubble"></div>';
        row.querySelector('.bubble').textContent = text;
        els.feed.appendChild(row);
        scrollToEnd();
        return row;
    }

    function appendBotShell() {
        const row = document.createElement('div');
        row.className = 'row bot';
        row.innerHTML = '<div class="who">agent</div><div class="bubble"></div>';
        els.feed.appendChild(row);
        scrollToEnd();
        return row;
    }

    function appendError(text) {
        const row = document.createElement('div');
        row.className = 'row error';
        row.innerHTML = '<div class="who">error</div><div class="bubble"></div>';
        row.querySelector('.bubble').textContent = text;
        els.feed.appendChild(row);
        scrollToEnd();
    }

    function appendSystem(text) {
        const row = document.createElement('div');
        row.className = 'row system';
        row.innerHTML = '<div class="who">system</div><div class="bubble"></div>';
        row.querySelector('.bubble').textContent = text;
        els.feed.appendChild(row);
        scrollToEnd();
    }

    function updateConvIdLabel() {
        els.convId.textContent = state.conversationId ? `conv: ${state.conversationId}` : '';
    }

    function scrollToEnd() {
        els.feed.scrollTop = els.feed.scrollHeight;
    }
})();

const chatbox = document.getElementById('chatbox');
const userInput = document.getElementById('userInput');
const authUser = document.getElementById('authUser');
const loginBtn = document.getElementById('loginBtn');
const registerBtn = document.getElementById('registerBtn');
const logoutBtn = document.getElementById('logoutBtn');
let seenNotifications = new Set();
let swRegistration = null;
let webPushEnabled = false;
let vapidPublicKey = '';

function quickSend(val) {
    userInput.value = val;
    send();
}

async function send() {
    const text = userInput.value.trim();
    if (!text) return;

    appendMessage('user', text);
    userInput.value = '';

    const loadingId = 'loading-' + Date.now();
    appendMessage('bot', '...', loadingId);

    try {
        const response = await fetch('/get', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: text })
        });
        const data = await response.json();

        const loadingNode = document.getElementById(loadingId);
        if (loadingNode) loadingNode.remove();
        appendMessage('bot', data.reply || 'I could not process that.');
    } catch (e) {
        const loadingNode = document.getElementById(loadingId);
        if (loadingNode) {
            loadingNode.querySelector('.text').innerText = 'Server error. Try again.';
        } else {
            appendMessage('bot', 'Server error. Try again.');
        }
    }
}

function appendMessage(sender, text, id = null) {
    const msgDiv = document.createElement('div');
    msgDiv.className = `${sender}-message message animate-in`;
    if (id) msgDiv.id = id;

    const avatar = sender === 'bot' ? '\u2728' : '\uD83D\uDC64';
    const formattedText = String(text || '').replace(/\n/g, '<br>');

    msgDiv.innerHTML = `
        <div class="avatar">${avatar}</div>
        <div class="text">${formattedText}</div>
    `;

    chatbox.appendChild(msgDiv);
    chatbox.scrollTo({ top: chatbox.scrollHeight, behavior: 'smooth' });
}

function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - (base64String.length % 4)) % 4);
    const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
    const rawData = atob(base64);
    const outputArray = new Uint8Array(rawData.length);
    for (let i = 0; i < rawData.length; ++i) {
        outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
}

async function ensurePushSubscription() {
    if (!webPushEnabled || !swRegistration || !vapidPublicKey) return;
    if (!('PushManager' in window)) return;

    if (Notification.permission === 'default') {
        try {
            await Notification.requestPermission();
        } catch (e) {
            return;
        }
    }
    if (Notification.permission !== 'granted') return;

    const existing = await swRegistration.pushManager.getSubscription();
    const sub = existing || await swRegistration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(vapidPublicKey)
    });

    await fetch('/push/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ subscription: sub.toJSON() })
    });
}

async function initWebPush() {
    if (!('serviceWorker' in navigator) || !('Notification' in window)) return;

    try {
        const configRes = await fetch('/push/public-key');
        if (!configRes.ok) return;

        const config = await configRes.json();
        webPushEnabled = !!config.enabled;
        vapidPublicKey = config.publicKey || '';
        if (!webPushEnabled || !vapidPublicKey) return;

        swRegistration = await navigator.serviceWorker.register('/sw.js');
        await ensurePushSubscription();
    } catch (e) {
        // Ignore push setup errors
    }
}

async function pollNotifications() {
    try {
        const res = await fetch('/notifications');
        if (!res.ok) return;

        const data = await res.json();
        const notifications = data.notifications || [];
        notifications.forEach((n) => {
            const key = `${n.title}|${n.message}`;
            if (seenNotifications.has(key)) return;
            seenNotifications.add(key);
            appendMessage('bot', `${n.title}\n${n.message}`);

            if ('Notification' in window && Notification.permission === 'granted') {
                new Notification(n.title, { body: n.message });
            }
        });
    } catch (e) {
        // Ignore polling errors
    }
}

setInterval(pollNotifications, 30000);
pollNotifications();

async function refreshAuthState() {
    try {
        const res = await fetch('/auth/me');
        if (!res.ok) return;
        const data = await res.json();

        if (data.authenticated) {
            authUser.textContent = data.user.username;
            loginBtn.style.display = 'none';
            registerBtn.style.display = 'none';
            logoutBtn.style.display = 'inline-flex';
        } else {
            authUser.textContent = 'Guest';
            loginBtn.style.display = 'inline-flex';
            registerBtn.style.display = 'inline-flex';
            logoutBtn.style.display = 'none';
        }

        await ensurePushSubscription();
    } catch (e) {
        // Ignore auth state errors
    }
}

async function doAuth(path) {
    const username = prompt('Username:');
    if (!username) return;

    const password = prompt('Password (min 6 chars):');
    if (!password) return;

    try {
        const res = await fetch(path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });
        const data = await res.json();

        if (!res.ok) {
            appendMessage('bot', data.error || 'Authentication failed.');
            return;
        }

        appendMessage('bot', `Welcome, ${data.user.username}!`);
        await refreshAuthState();
    } catch (e) {
        appendMessage('bot', 'Authentication server error.');
    }
}

loginBtn?.addEventListener('click', () => doAuth('/auth/login'));
registerBtn?.addEventListener('click', () => doAuth('/auth/register'));
logoutBtn?.addEventListener('click', async () => {
    try {
        await fetch('/auth/logout', { method: 'POST' });
        appendMessage('bot', 'Logged out.');
        await refreshAuthState();
    } catch (e) {
        // Ignore logout errors
    }
});

(async () => {
    await initWebPush();
    await refreshAuthState();
})();

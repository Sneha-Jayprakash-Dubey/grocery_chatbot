const chatbox = document.getElementById('chatbox');
const userInput = document.getElementById('userInput');

function quickSend(val) {
    userInput.value = val;
    send();
}

async function send() {
    const text = userInput.value.trim();
    if(!text) return;

    appendMessage('user', text);
    userInput.value = '';

    // Typing state
    const loadingId = 'loading-' + Date.now();
    appendMessage('bot', '...', loadingId);

    try {
        const response = await fetch('/get', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ message: text })
        });
        const data = await response.json();
        
        document.getElementById(loadingId).remove();
        appendMessage('bot', data.reply);
    } catch (e) {
        document.getElementById(loadingId).innerText = "Server Error. Try again.";
    }
}
function appendMessage(sender, text, id = null) {
    const msgDiv = document.createElement('div');
    // Added 'animate-in' class for the slide-up effect you liked
    msgDiv.className = `${sender}-message message animate-in`;
    if(id) msgDiv.id = id;
    
    const avatar = sender === 'bot' ? '✨' : '👤';
    
    // Replace newlines with <br> for proper list display
    const formattedText = text.replace(/\n/g, '<br>');
    
    msgDiv.innerHTML = `
        <div class="avatar">${avatar}</div>
        <div class="text">${formattedText}</div>
    `;
    
    chatbox.appendChild(msgDiv);
    chatbox.scrollTo({ top: chatbox.scrollHeight, behavior: 'smooth' });
}

/**
 * app.js
 * ------------------------------------------------------------
 * Frontend application logic, kept out of index.html so the
 * markup, styling, and behavior are three separate concerns
 * (same principle the backend follows across its modules).
 *
 * Notable pieces beyond the baseline tutorial's frontend:
 *   - Automatic reconnection with exponential backoff, so a
 *     dropped Wi-Fi connection recovers on its own instead of
 *     leaving the user stuck on a dead socket.
 *   - Multi-room UI (tab list + switching) and a private-message
 *     panel, both driven by the extended wire protocol.
 *   - Admin controls (mute/kick) rendered only for admins.
 *   - "Delivered" ticks using per-message ack from the server.
 * ------------------------------------------------------------
 */

(() => {
  // ---------- DOM references ----------
  const gate = document.getElementById("gate");
  const app = document.getElementById("app");
  const joinForm = document.getElementById("joinForm");
  const usernameInput = document.getElementById("username");
  const wsUrlInput = document.getElementById("wsUrl");
  const roomInput = document.getElementById("roomInput");
  const joinErr = document.getElementById("joinErr");

  const statusDot = document.getElementById("statusDot");
  const statusText = document.getElementById("statusText");
  const meEl = document.getElementById("me");

  const roomListEl = document.getElementById("roomList");
  const userListEl = document.getElementById("userList");
  const newRoomForm = document.getElementById("newRoomForm");
  const newRoomInput = document.getElementById("newRoomInput");

  const threadTitle = document.getElementById("threadTitle");
  const closeDmBtn = document.getElementById("closeDmBtn");
  const log = document.getElementById("log");
  const typingEl = document.getElementById("typing");
  const composer = document.getElementById("composer");
  const msgInput = document.getElementById("msgInput");
  const sendBtn = document.getElementById("sendBtn");
  const charCounter = document.getElementById("charCounter");

  const soundToggleBtn = document.getElementById("soundToggleBtn");
  const themeToggleBtn = document.getElementById("themeToggleBtn");
  const clearChatBtn = document.getElementById("clearChatBtn");

  // ---------- Connection / session state ----------
  let ws = null;
  let myUsername = null;
  let myIsAdmin = false;
  let currentRoom = null;
  let rooms = []; // [{name, memberCount}]
  let roomUsers = []; // usernames in currentRoom
  let onlineUsers = new Set(); // all usernames connected to the server
  let presenceByUser = new Map(); // username -> 'online'|'away'|'offline'

  let activeThread = { kind: "room", name: null }; // {kind:'room'|'dm', name}
  const dmThreads = new Map(); // username -> [{from,to,text,timestamp,id}]
  const roomMessageCache = new Map(); // room -> messages currently rendered (for thread switching without refetch flicker)

  let reconnectAttempts = 0;
  let intentionalClose = false;
  let typingTimeout = null;

  const wsProto = location.protocol === "https:" ? "wss:" : "ws:";
  wsUrlInput.value = `${wsProto}//${location.host || "localhost:5000"}/ws`;

  // ---------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------
  function fmtTime(ts) {
    return new Date(ts).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function escapeAttr(str) {
    return String(str).replace(/"/g, "&quot;");
  }

  function scrollToBottom() {
    log.scrollTop = log.scrollHeight;
  }

  function clearLog() {
    log.innerHTML = "";
  }

  function setConnectionStatus(state) {
    // state: 'live' | 'reconnecting' | 'dead'
    statusDot.className =
      "dot" +
      (state === "live"
        ? " live"
        : state === "reconnecting"
          ? " reconnecting"
          : "");
    statusText.textContent =
      state === "live"
        ? "connected"
        : state === "reconnecting"
          ? `reconnecting… (attempt ${reconnectAttempts})`
          : "disconnected";
  }

  // ---------------------------------------------------------
  // Client-only preferences: sound toggle, theme toggle,
  // clear-chat, character counter.
  // These are purely local UI state — nothing here is sent to
  // the server or changes the wire protocol in any way.
  // ---------------------------------------------------------
  let soundEnabled = true;

  function playNotifySound() {
    if (!soundEnabled) return;
    try {
      const ctx = new (window.AudioContext || window.webkitAudioContext)();
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = "sine";
      osc.frequency.value = 880;
      gain.gain.setValueAtTime(0.001, ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.15, ctx.currentTime + 0.01);
      gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.18);
      osc.connect(gain).connect(ctx.destination);
      osc.start();
      osc.stop(ctx.currentTime + 0.2);
    } catch {
      // Some browsers block AudioContext before any user gesture;
      // fail silently rather than throwing in the message pipeline.
    }
  }

  soundToggleBtn.addEventListener("click", () => {
    soundEnabled = !soundEnabled;
    soundToggleBtn.textContent = soundEnabled ? "🔔" : "🔕";
    soundToggleBtn.classList.toggle("off", !soundEnabled);
  });

  themeToggleBtn.addEventListener("click", () => {
    const isLight = document.body.classList.toggle("light-theme");
    themeToggleBtn.textContent = isLight ? "☀️" : "🌙";
  });

  clearChatBtn.addEventListener("click", () => {
    // Clears only what's rendered in the DOM right now. The underlying
    // room/DM history in roomMessageCache / dmThreads (and everything
    // on the server) is untouched, so switching threads or reconnecting
    // still shows the real history.
    clearLog();
    appendSystem("Chat view cleared (local only — history is unaffected).");
  });

  function updateCharCounter() {
    const len = msgInput.value.length;
    const max = Number(msgInput.getAttribute("maxlength")) || 1000;
    charCounter.textContent = `${len} / ${max}`;
    charCounter.classList.toggle("near-limit", len >= max * 0.9 && len < max);
    charCounter.classList.toggle("at-limit", len >= max);
  }
  updateCharCounter();

  // ---------------------------------------------------------
  // Rendering
  // ---------------------------------------------------------
  function appendMessage({ id, username, from, text, timestamp }, kind) {
    const author = kind === "dm" ? from : username;
    const mine = author === myUsername;
    const wrap = document.createElement("div");
    wrap.className =
      "msg" + (mine ? " mine" : "") + (kind === "dm" ? " dm" : "");
    if (id) wrap.dataset.msgId = id;

    const meta = document.createElement("div");
    meta.className = "meta";
    meta.textContent = `${mine ? "you" : author} · ${fmtTime(timestamp)}`;
    if (kind === "dm") {
      const badge = document.createElement("span");
      badge.className = "badge-dm";
      badge.textContent = "DM";
      meta.appendChild(badge);
    }

    const bubble = document.createElement("div");
    bubble.className = "bubble";
    bubble.textContent = text; // textContent -> immune to HTML/script injection

    wrap.appendChild(meta);
    wrap.appendChild(bubble);
    log.appendChild(wrap);
    scrollToBottom();
  }

  function appendSystem(text, danger = false) {
    const el = document.createElement("div");
    el.className = "system" + (danger ? " danger" : "");
    el.textContent = text;
    log.appendChild(el);
    scrollToBottom();
  }

  function markDelivered(id) {
    const el = log.querySelector(`[data-msg-id="${CSS.escape(id)}"] .meta`);
    if (el && !el.querySelector(".ticks")) {
      const tick = document.createElement("span");
      tick.className = "ticks";
      tick.textContent = "✓ delivered";
      el.appendChild(tick);
    }
  }

  function renderRoomList() {
    roomListEl.innerHTML = "";
    rooms.forEach((r) => {
      const item = document.createElement("div");
      item.className =
        "room-item" +
        (activeThread.kind === "room" && activeThread.name === r.name
          ? " active"
          : "");
      item.innerHTML = `<span>#${escapeAttr(r.name)}</span><span class="count">${r.memberCount}</span>`;
      item.addEventListener("click", () => switchToRoom(r.name));
      roomListEl.appendChild(item);
    });
  }

  function renderUserList() {
    userListEl.innerHTML = "";
    const names = Array.from(onlineUsers).sort((a, b) => a.localeCompare(b));
    names.forEach((name) => {
      if (name === myUsername) return;
      const presence = presenceByUser.get(name) || "online";
      const item = document.createElement("div");
      item.className = "user-item";

      const dot = document.createElement("span");
      dot.className = `presence-dot ${presence}`;
      item.appendChild(dot);

      const uname = document.createElement("span");
      uname.className = "uname";
      uname.textContent = name;
      item.appendChild(uname);

      if (myIsAdmin) {
        const actions = document.createElement("span");
        actions.className = "admin-actions";
        const muteBtn = document.createElement("button");
        muteBtn.textContent = "mute";
        muteBtn.title = "Toggle mute";
        muteBtn.addEventListener("click", (e) => {
          e.stopPropagation();
          send({ type: "mute", target: name });
        });
        const kickBtn = document.createElement("button");
        kickBtn.textContent = "kick";
        kickBtn.addEventListener("click", (e) => {
          e.stopPropagation();
          if (confirm(`Kick ${name}?`)) send({ type: "kick", target: name });
        });
        actions.appendChild(muteBtn);
        actions.appendChild(kickBtn);
        item.appendChild(actions);
      }

      item.addEventListener("click", () => switchToDm(name));
      userListEl.appendChild(item);
    });
  }

  function switchToRoom(name) {
    if (activeThread.kind === "room" && activeThread.name === name) return;
    activeThread = { kind: "room", name };
    threadTitle.textContent = `#${name}`;
    closeDmBtn.style.display = "none";
    if (name !== currentRoom) {
      send({ type: "switch_room", room: name });
      // clearLog() + render happens once 'room_switched' arrives, to avoid
      // flashing an empty log before history comes back.
    } else {
      renderThreadFromCache();
    }
    renderRoomList();
  }

  function switchToDm(username) {
    activeThread = { kind: "dm", name: username };
    threadTitle.textContent = `DM · ${username}`;
    closeDmBtn.style.display = "inline";
    renderThreadFromCache();
  }

  closeDmBtn.addEventListener("click", () => switchToRoom(currentRoom));

  function renderThreadFromCache() {
    clearLog();
    if (activeThread.kind === "room") {
      const msgs = roomMessageCache.get(activeThread.name) || [];
      msgs.forEach((m) => appendMessage(m, "room"));
    } else {
      const msgs = dmThreads.get(activeThread.name) || [];
      msgs.forEach((m) => appendMessage(m, "dm"));
    }
  }

  // ---------------------------------------------------------
  // Outbound
  // ---------------------------------------------------------
  function send(obj) {
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  }

  joinForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const username = usernameInput.value.trim();
    if (!username) return;
    myUsername = username;
    joinErr.textContent = "";
    intentionalClose = false;
    reconnectAttempts = 0;
    openSocket();
  });

  newRoomForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const name = newRoomInput.value.trim();
    if (!name) return;
    send({ type: "create_room", room: name });
    newRoomInput.value = "";
  });

  composer.addEventListener("submit", (e) => {
    e.preventDefault();
    const text = msgInput.value.trim();
    if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;
    if (activeThread.kind === "room") {
      send({ type: "message", text });
    } else {
      send({ type: "private_message", to: activeThread.name, text });
    }
    msgInput.value = "";
  });

  msgInput.addEventListener("input", () => {
    updateCharCounter();
    if (!ws || ws.readyState !== WebSocket.OPEN || activeThread.kind !== "room")
      return;
    clearTimeout(typingTimeout);
    send({ type: "typing" });
    typingTimeout = setTimeout(() => {}, 800);
  });

  // ---------------------------------------------------------
  // Connection lifecycle + reconnection with exponential backoff
  // ---------------------------------------------------------
  function openSocket() {
    const url = wsUrlInput.value.trim();
    setConnectionStatus("reconnecting");

    let socket;
    try {
      socket = new WebSocket(url);
    } catch {
      joinErr.textContent = "Invalid server address.";
      return;
    }
    ws = socket;

    socket.addEventListener("open", () => {
      reconnectAttempts = 0;
      const room = (roomInput.value || "").trim() || currentRoom || undefined;
      send({ type: "join", username: myUsername, room });
    });

    socket.addEventListener("message", (event) =>
      onServerMessage(JSON.parse(event.data)),
    );

    socket.addEventListener("close", () => {
      setConnectionStatus("dead");
      if (app.classList.contains("active"))
        appendSystem("Connection lost.", true);
      if (!intentionalClose) scheduleReconnect();
    });

    socket.addEventListener("error", () => {
      joinErr.textContent =
        "Could not connect. Check the server address and that the server is running.";
    });
  }

  function scheduleReconnect() {
    reconnectAttempts += 1;
    const delayMs = Math.min(16000, 1000 * 2 ** (reconnectAttempts - 1)); // 1s,2s,4s,8s,16s cap
    setConnectionStatus("reconnecting");
    appendSystem(`Reconnecting in ${Math.round(delayMs / 1000)}s…`);
    setTimeout(() => {
      if (!intentionalClose) openSocket();
    }, delayMs);
  }

  // ---------------------------------------------------------
  // Inbound message dispatch
  // ---------------------------------------------------------
  function onServerMessage(data) {
    switch (data.type) {
      case "welcome": {
        myUsername = data.username;
        myIsAdmin = !!data.isAdmin;
        currentRoom = data.room;
        rooms = data.rooms;
        roomUsers = data.users;
        onlineUsers = new Set(data.onlineUsers);
        activeThread = { kind: "room", name: currentRoom };

        gate.style.display = "none";
        app.classList.add("active");
        setConnectionStatus("live");
        meEl.innerHTML =
          `you are <strong>${escapeAttrText(myUsername)}</strong>` +
          (myIsAdmin ? ' <span class="admin-badge">admin</span>' : "");
        threadTitle.textContent = `#${currentRoom}`;

        roomMessageCache.set(currentRoom, data.history.slice());
        renderRoomList();
        renderUserList();
        renderThreadFromCache();
        appendSystem(`connected as ${myUsername} in #${currentRoom}`);
        break;
      }

      case "notification": {
        if (data.room) roomUsers = data.users;
        if (activeThread.kind === "room" && activeThread.name === data.room)
          appendSystem(data.text);
        break;
      }

      case "room_list": {
        rooms = data.rooms;
        renderRoomList();
        break;
      }

      case "presence": {
        presenceByUser.set(data.username, data.status);
        if (data.status === "offline") onlineUsers.delete(data.username);
        else onlineUsers.add(data.username);
        renderUserList();
        break;
      }

      case "room_switched": {
        currentRoom = data.room;
        activeThread = { kind: "room", name: currentRoom };
        threadTitle.textContent = `#${currentRoom}`;
        closeDmBtn.style.display = "none";
        roomMessageCache.set(currentRoom, data.history.slice());
        renderRoomList();
        renderThreadFromCache();
        break;
      }

      case "message": {
        const arr = roomMessageCache.get(data.room) || [];
        arr.push(data);
        roomMessageCache.set(data.room, arr);
        if (activeThread.kind === "room" && activeThread.name === data.room)
          appendMessage(data, "room");
        if (data.username !== myUsername) playNotifySound();
        break;
      }

      case "private_message": {
        const otherParty = data.from === myUsername ? data.to : data.from;
        const arr = dmThreads.get(otherParty) || [];
        arr.push(data);
        dmThreads.set(otherParty, arr);
        if (activeThread.kind === "dm" && activeThread.name === otherParty)
          appendMessage(data, "dm");
        else
          appendSystem(`New DM from ${data.from} — click their name to view.`);
        if (data.from !== myUsername) playNotifySound();
        break;
      }

      case "typing": {
        if (activeThread.kind === "room" && activeThread.name === data.room) {
          typingEl.textContent = `${data.username} is typing…`;
          clearTimeout(typingEl._t);
          typingEl._t = setTimeout(() => (typingEl.textContent = ""), 1500);
        }
        break;
      }

      case "delivered": {
        markDelivered(data.id);
        break;
      }

      case "error": {
        appendSystem(`⚠ ${data.text}`, true);
        break;
      }

      case "kicked": {
        appendSystem(`You were kicked by ${data.by}.`, true);
        intentionalClose = true; // don't auto-reconnect after a kick
        break;
      }

      case "system_shutdown": {
        appendSystem(data.text, true);
        break;
      }

      default:
        break;
    }
  }

  function escapeAttrText(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }
})();

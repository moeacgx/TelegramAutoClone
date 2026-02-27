let currentSourceId = null;
let currentQrSessionId = null;

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  const text = await response.text();
  const data = text ? JSON.parse(text) : {};

  if (!response.ok) {
    const detail = data.detail || JSON.stringify(data);
    throw new Error(detail);
  }
  return data;
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = text;
  }
}

async function refreshAuthStatus() {
  const status = await api("/api/auth/status");
  setText("auth-status", JSON.stringify(status, null, 2));
}

async function refreshSourceGroups() {
  const list = await api("/api/source-groups");
  const container = document.getElementById("source-group-list");
  container.innerHTML = "";

  for (const item of list) {
    const li = document.createElement("li");
    li.innerHTML = `
      <b>${item.title}</b> (${item.chat_id}) [id=${item.id}] [${item.enabled ? "启用" : "停用"}]
      <button data-action="select" data-id="${item.id}">选择</button>
      <button data-action="sync" data-id="${item.id}">同步话题</button>
      <button data-action="toggle" data-id="${item.id}" data-enabled="${item.enabled ? 0 : 1}">
        ${item.enabled ? "停用" : "启用"}
      </button>
    `;
    container.appendChild(li);
  }

  container.querySelectorAll("button").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = Number(btn.dataset.id);
      const action = btn.dataset.action;
      if (action === "select") {
        currentSourceId = id;
        setText("current-source-id", String(id));
        await refreshTopics();
      } else if (action === "sync") {
        await api(`/api/source-groups/${id}/sync-topics`, { method: "POST" });
        if (currentSourceId === id) {
          await refreshTopics();
        }
      } else if (action === "toggle") {
        const enabled = Number(btn.dataset.enabled) === 1;
        await api(`/api/source-groups/${id}/enabled`, {
          method: "POST",
          body: JSON.stringify({ enabled }),
        });
        await refreshSourceGroups();
      }
    });
  });
}

async function refreshTopics() {
  const body = document.getElementById("topics-body");
  body.innerHTML = "";
  if (!currentSourceId) {
    return;
  }

  const [topics, bindings] = await Promise.all([
    api(`/api/topics?source_group_id=${currentSourceId}`),
    api(`/api/bindings?source_group_id=${currentSourceId}`),
  ]);

  const bindingMap = new Map();
  for (const b of bindings) {
    bindingMap.set(String(b.topic_id), b);
  }

  for (const topic of topics) {
    const tr = document.createElement("tr");
    const binding = bindingMap.get(String(topic.topic_id));

    tr.innerHTML = `
      <td>${topic.topic_id}</td>
      <td>${topic.title}</td>
      <td>
        <input type="checkbox" ${topic.enabled ? "checked" : ""} data-topic-id="${topic.topic_id}" data-type="enabled" />
      </td>
      <td>
        <input placeholder="频道ID" data-topic-id="${topic.topic_id}" data-type="channel" value="${binding ? binding.channel_chat_id : ""}" />
        <button data-topic-id="${topic.topic_id}" data-type="bind">绑定</button>
        ${binding ? `<small>[${binding.active ? "生效" : "停用"}]</small>` : ""}
      </td>
    `;

    body.appendChild(tr);
  }

  body.querySelectorAll("input[data-type='enabled']").forEach((checkbox) => {
    checkbox.addEventListener("change", async () => {
      const topicId = Number(checkbox.dataset.topicId);
      await api(`/api/topics/${currentSourceId}/${topicId}/enabled`, {
        method: "POST",
        body: JSON.stringify({ enabled: checkbox.checked }),
      });
    });
  });

  body.querySelectorAll("button[data-type='bind']").forEach((button) => {
    button.addEventListener("click", async () => {
      const topicId = Number(button.dataset.topicId);
      const input = body.querySelector(`input[data-type='channel'][data-topic-id='${topicId}']`);
      const channelChatId = Number(input.value);
      if (!channelChatId) {
        alert("请输入频道ID");
        return;
      }
      await api("/api/bindings", {
        method: "POST",
        body: JSON.stringify({
          source_group_id: currentSourceId,
          topic_id: topicId,
          channel_chat_id: channelChatId,
        }),
      });
      await refreshBindings();
      await refreshTopics();
    });
  });
}

async function refreshStandby() {
  const list = await api("/api/channels/standby");
  const container = document.getElementById("standby-list");
  container.innerHTML = "";
  for (const item of list) {
    const li = document.createElement("li");
    li.textContent = `${item.title} (${item.chat_id})`;
    container.appendChild(li);
  }
}

async function refreshBindings() {
  const list = await api("/api/bindings");
  const container = document.getElementById("binding-list");
  container.innerHTML = "";
  for (const item of list) {
    const li = document.createElement("li");
    li.textContent = `source_group_id=${item.source_group_id}, topic_id=${item.topic_id}, channel=${item.channel_chat_id}, active=${item.active}`;
    container.appendChild(li);
  }
}

async function refreshBanned() {
  const list = await api("/api/channels/banned");
  const container = document.getElementById("banned-list");
  container.innerHTML = "";
  for (const item of list) {
    const li = document.createElement("li");
    li.textContent = `source_group_id=${item.source_group_id}, topic_id=${item.topic_id}, channel=${item.channel_chat_id}, reason=${item.reason || ""}`;
    container.appendChild(li);
  }
}

async function refreshQueue() {
  const list = await api("/api/queue/recovery");
  const container = document.getElementById("queue-list");
  container.innerHTML = "";
  for (const item of list) {
    const li = document.createElement("li");
    li.textContent = `#${item.id} status=${item.status}, source_group_id=${item.source_group_id}, topic_id=${item.topic_id}, old=${item.old_channel_chat_id}, new=${item.new_channel_chat_id || "-"}, retry=${item.retry_count}`;
    container.appendChild(li);
  }
}

async function refreshAll() {
  await refreshAuthStatus();
  await refreshSourceGroups();
  await refreshStandby();
  await refreshBindings();
  await refreshBanned();
  await refreshQueue();
  if (currentSourceId) {
    await refreshTopics();
  }
}

function wireActions() {
  document.getElementById("send-code-btn").addEventListener("click", async () => {
    try {
      const phone = document.getElementById("phone").value.trim();
      await api("/api/auth/phone/send", {
        method: "POST",
        body: JSON.stringify({ phone }),
      });
      alert("验证码已发送");
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("login-btn").addEventListener("click", async () => {
    try {
      const phone = document.getElementById("phone").value.trim();
      const code = document.getElementById("code").value.trim();
      const password = document.getElementById("password").value.trim();
      const result = await api("/api/auth/phone/login", {
        method: "POST",
        body: JSON.stringify({ phone, code, password: password || null }),
      });
      alert(result.ok ? "登录成功" : `登录结果: ${JSON.stringify(result)}`);
      await refreshAuthStatus();
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("create-qr-btn").addEventListener("click", async () => {
    try {
      const result = await api("/api/auth/qr/create", { method: "POST" });
      currentQrSessionId = result.session_id;
      const qrImage = document.getElementById("qr-image");
      qrImage.src = `data:image/png;base64,${result.qr_image_base64}`;
      setText("qr-status", `二维码已生成，session_id=${currentQrSessionId}`);
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("poll-qr-btn").addEventListener("click", async () => {
    try {
      if (!currentQrSessionId) {
        alert("请先生成二维码");
        return;
      }
      const result = await api(`/api/auth/qr/poll/${currentQrSessionId}`);
      setText("qr-status", `扫码状态: ${result.status}`);
      if (result.status === "authorized") {
        await refreshAuthStatus();
      }
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("add-source-btn").addEventListener("click", async () => {
    try {
      const chatRef = document.getElementById("source-chat-ref").value.trim();
      await api("/api/source-groups", {
        method: "POST",
        body: JSON.stringify({ chat_ref: chatRef }),
      });
      await refreshSourceGroups();
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("sync-topics-btn").addEventListener("click", async () => {
    try {
      if (!currentSourceId) {
        alert("请先选择任务组");
        return;
      }
      await api(`/api/source-groups/${currentSourceId}/sync-topics`, { method: "POST" });
      await refreshTopics();
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("refresh-standby-btn").addEventListener("click", async () => {
    try {
      await api("/api/channels/refresh-standby", { method: "POST" });
      await refreshStandby();
      await refreshBindings();
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("run-monitor-btn").addEventListener("click", async () => {
    try {
      await api("/api/queue/monitor/run-once", { method: "POST" });
      await refreshBanned();
      await refreshQueue();
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("run-recovery-btn").addEventListener("click", async () => {
    try {
      await api("/api/queue/recovery/run-once", { method: "POST" });
      await refreshQueue();
      await refreshBindings();
      await refreshStandby();
      if (currentSourceId) {
        await refreshTopics();
      }
    } catch (error) {
      alert(error.message);
    }
  });
}

(async function bootstrap() {
  wireActions();
  await refreshAll();
  setInterval(async () => {
    try {
      await refreshAll();
    } catch (error) {
      console.error(error);
    }
  }, 10000);
})();

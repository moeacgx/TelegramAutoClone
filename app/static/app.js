let currentSourceId = null;
let currentQrSessionId = null;
let autoRefreshTimer = null;
let refreshBusy = false;
const selectedStandbyIds = new Set();
const AUTO_REFRESH_KEY = "auto_refresh_enabled";
const POLL_INTERVAL_MS = 10000;

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

function updateStandbySelectedCount() {
  setText("standby-selected-count", String(selectedStandbyIds.size));
}

async function refreshAuthStatus() {
  const status = await api("/api/auth/status");
  setText("auth-status", JSON.stringify(status, null, 2));
}

async function refreshUpdateStatus() {
  const status = await api("/api/update/status");
  setText("update-status", JSON.stringify(status, null, 2));
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
      <button data-action="delete" data-id="${item.id}">删除</button>
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
        const result = await api(`/api/source-groups/${id}/sync-topics`, { method: "POST" });
        alert(`话题同步完成：总数 ${result.total ?? 0}，变更 ${result.changed ?? 0}`);
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
      } else if (action === "delete") {
        if (!confirm(`确认删除任务组 id=${id} 吗？将同时删除话题、绑定、封禁记录与恢复队列。`)) {
          return;
        }
        const result = await api(`/api/source-groups/${id}`, { method: "DELETE" });
        if (currentSourceId === id) {
          currentSourceId = null;
          setText("current-source-id", "未选择");
          const topicsBody = document.getElementById("topics-body");
          if (topicsBody) {
            topicsBody.innerHTML = "";
          }
        }
        await Promise.all([refreshSourceGroups(), refreshBindings(), refreshBanned(), refreshQueue()]);
        alert(
          `任务组 ${id} 已删除：topics=${result.topics || 0}, bindings=${result.topic_bindings || 0}, banned=${result.banned_channels || 0}, queue=${result.recovery_queue || 0}`
        );
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
        <input placeholder="频道ID/@用户名/链接" data-topic-id="${topic.topic_id}" data-type="channel" value="${binding ? binding.channel_chat_id : ""}" />
        <button data-topic-id="${topic.topic_id}" data-type="bind">绑定</button>
        <button data-topic-id="${topic.topic_id}" data-type="start-recovery">开始恢复</button>
        ${binding ? `<small>[${binding.active ? "生效" : "停用"}] ${binding.channel_title || ""}</small>` : ""}
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
      const channelRef = ((input && input.value) || "").trim();
      if (!channelRef) {
        alert("请输入频道ID/@用户名/链接");
        return;
      }
      try {
        button.disabled = true;
        await api("/api/bindings", {
          method: "POST",
          body: JSON.stringify({
            source_group_id: currentSourceId,
            topic_id: topicId,
            channel_ref: channelRef,
          }),
        });
        await refreshBindings();
        await refreshTopics();
      } catch (error) {
        alert(`绑定失败: ${error.message}`);
      } finally {
        button.disabled = false;
      }
    });
  });

  body.querySelectorAll("button[data-type='start-recovery']").forEach((button) => {
    button.addEventListener("click", async () => {
      const topicId = Number(button.dataset.topicId);
      const input = body.querySelector(`input[data-type='channel'][data-topic-id='${topicId}']`);
      const channelRef = (input.value || "").trim();
      if (!channelRef) {
        alert("请先输入并绑定频道ID/@用户名/链接");
        return;
      }
      if (!confirm(`确认开始恢复任务吗？\ntopic_id=${topicId}\n目标频道=${channelRef}`)) {
        return;
      }

      try {
        button.disabled = true;
        await api("/api/queue/recovery/manual-start", {
          method: "POST",
          body: JSON.stringify({
            source_group_id: currentSourceId,
            topic_id: topicId,
            channel_ref: channelRef,
            run_now: true,
          }),
        });
        await refreshQueue();
        await refreshBindings();
        await refreshStandby();
        await refreshTopics();
        alert(`已为 topic_id=${topicId} 创建并执行恢复任务`);
      } catch (error) {
        alert(`创建恢复任务失败: ${error.message}`);
      } finally {
        button.disabled = false;
      }
    });
  });
}

async function refreshStandby() {
  const list = await api("/api/channels/standby");
  const container = document.getElementById("standby-list");
  container.innerHTML = "";
  const validIds = new Set(list.map((item) => Number(item.chat_id)));

  for (const chatId of Array.from(selectedStandbyIds)) {
    if (!validIds.has(chatId)) {
      selectedStandbyIds.delete(chatId);
    }
  }

  for (const item of list) {
    const chatId = Number(item.chat_id);
    const li = document.createElement("li");

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = selectedStandbyIds.has(chatId);
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) {
        selectedStandbyIds.add(chatId);
      } else {
        selectedStandbyIds.delete(chatId);
      }
      updateStandbySelectedCount();
    });

    const label = document.createElement("span");
    label.textContent = ` ${item.title} (${chatId}) `;

    const deleteBtn = document.createElement("button");
    deleteBtn.textContent = "删除";
    deleteBtn.addEventListener("click", async () => {
      try {
        if (!confirm(`确认删除备用频道 ${chatId} 吗？`)) {
          return;
        }
        await api(`/api/channels/standby/${chatId}`, { method: "DELETE" });
        selectedStandbyIds.delete(chatId);
        await refreshStandby();
      } catch (error) {
        alert(error.message);
      }
    });

    li.appendChild(checkbox);
    li.appendChild(label);
    li.appendChild(deleteBtn);
    container.appendChild(li);
  }

  updateStandbySelectedCount();
}

async function refreshBindings() {
  const list = await api("/api/bindings");
  const container = document.getElementById("binding-list");
  container.innerHTML = "";
  for (const item of list) {
    const li = document.createElement("li");
    li.textContent = `任务组=${item.source_title || item.source_group_id}, 话题=${item.topic_title || item.topic_id}, 频道=${item.channel_title || "-"} (${item.channel_chat_id}), active=${item.active}`;
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

async function runQueueAction(item, action) {
  const id = Number(item.id);
  if (!id) {
    return;
  }

  let path = "";
  let options = { method: "POST" };
  if (action === "run") {
    path = `/api/queue/recovery/${id}/run-once`;
  } else if (action === "continue") {
    path = `/api/queue/recovery/${id}/continue`;
    options.body = JSON.stringify({ run_now: true });
  } else if (action === "restart") {
    if (!confirm(`确认从头重新执行任务 #${id} 吗？可能会产生重复克隆。`)) {
      return;
    }
    path = `/api/queue/recovery/${id}/restart`;
    options.body = JSON.stringify({ run_now: true });
  } else if (action === "stop") {
    path = `/api/queue/recovery/${id}/stop`;
  } else if (action === "delete") {
    if (!confirm(`确认删除任务 #${id} 吗？`)) {
      return;
    }
    path = `/api/queue/recovery/${id}`;
    options = { method: "DELETE" };
  } else {
    return;
  }

  await api(path, options);
  await refreshQueue();
  await refreshBindings();
  await refreshStandby();
  if (currentSourceId) {
    await refreshTopics();
  }
}

async function refreshQueue() {
  const list = await api("/api/queue/recovery");
  const container = document.getElementById("queue-list");
  container.innerHTML = "";
  for (const item of list) {
    const li = document.createElement("li");
    li.style.marginBottom = "8px";

    const status = String(item.status || "");
    const statusMap = {
      waiting_standby: "waiting_standby(等待备用频道)",
    };
    const statusLabel = statusMap[status] || status;
    const canContinue =
      status === "failed" ||
      status === "running" ||
      status === "pending" ||
      status === "stopped" ||
      status === "waiting_standby";
    const canRun = status === "pending" || status === "waiting_standby";
    const canStop =
      status === "running" ||
      status === "pending" ||
      status === "stopping" ||
      status === "waiting_standby";
    const canDelete = status !== "running" && status !== "stopping";

    li.innerHTML = `
      <div>
        <b>#${item.id}</b> status=${statusLabel}, source_group_id=${item.source_group_id}, topic_id=${item.topic_id}, old=${item.old_channel_chat_id}, new=${item.new_channel_chat_id || "-"}, retry=${item.retry_count}, checkpoint=${item.last_cloned_message_id || 0}
      </div>
      <div>${item.last_error ? `last_error=${item.last_error}` : ""}</div>
    `;

    const actions = document.createElement("div");
    actions.style.marginTop = "4px";

    const runBtn = document.createElement("button");
    runBtn.textContent = "执行该任务";
    runBtn.disabled = !canRun;
    runBtn.addEventListener("click", async () => {
      try {
        await runQueueAction(item, "run");
      } catch (error) {
        alert(error.message);
      }
    });

    const continueBtn = document.createElement("button");
    continueBtn.textContent = "继续(断点)";
    continueBtn.disabled = !canContinue;
    continueBtn.style.marginLeft = "6px";
    continueBtn.addEventListener("click", async () => {
      try {
        await runQueueAction(item, "continue");
      } catch (error) {
        alert(error.message);
      }
    });

    const restartBtn = document.createElement("button");
    restartBtn.textContent = "重跑(从头)";
    restartBtn.disabled = status === "done";
    restartBtn.style.marginLeft = "6px";
    restartBtn.addEventListener("click", async () => {
      try {
        await runQueueAction(item, "restart");
      } catch (error) {
        alert(error.message);
      }
    });

    const stopBtn = document.createElement("button");
    stopBtn.textContent = "停止";
    stopBtn.disabled = !canStop;
    stopBtn.style.marginLeft = "6px";
    stopBtn.addEventListener("click", async () => {
      try {
        await runQueueAction(item, "stop");
      } catch (error) {
        alert(error.message);
      }
    });

    const deleteBtn = document.createElement("button");
    deleteBtn.textContent = "删除";
    deleteBtn.disabled = !canDelete;
    deleteBtn.style.marginLeft = "6px";
    deleteBtn.addEventListener("click", async () => {
      try {
        await runQueueAction(item, "delete");
      } catch (error) {
        alert(error.message);
      }
    });

    actions.appendChild(runBtn);
    actions.appendChild(continueBtn);
    actions.appendChild(restartBtn);
    actions.appendChild(stopBtn);
    actions.appendChild(deleteBtn);
    li.appendChild(actions);
    container.appendChild(li);
  }
}

async function refreshAll() {
  await refreshAuthStatus();
  await refreshUpdateStatus();
  await refreshSourceGroups();
  await refreshStandby();
  await refreshBindings();
  await refreshBanned();
  await refreshQueue();
  if (currentSourceId) {
    await refreshTopics();
  }
}

async function safeRefreshAll() {
  if (refreshBusy) {
    return;
  }
  refreshBusy = true;
  try {
    await refreshAll();
  } finally {
    refreshBusy = false;
  }
}

async function silentPollBackend() {
  const paths = [
    "/api/auth/status",
    "/api/update/status",
    "/api/queue/recovery",
    "/api/channels/banned",
  ];
  await Promise.allSettled(paths.map((path) => api(path)));
}

function setAutoPollingEnabled(enabled) {
  if (autoRefreshTimer) {
    clearInterval(autoRefreshTimer);
    autoRefreshTimer = null;
  }
  localStorage.setItem(AUTO_REFRESH_KEY, enabled ? "1" : "0");
  if (!enabled) {
    return;
  }
  autoRefreshTimer = setInterval(async () => {
    try {
      await silentPollBackend();
    } catch (error) {
      console.error(error);
    }
  }, POLL_INTERVAL_MS);
}

function initAutoPollingToggle() {
  const toggle = document.getElementById("auto-polling-toggle");
  if (!toggle) {
    return;
  }
  const saved = localStorage.getItem(AUTO_REFRESH_KEY);
  toggle.checked = saved !== "0";
  setAutoPollingEnabled(toggle.checked);
  toggle.addEventListener("change", () => {
    setAutoPollingEnabled(toggle.checked);
  });
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
      setText("qr-status", "二维码已生成，请先扫码；可直接提交二级密码，系统会自动等待最多20秒扫码确认。");
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
      if (result.status === "need_password") {
        setText("qr-status", "扫码状态: 已扫码，需二级密码。请输入二级密码后点击“扫码后提交二级密码”。");
      } else {
        setText("qr-status", `扫码状态: ${result.status}`);
      }
      if (result.status === "authorized") {
        await refreshAuthStatus();
      }
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("qr-password-login-btn").addEventListener("click", async () => {
    const btn = document.getElementById("qr-password-login-btn");
    try {
      if (!currentQrSessionId) {
        alert("请先生成二维码并扫码");
        return;
      }

      const password = document.getElementById("password").value.trim();
      if (!password) {
        alert("请输入二级密码");
        return;
      }

      btn.disabled = true;
      setText("qr-status", "正在处理：自动等待扫码确认（最多20秒）...");
      const result = await api("/api/auth/password/login", {
        method: "POST",
        body: JSON.stringify({ password, session_id: currentQrSessionId }),
      });

      if (result.ok) {
        setText("qr-status", "扫码状态: 已通过二级密码完成登录");
        currentQrSessionId = null;
        await refreshAuthStatus();
        return;
      }
      alert(result.error || "二级密码登录失败");
    } catch (error) {
      alert(error.message);
    } finally {
      btn.disabled = false;
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

  document.getElementById("check-update-btn").addEventListener("click", async () => {
    try {
      const result = await api("/api/update/check", { method: "POST" });
      await refreshUpdateStatus();
      if (result.ok && result.has_update) {
        alert(`检测到新版本：${result.latest_digest}\n请点击“确认并更新”执行升级。`);
      } else if (result.ok) {
        alert("当前已是最新版本。");
      } else {
        alert(result.error || "更新检测失败");
      }
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("confirm-update-btn").addEventListener("click", async () => {
    try {
      if (!confirm("确认执行更新吗？系统将触发 watchtower 拉取并重启容器。")) {
        return;
      }
      const result = await api("/api/update/confirm", { method: "POST" });
      await refreshUpdateStatus();
      if (result.triggered) {
        alert("已触发更新，请等待容器重启后刷新页面。");
      } else {
        alert("已确认最新版本。当前未启用 watchtower HTTP 触发，请手动执行部署更新命令。");
      }
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
      const result = await api(`/api/source-groups/${currentSourceId}/sync-topics`, { method: "POST" });
      await refreshTopics();
      let message = `话题同步完成：总数 ${result.total ?? 0}，变更 ${result.changed ?? 0}`;
      const samples = result.changed_samples || [];
      if (samples.length > 0) {
        const lines = samples.slice(0, 5).map((x) => `topic_id=${x.topic_id}: ${x.old_title || "(空)"} -> ${x.new_title || "(空)"}`);
        message += `\n示例变更:\n${lines.join("\n")}`;
      }
      alert(message);
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

  document.getElementById("delete-selected-standby-btn").addEventListener("click", async () => {
    try {
      const chatIds = Array.from(selectedStandbyIds);
      if (chatIds.length === 0) {
        alert("请先勾选要删除的备用频道");
        return;
      }
      if (!confirm(`确认删除选中的 ${chatIds.length} 个备用频道吗？`)) {
        return;
      }
      const result = await api("/api/channels/standby/delete", {
        method: "POST",
        body: JSON.stringify({ chat_ids: chatIds }),
      });
      selectedStandbyIds.clear();
      await refreshStandby();
      const failedCount = (result.failed || []).length;
      alert(`批量删除完成：成功 ${result.removed}，失败 ${failedCount}`);
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("clear-standby-btn").addEventListener("click", async () => {
    try {
      if (!confirm("确认清空整个备用频道池吗？")) {
        return;
      }
      const result = await api("/api/channels/standby/clear", { method: "POST" });
      selectedStandbyIds.clear();
      await refreshStandby();
      alert(`已清空备用池，删除 ${result.cleared} 个频道`);
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("add-standby-batch-btn").addEventListener("click", async () => {
    try {
      const refsText = document.getElementById("standby-batch-input").value.trim();
      if (!refsText) {
        alert("请输入要添加的频道，一行一个");
        return;
      }
      const result = await api("/api/channels/standby/batch", {
        method: "POST",
        body: JSON.stringify({ refs_text: refsText }),
      });

      let summary = `批量添加完成：新增 ${result.added}，更新 ${result.updated}，失败 ${result.failed.length}，备用池总数 ${result.standby_count}`;
      if (result.failed.length > 0) {
        const topErrors = result.failed.slice(0, 8).map((x) => `${x.ref} => ${x.error}`);
        summary += `\n失败详情(最多8条)：\n${topErrors.join("\n")}`;
      } else {
        document.getElementById("standby-batch-input").value = "";
      }
      alert(summary);
      await refreshStandby();
      await refreshBindings();
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("clear-banned-btn").addEventListener("click", async () => {
    try {
      if (!confirm("确认清空封禁频道列表吗？")) {
        return;
      }
      const result = await api("/api/channels/banned/clear", { method: "POST" });
      await refreshBanned();
      alert(`已清空封禁列表 ${result.cleared} 条`);
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("run-monitor-btn").addEventListener("click", async () => {
    try {
      const result = await api("/api/queue/monitor/run-once", { method: "POST" });
      await refreshBanned();
      await refreshQueue();
      alert(
        `巡检完成：扫描 ${result.scanned}，不可访问 ${result.unavailable}，入队 ${result.enqueued}，跳过(任务组停用) ${result.skipped_source_disabled}`
      );
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

  document.getElementById("reset-running-queue-btn").addEventListener("click", async () => {
    try {
      if (!confirm("确认把所有 running 任务重置为 pending 吗？")) {
        return;
      }
      const result = await api("/api/queue/recovery/reset-running", { method: "POST" });
      await refreshQueue();
      alert(`已重置 ${result.reset_count} 个运行中任务`);
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("clear-recovery-queue-btn").addEventListener("click", async () => {
    try {
      if (!confirm("确认清空恢复队列中的任务吗？运行中的任务会保留。")) {
        return;
      }
      const result = await api("/api/queue/recovery/clear", {
        method: "POST",
        body: JSON.stringify({ include_running: false }),
      });
      await refreshQueue();
      await refreshBanned();
      alert(`已清空任务 ${result.deleted} 个，保留运行中任务 ${result.skipped_running} 个`);
    } catch (error) {
      alert(error.message);
    }
  });
}

(async function bootstrap() {
  wireActions();
  initAutoPollingToggle();
  await safeRefreshAll();
})();

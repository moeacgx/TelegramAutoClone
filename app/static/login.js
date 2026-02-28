async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    credentials: "same-origin",
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

function setStatus(text, isError = false) {
  const el = document.getElementById("login-status");
  if (!el) {
    return;
  }
  el.textContent = text;
  el.style.color = isError ? "#b42318" : "#526079";
}

async function checkAuthorized() {
  const status = await api("/api/panel-auth/status");
  if (status.authorized) {
    window.location.href = "/";
    return true;
  }
  return false;
}

function wireLoginForm() {
  const form = document.getElementById("panel-login-form");
  const input = document.getElementById("panel-password");
  const btn = document.getElementById("panel-login-btn");
  if (!form || !input || !btn) {
    return;
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const password = input.value.trim();
    if (!password) {
      setStatus("请输入后台密码", true);
      return;
    }

    try {
      btn.disabled = true;
      setStatus("正在验证...");
      await api("/api/panel-auth/login", {
        method: "POST",
        body: JSON.stringify({ password }),
      });
      setStatus("验证通过，正在进入后台...");
      window.location.href = "/";
    } catch (error) {
      setStatus(error.message || "验证失败", true);
    } finally {
      btn.disabled = false;
    }
  });
}

(async function bootstrap() {
  try {
    const redirected = await checkAuthorized();
    if (redirected) {
      return;
    }
    wireLoginForm();
  } catch (error) {
    setStatus(error.message || "初始化失败", true);
  }
})();

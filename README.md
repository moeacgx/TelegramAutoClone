# Telegram Auto Clone（多任务组）

## 功能
- 多超级群任务组管理（可添加多组源超级群）
- 论坛话题同步与勾选入工作列表
- 一话题一频道绑定
- 无引用克隆（文本/媒体）
- Bot 管理员频道自动纳入备用频道库
- 频道封禁检测、故障队列、自动替换与历史回补
- 固定群组通知（封禁、恢复完成、失败）
- 后台页面密码门禁（仅页面访问受保护）

## 启动
```bash
cp .env.example .env
# 修改 .env 中 APP_IMAGE 为你的镜像地址
# 必填：PANEL_PASSWORD

docker compose pull app
docker compose up -d
```

打开：`http://localhost:8000`

## 后台密码验证

### 必填配置
```text
PANEL_PASSWORD=change-this-password
PANEL_SESSION_TTL_SECONDS=86400
```

说明：
- `PANEL_PASSWORD` 未配置时，应用会拒绝启动。
- 登录成功后写入 `HttpOnly Cookie`，默认 24 小时有效。
- 面板左侧提供“退出后台”按钮，可立即清除会话。

### 安全边界说明（当前实现）
- 仅保护页面访问：
  - 未登录访问 `/` 会跳转到 `/login`。
  - 登录后可访问管理台页面。
- **API 不做会话校验**（例如 `/api/*` 可直接调用）。

如果你需要更严格防护，建议在反向代理层（Nginx/Caddy）额外加 BasicAuth 或 IP 白名单。

## 本地测试（local profile）

当你修改了本地代码，想先验证再推送时：

```bash
docker compose --profile local up --build app-local
```

默认访问地址：`http://localhost:8001`  
可在 `.env` 里通过 `LOCAL_APP_PORT` 调整端口。

## 在线更新

### 手动更新（推荐）
```bash
docker compose pull app
docker compose up -d app
```

### 自动更新（watchtower）
```bash
docker compose --profile autoupdate up -d
```

说明：
- 当前默认模式为“手动确认更新”，watchtower 不会周期自动更新。
- 仅会更新打了 `com.centurylinklabs.watchtower.enable=true` 标签的服务。
- 已启用 `watchtower` HTTP API，可在管理台触发“确认并更新”。

### 面板检测更新 / 手动确认更新

管理台新增“系统更新”卡片，支持：
- 检查更新（对比 `APP_IMAGE` 当前 tag 的远端 digest）
- 检测到新版本时可发送 Telegram 通知（`UPDATE_NOTIFY_ENABLED=true`）
- 点击“确认并更新”后触发 watchtower 拉取并重启

需要在 `.env` 配置：
```text
WATCHTOWER_HTTP_TOKEN=change-this-token
WATCHTOWER_URL=http://watchtower:8080
UPDATE_CHECK_INTERVAL_SECONDS=600
UPDATE_NOTIFY_ENABLED=true
```

可选：若你需要恢复“自动轮询更新”，可自行在 `docker-compose.yml` 的 watchtower `command` 增加：
`--http-api-periodic-polls --interval 300`

## 镜像自动构建（GitHub Actions）

- 仓库已包含工作流：`.github/workflows/docker-image.yml`
- 触发规则：
  - push 到 `main`：构建并推送 `latest` + `sha-*`
  - push 标签 `v*`：构建并推送对应 tag
  - PR 到 `main`：仅构建校验，不推送

默认镜像地址：
```text
ghcr.io/moeacgx/telegramautoclone
```

使用前请确认：
1. 仓库启用 GitHub Actions。
2. 目标部署机可拉取 GHCR 镜像（若包是私有，需要先 `docker login ghcr.io`）。
3. `.env` 的 `APP_IMAGE` 指向你的 GHCR 镜像，例如：
```text
APP_IMAGE=ghcr.io/moeacgx/telegramautoclone:latest
```

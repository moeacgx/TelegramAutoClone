# Telegram Auto Clone（多任务组）

## 功能
- 多超级群任务组管理（可添加多组源超级群）
- 论坛话题同步与勾选入工作列表
- 一话题一频道绑定
- 无引用克隆（文本/媒体）
- 面板运行时设置：MD5 修改开关、下载组并发
- Bot 管理员频道自动纳入备用频道库
- 频道封禁检测、故障队列、自动替换与历史回补
- 固定群组通知（封禁、恢复完成、失败）
- 后台页面密码门禁（仅页面访问受保护）
- 面板内自更新：GitHub Release 更新包 + 持久目录切换 + 容器重启

## 启动
```bash
cp .env.example .env
# 必填：PANEL_PASSWORD
# 可选：APP_IMAGE 仅用于首次部署容器镜像

docker compose pull app
docker compose up -d
```

打开：`http://localhost:8141`

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

## 运行时克隆设置

管理台新增“系统设置”卡片，支持运行时调整：
- `下载后修改媒体 MD5`：开启后，媒体消息强制走“下载 → 尝试改写文件 MD5 → 上传”分支。
- `下载组并发`：按组控制预下载并发；单个媒体算一组，相册算一组，上传仍按原顺序串行提交。

说明：
- 文本消息不会进入下载分支，仍保留原有 `formatting_entities`。
- 普通文档/压缩包默认不改文件字节，避免破坏文件内容。
- 若运行环境安装了 `cryptg` / `fasttelethonhelper` / `ffmpeg`，会自动启用对应的下载、上传和音视频改写优化。

## 本地测试（local profile）

当你修改了本地代码，想先验证再推送时：

```bash
docker compose --profile local up --build app-local
```

默认访问地址：`http://localhost:8001`
可在 `.env` 里通过 `LOCAL_APP_PORT` 调整端口。

## 在线更新

### 面板内一键更新（推荐）

管理台“更新中心”现在采用和 `Telegram-Panel` 类似的更新机制：

1. 点击“检查更新”，读取 GitHub 最新 Release。
2. 点击“下载更新并重启”，下载匹配架构的更新包到 `/app/data/self_update/current`。
3. 应用请求退出后，容器按 `restart: unless-stopped` 自动拉起；入口脚本会优先从持久目录启动更新后的程序。

说明：
- 当前默认只支持 **Docker 容器内** 自更新。
- 当前 Release 更新包默认提供 `linux-x64`。
- 若仓库是私有仓库，可配置 `UPDATE_GITHUB_TOKEN`。
- 面板内自更新不依赖 `watchtower`，也不要求手工 `docker compose pull`。

### 首次部署 / 手动回滚

首次部署或你想手动切回镜像内版本时：

```bash
docker compose pull app
docker compose up -d app
```

如果你要清空已下载的自更新程序，可删除持久目录：`./data/self_update`。

## 自更新配置

`.env` 里可用的核心配置：

```text
UPDATE_REPOSITORY=moeacgx/TelegramAutoClone
UPDATE_GITHUB_TOKEN=
SELF_UPDATE_ENABLED=true
SELF_UPDATE_DOCKER_ONLY=true
SELF_UPDATE_WORK_DIR=/app/data/self_update
SELF_UPDATE_EXECUTABLE_NAME=telegram-auto-clone
SELF_UPDATE_ASSET_PREFIX=telegram-auto-clone
SELF_UPDATE_RESTART_DELAY_SECONDS=2
UPDATE_CHECK_INTERVAL_SECONDS=600
UPDATE_HTTP_TIMEOUT_SECONDS=15
UPDATE_NOTIFY_ENABLED=true
```

版本号不再放在 `.env`；开发态默认读取仓库根目录的 `VERSION`，正式镜像和自更新包会在构建/发布时写入各自的 `VERSION` 文件。

## 发布产物

仓库现在包含两条 GitHub Actions：
- `.github/workflows/docker-image.yml`：继续构建并推送容器镜像。
- `.github/workflows/release-package.yml`：在推送 `v*` 标签时构建 `PyInstaller` Linux 更新包，并自动上传到 GitHub Release。

默认更新包资产名：
```text
telegram-auto-clone-linux-x64.zip
```

## 镜像自动构建（GitHub Actions）

默认镜像地址：
```text
ghcr.io/moeacgx/telegramautoclone
```

使用前请确认：
1. 仓库启用 GitHub Actions。
2. 目标部署机可拉取 GHCR 镜像（若包是私有，需要先 `docker login ghcr.io`）。
3. `.env` 的 `APP_IMAGE` 指向你的镜像地址（仅首次部署需要），例如：
```text
APP_IMAGE=ghcr.io/moeacgx/telegramautoclone:latest
```
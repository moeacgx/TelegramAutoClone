# Telegram Auto Clone（多任务组）

## 功能
- 多超级群任务组管理（可添加多组源超级群）
- 论坛话题同步与勾选入工作列表
- 一话题一频道绑定
- 无引用克隆（文本/媒体）
- Bot 管理员频道自动纳入备用频道库
- 频道封禁检测、故障队列、自动替换与历史回补
- 固定群组通知（封禁、恢复完成、失败）

## 启动
```bash
cp .env.example .env
# 修改 .env 中 APP_IMAGE 为你的镜像地址
docker compose pull app
docker compose up -d
```

打开：`http://localhost:8000`

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
- `watchtower` 会按 `WATCHTOWER_INTERVAL` 周期检查并更新 `app` 容器镜像。
- 仅会更新打了 `com.centurylinklabs.watchtower.enable=true` 标签的服务。

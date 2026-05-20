# family-cron-mcp

Pull-on-chat reminders MCP for Tyler-Computer LibreChat.

## Why custom?

The two off-the-shelf cron MCPs both crashed on Railway's Alpine LibreChat:

- **jolks/mcp-cron** — Go binary built against glibc; segfaults silently on
  musl libc. Looked like a 60s MCP-init timeout.
- **liao1fan/schedule-task-mcp** — uses `node:sqlite` which requires
  Node 22.5+; LibreChat-dev ships Node 20.

This one is **pure Node 20 + Mongo**, ~280 lines, no native deps.

## Tools

| Tool                  | Use                                                                                          |
|-----------------------|----------------------------------------------------------------------------------------------|
| `schedule_reminder`   | Save a reminder. `when` accepts cron, `"in N minutes"`, `"tomorrow at HH:MM"`, or ISO date.  |
| `list_reminders`      | Return pending + fired-but-unacknowledged for the calling user (max 20).                     |
| `acknowledge_reminder`| Mark a fired one-shot as done (hides from `list_reminders`).                                 |
| `cancel_reminder`     | Delete a pending or recurring reminder; stops future fires.                                  |

## Semantics — pull, not push

LibreChat MCPs can't push into a conversation. So:

1. User: "remind me to call mom tomorrow at 9am"
2. AI calls `schedule_reminder(when="tomorrow at 9:00", message="call mom")`.
3. Tomorrow 9:00 — the MCP marks the doc `status=fired` in Mongo.
4. User opens chat later. AI calls `list_reminders` at conversation start.
5. The fired reminder surfaces. User says "thanks" → AI `acknowledge_reminder(id)`.

Recurring reminders (cron) fire repeatedly but only show up in
`list_reminders` once per fire window (last fire time is tracked).

## Identity

One MCP child per (user, server) pair. Identity comes in via these env vars
that LibreChat substitutes at spawn time:

- `LIBRECHAT_USER_ID` (required — refuses without)
- `LIBRECHAT_USER_USERNAME`
- `LIBRECHAT_USER_ROLE` (`ADMIN` can ack/cancel anyone's; users only their own)

## Persistence

Reminders are written to Mongo collection `family_reminders` in the same db
LibreChat uses (`test`). Indexed on `(user_id, status, fire_at)`.

On boot the MCP rehydrates the calling user's pending reminders into
node-cron / setTimeout. If the MCP child has been dead and a one-shot's
`fire_at` is in the past, it fires immediately on next spawn.

## Env

| Var                          | Default                       |
|------------------------------|-------------------------------|
| `MONGODB_URI` *(required)*   | —                             |
| `MONGO_URI` (fallback)       | —                             |
| `CRON_DB`                    | `test`                        |
| `CRON_COLLECTION`            | `family_reminders`            |
| `LIBRECHAT_USER_ID`          | (passed by LibreChat)         |
| `LIBRECHAT_USER_USERNAME`    | (passed by LibreChat)         |
| `LIBRECHAT_USER_ROLE`        | `USER`                        |

## Install

```yaml
mcpServers:
  scheduled-tasks:
    type: stdio
    command: npx
    args:
      - -y
      - "https://github.com/NOVA-LC/family-cron-mcp/archive/refs/heads/main.tar.gz"
    env:
      NPM_CONFIG_CACHE: "/tmp/.npm"
      MONGODB_URI: "${MONGODB_URI}"
      CRON_DB: "test"
      LIBRECHAT_USER_ID: "{{LIBRECHAT_USER_ID}}"
      LIBRECHAT_USER_USERNAME: "{{LIBRECHAT_USER_USERNAME}}"
      LIBRECHAT_USER_ROLE: "{{LIBRECHAT_USER_ROLE}}"
    initTimeout: 60000
    timeout: 60000
    chatMenu: true
```

## License

MIT

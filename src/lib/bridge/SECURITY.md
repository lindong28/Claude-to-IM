# Claude-to-IM Security

## Threat Model

The bridge exposes an LLM to messages from IM platforms. Key threats:

1. **Unauthorized access**: Anyone who messages the bot gets LLM access
2. **Prompt injection**: Malicious input via IM messages
3. **Command injection**: Path traversal or shell metacharacters in /cwd commands
4. **Denial of service**: Message flooding
5. **Permission bypass**: Forged callback queries or double-click race conditions

## Mitigations

### Authentication & Authorization

Each adapter implements `isAuthorized(userId, chatId)`:
- **Telegram**: `telegram_bridge_allowed_users` CSV whitelist
- **Discord**: `bridge_discord_allowed_users`, `_allowed_channels`, `_allowed_guilds` with group policy
- **Feishu**: `bridge_feishu_allowed_users` + group policy + mention requirement

Unauthorized messages are silently dropped (no response leak).

For a fixed Feishu group deployment, require all three controls together: non-empty allowed users, group policy `allowlist` with non-empty allowed groups, and mention-required. Apply the same user/group authorization to message events and card callbacks.

### Input Validation (`security/validators.ts`)

- `validateWorkingDirectory()`: Rejects relative paths, `..` traversal, shell metacharacters (`|;&$`)
- `validateSessionId()`: Hex/UUID format, 32-64 chars
- `isDangerousInput()`: Detects path traversal, command injection, null bytes, control characters
- `sanitizeInput()`: Strips control characters (except `\n`, `\t`), enforces max length (10,000 chars)
- `validateMode()`: Whitelist (`plan`, `code`, `ask`)

### Rate Limiting (`security/rate-limiter.ts`)

Token bucket algorithm: 20 messages/minute per chat ID. Idle buckets cleaned up periodically.

### Permission Security

- **Origin validation**: Callback must come from same chat AND same message ID as the original permission prompt
- **Atomic dedup**: `markPermissionLinkResolved()` uses atomic check-and-set to prevent race conditions from concurrent button clicks
- **In-memory dedup**: `recentPermissionForwards` map prevents duplicate forwarding (30s window)

Codex approval policy `never` is non-interactive: no permission request/card is generated, and Feishu `card.action.trigger` is unnecessary. Do not add a callback or auto-approve setting to simulate a permission flow; sandbox configuration is the authorization boundary.

### Fixed Session Recovery

`fixed-confirm-recovery` fails closed on explicit resume failure. Recovery confirmation is scoped to the same authorized channel/chat and only arms the next ordinary message; confirmation itself performs no provider call. Persistence must succeed before recovery is acknowledged or a replacement thread becomes authoritative.

### Audit Logging

All inbound and outbound messages are logged via `store.insertAuditLog()` with:
- Channel type, chat ID, direction, message ID, truncated summary
- Dangerous input blocks are logged with `[BLOCKED]` prefix
- Truncated inputs are logged with `[TRUNCATED]` prefix

Host-side Codex call-envelope/rollout evidence is private operational data and needs an explicit retention policy; the bridge does not prune it automatically. Missing, ambiguous, replaced/rotated, shortened, or unparsable rollout evidence is audit-unavailable and cannot establish same-condition retry eligibility. Checkpoint comparison cannot detect the extreme case where the same inode is truncated and regrown beyond its old byte size; treat evidence with that possibility as untrusted.

### Transport Security

- All platform APIs use HTTPS
- Bot tokens are stored in the host's settings store (not in bridge code)
- Token masking in UI prevents accidental exposure

## Recommendations for Deployments

1. Always configure `allowed_users` — never run with open access
2. Use separate bot tokens for bridge vs. notifications
3. Monitor audit logs for unusual patterns
4. Keep bot token rotation in your operational runbook
5. Consider network-level restrictions (firewall, VPN) for the host application

/**
 * Unit tests for bridge permission-broker.
 *
 * Tests cover:
 * - handlePermissionCallback: action parsing, chat validation, dedup
 * - Permission resolution via PermissionGateway
 * - Callback data parsing with colons in permId
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { initBridgeContext } from '../../lib/bridge/context';
import { forwardPermissionRequest, handlePermissionCallback } from '../../lib/bridge/permission-broker';
import { BaseChannelAdapter } from '../../lib/bridge/channel-adapter';
import { PLATFORM_LIMITS } from '../../lib/bridge/types';
import type { ChannelType, OutboundMessage, SendResult } from '../../lib/bridge/types';
import type { BridgeStore, PermissionGateway, PermissionResolution } from '../../lib/bridge/host';

// ── Mock Store ──────────────────────────────────────────────

function createMockStore() {
  const links = new Map<string, { chatId: string; messageId: string; resolved: boolean; suggestions: string }>();

  return {
    links,
    getSetting: (_key?: string): string | null => null,
    getChannelBinding: () => null,
    upsertChannelBinding: () => ({} as any),
    updateChannelBinding: () => {},
    listChannelBindings: () => [],
    getSession: () => null,
    createSession: () => ({ id: '1', working_directory: '', model: '' }),
    updateSessionProviderId: () => {},
    addMessage: () => {},
    getMessages: () => ({ messages: [] }),
    acquireSessionLock: () => true,
    renewSessionLock: () => {},
    releaseSessionLock: () => {},
    setSessionRuntimeStatus: () => {},
    updateSdkSessionId: () => {},
    updateSessionModel: () => {},
    syncSdkTasks: () => {},
    getProvider: () => undefined,
    getDefaultProviderId: () => null,
    insertAuditLog: () => {},
    checkDedup: () => false,
    insertDedup: () => {},
    cleanupExpiredDedup: () => {},
    insertOutboundRef: () => {},
    insertPermissionLink: () => {},
    getPermissionLink: (id: string) => {
      return links.get(id) ?? null;
    },
    markPermissionLinkResolved: (id: string) => {
      const link = links.get(id);
      if (!link || link.resolved) return false;
      link.resolved = true;
      return true;
    },
    listPendingPermissionLinksByChat: (chatId: string) => {
      return [...links.values()].filter(l => l.chatId === chatId && !l.resolved);
    },
    getChannelOffset: () => '0',
    setChannelOffset: () => {},
  };
}

// ── Mock Permission Gateway ─────────────────────────────────

function createMockGateway() {
  const resolved: Array<{ id: string; resolution: PermissionResolution }> = [];
  return {
    resolved,
    resolvePendingPermission(id: string, resolution: PermissionResolution) {
      resolved.push({ id, resolution });
      return true;
    },
  };
}

type MockStore = ReturnType<typeof createMockStore>;
type MockGateway = ReturnType<typeof createMockGateway>;

function setupContext(store: MockStore, gateway: MockGateway) {
  delete (globalThis as Record<string, unknown>)['__bridge_context__'];
  initBridgeContext({
    store: store as unknown as BridgeStore,
    llm: { streamChat: () => new ReadableStream() },
    permissions: gateway,
    lifecycle: {},
  });
}

// ── Tests ───────────────────────────────────────────────────

describe('permission-broker', () => {
  let store: MockStore;
  let gateway: MockGateway;

  beforeEach(() => {
    store = createMockStore();
    gateway = createMockGateway();
    setupContext(store, gateway);
  });

  it('returns false for non-perm callback data', () => {
    assert.equal(handlePermissionCallback('other:data', '123'), false);
  });

  it('returns false when permission link not found', () => {
    assert.equal(handlePermissionCallback('perm:allow:unknown-id', '123'), false);
  });

  it('returns false when chatId does not match', () => {
    store.links.set('perm-1', {
      chatId: '999',
      messageId: 'msg-1',
      resolved: false,
      suggestions: '',
    });

    assert.equal(handlePermissionCallback('perm:allow:perm-1', '123'), false);
  });

  it('returns false when messageId does not match', () => {
    store.links.set('perm-1', {
      chatId: '123',
      messageId: 'msg-1',
      resolved: false,
      suggestions: '',
    });

    assert.equal(handlePermissionCallback('perm:allow:perm-1', '123', 'wrong-msg'), false);
  });

  it('resolves allow action correctly', () => {
    store.links.set('perm-1', {
      chatId: '123',
      messageId: 'msg-1',
      resolved: false,
      suggestions: '',
    });

    const result = handlePermissionCallback('perm:allow:perm-1', '123');
    assert.ok(result);
    assert.equal(gateway.resolved.length, 1);
    assert.equal(gateway.resolved[0].resolution.behavior, 'allow');
  });

  it('resolves deny action correctly', () => {
    store.links.set('perm-2', {
      chatId: '456',
      messageId: 'msg-2',
      resolved: false,
      suggestions: '',
    });

    const result = handlePermissionCallback('perm:deny:perm-2', '456');
    assert.ok(result);
    assert.equal(gateway.resolved[0].resolution.behavior, 'deny');
    assert.equal(gateway.resolved[0].resolution.message, 'Denied via IM bridge');
  });

  it('prevents duplicate resolution', () => {
    store.links.set('perm-3', {
      chatId: '123',
      messageId: 'msg-3',
      resolved: false,
      suggestions: '',
    });

    const first = handlePermissionCallback('perm:allow:perm-3', '123');
    assert.ok(first);

    const second = handlePermissionCallback('perm:allow:perm-3', '123');
    assert.equal(second, false);
    assert.equal(gateway.resolved.length, 1);
  });

  it('handles permId with colons', () => {
    store.links.set('perm:with:colons', {
      chatId: '123',
      messageId: 'msg-4',
      resolved: false,
      suggestions: '',
    });

    const result = handlePermissionCallback('perm:allow:perm:with:colons', '123');
    assert.ok(result);
    assert.equal(gateway.resolved[0].id, 'perm:with:colons');
  });

  it('allow_session passes suggestions as updatedPermissions', () => {
    const suggestions = JSON.stringify([{ type: 'allow', toolName: 'Bash' }]);
    store.links.set('perm-4', {
      chatId: '123',
      messageId: 'msg-5',
      resolved: false,
      suggestions,
    });

    const result = handlePermissionCallback('perm:allow_session:perm-4', '123');
    assert.ok(result);
    assert.equal(gateway.resolved[0].resolution.behavior, 'allow');
    assert.ok((gateway.resolved[0].resolution as any).updatedPermissions);
  });

  it('redacts approval input, preserves realistic commands, and marks oversized input', async () => {
    class CapturingAdapter extends BaseChannelAdapter {
      readonly channelType = 'feishu' as const;
      sent: OutboundMessage[] = [];
      async start() {}
      async stop() {}
      isRunning() { return true; }
      async consumeOne() { return null; }
      async send(message: OutboundMessage): Promise<SendResult> {
        this.sent.push(message);
        return { ok: true, messageId: 'permission-message' };
      }
      validateConfig() { return null; }
      isAuthorized() { return true; }
    }
    const adapter = new CapturingAdapter();
    const secret = 'permission-secret-canary';
    store.getSetting = (key?: string) => key === 'bridge_feishu_app_secret' ? secret : null;
    setupContext(store, gateway);
    const exactCommand = `printf ${'x'.repeat(420)} ${secret}`;

    await forwardPermissionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'chat-1', userId: 'user-1' },
      'permission-long-input',
      'Bash',
      { command: exactCommand },
    );

    assert.equal(adapter.sent.length, 1);
    assert.match(adapter.sent[0].text, /Tool: <code>Bash<\/code>/);
    assert.match(adapter.sent[0].text, new RegExp(`printf x{420} \\[REDACTED\\]`));
    assert.equal(adapter.sent[0].text.includes(secret), false);
    assert.doesNotMatch(adapter.sent[0].text, /TRUNCATED/);

    await forwardPermissionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'chat-2', userId: 'user-1' },
      'permission-oversized-input',
      'Bash',
      { command: 'x'.repeat(50_000) },
    );

    assert.equal(adapter.sent.length, 2);
    assert.match(adapter.sent[1].text, /TRUNCATED.*fit.*feishu/i);
    assert.ok(adapter.sent[1].text.length <= 30_000);
  });

  it('keeps an oversized approval prompt atomic and within every channel limit', async () => {
    class CapturingAdapter extends BaseChannelAdapter {
      sent: OutboundMessage[] = [];
      constructor(readonly channelType: ChannelType) { super(); }
      async start() {}
      async stop() {}
      isRunning() { return true; }
      async consumeOne() { return null; }
      async send(message: OutboundMessage): Promise<SendResult> {
        this.sent.push(message);
        return { ok: true, messageId: `${this.channelType}-permission-message` };
      }
      validateConfig() { return null; }
      isAuthorized() { return true; }
    }

    for (const channelType of ['telegram', 'discord', 'slack', 'feishu', 'qq', 'weixin'] as const) {
      const adapter = new CapturingAdapter(channelType);
      await forwardPermissionRequest(
        adapter,
        { channelType, chatId: `chat-${channelType}`, userId: 'user-1' },
        `permission-atomic-${channelType}`,
        'Bash',
        { command: `printf '${'\"<&'.repeat(6_000)}'` },
      );

      assert.equal(adapter.sent.length, 1, `${channelType} approval must be one message`);
      const message = adapter.sent[0];
      assert.ok(message.text.length <= PLATFORM_LIMITS[channelType], channelType);
      assert.match(message.text, /TRUNCATED.*fit/i, channelType);
      if (message.parseMode === 'HTML') {
        assert.equal((message.text.match(/<pre>/g) || []).length, 1, channelType);
        assert.equal((message.text.match(/<\/pre>/g) || []).length, 1, channelType);
        assert.equal(message.inlineButtons?.length, 1, channelType);
      }
    }
  });

  it('redacts secrets that require JSON escaping before rendering approval input', async () => {
    class CapturingAdapter extends BaseChannelAdapter {
      readonly channelType = 'feishu' as const;
      sent: OutboundMessage[] = [];
      async start() {}
      async stop() {}
      isRunning() { return true; }
      async consumeOne() { return null; }
      async send(message: OutboundMessage): Promise<SendResult> {
        this.sent.push(message);
        return { ok: true, messageId: 'permission-message' };
      }
      validateConfig() { return null; }
      isAuthorized() { return true; }
    }
    const adapter = new CapturingAdapter();
    const secret = 'secret-with-"quote\\slash';
    store.getSetting = (key?: string) => key === 'bridge_feishu_app_secret' ? secret : null;
    setupContext(store, gateway);

    await forwardPermissionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'chat-json-secret', userId: 'user-1' },
      'permission-json-secret',
      'Bash',
      { command: `printf %s '${secret}'` },
    );

    const rendered = adapter.sent[0].text;
    assert.doesNotMatch(rendered, /secret-with-/);
    assert.match(rendered, /\[REDACTED\]/);
  });

  it('retries a rejected formatted approval as one plain message and logs the cause', async () => {
    class FallbackAdapter extends BaseChannelAdapter {
      readonly channelType = 'telegram' as const;
      sent: OutboundMessage[] = [];
      async start() {}
      async stop() {}
      isRunning() { return true; }
      async consumeOne() { return null; }
      async send(message: OutboundMessage): Promise<SendResult> {
        this.sent.push(message);
        if (this.sent.length === 1) {
          return { ok: false, error: 'formatted approval rejected', httpStatus: 400 } as SendResult;
        }
        return { ok: true, messageId: 'plain-fallback-message' };
      }
      validateConfig() { return null; }
      isAuthorized() { return true; }
    }
    const adapter = new FallbackAdapter();
    const warnings: string[] = [];
    const originalWarn = console.warn;
    console.warn = (...args: unknown[]) => warnings.push(args.map(String).join(' '));
    try {
      await forwardPermissionRequest(
        adapter,
        { channelType: 'telegram', chatId: 'chat-fallback', userId: 'user-1' },
        'permission-delivery-fallback',
        'Bash',
        { command: 'printf safe' },
      );
    } finally {
      console.warn = originalWarn;
    }

    assert.equal(adapter.sent.length, 2);
    assert.equal(adapter.sent[1].parseMode, 'plain');
    assert.equal(adapter.sent[1].inlineButtons, undefined);
    assert.match(adapter.sent[1].text, /\/perm allow permission-delivery-fallback/);
    assert.match(adapter.sent[1].text, /\/perm allow_session permission-delivery-fallback/);
    assert.match(adapter.sent[1].text, /\/perm deny permission-delivery-fallback/);
    assert.ok(adapter.sent[1].text.length <= PLATFORM_LIMITS.telegram);
    assert.equal(warnings.filter((line) => line.includes('formatted approval rejected')).length, 1);
  });

  it('does not tell a Feishu 1:1 chat to mention the bot in the plain fallback', async () => {
    class FallbackAdapter extends BaseChannelAdapter {
      readonly channelType = 'feishu' as const;
      sent: OutboundMessage[] = [];
      async start() {}
      async stop() {}
      isRunning() { return true; }
      async consumeOne() { return null; }
      async send(message: OutboundMessage): Promise<SendResult> {
        this.sent.push(message);
        return this.sent.length === 1
          ? { ok: false, error: 'formatted approval rejected', httpStatus: 400 } as SendResult
          : { ok: true, messageId: 'plain-fallback-message' };
      }
      validateConfig() { return null; }
      isAuthorized() { return true; }
    }
    const adapter = new FallbackAdapter();
    store.getSetting = (key?: string) => key === 'bridge_feishu_require_mention' ? 'true' : null;
    setupContext(store, gateway);

    await forwardPermissionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'direct-permission-fallback', userId: 'user-1', isGroup: false },
      'permission-direct-fallback',
      'Bash',
      { command: 'pwd' },
    );

    assert.equal(adapter.sent.length, 2);
    assert.doesNotMatch(adapter.sent[1].text, /mention the bot/i);
    assert.match(adapter.sent[1].text, /reply with one of these commands/i);
  });
});

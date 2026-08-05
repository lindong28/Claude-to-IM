/**
 * Unit tests for bridge delivery-layer.
 *
 * Tests cover:
 * - Text chunking at platform limits
 * - Dedup check and insert
 * - Retry with backoff on server errors
 * - HTML fallback on parse errors
 * - Audit log and outbound ref tracking
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { initBridgeContext } from '../../lib/bridge/context';
import { deliver } from '../../lib/bridge/delivery-layer';
import type { BaseChannelAdapter } from '../../lib/bridge/channel-adapter';
import type { BridgeStore, LLMProvider, PermissionGateway, LifecycleHooks } from '../../lib/bridge/host';
import type { OutboundMessage, SendResult } from '../../lib/bridge/types';

// ── Mock Adapter ────────────────────────────────────────────

function createMockAdapter(opts?: {
  sendFn?: (msg: OutboundMessage) => Promise<SendResult>;
}): BaseChannelAdapter {
  const sendFn = opts?.sendFn ?? (async () => ({ ok: true, messageId: 'msg-1' }));
  return {
    channelType: 'telegram',
    start: async () => {},
    stop: async () => {},
    isRunning: () => true,
    consumeOne: async () => null,
    send: sendFn,
    validateConfig: () => null,
    isAuthorized: () => true,
  } as unknown as BaseChannelAdapter;
}

// ── Mock Store ──────────────────────────────────────────────

function createMockStore(settings: Record<string, string> = {}) {
  const auditLogs: Array<{ chatId: string; direction: string; summary: string }> = [];
  const outboundRefs: Array<{ platformMessageId: string; purpose: string }> = [];
  const dedupKeys = new Set<string>();

  return {
    auditLogs,
    outboundRefs,
    dedupKeys,
    getSetting: (key: string) => settings[key] ?? null,
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
    insertAuditLog: (entry: any) => { auditLogs.push(entry); },
    checkDedup: (key: string) => dedupKeys.has(key),
    insertDedup: (key: string) => { dedupKeys.add(key); },
    cleanupExpiredDedup: () => {},
    insertOutboundRef: (ref: any) => { outboundRefs.push(ref); },
    insertPermissionLink: () => {},
    getPermissionLink: () => null,
    markPermissionLinkResolved: () => false,
    listPendingPermissionLinksByChat: () => [],
    getChannelOffset: () => '0',
    setChannelOffset: () => {},
  };
}

type MockStore = ReturnType<typeof createMockStore>;

function setupContext(store: MockStore) {
  delete (globalThis as Record<string, unknown>)['__bridge_context__'];
  initBridgeContext({
    store: store as unknown as BridgeStore,
    llm: { streamChat: () => new ReadableStream() },
    permissions: { resolvePendingPermission: () => false },
    lifecycle: {},
  });
}

// ── Tests ───────────────────────────────────────────────────

describe('delivery-layer', () => {
  let store: MockStore;

  beforeEach(() => {
    store = createMockStore();
    setupContext(store as MockStore);
  });

  it('delivers a short message in one chunk', async () => {
    const sentMessages: string[] = [];
    const adapter = createMockAdapter({
      sendFn: async (msg) => {
        sentMessages.push(msg.text);
        return { ok: true, messageId: 'msg-1' };
      },
    });

    const result = await deliver(adapter, {
      address: { channelType: 'telegram', chatId: '123' },
      text: 'Hello!',
      parseMode: 'plain',
    });

    assert.ok(result.ok);
    assert.equal(sentMessages.length, 1);
    assert.equal(sentMessages[0], 'Hello!');
  });

  it('redacts known secrets at the unified exit for replies, errors, and question cards', async () => {
    store = createMockStore({ bridge_feishu_app_secret: 'unified-secret-canary' });
    setupContext(store);
    const sent: OutboundMessage[] = [];
    const adapter = createMockAdapter({
      sendFn: async (message) => {
        sent.push(message);
        return { ok: true, messageId: `msg-${sent.length}` };
      },
    });

    await deliver(adapter, {
      address: { channelType: 'telegram', chatId: 'reply' },
      text: 'final unified-secret-canary',
    });
    await deliver(adapter, {
      address: { channelType: 'telegram', chatId: 'error' },
      text: 'error unified-secret-canary',
    });
    await deliver(adapter, {
      address: { channelType: 'feishu', chatId: 'card' },
      text: 'fallback unified-secret-canary',
      questionCard: {
        questionRequestId: 'ask-redaction',
        generation: 'generation',
        questions: [{
          question: 'Reveal unified-secret-canary?',
          header: 'unified-secret-canary',
          options: [
            { label: 'unified-secret-canary', description: 'secret option' },
            { label: 'Safe', description: 'safe option' },
          ],
          multiSelect: false,
        }],
      },
    });

    assert.equal(sent.length, 3);
    assert.doesNotMatch(JSON.stringify(sent), /unified-secret-canary/);
    assert.match(JSON.stringify(sent), /\[REDACTED\]/);
    assert.doesNotMatch(JSON.stringify(store.auditLogs), /unified-secret-canary/);
  });

  it('chunks long messages at platform limit', async () => {
    const sentMessages: string[] = [];
    const adapter = createMockAdapter({
      sendFn: async (msg) => {
        sentMessages.push(msg.text);
        return { ok: true, messageId: `msg-${sentMessages.length}` };
      },
    });

    // Generate text longer than telegram limit (4096)
    const longText = 'Line\n'.repeat(2000);
    const result = await deliver(adapter, {
      address: { channelType: 'telegram', chatId: '123' },
      text: longText,
      parseMode: 'plain',
    });

    assert.ok(result.ok);
    assert.ok(sentMessages.length > 1, `Expected multiple chunks, got ${sentMessages.length}`);
    // Verify no chunk exceeds limit
    for (const chunk of sentMessages) {
      assert.ok(chunk.length <= 4096, `Chunk exceeded limit: ${chunk.length}`);
    }
  });

  it('skips delivery when dedup key already exists', async () => {
    store.dedupKeys.add('dedup-1');
    const adapter = createMockAdapter();

    const result = await deliver(adapter, {
      address: { channelType: 'telegram', chatId: '123' },
      text: 'Duplicate',
      parseMode: 'plain',
    }, { dedupKey: 'dedup-1' });

    assert.ok(result.ok);
    // No audit log should be created for deduped messages
    assert.equal(store.auditLogs.length, 0);
  });

  it('records audit log on successful delivery', async () => {
    const adapter = createMockAdapter();

    await deliver(adapter, {
      address: { channelType: 'telegram', chatId: '123' },
      text: 'Test message',
      parseMode: 'plain',
    });

    assert.equal(store.auditLogs.length, 1);
    assert.equal(store.auditLogs[0].direction, 'outbound');
    assert.equal(store.auditLogs[0].chatId, '123');
  });

  it('tracks outbound refs when sessionId is provided', async () => {
    const adapter = createMockAdapter();

    await deliver(adapter, {
      address: { channelType: 'telegram', chatId: '123' },
      text: 'Test',
      parseMode: 'plain',
    }, { sessionId: 'session-1' });

    assert.equal(store.outboundRefs.length, 1);
    assert.equal(store.outboundRefs[0].platformMessageId, 'msg-1');
    assert.equal(store.outboundRefs[0].purpose, 'response');
  });

  it('returns error on send failure', async () => {
    const adapter = createMockAdapter({
      sendFn: async () => ({ ok: false, error: 'Not Found', httpStatus: 404 } as SendResult),
    });

    const result = await deliver(adapter, {
      address: { channelType: 'telegram', chatId: '123' },
      text: 'Test',
      parseMode: 'plain',
    });

    assert.equal(result.ok, false);
    assert.ok(result.error);
  });
});

import { test } from 'node:test';
import assert from 'node:assert/strict';

import { BaseChannelAdapter } from '../../lib/bridge/channel-adapter';
import { initBridgeContext } from '../../lib/bridge/context';
import { _testOnly } from '../../lib/bridge/bridge-manager';
import type { BridgeStore, StreamChatParams } from '../../lib/bridge/host';
import type { ChannelBinding, InboundMessage, OutboundMessage, SendResult } from '../../lib/bridge/types';

function sse(type: string, data: unknown): string {
  return `data: ${JSON.stringify({ type, data: typeof data === 'string' ? data : JSON.stringify(data) })}\n\n`;
}

function stream(...chunks: string[]): ReadableStream<string> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

function createStore(): BridgeStore {
  const bindings = new Map<string, ChannelBinding>();
  const sessions = new Map<string, { id: string; working_directory: string; model: string }>();
  const messages = new Map<string, Array<{ role: string; content: string }>>();
  let sequence = 0;
  return {
    getSetting: (key) => key === 'bridge_default_work_dir' ? '/fixed' : null,
    getChannelBinding: (channelType, chatId) => bindings.get(`${channelType}:${chatId}`) ?? null,
    upsertChannelBinding(data) {
      const key = `${data.channelType}:${data.chatId}`;
      const existing = bindings.get(key);
      if (existing) return existing;
      const binding: ChannelBinding = {
        id: `binding-${++sequence}`,
        channelType: data.channelType,
        chatId: data.chatId,
        codepilotSessionId: data.codepilotSessionId,
        sdkSessionId: data.sdkSessionId || '',
        workingDirectory: data.workingDirectory,
        model: data.model,
        mode: 'code',
        active: true,
        createdAt: new Date(0).toISOString(),
        updatedAt: new Date(0).toISOString(),
      };
      bindings.set(key, binding);
      return binding;
    },
    updateChannelBinding(id, patch) {
      for (const [key, binding] of bindings) {
        if (binding.id === id) bindings.set(key, { ...binding, ...patch });
      }
    },
    listChannelBindings: () => [...bindings.values()],
    getSession: (id) => sessions.get(id) ?? null,
    createSession(_name, model, _system, cwd) {
      const session = { id: `session-${++sequence}`, working_directory: cwd || '', model };
      sessions.set(session.id, session);
      return session;
    },
    updateSessionProviderId() {},
    addMessage(sessionId, role, content) {
      const history = messages.get(sessionId) || [];
      history.push({ role, content });
      messages.set(sessionId, history);
    },
    getMessages: (sessionId) => ({ messages: [...(messages.get(sessionId) || [])] }),
    acquireSessionLock: () => true,
    renewSessionLock() {},
    releaseSessionLock() {},
    setSessionRuntimeStatus() {},
    updateSdkSessionId(sessionId, sdkSessionId) {
      for (const [key, binding] of bindings) {
        if (binding.codepilotSessionId === sessionId) bindings.set(key, { ...binding, sdkSessionId });
      }
    },
    updateSessionModel() {},
    syncSdkTasks() {},
    getProvider: () => undefined,
    getDefaultProviderId: () => null,
    insertAuditLog() {},
    checkDedup: () => false,
    insertDedup() {},
    cleanupExpiredDedup() {},
    insertOutboundRef() {},
    insertPermissionLink() {},
    getPermissionLink: () => null,
    markPermissionLinkResolved: () => false,
    listPendingPermissionLinksByChat: () => [],
    getChannelOffset: () => '0',
    setChannelOffset() {},
  } as BridgeStore;
}

class Adapter extends BaseChannelAdapter {
  readonly channelType = 'feishu';
  async start() {}
  async stop() {}
  isRunning() { return true; }
  async consumeOne() { return null; }
  async send(_message: OutboundMessage): Promise<SendResult> { return { ok: true }; }
  validateConfig() { return null; }
  isAuthorized() { return true; }
}

function inbound(chatId: string, text: string): InboundMessage {
  return {
    messageId: `${chatId}-${text}`,
    address: { channelType: 'feishu', chatId, userId: 'allowed-user' },
    text,
    timestamp: 0,
  };
}

test('keeps Codex thread and history isolated by allowed group', async () => {
  delete (globalThis as Record<string, unknown>).__bridge_context__;
  delete (globalThis as Record<string, unknown>).__bridge_manager__;
  const calls: StreamChatParams[] = [];
  initBridgeContext({
    store: createStore(),
    llm: {
      streamChat(params) {
        calls.push(params);
        return stream(
          sse('status', { session_id: `thread-${params.sessionId}` }),
          sse('text', `response-${params.sessionId}`),
        );
      },
    },
    permissions: { resolvePendingPermission: () => false },
    lifecycle: {},
  });
  const adapter = new Adapter();

  await _testOnly.handleMessage(adapter, inbound('group-a', 'group A first'));
  await _testOnly.handleMessage(adapter, inbound('group-b', 'group B first'));
  await _testOnly.handleMessage(adapter, inbound('group-a', 'group A second'));

  assert.equal(calls[0].sessionId, calls[2].sessionId);
  assert.notEqual(calls[0].sessionId, calls[1].sessionId);
  assert.equal(calls[2].sdkSessionId, `thread-${calls[0].sessionId}`);
  assert.match(JSON.stringify(calls[2].conversationHistory), /group A first/);
  assert.doesNotMatch(JSON.stringify(calls[1].conversationHistory), /group A/);
  assert.doesNotMatch(JSON.stringify(calls[2].conversationHistory), /group B/);
});

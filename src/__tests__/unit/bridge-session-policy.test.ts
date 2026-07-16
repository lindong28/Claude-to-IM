import { beforeEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { BaseChannelAdapter } from '../../lib/bridge/channel-adapter';
import { initBridgeContext } from '../../lib/bridge/context';
import { _testOnly } from '../../lib/bridge/bridge-manager';
import * as feishuModule from '../../lib/bridge/adapters/feishu-adapter';
import * as router from '../../lib/bridge/channel-router';
import type {
  BridgeStore,
  ExternalHealthEvent,
  LLMProvider,
  StreamChatParams,
} from '../../lib/bridge/host';
import type {
  ChannelBinding,
  InboundMessage,
  OutboundMessage,
  SendResult,
} from '../../lib/bridge/types';

const { FeishuAdapter } = feishuModule;

const OLD_THREAD = 'thread_old_canary';
const NEW_THREAD = 'thread_new_canary';
const GROUP_A = 'group_a_canary';
const GROUP_B = 'group_b_canary';
const USER_OK = 'user_allowed_canary';
const USER_BAD = 'user_denied_canary';

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

function createStore(settings: Record<string, string>) {
  const bindings = new Map<string, ChannelBinding>();
  const sessions = new Map<string, { id: string; working_directory: string; model: string }>();
  const messages = new Map<string, Array<{ role: string; content: string }>>();
  let creates = 0;
  let updates = 0;
  const queuedUpdateFailures: boolean[] = [];

  const store: BridgeStore & {
    bindings: typeof bindings;
    sessions: typeof sessions;
    creates: () => number;
    updates: () => number;
    queueUpdateFailures: (...failures: boolean[]) => void;
  } = {
    bindings,
    sessions,
    creates: () => creates,
    updates: () => updates,
    queueUpdateFailures: (...failures) => queuedUpdateFailures.push(...failures),
    getSetting: (key) => settings[key] ?? null,
    getChannelBinding: (channelType, chatId) => bindings.get(`${channelType}:${chatId}`) ?? null,
    upsertChannelBinding(data) {
      const key = `${data.channelType}:${data.chatId}`;
      const existing = bindings.get(key);
      if (existing) return existing;
      const now = new Date(0).toISOString();
      const binding: ChannelBinding = {
        id: `binding-${++creates}`,
        channelType: data.channelType,
        chatId: data.chatId,
        codepilotSessionId: data.codepilotSessionId,
        sdkSessionId: data.sdkSessionId || '',
        workingDirectory: data.workingDirectory,
        model: data.model,
        mode: 'code',
        active: true,
        createdAt: now,
        updatedAt: now,
      };
      bindings.set(key, binding);
      return binding;
    },
    updateChannelBinding(id, patch) {
      if (queuedUpdateFailures.shift()) throw new Error('persist secret-storage-canary');
      for (const [key, binding] of bindings) {
        if (binding.id === id) {
          updates++;
          bindings.set(key, { ...binding, ...patch });
        }
      }
    },
    listChannelBindings: (type) => [...bindings.values()].filter((b) => !type || b.channelType === type),
    getSession: (id) => sessions.get(id) ?? null,
    createSession(_name, model, _system, cwd) {
      const session = { id: `session-${++creates}`, working_directory: cwd || '', model };
      sessions.set(session.id, session);
      return session;
    },
    updateSessionProviderId() {},
    addMessage(sessionId, role, content) {
      const list = messages.get(sessionId) || [];
      list.push({ role, content });
      messages.set(sessionId, list);
    },
    getMessages(sessionId) { return { messages: [...(messages.get(sessionId) || [])] }; },
    acquireSessionLock: () => true,
    renewSessionLock() {},
    releaseSessionLock() {},
    setSessionRuntimeStatus() {},
    updateSdkSessionId(sessionId, sdkSessionId) {
      for (const [key, binding] of bindings) {
        if (binding.codepilotSessionId === sessionId) {
          bindings.set(key, { ...binding, sdkSessionId });
        }
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
  };
  return store;
}

class TestAdapter extends BaseChannelAdapter {
  readonly channelType = 'feishu';
  sent: OutboundMessage[] = [];
  beforeSend?: () => void;
  async start() {}
  async stop() {}
  isRunning() { return true; }
  async consumeOne() { return null; }
  async send(message: OutboundMessage): Promise<SendResult> {
    this.beforeSend?.();
    this.sent.push(message);
    return { ok: true, messageId: `sent-${this.sent.length}` };
  }
  validateConfig() { return null; }
  isAuthorized(userId: string, chatId: string) {
    return userId === USER_OK && [GROUP_A, GROUP_B].includes(chatId);
  }
}

function inbound(chatId: string, userId: string, text: string, raw?: unknown): InboundMessage {
  return {
    messageId: `message-${chatId}-${text}`,
    address: { channelType: 'feishu', chatId, userId },
    text,
    timestamp: 0,
    raw,
  };
}

function seedBinding(store: ReturnType<typeof createStore>, chatId: string, sdkSessionId = OLD_THREAD) {
  const session = store.createSession('seed', '', undefined, '/fixed');
  const binding = store.upsertChannelBinding({
    channelType: 'feishu',
    chatId,
    codepilotSessionId: session.id,
    sdkSessionId,
    workingDirectory: '/fixed',
    model: '',
  });
  store.updateChannelBinding(binding.id, { sdkSessionId });
  return store.getChannelBinding('feishu', chatId)!;
}

await describe('fixed-confirm-recovery session policy', { concurrency: 1 }, () => {
  let store: ReturnType<typeof createStore>;
  let adapter: TestAdapter;
  let calls: StreamChatParams[];
  let healthEvents: ExternalHealthEvent[];

  beforeEach(() => {
    delete (globalThis as Record<string, unknown>).__bridge_context__;
    delete (globalThis as Record<string, unknown>).__bridge_manager__;
    calls = [];
    healthEvents = [];
    store = createStore({
      bridge_session_policy: 'fixed-confirm-recovery',
      bridge_default_work_dir: '/fixed',
      bridge_feishu_group_policy: 'allowlist',
      bridge_feishu_group_allow_from: `${GROUP_A},${GROUP_B}`,
      bridge_feishu_allowed_users: USER_OK,
      bridge_runtime: 'codex',
    });
    const llm: LLMProvider = {
      streamChat(params) {
        calls.push(params);
        if (params.forceFreshThread) {
          assert.equal(
            store.getChannelBinding('feishu', GROUP_A)?.recoveryState,
            'pending',
            'armed authorization must be persisted as consumed before provider side effects',
          );
          return stream(sse('status', { session_id: NEW_THREAD }), sse('text', 'fresh response'));
        }
        if (params.sdkSessionId) {
          return stream(sse('recovery_required', 'resume unavailable'));
        }
        return stream(sse('status', { session_id: 'default-new' }), sse('text', 'default response'));
      },
    };
    initBridgeContext({
      store,
      llm,
      permissions: { resolvePendingPermission: () => false },
      lifecycle: { onExternalHealth: (event) => healthEvents.push(event) },
    });
    adapter = new TestAdapter();
  });

  for (const command of ['/cwd /other', '/new /other', '/bind 00000000-0000-0000-0000-000000000000']) {
    it(`rejects ${command.split(' ')[0]} before any binding mutation`, async () => {
      const beforeCreates = store.creates();
      const beforeUpdates = store.updates();
      await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, command));
      assert.equal(store.creates(), beforeCreates);
      assert.equal(store.updates(), beforeUpdates);
      assert.match(adapter.sent.at(-1)!.text, /fixed session policy/i);
    });
  }

  it('keeps the old thread, arms only the same authorized binding, then consumes one fresh attempt', async () => {
    seedBinding(store, GROUP_A);
    seedBinding(store, GROUP_B);

    adapter.beforeSend = () => {
      assert.equal(store.getChannelBinding('feishu', GROUP_A)!.recoveryState, 'pending');
    };
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, 'first marker'));
    adapter.beforeSend = undefined;
    let groupA = store.getChannelBinding('feishu', GROUP_A)!;
    assert.equal(groupA.sdkSessionId, OLD_THREAD);
    assert.equal(groupA.recoveryState, 'pending');
    assert.match(adapter.sent.at(-1)!.text, /recover confirm/);
    assert.deepEqual(healthEvents.at(-1), { component: 'codex', state: 'error' });

    const callsWhilePending = calls.length;
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, 'must confirm first'));
    assert.equal(calls.length, callsWhilePending, 'pending messages must not call the provider');
    assert.match(adapter.sent.at(-1)!.text, /recover confirm/i);

    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_BAD, '/recover confirm'));
    groupA = store.getChannelBinding('feishu', GROUP_A)!;
    assert.equal(groupA.recoveryState, 'pending');

    await _testOnly.handleMessage(adapter, inbound(GROUP_B, USER_OK, '/recover confirm'));
    assert.equal(store.getChannelBinding('feishu', GROUP_A)!.recoveryState, 'pending');
    assert.notEqual(store.getChannelBinding('feishu', GROUP_B)!.recoveryState, 'armed');

    const callsBeforeConfirm = calls.length;
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, '/recover confirm'));
    assert.equal(calls.length, callsBeforeConfirm, 'confirmation must not send an internal prompt');
    assert.equal(store.getChannelBinding('feishu', GROUP_A)!.recoveryState, 'armed');
    assert.match(adapter.sent.at(-1)!.text, /next message/i);

    adapter.beforeSend = () => {
      const persisted = store.getChannelBinding('feishu', GROUP_A)!;
      assert.equal(persisted.sdkSessionId, NEW_THREAD);
      assert.equal(persisted.recoveryState, undefined);
    };
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, 'second marker'));
    adapter.beforeSend = undefined;
    groupA = store.getChannelBinding('feishu', GROUP_A)!;
    assert.equal(calls.filter((call) => call.forceFreshThread).length, 1);
    assert.equal(groupA.sdkSessionId, NEW_THREAD);
    assert.equal(groupA.recoveryState, undefined);
    assert.match(adapter.sent.at(-1)!.text, /replacement/i);
    assert.deepEqual(healthEvents.at(-1), { component: 'codex', state: 'success' });

    const rendered = adapter.sent.map((message) => message.text).join('\n');
    assert.doesNotMatch(rendered, new RegExp([OLD_THREAD, NEW_THREAD, GROUP_A, GROUP_B, USER_OK, USER_BAD].join('|')));
  });

  it('rejects recovery confirmation when no pending state exists', async () => {
    seedBinding(store, GROUP_A, '');
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, '/recover confirm'));
    assert.notEqual(store.getChannelBinding('feishu', GROUP_A)!.recoveryState, 'armed');
    assert.match(adapter.sent.at(-1)!.text, /no recovery/i);
  });

  it('consumes a failed fresh attempt and requires a new confirmation', async () => {
    seedBinding(store, GROUP_A);
    const failingLlm: LLMProvider = {
      streamChat(params) {
        calls.push(params);
        if (params.forceFreshThread) return stream(sse('error', 'fresh failed'));
        return stream(sse('recovery_required', 'resume unavailable'));
      },
    };
    initBridgeContext({
      store,
      llm: failingLlm,
      permissions: { resolvePendingPermission: () => false },
      lifecycle: { onExternalHealth: (event) => healthEvents.push(event) },
    });

    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, 'resume marker'));
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, '/recover confirm'));
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, 'fresh failure marker'));
    assert.equal(calls.filter((call) => call.forceFreshThread).length, 1);
    assert.equal(store.getChannelBinding('feishu', GROUP_A)!.sdkSessionId, OLD_THREAD);
    assert.equal(store.getChannelBinding('feishu', GROUP_A)!.recoveryState, 'pending');

    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, 'not reconfirmed marker'));
    assert.equal(calls.filter((call) => call.forceFreshThread).length, 1);
  });

  it('does not call the provider when consuming armed state cannot be persisted', async () => {
    const binding = seedBinding(store, GROUP_A);
    store.updateChannelBinding(binding.id, { recoveryState: 'pending' });
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, '/recover confirm'));
    const callsBefore = calls.length;
    store.queueUpdateFailures(true);

    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, 'precommit failure'));

    assert.equal(calls.length, callsBefore);
    assert.equal(store.getChannelBinding('feishu', GROUP_A)!.recoveryState, 'armed');
    assert.match(adapter.sent.at(-1)!.text, /recovery state could not be saved/i);
    assert.doesNotMatch(adapter.sent.at(-1)!.text, /secret-storage-canary/);
  });

  it('does not advertise recovery confirmation when pending state persistence fails', async () => {
    seedBinding(store, GROUP_A);
    store.queueUpdateFailures(true);

    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, 'resume persistence failure'));

    assert.equal(calls.length, 1);
    assert.equal(store.getChannelBinding('feishu', GROUP_A)!.recoveryState, undefined);
    assert.match(adapter.sent.at(-1)!.text, /recovery state could not be saved/i);
    assert.doesNotMatch(adapter.sent.at(-1)!.text, /recover confirm|secret-storage-canary/i);
  });

  it('keeps confirmation retryable when armed state persistence fails', async () => {
    const binding = seedBinding(store, GROUP_A);
    store.updateChannelBinding(binding.id, { recoveryState: 'pending' });
    store.queueUpdateFailures(true);

    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, '/recover confirm'));

    assert.equal(calls.length, 0);
    assert.equal(store.getChannelBinding('feishu', GROUP_A)!.recoveryState, 'pending');
    assert.match(adapter.sent.at(-1)!.text, /recovery state could not be saved/i);
  });

  it('keeps a consumed replacement attempt pending when final thread persistence fails', async () => {
    const binding = seedBinding(store, GROUP_A);
    store.updateChannelBinding(binding.id, { recoveryState: 'pending' });
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, '/recover confirm'));
    store.queueUpdateFailures(false, true);

    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, 'final persistence failure'));

    assert.equal(calls.filter((call) => call.forceFreshThread).length, 1);
    assert.equal(store.getChannelBinding('feishu', GROUP_A)!.sdkSessionId, OLD_THREAD);
    assert.equal(store.getChannelBinding('feishu', GROUP_A)!.recoveryState, 'pending');
    assert.match(adapter.sent.at(-1)!.text, /recovery state could not be saved/i);
    assert.doesNotMatch(adapter.sent.at(-1)!.text, /replacement session created/i);
  });

  it('keeps default mutable-session behavior when the policy is unset', async () => {
    const mutableStore = createStore({ bridge_default_work_dir: '/default' });
    initBridgeContext({
      store: mutableStore,
      llm: { streamChat: () => stream(sse('text', 'ok')) },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });
    await _testOnly.handleMessage(adapter, inbound(GROUP_A, USER_OK, '/cwd /changed'));
    assert.equal(mutableStore.getChannelBinding('feishu', GROUP_A)!.workingDirectory, '/changed');
  });

});

function feishuEvent(chatId: string, userId: string, messageId: string, topic: string) {
  return {
    sender: {
      sender_id: { open_id: userId },
      sender_type: 'user',
    },
    message: {
      message_id: messageId,
      chat_id: chatId,
      chat_type: 'group',
      message_type: 'text',
      content: JSON.stringify({ text: '@_user_1 topic marker' }),
      create_time: '1',
      root_id: `root-${topic}`,
      parent_id: `parent-${topic}`,
      thread_id: `thread-${topic}`,
      mentions: [{ key: '@_user_1', id: { open_id: 'bot_canary' }, name: 'bot' }],
    },
  };
}

await describe('Feishu policy normalization and identifier-safe filters', { concurrency: 1 }, () => {
  let store: ReturnType<typeof createStore>;
  let adapter: InstanceType<typeof FeishuAdapter>;
  let healthEvents: ExternalHealthEvent[];

  beforeEach(() => {
    delete (globalThis as Record<string, unknown>).__bridge_context__;
    delete (globalThis as Record<string, unknown>).__bridge_manager__;
    store = createStore({
      bridge_feishu_enabled: 'true',
      bridge_feishu_app_id: 'app_canary',
      bridge_feishu_app_secret: 'secret_canary',
      bridge_feishu_allowed_users: USER_OK,
      bridge_feishu_group_policy: 'allowlist',
      bridge_feishu_group_allow_from: `${GROUP_A},${GROUP_B}`,
      bridge_feishu_require_mention: 'true',
      bridge_default_work_dir: '/fixed',
    });
    healthEvents = [];
    initBridgeContext({
      store,
      llm: { streamChat: () => stream(sse('text', 'ok')) },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: { onExternalHealth: (event) => healthEvents.push(event) },
    });
    adapter = new FeishuAdapter();
    (adapter as any).botIds.add('bot_canary');
  });

  it('normalizes topic metadata to the group binding and isolates different groups', async () => {
    await (adapter as any).processIncomingEvent(feishuEvent(GROUP_A, USER_OK, 'message-a1', 'topic-1'));
    await (adapter as any).processIncomingEvent(feishuEvent(GROUP_A, USER_OK, 'message-a2', 'topic-2'));
    await (adapter as any).processIncomingEvent(feishuEvent(GROUP_B, USER_OK, 'message-b1', 'topic-1'));

    const first = await adapter.consumeOne();
    const second = await adapter.consumeOne();
    const third = await adapter.consumeOne();
    assert.equal(first!.address.chatId, GROUP_A);
    assert.equal(second!.address.chatId, GROUP_A);
    assert.equal(third!.address.chatId, GROUP_B);
    assert.equal(router.resolve(first!.address).id, router.resolve(second!.address).id);
    assert.notEqual(router.resolve(first!.address).id, router.resolve(third!.address).id);
    assert.equal(healthEvents.filter((event) => event.component === 'feishu' && event.state === 'accepted-inbound').length, 3);
  });

  it('validates explicit policy and boolean settings without changing open defaults', () => {
    assert.equal(adapter.validateConfig(), null);
    const invalidPolicyStore = createStore({
      bridge_feishu_enabled: 'true',
      bridge_feishu_app_id: 'app',
      bridge_feishu_app_secret: 'secret',
      bridge_feishu_group_policy: 'anything',
    });
    initBridgeContext({
      store: invalidPolicyStore,
      llm: { streamChat: () => stream() },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });
    assert.match(new FeishuAdapter().validateConfig() || '', /bridge_feishu_group_policy/);

    const invalidBooleanStore = createStore({
      bridge_feishu_enabled: 'true',
      bridge_feishu_app_id: 'app',
      bridge_feishu_app_secret: 'secret',
      bridge_feishu_require_mention: 'yes',
    });
    initBridgeContext({
      store: invalidBooleanStore,
      llm: { streamChat: () => stream() },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });
    assert.match(new FeishuAdapter().validateConfig() || '', /bridge_feishu_require_mention/);
  });

  it('never writes full user or group identifiers to filter logs', async () => {
    const lines: string[] = [];
    const originalWarn = console.warn;
    const originalLog = console.log;
    console.warn = (...args: unknown[]) => lines.push(args.join(' '));
    console.log = (...args: unknown[]) => lines.push(args.join(' '));
    try {
      await (adapter as any).processIncomingEvent(feishuEvent(GROUP_A, USER_BAD, 'message-denied-user', 'topic'));
      await (adapter as any).processIncomingEvent(feishuEvent('group_denied_canary', USER_OK, 'message-denied-group', 'topic'));
      const noMention = feishuEvent(GROUP_A, USER_OK, 'message-no-mention', 'topic');
      noMention.message.mentions = [];
      await (adapter as any).processIncomingEvent(noMention);
    } finally {
      console.warn = originalWarn;
      console.log = originalLog;
    }
    const rendered = lines.join('\n');
    assert.doesNotMatch(rendered, new RegExp([GROUP_A, USER_BAD, 'group_denied_canary'].join('|')));
    assert.match(rendered, /ref=[a-f0-9]{12}/);
  });

  it('rejects unauthorized card actors before enqueue or accepted health', async () => {
    for (const [chatId, userId] of [
      [GROUP_A, USER_BAD],
      ['group_denied_card_canary', USER_OK],
    ]) {
      const result = await (adapter as any).handleCardAction({
        action: { value: { callback_data: 'perm:allow:permission-card-canary' } },
        context: { open_chat_id: chatId, open_message_id: 'card-message-canary' },
        operator: { open_id: userId },
      });
      assert.deepEqual(result, { toast: { type: 'info', content: '已收到' } });
    }
    assert.equal(await adapter.consumeOne(), null);
    assert.equal(healthEvents.length, 0);
  });

  it('accepts a card action only when both actor and group are allowlisted', async () => {
    await (adapter as any).handleCardAction({
      action: { value: { callback_data: 'perm:allow:permission-card-canary' } },
      context: { open_chat_id: GROUP_A, open_message_id: 'card-message-canary' },
      operator: { open_id: USER_OK },
    });
    const queued = await adapter.consumeOne();
    assert.equal(queued?.address.chatId, GROUP_A);
    assert.equal(queued?.address.userId, USER_OK);
    assert.deepEqual(healthEvents, [{ component: 'feishu', state: 'accepted-inbound' }]);
  });

  it('does not print the resolved bot open id in the startup log', () => {
    const botId = 'bot-open-id-startup-secret-canary';
    const rendered = (feishuModule as any)._testOnly.formatBotStartupLog(botId);
    assert.match(rendered, /botOpenId:\s*(resolved|ref=)/i);
    assert.doesNotMatch(rendered, new RegExp(botId));
  });

  it('translates SDK websocket lifecycle logs into identifier-free health events', () => {
    const logger = (feishuModule as any)._testOnly.createWsHealthLogger(
      (event: ExternalHealthEvent) => healthEvents.push(event),
    );
    logger.debug('[ws]', 'ws connect success');
    logger.debug(['[ws]', 'reconnect success']);
    logger.debug('[ws]', 'client closed');
    logger.error('[ws]', 'ws connect failed', 'secret_health_canary');
    logger.error('[ws]', 'invoke event failed', 'secret_health_canary');
    assert.deepEqual(healthEvents, [
      { component: 'feishu', state: 'connected' },
      { component: 'feishu', state: 'connected' },
      { component: 'feishu', state: 'disconnected' },
      { component: 'feishu', state: 'disconnected' },
    ]);
  });
});

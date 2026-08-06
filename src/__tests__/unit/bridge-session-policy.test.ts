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
  PendingQuestionRecord,
  StreamChatParams,
} from '../../lib/bridge/host';
import type {
  ChannelType,
  ChannelBinding,
  InboundMessage,
  OutboundMessage,
  SendResult,
} from '../../lib/bridge/types';

const { FeishuAdapter } = feishuModule;

const OLD_THREAD = 'thread_old_canary';
const NEW_THREAD = 'thread_new_canary';
let groupSequence = 0;
const USER_OK = 'user_allowed_canary';
const USER_BAD = 'user_denied_canary';

function allocateRateLimitIsolatedGroups(): { a: string; b: string } {
  // delivery-layer owns a process-wide per-chat rate limiter; every test gets
  // fresh chat IDs so start/queue acknowledgements cannot exhaust another test.
  groupSequence += 1;
  return {
    a: `group_a_canary_${groupSequence}`,
    b: `group_b_canary_${groupSequence}`,
  };
}

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
  readonly channelType: ChannelType = 'feishu';
  sent: OutboundMessage[] = [];
  beforeSend?: (message: OutboundMessage) => void;
  async start() {}
  async stop() {}
  isRunning() { return true; }
  async consumeOne() { return null; }
  async send(message: OutboundMessage): Promise<SendResult> {
    this.beforeSend?.(message);
    this.sent.push(message);
    return { ok: true, messageId: `sent-${this.sent.length}` };
  }
  validateConfig() { return null; }
  isAuthorized(userId: string, chatId: string) {
    return userId === USER_OK && this.allowedGroups.includes(chatId);
  }

  constructor(private readonly allowedGroups: readonly string[] = []) {
    super();
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
  let groups: { a: string; b: string };
  let store: ReturnType<typeof createStore>;
  let adapter: TestAdapter;
  let calls: StreamChatParams[];
  let healthEvents: ExternalHealthEvent[];

  beforeEach(() => {
    groups = allocateRateLimitIsolatedGroups();
    delete (globalThis as Record<string, unknown>).__bridge_context__;
    delete (globalThis as Record<string, unknown>).__bridge_manager__;
    calls = [];
    healthEvents = [];
    store = createStore({
      bridge_session_policy: 'fixed-confirm-recovery',
      bridge_default_work_dir: '/fixed',
      bridge_feishu_group_policy: 'allowlist',
      bridge_feishu_group_allow_from: `${groups.a},${groups.b}`,
      bridge_feishu_allowed_users: USER_OK,
      bridge_runtime: 'codex',
    });
    const llm: LLMProvider = {
      streamChat(params) {
        calls.push(params);
        if (params.forceFreshThread) {
          assert.equal(
            store.getChannelBinding('feishu', groups.a)?.recoveryState,
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
    adapter = new TestAdapter([groups.a, groups.b]);
  });

  for (const command of ['/cwd /other', '/new /other', '/bind 00000000-0000-0000-0000-000000000000']) {
    it(`rejects ${command.split(' ')[0]} before any binding mutation`, async () => {
      const beforeCreates = store.creates();
      const beforeUpdates = store.updates();
      await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, command));
      assert.equal(store.creates(), beforeCreates);
      assert.equal(store.updates(), beforeUpdates);
      assert.match(adapter.sent.at(-1)!.text, /fixed session policy/i);
    });
  }

  it('keeps the old thread, arms only the same authorized binding, then consumes one fresh attempt', async () => {
    seedBinding(store, groups.a);
    seedBinding(store, groups.b);

    adapter.beforeSend = (message) => {
      if (message.text === 'Task started.') return;
      assert.equal(store.getChannelBinding('feishu', groups.a)!.recoveryState, 'pending');
    };
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, 'first marker'));
    adapter.beforeSend = undefined;
    let groupA = store.getChannelBinding('feishu', groups.a)!;
    assert.equal(groupA.sdkSessionId, OLD_THREAD);
    assert.equal(groupA.recoveryState, 'pending');
    assert.match(adapter.sent.at(-1)!.text, /recover confirm/);
    assert.deepEqual(healthEvents.at(-1), { component: 'codex', state: 'error' });

    const callsWhilePending = calls.length;
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, 'must confirm first'));
    assert.equal(calls.length, callsWhilePending, 'pending messages must not call the provider');
    assert.match(adapter.sent.at(-1)!.text, /recover confirm/i);

    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_BAD, '/recover confirm'));
    groupA = store.getChannelBinding('feishu', groups.a)!;
    assert.equal(groupA.recoveryState, 'pending');

    await _testOnly.handleMessage(adapter, inbound(groups.b, USER_OK, '/recover confirm'));
    assert.equal(store.getChannelBinding('feishu', groups.a)!.recoveryState, 'pending');
    assert.notEqual(store.getChannelBinding('feishu', groups.b)!.recoveryState, 'armed');

    const callsBeforeConfirm = calls.length;
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, '/recover confirm'));
    assert.equal(calls.length, callsBeforeConfirm, 'confirmation must not send an internal prompt');
    assert.equal(store.getChannelBinding('feishu', groups.a)!.recoveryState, 'armed');
    assert.match(adapter.sent.at(-1)!.text, /next message/i);

    adapter.beforeSend = (message) => {
      if (message.text === 'Task started.') return;
      const persisted = store.getChannelBinding('feishu', groups.a)!;
      assert.equal(persisted.sdkSessionId, NEW_THREAD);
      assert.equal(persisted.recoveryState, undefined);
    };
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, 'second marker'));
    adapter.beforeSend = undefined;
    groupA = store.getChannelBinding('feishu', groups.a)!;
    assert.equal(calls.filter((call) => call.forceFreshThread).length, 1);
    assert.equal(groupA.sdkSessionId, NEW_THREAD);
    assert.equal(groupA.recoveryState, undefined);
    assert.match(adapter.sent.at(-1)!.text, /replacement/i);
    assert.deepEqual(healthEvents.at(-1), { component: 'codex', state: 'success' });

    const rendered = adapter.sent.map((message) => message.text).join('\n');
    assert.doesNotMatch(rendered, new RegExp([OLD_THREAD, NEW_THREAD, groups.a, groups.b, USER_OK, USER_BAD].join('|')));
  });

  it('rejects recovery confirmation when no pending state exists', async () => {
    seedBinding(store, groups.a, '');
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, '/recover confirm'));
    assert.notEqual(store.getChannelBinding('feishu', groups.a)!.recoveryState, 'armed');
    assert.match(adapter.sent.at(-1)!.text, /no recovery/i);
  });

  it('consumes a failed fresh attempt and requires a new confirmation', async () => {
    seedBinding(store, groups.a);
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

    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, 'resume marker'));
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, '/recover confirm'));
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, 'fresh failure marker'));
    assert.equal(calls.filter((call) => call.forceFreshThread).length, 1);
    assert.equal(store.getChannelBinding('feishu', groups.a)!.sdkSessionId, OLD_THREAD);
    assert.equal(store.getChannelBinding('feishu', groups.a)!.recoveryState, 'pending');

    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, 'not reconfirmed marker'));
    assert.equal(calls.filter((call) => call.forceFreshThread).length, 1);
  });

  it('does not call the provider when consuming armed state cannot be persisted', async () => {
    const binding = seedBinding(store, groups.a);
    store.updateChannelBinding(binding.id, { recoveryState: 'pending' });
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, '/recover confirm'));
    const callsBefore = calls.length;
    store.queueUpdateFailures(true);

    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, 'precommit failure'));

    assert.equal(calls.length, callsBefore);
    assert.equal(store.getChannelBinding('feishu', groups.a)!.recoveryState, 'armed');
    assert.match(adapter.sent.at(-1)!.text, /recovery state could not be saved/i);
    assert.doesNotMatch(adapter.sent.at(-1)!.text, /secret-storage-canary/);
  });

  it('does not advertise recovery confirmation when pending state persistence fails', async () => {
    seedBinding(store, groups.a);
    store.queueUpdateFailures(true);

    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, 'resume persistence failure'));

    assert.equal(calls.length, 1);
    assert.equal(store.getChannelBinding('feishu', groups.a)!.recoveryState, undefined);
    assert.match(adapter.sent.at(-1)!.text, /recovery state could not be saved/i);
    assert.doesNotMatch(adapter.sent.at(-1)!.text, /recover confirm|secret-storage-canary/i);
  });

  it('keeps confirmation retryable when armed state persistence fails', async () => {
    const binding = seedBinding(store, groups.a);
    store.updateChannelBinding(binding.id, { recoveryState: 'pending' });
    store.queueUpdateFailures(true);

    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, '/recover confirm'));

    assert.equal(calls.length, 0);
    assert.equal(store.getChannelBinding('feishu', groups.a)!.recoveryState, 'pending');
    assert.match(adapter.sent.at(-1)!.text, /recovery state could not be saved/i);
  });

  it('keeps a consumed replacement attempt pending when final thread persistence fails', async () => {
    const binding = seedBinding(store, groups.a);
    store.updateChannelBinding(binding.id, { recoveryState: 'pending' });
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, '/recover confirm'));
    store.queueUpdateFailures(false, true);

    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, 'final persistence failure'));

    assert.equal(calls.filter((call) => call.forceFreshThread).length, 1);
    assert.equal(store.getChannelBinding('feishu', groups.a)!.sdkSessionId, OLD_THREAD);
    assert.equal(store.getChannelBinding('feishu', groups.a)!.recoveryState, 'pending');
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
    await _testOnly.handleMessage(adapter, inbound(groups.a, USER_OK, '/cwd /changed'));
    assert.equal(mutableStore.getChannelBinding('feishu', groups.a)!.workingDirectory, '/changed');
  });

  it('rejects /mode changes when the independent fixed-mode opt-in is active', async () => {
    const fixedModeGroup = 'group_fixed_mode_canary';
    const fixedModeStore = createStore({
      bridge_default_work_dir: '/fixed',
      bridge_default_mode: 'code',
      bridge_fixed_mode: 'code',
    });
    initBridgeContext({
      store: fixedModeStore,
      llm: { streamChat: () => stream(sse('text', 'ok')) },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });
    seedBinding(fixedModeStore, fixedModeGroup);
    await _testOnly.handleMessage(adapter, inbound(fixedModeGroup, USER_OK, '/mode plan'));
    assert.equal(fixedModeStore.getChannelBinding('feishu', fixedModeGroup)!.mode, 'code');
    assert.match(adapter.sent.at(-1)!.text, /fixed.*code/i);
  });

  it('preserves mutable /mode behavior when fixed mode is not opted in', async () => {
    const mutableModeGroup = 'group_mutable_mode_canary';
    const mutableStore = createStore({ bridge_default_work_dir: '/default' });
    initBridgeContext({
      store: mutableStore,
      llm: { streamChat: () => stream(sse('text', 'ok')) },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });
    await _testOnly.handleMessage(adapter, inbound(mutableModeGroup, USER_OK, '/mode plan'));
    assert.equal(mutableStore.getChannelBinding('feishu', mutableModeGroup)!.mode, 'plan');
  });

  it('dispatches a restart card-answer resume through the session lock without blocking callback consumption', async () => {
    const restartGroup = 'group_restart_question_canary';
    const restartStore = createStore({ bridge_default_work_dir: '/fixed' });
    const binding = seedBinding(restartStore, restartGroup);
    const pending = new Map<string, PendingQuestionRecord>();
    pending.set('ask-restart-manager', {
      questionRequestId: 'ask-restart-manager',
      channelType: 'feishu',
      chatId: restartGroup,
      sessionId: binding.codepilotSessionId,
      questions: [{
        question: 'Database?',
        header: 'Database',
        options: [
          { label: 'PostgreSQL', description: 'Relational' },
          { label: 'MongoDB', description: 'Document' },
        ],
        multiSelect: false,
      }],
      answers: {},
      state: 'sent',
      generation: 'restart-generation',
      messageId: 'restart-card',
      createdAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    restartStore.getPendingQuestion = (id) => pending.get(id) ?? null;
    restartStore.listPendingQuestions = () => [...pending.values()];
    restartStore.transitionPendingQuestion = (id, expected, update) => {
      const current = pending.get(id);
      if (!current || !expected.includes(current.state)) return false;
      pending.set(id, { ...current, ...update });
      return true;
    };

    let releaseProvider!: () => void;
    let markProviderStarted!: () => void;
    const providerStarted = new Promise<void>((resolve) => { markProviderStarted = resolve; });
    let providerCalls = 0;
    initBridgeContext({
      store: restartStore,
      llm: {
        streamChat() {
          providerCalls += 1;
          markProviderStarted();
          return new ReadableStream({
            start(controller) {
              releaseProvider = () => {
                controller.enqueue(sse('text', 'resumed response'));
                controller.close();
              };
            },
          });
        },
      },
      permissions: {
        resolvePendingPermission: () => false,
        resolvePendingQuestion: () => false,
      },
      lifecycle: {},
    });

    const callback = inbound(restartGroup, USER_OK, '');
    callback.callbackData = 'ask:submit:ask-restart-manager:restart-generation';
    callback.callbackMessageId = 'restart-card';
    callback.callbackFormValue = { q_0: 'PostgreSQL' };
    const callbackHandling = _testOnly.handleMessage(adapter, callback);
    const dispatchResult = await Promise.race([
      callbackHandling.then(() => 'returned'),
      new Promise<string>((resolve) => setTimeout(() => resolve('blocked'), 30)),
    ]);

    if (dispatchResult === 'blocked') {
      releaseProvider();
      await callbackHandling;
    }
    assert.equal(dispatchResult, 'returned');
    await Promise.race([
      providerStarted,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error('provider dispatch timed out')), 100)),
    ]);
    assert.equal(providerCalls, 1);
    const managerState = (globalThis as unknown as {
      __bridge_manager__: { sessionLocks: Map<string, Promise<void>> };
    }).__bridge_manager__;
    assert.equal(managerState.sessionLocks.has(binding.codepilotSessionId), true);

    releaseProvider();
    await managerState.sessionLocks.get(binding.codepilotSessionId);
    assert.equal(managerState.sessionLocks.has(binding.codepilotSessionId), false);
    assert.equal(pending.get('ask-restart-manager')!.state, 'answered');
  });

  it('acknowledges a second Feishu task while it is queued behind the session lock', async () => {
    const queueGroup = 'group_queue_ack_canary';
    const queueStore = createStore({ bridge_default_work_dir: '/fixed' });
    seedBinding(queueStore, queueGroup, '');
    let releaseFirst!: () => void;
    let providerCalls = 0;
    initBridgeContext({
      store: queueStore,
      llm: {
        streamChat() {
          providerCalls += 1;
          if (providerCalls === 1) {
            return new ReadableStream({
              start(controller) {
                releaseFirst = () => {
                  controller.enqueue(sse('text', 'first complete'));
                  controller.close();
                };
              },
            });
          }
          return stream(sse('text', 'second complete'));
        },
      },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });

    (_testOnly as any).dispatchSessionMessage(adapter, inbound(queueGroup, USER_OK, 'first task'));
    await new Promise((resolve) => setImmediate(resolve));
    (_testOnly as any).dispatchSessionMessage(adapter, inbound(queueGroup, USER_OK, 'second task'));
    await new Promise((resolve) => setImmediate(resolve));

    assert.equal(providerCalls, 1);
    assert.match(adapter.sent.map((message) => message.text).join('\n'), /Task queued\./);

    releaseFirst();
    const managerState = (globalThis as unknown as {
      __bridge_manager__: { sessionLocks: Map<string, Promise<void>> };
    }).__bridge_manager__;
    await managerState.sessionLocks.values().next().value;
    assert.equal(providerCalls, 2);
  });

  it('does not consume a QQ passive-reply slot for the Feishu-only start acknowledgement', async () => {
    class QqTestAdapter extends TestAdapter {
      override readonly channelType = 'qq' as const;
    }
    const qqAdapter = new QqTestAdapter();
    const qqStore = createStore({ bridge_default_work_dir: '/fixed' });
    initBridgeContext({
      store: qqStore,
      llm: { streamChat: () => stream(sse('text', 'qq complete')) },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });

    await _testOnly.handleMessage(qqAdapter, {
      ...inbound('qq-user', USER_OK, 'qq task'),
      address: { channelType: 'qq', chatId: 'qq-user', userId: USER_OK },
    });

    assert.doesNotMatch(qqAdapter.sent.map((message) => message.text).join('\n'), /Task started\./);
    assert.match(qqAdapter.sent.at(-1)!.text, /qq complete/);
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
  let groups: { a: string; b: string };
  let store: ReturnType<typeof createStore>;
  let adapter: InstanceType<typeof FeishuAdapter>;
  let healthEvents: ExternalHealthEvent[];

  beforeEach(() => {
    groups = allocateRateLimitIsolatedGroups();
    delete (globalThis as Record<string, unknown>).__bridge_context__;
    delete (globalThis as Record<string, unknown>).__bridge_manager__;
    store = createStore({
      bridge_feishu_enabled: 'true',
      bridge_feishu_app_id: 'app_canary',
      bridge_feishu_app_secret: 'secret_canary',
      bridge_feishu_allowed_users: USER_OK,
      bridge_feishu_group_policy: 'allowlist',
      bridge_feishu_group_allow_from: `${groups.a},${groups.b}`,
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
    await (adapter as any).processIncomingEvent(feishuEvent(groups.a, USER_OK, 'message-a1', 'topic-1'));
    await (adapter as any).processIncomingEvent(feishuEvent(groups.a, USER_OK, 'message-a2', 'topic-2'));
    await (adapter as any).processIncomingEvent(feishuEvent(groups.b, USER_OK, 'message-b1', 'topic-1'));

    const first = await adapter.consumeOne();
    const second = await adapter.consumeOne();
    const third = await adapter.consumeOne();
    assert.equal(first!.address.chatId, groups.a);
    assert.equal(second!.address.chatId, groups.a);
    assert.equal(third!.address.chatId, groups.b);
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
      await (adapter as any).processIncomingEvent(feishuEvent(groups.a, USER_BAD, 'message-denied-user', 'topic'));
      await (adapter as any).processIncomingEvent(feishuEvent('group_denied_canary', USER_OK, 'message-denied-group', 'topic'));
      const noMention = feishuEvent(groups.a, USER_OK, 'message-no-mention', 'topic');
      noMention.message.mentions = [];
      await (adapter as any).processIncomingEvent(noMention);
    } finally {
      console.warn = originalWarn;
      console.log = originalLog;
    }
    const rendered = lines.join('\n');
    assert.doesNotMatch(rendered, new RegExp([groups.a, USER_BAD, 'group_denied_canary'].join('|')));
    assert.match(rendered, /ref=[a-f0-9]{12}/);
  });

  it('rejects unauthorized card actors before enqueue or accepted health', async () => {
    for (const [chatId, userId] of [
      [groups.a, USER_BAD],
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
      action: {
        value: { callback_data: 'ask:submit:question-card-canary:generation-canary' },
        form_value: { q_0: 'Choice A', other_0: '' },
      },
      context: { open_chat_id: groups.a, open_message_id: 'card-message-canary' },
      operator: { open_id: USER_OK },
    });
    const queued = await adapter.consumeOne();
    assert.equal(queued?.address.chatId, groups.a);
    assert.equal(queued?.address.userId, USER_OK);
    assert.deepEqual(queued?.callbackFormValue, { q_0: 'Choice A', other_0: '' });
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

await describe('Feishu streaming-card failure fallback', { concurrency: 1 }, () => {
  it('emits start and completion messages and logs one warning when card creation fails', async () => {
    delete (globalThis as Record<string, unknown>).__bridge_context__;
    delete (globalThis as Record<string, unknown>).__bridge_manager__;
    const store = createStore({ bridge_default_work_dir: '/fixed' });
    initBridgeContext({
      store,
      llm: { streamChat: () => stream(sse('status', { session_id: 'sdk-card-fallback' }), sse('text', 'completed response')) },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });
    const sent: any[] = [];
    let cardCreateCalls = 0;
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      cardkit: { v1: { card: { create: async () => {
        cardCreateCalls += 1;
        throw new Error('cardkit unavailable canary');
      } } } },
      im: {
        message: {
          create: async (payload: any) => {
            sent.push(payload);
            return { code: 0, data: { message_id: `message-${sent.length}` } };
          },
        },
        messageReaction: { create: async () => ({ code: 0, data: {} }), delete: async () => ({ code: 0 }) },
      },
    };
    const cardGroup = 'group_card_fallback_canary';
    adapter.lastIncomingMessageId.set(cardGroup, 'incoming-card-fallback');
    const warnings: string[] = [];
    const originalWarn = console.warn;
    console.warn = (...args: unknown[]) => warnings.push(args.join(' '));
    try {
      await _testOnly.handleMessage(adapter, inbound(cardGroup, USER_OK, 'card fallback request'));
      await new Promise((resolve) => setImmediate(resolve));
    } finally {
      console.warn = originalWarn;
    }

    const rendered = sent.map((payload) => payload.data.content).join('\n');
    assert.match(rendered, /Task started/);
    assert.match(rendered, /completed response/);
    assert.equal(cardCreateCalls, 1);
    assert.equal(warnings.filter((line) => line.includes('cardkit unavailable canary')).length, 1);
  });

  it('sends only a completion notice when the full card content is visible but finalization fails', async () => {
    delete (globalThis as Record<string, unknown>).__bridge_context__;
    delete (globalThis as Record<string, unknown>).__bridge_manager__;
    const store = createStore({ bridge_default_work_dir: '/fixed' });
    initBridgeContext({
      store,
      llm: { streamChat: () => stream(sse('text', 'complete card answer')) },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });
    class VisibleCardAdapter extends TestAdapter {
      onStreamText() {}
      async onStreamEnd() { return 'content-visible' as const; }
    }
    const visibleAdapter = new VisibleCardAdapter();

    await _testOnly.handleMessage(
      visibleAdapter,
      inbound('group_visible_card_canary', USER_OK, 'visible card request'),
    );

    const ordinaryMessages = visibleAdapter.sent.map((message) => message.text);
    assert.deepEqual(ordinaryMessages, [
      'Task started.',
      'Task completed. The reply above may be missing its final formatting.',
    ]);
  });
});

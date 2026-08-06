import { beforeEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { BaseChannelAdapter } from '../../lib/bridge/channel-adapter';
import { initBridgeContext } from '../../lib/bridge/context';
import {
  QuestionBroker,
  parseQuestionAnswers,
  validateAskQuestions,
} from '../../lib/bridge/question-broker';
import { buildQuestionCard } from '../../lib/bridge/markdown/feishu';
import type {
  BridgeStore,
  PendingQuestionRecord,
  PermissionResolution,
} from '../../lib/bridge/host';
import type { OutboundMessage, SendResult } from '../../lib/bridge/types';

const QUESTIONS = [
  {
    question: 'Database?',
    header: 'Database',
    options: [
      { label: 'PostgreSQL', description: 'Relational' },
      { label: 'MongoDB', description: 'Document' },
    ],
    multiSelect: false,
  },
  {
    question: 'Features?',
    header: 'Features',
    options: [
      { label: 'Auth', description: 'Authentication' },
      { label: 'Cache', description: 'Caching' },
    ],
    multiSelect: true,
  },
];

function createStore(settingOverrides: Record<string, string> = {}) {
  const questions = new Map<string, PendingQuestionRecord>();
  const settings: Record<string, string> = {
    bridge_feishu_app_secret: 'card-secret-canary',
    ...settingOverrides,
  };
  return {
    questions,
    getSetting: (key: string) => settings[key] ?? null,
    savePendingQuestion(record: PendingQuestionRecord) {
      questions.set(record.questionRequestId, structuredClone(record));
    },
    getPendingQuestion(id: string) {
      return questions.get(id) ? structuredClone(questions.get(id)!) : null;
    },
    listPendingQuestions() {
      return [...questions.values()].map((record) => structuredClone(record));
    },
    transitionPendingQuestion(id: string, expected: PendingQuestionRecord['state'][], update: Partial<PendingQuestionRecord>) {
      const current = questions.get(id);
      if (!current || !expected.includes(current.state)) return false;
      questions.set(id, { ...current, ...structuredClone(update) });
      return true;
    },
  };
}

class TestAdapter extends BaseChannelAdapter {
  readonly channelType: string = 'feishu';
  readonly supportsQuestionCards: boolean = true;
  sent: OutboundMessage[] = [];
  failQuestionCards = false;
  async start() {}
  async stop() {}
  isRunning() { return true; }
  async consumeOne() { return null; }
  async send(message: OutboundMessage): Promise<SendResult> {
    this.sent.push(structuredClone(message));
    if (this.failQuestionCards && message.questionCard) {
      return { ok: false, error: 'injected send failure', httpStatus: 400 } as SendResult;
    }
    return { ok: true, messageId: `card-${this.sent.length}` };
  }
  validateConfig() { return null; }
  isAuthorized(userId: string, chatId: string) { return userId === 'allowed' && chatId === 'group'; }
}

function init(store: ReturnType<typeof createStore>, resolutions: Array<{ id: string; value: PermissionResolution }>, resolveResult = true) {
  initBridgeContext({
    store: store as unknown as BridgeStore,
    llm: { streamChat: () => new ReadableStream() },
    permissions: {
      resolvePendingPermission: () => false,
      resolvePendingQuestion(id, value) {
        resolutions.push({ id, value });
        return resolveResult;
      },
    },
    lifecycle: {},
  });
}

describe('AskUserQuestion input and card mapping', () => {
  it('accepts the pinned SDK full domain and rejects out-of-domain inputs', () => {
    assert.deepEqual(validateAskQuestions({ questions: QUESTIONS }), QUESTIONS);
    assert.throws(() => validateAskQuestions({ questions: [] }), /1.*4/);
    assert.throws(() => validateAskQuestions({ questions: [...QUESTIONS, ...QUESTIONS, QUESTIONS[0]] }), /1.*4/);
  });

  it('renders multi-question, multi-select, and Other free-input controls', () => {
    const card = JSON.parse(buildQuestionCard(QUESTIONS, 'ask-1', 'gen-1', 'group'));
    const serialized = JSON.stringify(card);
    assert.match(serialized, /Database\?/);
    assert.match(serialized, /Features\?/);
    assert.match(serialized, /select_static/);
    assert.match(serialized, /multi_select_static/);
    assert.match(serialized, /Other/);
    assert.match(serialized, /input/);
    assert.match(serialized, /ask:submit:ask-1:gen-1/);
    const form = card.body.elements[0];
    const interactiveTags = new Set(['button', 'input', 'select_static', 'multi_select_static']);
    const names: string[] = [];
    const visit = (elements: Array<Record<string, unknown>>) => {
      for (const element of elements) {
        if (interactiveTags.has(String(element.tag))) {
          assert.equal(typeof element.name, 'string', `${String(element.tag)} must have a name inside a form`);
          assert.ok(String(element.name).trim(), `${String(element.tag)} name must be non-empty`);
          names.push(String(element.name));
        }
        if (Array.isArray(element.elements)) visit(element.elements as Array<Record<string, unknown>>);
        if (Array.isArray(element.columns)) {
          for (const column of element.columns as Array<Record<string, unknown>>) {
            if (Array.isArray(column.elements)) visit(column.elements as Array<Record<string, unknown>>);
          }
        }
      }
    };
    visit(form.elements);
    assert.equal(new Set(names).size, names.length, 'interactive names must be unique within the form');
  });

  it('maps complete single, multi-select, and Other answers to SDK strings', () => {
    assert.deepEqual(parseQuestionAnswers(QUESTIONS, {
      q_0: 'PostgreSQL',
      q_1: ['Auth', 'Other'],
      other_1: 'Metrics',
    }), {
      'Database?': 'PostgreSQL',
      'Features?': 'Auth, Metrics',
    });
  });

  it('rejects missing answers and invalid option combinations without consuming the question', () => {
    assert.equal(parseQuestionAnswers(QUESTIONS, { q_0: 'PostgreSQL' }), null);
    assert.equal(parseQuestionAnswers(QUESTIONS, { q_0: 'Unknown', q_1: ['Auth'] }), null);
    assert.equal(parseQuestionAnswers(QUESTIONS, { q_0: 'PostgreSQL', q_1: ['Other'] }), null);
  });
});

describe('pending question lifecycle', () => {
  let store: ReturnType<typeof createStore>;
  let adapter: TestAdapter;
  let resolutions: Array<{ id: string; value: PermissionResolution }>;

  beforeEach(() => {
    store = createStore();
    adapter = new TestAdapter();
    resolutions = [];
    init(store, resolutions);
  });

  it('persists before send and falls back to visible text without hanging on send failure', async () => {
    adapter.failQuestionCards = true;
    const broker = new QuestionBroker({ actionTimeoutMs: 10_000 });
    await broker.forwardQuestionRequest(adapter, { channelType: 'feishu', chatId: 'group' }, 'ask-send-fail', QUESTIONS, 'session');
    const record = store.questions.get('ask-send-fail')!;
    assert.equal(record.state, 'fallback-pending');
    assert.equal(resolutions.length, 1);
    assert.equal(resolutions[0].value.behavior, 'deny');
    assert.equal(adapter.sent.length, 2);
    assert.match(adapter.sent[1].text, /Database\?/);
    const fallbackAnswer = JSON.stringify({
      'Database?': 'PostgreSQL',
      'Features?': 'Auth, Cache',
    });
    const resumed = broker.handleFallbackAnswer({ channelType: 'feishu', chatId: 'group' }, fallbackAnswer);
    assert.equal(resumed.accepted, true);
    assert.match(resumed.resumePrompt || '', /Continue the original task/);
    assert.equal(broker.handleFallbackAnswer({ channelType: 'feishu', chatId: 'group' }, fallbackAnswer).handled, false);
    broker.dispose();
  });

  it('fails before delivery when the host cannot read pending questions back', async () => {
    const incompleteStore = createStore();
    delete (incompleteStore as Partial<typeof incompleteStore>).getPendingQuestion;
    init(incompleteStore, resolutions);
    const broker = new QuestionBroker({ actionTimeoutMs: 20 });

    await assert.rejects(
      broker.forwardQuestionRequest(
        adapter,
        { channelType: 'feishu', chatId: 'group-missing-getter' },
        'ask-missing-getter',
        [QUESTIONS[0]],
        'session',
      ),
      /pending-question persistence.*getPendingQuestion/i,
    );
    assert.equal(adapter.sent.length, 0);
    broker.dispose();
  });

  it('moves a sent card to fallback-pending on action timeout and releases the provider request', async () => {
    const broker = new QuestionBroker({ actionTimeoutMs: 20 });
    await broker.forwardQuestionRequest(adapter, { channelType: 'feishu', chatId: 'group' }, 'ask-timeout', [QUESTIONS[0]], 'session');
    assert.equal(store.questions.get('ask-timeout')!.state, 'sent');
    await new Promise((resolve) => setTimeout(resolve, 40));
    assert.equal(store.questions.get('ask-timeout')!.state, 'fallback-pending');
    assert.equal(resolutions.at(-1)?.value.behavior, 'deny');
    assert.match(adapter.sent.at(-1)!.text, /reply/i);
    broker.dispose();
  });

  it('expires an older answerable fallback before a newer card enters text fallback', async () => {
    store.savePendingQuestion({
      questionRequestId: 'ask-existing-answerable-fallback',
      channelType: 'feishu',
      chatId: 'group-timeout-consolidation',
      sessionId: 'session',
      questions: [{ ...QUESTIONS[0], question: 'Old fallback question?' }],
      answers: {},
      state: 'fallback-pending',
      generation: 'old-fallback-generation',
      createdAt: new Date(0).toISOString(),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    const broker = new QuestionBroker({ actionTimeoutMs: 20 });
    await broker.forwardQuestionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'group-timeout-consolidation' },
      'ask-new-timeout-fallback',
      [{ ...QUESTIONS[0], question: 'New fallback question?' }],
      'session',
    );

    await new Promise((resolve) => setTimeout(resolve, 40));

    assert.equal(store.questions.get('ask-existing-answerable-fallback')!.state, 'expired');
    assert.equal(store.questions.get('ask-new-timeout-fallback')!.state, 'fallback-pending');
    assert.equal(
      [...store.questions.values()].filter((record) => record.state === 'fallback-pending').length,
      1,
    );
    assert.ok(adapter.sent.some((message) => /New fallback question/.test(message.text)));
    assert.match(adapter.sent.at(-1)!.text, /earlier pending question.*closed.*latest question prompt/i);
    broker.dispose();
  });

  it('expires a failed text fallback so the next unrelated message is not swallowed', async () => {
    class FailingFallbackAdapter extends TestAdapter {
      override async send(message: OutboundMessage): Promise<SendResult> {
        if (message.questionCard) return super.send(message);
        this.sent.push(structuredClone(message));
        return { ok: false, error: 'fallback delivery rejected', httpStatus: 400 } as SendResult;
      }
    }
    const failingAdapter = new FailingFallbackAdapter();
    const warnings: string[] = [];
    const originalWarn = console.warn;
    console.warn = (...args: unknown[]) => warnings.push(args.map(String).join(' '));
    const broker = new QuestionBroker({ actionTimeoutMs: 20 });
    try {
      await broker.forwardQuestionRequest(
        failingAdapter,
        { channelType: 'feishu', chatId: 'group-failed-fallback' },
        'ask-failed-fallback',
        [QUESTIONS[0]],
        'session',
      );
      await new Promise((resolve) => setTimeout(resolve, 40));
    } finally {
      console.warn = originalWarn;
    }

    assert.equal(store.questions.get('ask-failed-fallback')!.state, 'expired');
    assert.equal(resolutions.at(-1)?.value.behavior, 'deny');
    assert.equal(resolutions.at(-1)?.value.message, 'Interactive question fallback delivery failed');
    assert.equal(broker.handleFallbackAnswer(
      { channelType: 'feishu', chatId: 'group-failed-fallback' },
      'unrelated request',
    ).handled, false);
    assert.equal(warnings.some((line) => /fallback.*delivery.*failed/i.test(line)), true);
    broker.dispose();
  });

  it('waits until the outer expiry by default instead of entering the old five-minute fallback', async () => {
    const broker = new QuestionBroker({ expiryMs: 20 });
    await broker.forwardQuestionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'group-default-long-wait' },
      'ask-default-long-wait',
      [QUESTIONS[0]],
      'session',
    );
    assert.equal(store.questions.get('ask-default-long-wait')!.state, 'sent');

    await new Promise((resolve) => setTimeout(resolve, 40));

    assert.equal(store.questions.get('ask-default-long-wait')!.state, 'expired');
    assert.equal(resolutions.at(-1)?.value.behavior, 'deny');
    assert.equal(adapter.sent.length, 2);
    assert.match(adapter.sent.at(-1)!.text, /expired.*send the request again/i);
    broker.dispose();
  });

  it('reads a shorter wait from the runtime setting and enters text fallback', async () => {
    store = createStore({ bridge_question_card_wait_seconds: '1' });
    init(store, resolutions);
    const broker = new QuestionBroker({ expiryMs: 2_000 });
    await broker.forwardQuestionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'group-configured-short-wait' },
      'ask-configured-short-wait',
      [QUESTIONS[0]],
      'session',
    );

    await new Promise((resolve) => setTimeout(resolve, 1_050));

    assert.equal(store.questions.get('ask-configured-short-wait')!.state, 'fallback-pending');
    assert.equal(resolutions.at(-1)?.value.behavior, 'deny');
    assert.match(adapter.sent.at(-1)!.text, /reply in text/i);
    broker.dispose();
  });

  it('expires instead of falling back when the configured wait reaches the remaining outer bound', async () => {
    store = createStore({ bridge_question_card_wait_seconds: '86400' });
    init(store, resolutions);
    const broker = new QuestionBroker({ expiryMs: 20 });
    await broker.forwardQuestionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'group-configured-at-expiry' },
      'ask-configured-at-expiry',
      [QUESTIONS[0]],
      'session',
    );

    await new Promise((resolve) => setTimeout(resolve, 40));

    assert.equal(store.questions.get('ask-configured-at-expiry')!.state, 'expired');
    assert.match(adapter.sent.at(-1)!.text, /expired.*send the request again/i);
    broker.dispose();
  });

  it('treats a malformed store wait as the 24-hour outer-bound default', async () => {
    store = createStore({ bridge_question_card_wait_seconds: 'not-a-duration' });
    init(store, resolutions);
    const broker = new QuestionBroker({ expiryMs: 20 });
    await broker.forwardQuestionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'group-malformed-wait' },
      'ask-malformed-wait',
      [QUESTIONS[0]],
      'session',
    );

    await new Promise((resolve) => setTimeout(resolve, 40));

    assert.equal(store.questions.get('ask-malformed-wait')!.state, 'expired');
    assert.match(adapter.sent.at(-1)!.text, /expired/i);
    broker.dispose();
  });

  it('routes adapters without question-card capability directly to text fallback', async () => {
    class TextOnlyAdapter extends TestAdapter {
      override readonly channelType: string = 'telegram';
      override readonly supportsQuestionCards: boolean = false;
    }
    const textOnly = new TextOnlyAdapter();
    const broker = new QuestionBroker({ actionTimeoutMs: 10_000 });
    await broker.forwardQuestionRequest(
      textOnly,
      { channelType: 'telegram', chatId: 'group' },
      'ask-text-only',
      [QUESTIONS[0]],
      'session',
    );

    assert.equal(store.questions.get('ask-text-only')!.state, 'fallback-pending');
    assert.equal(textOnly.sent.length, 1);
    assert.equal(textOnly.sent[0].questionCard, undefined);
    assert.match(textOnly.sent[0].text, /reply in text/i);
    assert.equal(resolutions.at(-1)?.value.behavior, 'deny');
    broker.dispose();
  });

  it('tells mention-required Feishu groups how to answer the text fallback', async () => {
    class TextOnlyFeishuAdapter extends TestAdapter {
      override readonly supportsQuestionCards = false;
    }
    store = createStore({ bridge_feishu_require_mention: 'true' });
    init(store, resolutions);
    const textOnly = new TextOnlyFeishuAdapter();
    const broker = new QuestionBroker();
    await broker.forwardQuestionRequest(
      textOnly,
      { channelType: 'feishu', chatId: 'group-mention-fallback', isGroup: true },
      'ask-mention-fallback',
      [QUESTIONS[0]],
      'session',
    );

    assert.match(textOnly.sent.at(-1)!.text, /mention the bot.*reply in text/i);
    broker.dispose();
  });

  it('does not tell a Feishu 1:1 chat to mention the bot in text fallback', async () => {
    class TextOnlyFeishuAdapter extends TestAdapter {
      override readonly supportsQuestionCards = false;
    }
    store = createStore({ bridge_feishu_require_mention: 'true' });
    init(store, resolutions);
    const textOnly = new TextOnlyFeishuAdapter();
    const broker = new QuestionBroker();
    await broker.forwardQuestionRequest(
      textOnly,
      { channelType: 'feishu', chatId: 'direct-no-mention-fallback', userId: 'user-1', isGroup: false },
      'ask-direct-no-mention-fallback',
      [QUESTIONS[0]],
      'session',
    );

    assert.doesNotMatch(textOnly.sent.at(-1)!.text, /mention the bot/i);
    assert.match(textOnly.sent.at(-1)!.text, /reply in text/i);
    broker.dispose();
  });

  it('expires stale text fallback without consuming the new message', () => {
    const broker = new QuestionBroker({ actionTimeoutMs: 10_000 });
    store.savePendingQuestion({
      questionRequestId: 'ask-stale-fallback',
      channelType: 'feishu',
      chatId: 'group',
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'fallback-pending',
      generation: 'old',
      createdAt: new Date(0).toISOString(),
      expiresAt: new Date(1).toISOString(),
    });

    const result = broker.handleFallbackAnswer({ channelType: 'feishu', chatId: 'group' }, 'unrelated new request');
    assert.equal(result.handled, false);
    assert.equal(store.questions.get('ask-stale-fallback')!.state, 'expired');
    broker.dispose();
  });

  it('rejects an ambiguous fallback answer after expiring older active siblings', () => {
    const broker = new QuestionBroker({ actionTimeoutMs: 10_000 });
    for (const [id, createdAt, question] of [
      ['ask-fallback-old', 1, 'Old question?'],
      ['ask-fallback-new', 2, 'Newest question?'],
    ] as const) {
      store.savePendingQuestion({
        questionRequestId: id,
        channelType: 'feishu',
        chatId: 'group-overlapping-fallbacks',
        sessionId: 'session',
        questions: [{ ...QUESTIONS[0], question }],
        answers: {},
        state: 'fallback-pending',
        generation: id,
        createdAt: new Date(createdAt).toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      });
    }

    const result = broker.handleFallbackAnswer(
      { channelType: 'feishu', chatId: 'group-overlapping-fallbacks' },
      'Continue',
    );

    assert.equal(result.accepted, false);
    assert.match(result.error || '', /multiple.*latest.*reply again/i);
    assert.equal(store.questions.get('ask-fallback-old')!.state, 'expired');
    assert.equal(store.questions.get('ask-fallback-new')!.state, 'fallback-pending');
    assert.deepEqual(store.questions.get('ask-fallback-new')!.answers, {});
    broker.dispose();
  });

  it('does not match a fallback from another channel with the same chat id', () => {
    store.savePendingQuestion({
      questionRequestId: 'ask-telegram-collision',
      channelType: 'telegram',
      chatId: 'shared-chat-id',
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'fallback-pending',
      generation: 'telegram-generation',
      createdAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    const broker = new QuestionBroker();

    const result = broker.handleFallbackAnswer(
      { channelType: 'feishu', chatId: 'shared-chat-id' },
      'must remain a normal Feishu message',
    );

    assert.equal(result.handled, false);
    assert.equal(store.questions.get('ask-telegram-collision')!.state, 'fallback-pending');
    broker.dispose();
  });

  it('restores legacy Feishu fallback wording in the mention-safe direction without allowlist inference', async () => {
    store = createStore({
      bridge_feishu_require_mention: 'true',
      bridge_feishu_group_policy: 'open',
    });
    init(store, resolutions);
    store.savePendingQuestion({
      questionRequestId: 'ask-legacy-group',
      channelType: 'feishu',
      chatId: 'legacy-group',
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'fallback-pending',
      generation: 'legacy',
      createdAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    const broker = new QuestionBroker();

    await broker.restorePendingQuestions(adapter);

    assert.match(adapter.sent.at(-1)!.text, /mention the bot.*reply in text/i);
    broker.dispose();
  });

  it('reissues after restart, rejects the old card, and resumes exactly once from the new card', async () => {
    const first = new QuestionBroker({ actionTimeoutMs: 10_000, generation: () => 'old-gen' });
    await first.forwardQuestionRequest(adapter, { channelType: 'feishu', chatId: 'group' }, 'ask-restart', [QUESTIONS[0]], 'session');
    const oldMessageId = store.questions.get('ask-restart')!.messageId!;
    first.dispose();

    resolutions = [];
    init(store, resolutions, false);
    const second = new QuestionBroker({ actionTimeoutMs: 10_000, generation: () => 'new-gen' });
    await second.restorePendingQuestions(adapter);
    const restored = store.questions.get('ask-restart')!;
    assert.equal(restored.state, 'sent');
    assert.equal(restored.generation, 'new-gen');
    assert.notEqual(restored.messageId, oldMessageId);

    const old = second.handleQuestionCallback(
      'ask:submit:ask-restart:old-gen',
      'group',
      oldMessageId,
      { q_0: 'PostgreSQL' },
    );
    assert.equal(old.accepted, false);

    const answered = second.handleQuestionCallback(
      'ask:submit:ask-restart:new-gen',
      'group',
      restored.messageId,
      { q_0: 'PostgreSQL' },
    );
    assert.equal(answered.accepted, true);
    assert.match(answered.resumePrompt || '', /Database\?.*PostgreSQL/s);
    assert.equal(store.questions.get('ask-restart')!.state, 'answered');

    const duplicate = second.handleQuestionCallback(
      'ask:submit:ask-restart:new-gen',
      'group',
      restored.messageId,
      { q_0: 'PostgreSQL' },
    );
    assert.equal(duplicate.accepted, false);
    second.dispose();
  });

  it('returns a mention-safe group type for a legacy persisted Feishu card callback', () => {
    store = createStore({
      bridge_feishu_require_mention: 'true',
      bridge_feishu_group_policy: 'open',
    });
    init(store, resolutions, false);
    store.savePendingQuestion({
      questionRequestId: 'ask-legacy-callback',
      channelType: 'feishu',
      chatId: 'legacy-callback-group',
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'sent',
      generation: 'legacy-generation',
      messageId: 'legacy-message',
      createdAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    const broker = new QuestionBroker();

    const result = broker.handleQuestionCallback(
      'ask:submit:ask-legacy-callback:legacy-generation',
      'legacy-callback-group',
      'legacy-message',
      { q_0: 'PostgreSQL' },
    );

    assert.equal(result.accepted, true);
    assert.equal(result.isGroup, true);
    broker.dispose();
  });

  it('preserves a persisted 1:1 type across restart callbacks', () => {
    init(store, resolutions, false);
    store.savePendingQuestion({
      questionRequestId: 'ask-direct-callback',
      channelType: 'feishu',
      chatId: 'direct-callback-chat',
      isGroup: false,
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'sent',
      generation: 'direct-generation',
      messageId: 'direct-message',
      createdAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    const broker = new QuestionBroker();

    const result = broker.handleQuestionCallback(
      'ask:submit:ask-direct-callback:direct-generation',
      'direct-callback-chat',
      'direct-message',
      { q_0: 'PostgreSQL' },
    );

    assert.equal(result.accepted, true);
    assert.equal(result.isGroup, false);
    broker.dispose();
  });

  it('reissues a persisted card at most once across repeated restarts', async () => {
    const first = new QuestionBroker({ actionTimeoutMs: 10_000, generation: () => 'original' });
    await first.forwardQuestionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'group' },
      'ask-bounded-reissue',
      [QUESTIONS[0]],
      'session',
    );
    first.dispose();

    const second = new QuestionBroker({ actionTimeoutMs: 10_000, generation: () => 'one-reissue' });
    await second.restorePendingQuestions(adapter);
    second.dispose();
    assert.equal(store.questions.get('ask-bounded-reissue')!.state, 'sent');
    assert.equal(store.questions.get('ask-bounded-reissue')!.reissueCount, 1);
    assert.equal(adapter.sent.filter((message) => message.questionCard).length, 2);

    const third = new QuestionBroker({ actionTimeoutMs: 10_000, generation: () => 'must-not-be-used' });
    await third.restorePendingQuestions(adapter);

    assert.equal(store.questions.get('ask-bounded-reissue')!.state, 'fallback-pending');
    assert.equal(adapter.sent.filter((message) => message.questionCard).length, 2);
    assert.match(adapter.sent.at(-1)!.text, /reply in text/i);
    third.dispose();
  });

  it('restores at most one answerable fallback per chat and shows only its prompt', async () => {
    for (const [id, createdAt, question] of [
      ['ask-restore-old', 1_000, 'Old restart question?'],
      ['ask-restore-new', 2_000, 'Newest restart question?'],
    ] as const) {
      store.savePendingQuestion({
        questionRequestId: id,
        channelType: 'feishu',
        chatId: 'group-restart-consolidation',
        isGroup: true,
        sessionId: 'session',
        questions: [{ ...QUESTIONS[0], question }],
        answers: {},
        state: 'sent',
        generation: id,
        reissueCount: 1,
        createdAt: new Date(createdAt).toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      });
    }
    init(store, resolutions, false);
    const broker = new QuestionBroker();

    await broker.restorePendingQuestions(adapter);

    assert.equal(store.questions.get('ask-restore-old')!.state, 'expired');
    assert.equal(store.questions.get('ask-restore-new')!.state, 'fallback-pending');
    assert.equal(adapter.sent.length, 2);
    assert.doesNotMatch(adapter.sent[0].text, /Old restart question/);
    assert.match(adapter.sent[0].text, /Newest restart question/);
    assert.match(adapter.sent[1].text, /earlier pending question.*closed.*latest question prompt/i);
    broker.dispose();
  });

  it('does not point to a latest prompt when consolidation immediately expires the survivor', async () => {
    for (let index = 0; index < 4; index += 1) {
      store.savePendingQuestion({
        questionRequestId: `ask-exhausted-${index}`,
        channelType: 'feishu',
        chatId: 'group-exhausted-consolidation',
        sessionId: 'session',
        questions: [QUESTIONS[0]],
        answers: {},
        state: 'fallback-pending',
        generation: `generation-${index}`,
        fallbackReissueCount: 1,
        createdAt: new Date(index).toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      });
    }
    const broker = new QuestionBroker();
    await broker.restorePendingQuestions(adapter);

    assert.equal(adapter.sent.length, 1);
    assert.match(adapter.sent[0].text, /expired.*send the request again/i);
    assert.doesNotMatch(adapter.sent[0].text, /latest question prompt/i);
    assert.ok([...store.questions.values()].every((record) => record.state === 'expired'));
    broker.dispose();
  });

  it('closes an in-flight pending-send record that becomes sent between /stop iterations', async () => {
    for (const [id, state, createdAt] of [
      ['ask-stop-old', 'sent', 1],
      ['ask-stop-inflight', 'pending-send', 2],
    ] as const) {
      store.savePendingQuestion({
        questionRequestId: id,
        channelType: 'feishu',
        chatId: 'group-stop-interleaving',
        sessionId: 'session',
        questions: [QUESTIONS[0]],
        answers: {},
        state,
        generation: id,
        messageId: state === 'sent' ? `${id}-card` : undefined,
        createdAt: new Date(createdAt).toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      });
    }
    let interleaved = false;
    const originalTransition = store.transitionPendingQuestion.bind(store);
    store.transitionPendingQuestion = (id, expected, update) => {
      const result = originalTransition(id, expected, update);
      if (id === 'ask-stop-old' && result && !interleaved) {
        interleaved = true;
        queueMicrotask(() => {
          originalTransition('ask-stop-inflight', ['pending-send'], {
            state: 'sent',
            messageId: 'ask-stop-inflight-card',
          });
        });
      }
      return result;
    };
    const broker = new QuestionBroker();
    const result = await broker.closePendingQuestionsForChat(
      adapter,
      { channelType: 'feishu', chatId: 'group-stop-interleaving' },
    );

    assert.deepEqual(result, { found: 2, transitioned: 2, released: 2, expired: 2, remaining: 0 });
    assert.equal(store.questions.get('ask-stop-old')!.state, 'expired');
    assert.equal(store.questions.get('ask-stop-inflight')!.state, 'expired');
    assert.equal(
      broker.handleQuestionCallback(
        'ask:submit:ask-stop-inflight:ask-stop-inflight',
        'group-stop-interleaving',
        'ask-stop-inflight-card',
        { q_0: 'PostgreSQL' },
      ).accepted,
      false,
    );
    broker.dispose();
  });

  it('continues restoring other chats after one adapter delivery throws', async () => {
    class ThrowOnceAdapter extends TestAdapter {
      calls = 0;
      override async send(message: OutboundMessage): Promise<SendResult> {
        this.calls += 1;
        if (this.calls === 1) throw new Error('injected restore delivery failure');
        return super.send(message);
      }
    }
    const throwingAdapter = new ThrowOnceAdapter();
    for (const [id, chatId] of [
      ['ask-restore-throws', 'group-restore-throws'],
      ['ask-restore-continues', 'group-restore-continues'],
    ] as const) {
      store.savePendingQuestion({
        questionRequestId: id,
        channelType: 'feishu',
        chatId,
        sessionId: 'session',
        questions: [QUESTIONS[0]],
        answers: {},
        state: 'fallback-pending',
        generation: id,
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      });
    }
    const warnings: string[] = [];
    const originalWarn = console.warn;
    console.warn = (...args: unknown[]) => warnings.push(args.map(String).join(' '));
    const broker = new QuestionBroker();
    try {
      await broker.restorePendingQuestions(throwingAdapter);
    } finally {
      console.warn = originalWarn;
    }

    assert.equal(throwingAdapter.calls, 2);
    assert.equal(store.questions.get('ask-restore-throws')!.state, 'expired');
    assert.equal(store.questions.get('ask-restore-continues')!.state, 'fallback-pending');
    assert.match(warnings.join('\n'), /restore.*injected restore delivery failure/i);
    broker.dispose();
  });

  it('releases provider waits when lifecycle compare-and-set loses', async () => {
    const broker = new QuestionBroker();

    store.transitionPendingQuestion = (id, expected, update) => {
      const current = store.questions.get(id);
      if (!current || !expected.includes(current.state)) return false;
      store.questions.set(id, { ...current, ...update, state: 'expired' });
      return false;
    };
    await broker.forwardQuestionRequest(
      adapter,
      { channelType: 'feishu', chatId: 'group-card-cas-loss' },
      'ask-card-cas-loss',
      [QUESTIONS[0]],
      'session',
    );

    class TextOnlyAdapter extends TestAdapter {
      override readonly supportsQuestionCards = false;
    }
    await broker.forwardQuestionRequest(
      new TextOnlyAdapter(),
      { channelType: 'feishu', chatId: 'group-fallback-cas-loss' },
      'ask-fallback-cas-loss',
      [QUESTIONS[0]],
      'session',
    );

    store.savePendingQuestion({
      questionRequestId: 'ask-expire-cas-loss',
      channelType: 'feishu',
      chatId: 'group-expire-cas-loss',
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'sent',
      generation: 'expire-generation',
      createdAt: new Date(0).toISOString(),
      expiresAt: new Date(1).toISOString(),
    });
    await broker.restorePendingQuestions(adapter);

    assert.deepEqual(
      resolutions.map(({ id }) => id),
      ['ask-card-cas-loss', 'ask-fallback-cas-loss', 'ask-expire-cas-loss'],
    );
    assert.ok(resolutions.every(({ value }) => value.behavior === 'deny'));
    broker.dispose();
  });

  it('expires a persisted text fallback after its one visible restart reminder is exhausted', async () => {
    store.savePendingQuestion({
      questionRequestId: 'ask-bounded-fallback-repost',
      channelType: 'feishu',
      chatId: 'group-bounded-fallback-repost',
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'fallback-pending',
      generation: 'generation',
      createdAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });

    for (let restart = 0; restart < 3; restart += 1) {
      const broker = new QuestionBroker();
      await broker.restorePendingQuestions(adapter);
      broker.dispose();
    }

    assert.equal(store.questions.get('ask-bounded-fallback-repost')!.state, 'expired');
    assert.equal(store.questions.get('ask-bounded-fallback-repost')!.fallbackReissueCount, 1);
    assert.equal(adapter.sent.length, 2);
    assert.match(adapter.sent[0].text, /reply in text/i);
    assert.match(adapter.sent[1].text, /expired.*send the request again/i);
    const unrelated = new QuestionBroker().handleFallbackAnswer(
      { channelType: 'feishu', chatId: 'group-bounded-fallback-repost' },
      'deploy to prod now',
    );
    assert.equal(unrelated.handled, false);
  });

  it('warns and treats an invalid createdAt as older during consolidation', async () => {
    for (const [id, createdAt, question] of [
      ['zzz-invalid-created-at', 'not-a-date', 'Invalid legacy timestamp?'],
      ['aaa-valid-newest', new Date(2_000).toISOString(), 'Actually newest?'],
    ] as const) {
      store.savePendingQuestion({
        questionRequestId: id,
        channelType: 'feishu',
        chatId: 'group-invalid-created-at',
        sessionId: 'session',
        questions: [{ ...QUESTIONS[0], question }],
        answers: {},
        state: 'sent',
        generation: id,
        reissueCount: 1,
        createdAt,
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      });
    }
    const warnings: string[] = [];
    const originalWarn = console.warn;
    console.warn = (...args: unknown[]) => warnings.push(args.map(String).join(' '));
    const broker = new QuestionBroker();
    try {
      await broker.restorePendingQuestions(adapter);
    } finally {
      console.warn = originalWarn;
      broker.dispose();
    }

    assert.equal(store.questions.get('zzz-invalid-created-at')!.state, 'expired');
    assert.equal(store.questions.get('aaa-valid-newest')!.state, 'fallback-pending');
    assert.match(warnings.join('\n'), /invalid createdAt.*zzz-invalid-created-at/i);
  });

  it('expires stale persisted questions instead of reissuing them', async () => {
    const broker = new QuestionBroker({ actionTimeoutMs: 10_000 });
    store.savePendingQuestion({
      questionRequestId: 'ask-expired',
      channelType: 'feishu',
      chatId: 'group',
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'sent',
      generation: 'old',
      messageId: 'old-card',
      createdAt: new Date(0).toISOString(),
      expiresAt: new Date(1).toISOString(),
    });
    await broker.restorePendingQuestions(adapter);
    assert.equal(store.questions.get('ask-expired')!.state, 'expired');
    assert.equal(adapter.sent.length, 1);
    assert.match(adapter.sent[0].text, /expired.*send the request again/i);
    broker.dispose();
  });

  it('consolidates multiple expired restart notices to one message per chat', async () => {
    for (const id of ['ask-expired-batch-a', 'ask-expired-batch-b']) {
      store.savePendingQuestion({
        questionRequestId: id,
        channelType: 'feishu',
        chatId: 'group-expired-batch',
        sessionId: 'session',
        questions: [QUESTIONS[0]],
        answers: {},
        state: 'sent',
        generation: id,
        createdAt: new Date(0).toISOString(),
        expiresAt: new Date(1).toISOString(),
      });
    }
    const broker = new QuestionBroker();

    await broker.restorePendingQuestions(adapter);

    assert.equal(store.questions.get('ask-expired-batch-a')!.state, 'expired');
    assert.equal(store.questions.get('ask-expired-batch-b')!.state, 'expired');
    assert.equal(adapter.sent.length, 1);
    assert.match(adapter.sent[0].text, /expired/i);
    broker.dispose();
  });

  it('rejects a card callback after the 24-hour outer expiry', () => {
    const broker = new QuestionBroker();
    store.savePendingQuestion({
      questionRequestId: 'ask-expired-callback',
      channelType: 'feishu',
      chatId: 'group',
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'sent',
      generation: 'generation',
      messageId: 'card-expired',
      createdAt: new Date(0).toISOString(),
      expiresAt: new Date(1).toISOString(),
    });

    const result = broker.handleQuestionCallback(
      'ask:submit:ask-expired-callback:generation',
      'group',
      'card-expired',
      { q_0: 'PostgreSQL' },
    );

    assert.equal(result.accepted, false);
    assert.match(result.error || '', /expired/i);
    assert.equal(store.questions.get('ask-expired-callback')!.state, 'expired');
    broker.dispose();
  });

  it('fails closed at the provider when an expired callback loses its state transition', () => {
    const broker = new QuestionBroker();
    store.savePendingQuestion({
      questionRequestId: 'ask-expiry-race',
      channelType: 'feishu',
      chatId: 'group-expiry-race',
      sessionId: 'session',
      questions: [QUESTIONS[0]],
      answers: {},
      state: 'sent',
      generation: 'generation',
      messageId: 'card-expiry-race',
      createdAt: new Date(0).toISOString(),
      expiresAt: new Date(1).toISOString(),
    });
    store.transitionPendingQuestion = () => false;

    const result = broker.handleQuestionCallback(
      'ask:submit:ask-expiry-race:generation',
      'group-expiry-race',
      'card-expiry-race',
      { q_0: 'PostgreSQL' },
    );

    assert.equal(result.accepted, false);
    assert.match(result.error || '', /expired/i);
    assert.equal(resolutions.length, 1);
    assert.equal(resolutions[0].value.behavior, 'deny');
    broker.dispose();
  });
});

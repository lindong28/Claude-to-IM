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

function createStore() {
  const questions = new Map<string, PendingQuestionRecord>();
  const settings: Record<string, string> = {
    bridge_feishu_app_secret: 'card-secret-canary',
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
    const resumed = broker.handleFallbackAnswer('group', fallbackAnswer);
    assert.equal(resumed.accepted, true);
    assert.match(resumed.resumePrompt || '', /Continue the original task/);
    assert.equal(broker.handleFallbackAnswer('group', fallbackAnswer).handled, false);
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

    const result = broker.handleFallbackAnswer('group', 'unrelated new request');
    assert.equal(result.handled, false);
    assert.equal(store.questions.get('ask-stale-fallback')!.state, 'expired');
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
    assert.equal(adapter.sent.length, 0);
    broker.dispose();
  });
});

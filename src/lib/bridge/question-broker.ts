import crypto from 'node:crypto';

import type { BaseChannelAdapter } from './channel-adapter.js';
import type { ChannelAddress, OutboundMessage } from './types.js';
import type {
  AskQuestion,
  PendingQuestionRecord,
  PendingQuestionState,
} from './host.js';
import { getBridgeContext } from './context.js';
import { deliver } from './delivery-layer.js';
import { knownOutboundSecrets, redactLiterals } from './security/outbound-redaction.js';

const DEFAULT_EXPIRY_MS = 24 * 60 * 60_000;
const QUESTION_WAIT_SETTING = 'bridge_question_card_wait_seconds';
const ACTIVE_QUESTION_STATES: PendingQuestionState[] = ['pending-send', 'sent', 'fallback-pending'];

interface LifecycleTransition {
  transitioned: boolean;
  resolved: boolean;
}

export interface QuestionCallbackResult {
  handled: boolean;
  accepted: boolean;
  error?: string;
  resumePrompt?: string;
  /** Durable conversation kind for a callback-resumed turn. */
  isGroup?: boolean;
}

function effectiveRecordIsGroup(record: PendingQuestionRecord): boolean | undefined {
  if (typeof record.isGroup === 'boolean') return record.isGroup;
  // Legacy Feishu records predate persisted chat_type. Card callbacks do not
  // carry chat_type, so fail in the mention-safe direction after restart: an
  // extra mention instruction in an old 1:1 is preferable to a bare group
  // reply that the inbound mention filter silently drops.
  return record.channelType === 'feishu' ? true : undefined;
}

function recordAddress(record: PendingQuestionRecord): ChannelAddress {
  return {
    channelType: record.channelType,
    chatId: record.chatId,
    isGroup: effectiveRecordIsGroup(record),
  };
}

export function validateAskQuestions(input: unknown): AskQuestion[] {
  const questions = (input as { questions?: unknown })?.questions;
  if (!Array.isArray(questions) || questions.length < 1 || questions.length > 4) {
    throw new Error('AskUserQuestion requires 1-4 questions');
  }

  return questions.map((raw, index) => {
    const question = raw as Partial<AskQuestion>;
    if (
      typeof question.question !== 'string'
      || !question.question.trim()
      || typeof question.header !== 'string'
      || !question.header.trim()
      || !Array.isArray(question.options)
      || question.options.length < 2
      || question.options.length > 4
      || typeof question.multiSelect !== 'boolean'
    ) {
      throw new Error(`Invalid AskUserQuestion question at index ${index}`);
    }
    const options = question.options.map((rawOption) => {
      const option = rawOption as { label?: unknown; description?: unknown };
      if (typeof option.label !== 'string' || !option.label.trim() || typeof option.description !== 'string') {
        throw new Error(`Invalid AskUserQuestion option at question ${index}`);
      }
      return { label: option.label, description: option.description };
    });
    return {
      question: question.question,
      header: question.header,
      options,
      multiSelect: question.multiSelect,
    };
  });
}

function values(value: unknown): string[] {
  if (typeof value === 'string') return value ? [value] : [];
  if (Array.isArray(value)) {
    return value.filter((entry): entry is string => typeof entry === 'string' && entry.length > 0);
  }
  return [];
}

export function parseQuestionAnswers(
  questions: AskQuestion[],
  formValue: Record<string, unknown>,
): Record<string, string> | null {
  const answers: Record<string, string> = {};

  for (let index = 0; index < questions.length; index += 1) {
    const question = questions[index];
    const selected = values(formValue[`q_${index}`]);
    if (selected.length === 0 || (!question.multiSelect && selected.length !== 1)) return null;

    const allowed = new Set(question.options.map((option) => option.label));
    const answerParts: string[] = [];
    for (const selection of selected) {
      if (selection === 'Other') {
        const rawOther = formValue[`other_${index}`];
        const other = typeof rawOther === 'string'
          ? rawOther.trim()
          : '';
        if (!other) return null;
        answerParts.push(other);
      } else if (allowed.has(selection)) {
        answerParts.push(selection);
      } else {
        return null;
      }
    }
    answers[question.question] = answerParts.join(', ');
  }

  return answers;
}

function fallbackText(questions: AskQuestion[], requireMention = false): string {
  const lines = [requireMention
    ? 'Question card unavailable or expired. Mention the bot and reply in text to continue:'
    : 'Question card unavailable or expired. Reply in text to continue:'];
  questions.forEach((question, index) => {
    lines.push('', `${index + 1}. ${question.question}`);
    lines.push(`Options: ${question.options.map((option) => option.label).join(', ')}, or any other text`);
    if (question.multiSelect) lines.push('Multiple selections may be comma-separated.');
  });
  if (questions.length > 1) {
    lines.push('', 'Reply as a JSON object mapping each exact question to its answer.');
  }
  return lines.join('\n');
}

function expiryText(): string {
  return 'Question expired. Send the request again to continue.';
}

function requiresMention(address: ChannelAddress): boolean {
  if (address.channelType !== 'feishu' || address.isGroup !== true) return false;
  return getBridgeContext().store.getSetting('bridge_feishu_require_mention') !== 'false';
}

function resumePrompt(record: PendingQuestionRecord, answers: Record<string, string>): string {
  const lines = ['Answers to the pending AskUserQuestion request:'];
  for (const question of record.questions) {
    lines.push(`- ${question.question}: ${answers[question.question]}`);
  }
  lines.push('Continue the original task using these answers.');
  return lines.join('\n');
}

function redactQuestions(questions: AskQuestion[], secrets: readonly string[]): AskQuestion[] {
  return questions.map((question) => ({
    ...question,
    question: redactLiterals(question.question, secrets),
    header: redactLiterals(question.header, secrets),
    options: question.options.map((option) => ({
      label: redactLiterals(option.label, secrets),
      description: redactLiterals(option.description, secrets),
    })),
  }));
}

export class QuestionBroker {
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly actionTimeoutMs: number | undefined;
  private readonly expiryMs: number;
  private readonly generate: () => string;

  constructor(options: {
    actionTimeoutMs?: number;
    expiryMs?: number;
    generation?: () => string;
  } = {}) {
    this.actionTimeoutMs = options.actionTimeoutMs;
    this.expiryMs = options.expiryMs ?? DEFAULT_EXPIRY_MS;
    this.generate = options.generation ?? (() => crypto.randomUUID());
  }

  async forwardQuestionRequest(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
    questionRequestId: string,
    questionsInput: unknown,
    sessionId: string,
    replyToMessageId?: string,
  ): Promise<void> {
    const questions = validateAskQuestions({ questions: questionsInput });
    const now = Date.now();
    const record: PendingQuestionRecord = {
      questionRequestId,
      channelType: adapter.channelType,
      chatId: address.chatId,
      isGroup: address.isGroup,
      sessionId,
      questions,
      answers: {},
      state: 'pending-send',
      generation: this.generate(),
      replyToMessageId,
      createdAt: new Date(now).toISOString(),
      expiresAt: new Date(now + this.expiryMs).toISOString(),
    };
    const { store } = getBridgeContext();
    if (
      !store.savePendingQuestion
      || !store.getPendingQuestion
      || !store.listPendingQuestions
      || !store.transitionPendingQuestion
    ) {
      throw new Error(
        'Host pending-question persistence requires savePendingQuestion, getPendingQuestion, listPendingQuestions, and transitionPendingQuestion',
      );
    }
    store.savePendingQuestion(record);
    await this.sendCardOrFallback(adapter, address, record);
  }

  async restorePendingQuestions(adapter: BaseChannelAdapter): Promise<void> {
    const { store } = getBridgeContext();
    if (!store.listPendingQuestions || !store.transitionPendingQuestion || !store.getPendingQuestion) return;

    const byChat = new Map<string, PendingQuestionRecord[]>();
    for (const record of store.listPendingQuestions()) {
      if (record.channelType !== adapter.channelType) continue;
      if (!ACTIVE_QUESTION_STATES.includes(record.state)) continue;
      const records = byChat.get(record.chatId) ?? [];
      records.push(record);
      byChat.set(record.chatId, records);
    }

    for (const records of byChat.values()) {
      sortQuestionsByAge(records, 'restart consolidation');
      const active = records.filter((record) => Date.parse(record.expiresAt) > Date.now());
      const stale = records.filter((record) => Date.parse(record.expiresAt) <= Date.now());
      let expiredTransitions = 0;
      for (const record of stale) {
        const outcome = await this.expire(adapter, recordAddress(record), record.questionRequestId, false);
        if (outcome.transitioned) expiredTransitions += 1;
      }

      if (active.length === 0) {
        if (expiredTransitions > 0) {
          const newest = stale.at(-1)!;
          await this.sendExpiryNotice(adapter, recordAddress(newest), newest);
        }
        continue;
      }

      const newest = active.at(-1)!;
      let consolidated = 0;
      for (const older of active.slice(0, -1)) {
        const outcome = await this.expire(adapter, recordAddress(older), older.questionRequestId, false);
        if (outcome.transitioned) consolidated += 1;
      }

      try {
        await this.restoreRecord(adapter, newest);
        const restored = store.getPendingQuestion(newest.questionRequestId);
        if (consolidated > 0 && restored && ACTIVE_QUESTION_STATES.includes(restored.state)) {
          await this.sendConsolidationNotice(adapter, recordAddress(restored), restored);
        }
      } catch (error) {
        console.warn('[question-broker] Failed to restore pending question:', error instanceof Error ? error.message : error);
        const current = store.getPendingQuestion(newest.questionRequestId);
        if (current && ['pending-send', 'sent', 'fallback-pending'].includes(current.state)) {
          await this.expire(adapter, recordAddress(current), current.questionRequestId, false);
        }
      }
    }
  }

  handleQuestionCallback(
    callbackData: string,
    callbackChatId: string,
    callbackMessageId: string | undefined,
    formValue: Record<string, unknown>,
  ): QuestionCallbackResult {
    const match = /^ask:submit:([^:]+):([^:]+)$/.exec(callbackData);
    if (!match) return { handled: false, accepted: false };
    const [, questionRequestId, generation] = match;
    const { store, permissions } = getBridgeContext();
    const record = store.getPendingQuestion?.(questionRequestId);
    if (
      !record
      || record.state !== 'sent'
      || record.chatId !== callbackChatId
      || record.generation !== generation
      || !callbackMessageId
      || record.messageId !== callbackMessageId
    ) {
      return { handled: true, accepted: false, error: 'Question is no longer active.' };
    }
    if (Date.parse(record.expiresAt) <= Date.now()) {
      const transitioned = store.transitionPendingQuestion?.(questionRequestId, ['sent'], { state: 'expired' }) ?? false;
      this.clearTimer(questionRequestId);
      permissions.resolvePendingQuestion?.(questionRequestId, {
        behavior: 'deny',
        message: transitioned
          ? 'Interactive question expired'
          : 'Interactive question expiry state changed',
      });
      return { handled: true, accepted: false, error: 'Question has expired.' };
    }

    const answers = parseQuestionAnswers(record.questions, formValue);
    if (!answers) {
      return { handled: true, accepted: false, error: 'Please answer every question with valid selections.' };
    }

    const claimed = store.transitionPendingQuestion?.(questionRequestId, ['sent'], {
      state: 'answered',
      answers,
    }) ?? false;
    if (!claimed) return { handled: true, accepted: false, error: 'Question was already answered.' };
    this.clearTimer(questionRequestId);

    const resolvedLive = permissions.resolvePendingQuestion?.(questionRequestId, {
      behavior: 'allow',
      updatedInput: { questions: record.questions, answers },
    }) ?? false;
    return {
      handled: true,
      accepted: true,
      isGroup: effectiveRecordIsGroup(record),
      ...(resolvedLive ? {} : { resumePrompt: resumePrompt(record, answers) }),
    };
  }

  handleFallbackAnswer(address: ChannelAddress, text: string): QuestionCallbackResult {
    const { store } = getBridgeContext();
    const candidates = store.listPendingQuestions?.().filter(
      (record) => record.channelType === address.channelType
        && record.chatId === address.chatId
        && record.state === 'fallback-pending',
    ) ?? [];
    const active = candidates.filter((record) => {
      if (Date.parse(record.expiresAt) > Date.now()) return true;
      store.transitionPendingQuestion?.(record.questionRequestId, ['fallback-pending'], {
        state: 'expired',
      });
      return false;
    });
    if (active.length === 0) return { handled: false, accepted: false };
    sortQuestionsByAge(active, 'fallback answer');
    const record = active.at(-1)!;
    if (active.length > 1) {
      const { permissions } = getBridgeContext();
      for (const older of active.slice(0, -1)) {
        const transitioned = store.transitionPendingQuestion?.(older.questionRequestId, ['fallback-pending'], {
          state: 'expired',
        }) ?? false;
        this.clearTimer(older.questionRequestId);
        permissions.resolvePendingQuestion?.(older.questionRequestId, {
          behavior: 'deny',
          message: transitioned
            ? 'Interactive question superseded by a newer text fallback'
            : 'Interactive question fallback state changed',
        });
      }
      return {
        handled: true,
        accepted: false,
        error: 'Multiple pending questions were consolidated. Read the latest prompt and reply again.',
      };
    }
    let answers: Record<string, string> | null = null;
    if (record.questions.length === 1) {
      const answer = text.trim();
      if (answer) answers = { [record.questions[0].question]: answer };
    } else {
      try {
        const parsed = JSON.parse(text) as Record<string, unknown>;
        if (record.questions.every((question) => {
          const value = parsed[question.question];
          return typeof value === 'string' && Boolean(value.trim());
        })) {
          answers = Object.fromEntries(record.questions.map((question) => [question.question, String(parsed[question.question]).trim()]));
        }
      } catch { /* invalid fallback answer; keep pending */ }
    }
    if (!answers) return { handled: true, accepted: false, error: 'Answer format is incomplete.' };
    const claimed = store.transitionPendingQuestion?.(record.questionRequestId, ['fallback-pending'], {
      state: 'answered',
      answers,
    }) ?? false;
    if (!claimed) return { handled: true, accepted: false, error: 'Question was already answered.' };
    return {
      handled: true,
      accepted: true,
      resumePrompt: resumePrompt(record, answers),
      isGroup: effectiveRecordIsGroup(record),
    };
  }

  dispose(): void {
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
  }

  /** Stop a chat safely by terminating every pending question and releasing every live wait. */
  async closePendingQuestionsForChat(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
  ): Promise<{ found: number; transitioned: number; released: number; expired: number; remaining: number }> {
    const { store } = getBridgeContext();
    const candidates = store.listPendingQuestions?.().filter(
      (record) => record.channelType === adapter.channelType
        && record.chatId === address.chatId
        && ACTIVE_QUESTION_STATES.includes(record.state),
    ) ?? [];
    if (candidates.length === 0) {
      return { found: 0, transitioned: 0, released: 0, expired: 0, remaining: 0 };
    }
    sortQuestionsByAge(candidates, '/stop');
    let transitioned = 0;
    let released = 0;
    let expired = 0;
    for (const record of candidates) {
      const outcome = await this.expire(adapter, address, record.questionRequestId, false);
      if (outcome.transitioned) {
        transitioned += 1;
        expired += 1;
      }
      if (outcome.resolved) released += 1;
    }
    const remaining = candidates.filter((candidate) => {
      const current = store.getPendingQuestion?.(candidate.questionRequestId);
      return current ? ACTIVE_QUESTION_STATES.includes(current.state) : false;
    }).length;
    return {
      found: candidates.length,
      transitioned,
      released,
      expired,
      remaining,
    };
  }

  async failQuestionDelivery(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
    questionRequestId: string,
    replyToMessageId?: string,
    sessionId?: string,
  ): Promise<void> {
    const { store, permissions } = getBridgeContext();
    if (!store.getPendingQuestion?.(questionRequestId)) {
      try {
        const result = await deliver(adapter, {
          address,
          text: 'Could not deliver the question. Send the request again to continue.',
          parseMode: 'plain',
          replyToMessageId,
        }, { sessionId });
        if (!result.ok) {
          console.warn(`[question-broker] Question failure notice delivery failed: ${result.error || 'unknown error'}`);
        }
      } catch (error) {
        console.warn('[question-broker] Question failure notice delivery failed:', error instanceof Error ? error.message : error);
      } finally {
        permissions.resolvePendingQuestion?.(questionRequestId, {
          behavior: 'deny',
          message: 'Interactive question could not be delivered',
        });
      }
      return;
    }
    await this.expire(
      adapter,
      address,
      questionRequestId,
      true,
      'Could not deliver the question. Send the request again to continue.',
      'Interactive question could not be delivered',
    );
  }

  private async restoreRecord(adapter: BaseChannelAdapter, record: PendingQuestionRecord): Promise<void> {
    const { store, permissions } = getBridgeContext();
    const address = recordAddress(record);
    if (record.state === 'fallback-pending') {
      if ((record.fallbackReissueCount ?? 0) >= 1) {
        await this.expire(adapter, address, record.questionRequestId);
        return;
      }
      const claimed = store.transitionPendingQuestion?.(record.questionRequestId, ['fallback-pending'], {
        fallbackReissueCount: (record.fallbackReissueCount ?? 0) + 1,
      }) ?? false;
      if (!claimed) {
        permissions.resolvePendingQuestion?.(record.questionRequestId, {
          behavior: 'deny',
          message: 'Interactive question restore state changed',
        });
        return;
      }
      const result = await deliver(adapter, {
        address,
        text: fallbackText(record.questions, requiresMention(address)),
        parseMode: 'plain',
        replyToMessageId: record.replyToMessageId,
      }, { sessionId: record.sessionId });
      if (!result.ok) {
        console.warn(`[question-broker] Fallback repost delivery failed: ${result.error || 'unknown error'}`);
        const transitioned = store.transitionPendingQuestion?.(
          record.questionRequestId,
          ['fallback-pending'],
          { state: 'expired' },
        ) ?? false;
        permissions.resolvePendingQuestion?.(record.questionRequestId, {
          behavior: 'deny',
          message: transitioned
            ? 'Interactive question fallback delivery failed'
            : 'Interactive question fallback delivery state changed',
        });
      }
      return;
    }
    if ((record.reissueCount ?? 0) >= 1) {
      await this.fallback(adapter, address, record.questionRequestId, [record.state]);
      return;
    }
    const nextGeneration = this.generate();
    const transitioned = store.transitionPendingQuestion?.(record.questionRequestId, [record.state], {
      state: 'pending-send',
      generation: nextGeneration,
      reissueCount: (record.reissueCount ?? 0) + 1,
      messageId: undefined,
    }) ?? false;
    if (!transitioned) {
      permissions.resolvePendingQuestion?.(record.questionRequestId, {
        behavior: 'deny',
        message: 'Interactive question restore state changed',
      });
      return;
    }
    const next = store.getPendingQuestion?.(record.questionRequestId);
    if (next) {
      await this.sendCardOrFallback(adapter, address, next);
    } else {
      permissions.resolvePendingQuestion?.(record.questionRequestId, {
        behavior: 'deny',
        message: 'Interactive question restore record unavailable',
      });
    }
  }

  private async sendCardOrFallback(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
    record: PendingQuestionRecord,
  ): Promise<void> {
    const { store } = getBridgeContext();
    if (!adapter.supportsQuestionCards) {
      await this.fallback(adapter, address, record.questionRequestId, ['pending-send']);
      return;
    }
    const secrets = knownOutboundSecrets(store);
    const message: OutboundMessage = {
      address,
      text: redactLiterals(fallbackText(record.questions, requiresMention(address)), secrets),
      parseMode: 'plain',
      questionCard: {
        questionRequestId: record.questionRequestId,
        generation: record.generation,
        questions: redactQuestions(record.questions, secrets),
      },
      replyToMessageId: record.replyToMessageId,
    };
    const result = await deliver(adapter, message, { sessionId: record.sessionId });
    if (!result.ok || !result.messageId) {
      await this.fallback(adapter, address, record.questionRequestId, ['pending-send']);
      return;
    }
    const transitioned = store.transitionPendingQuestion?.(record.questionRequestId, ['pending-send'], {
      state: 'sent',
      messageId: result.messageId,
    }) ?? false;
    if (transitioned) {
      this.scheduleTimeout(adapter, address, record.questionRequestId);
    } else {
      getBridgeContext().permissions.resolvePendingQuestion?.(record.questionRequestId, {
        behavior: 'deny',
        message: 'Interactive question delivery state changed',
      });
    }
  }

  private scheduleTimeout(adapter: BaseChannelAdapter, address: ChannelAddress, questionRequestId: string): void {
    this.clearTimer(questionRequestId);
    const { store } = getBridgeContext();
    const record = store.getPendingQuestion?.(questionRequestId);
    if (!record) {
      getBridgeContext().permissions.resolvePendingQuestion?.(questionRequestId, {
        behavior: 'deny',
        message: 'Interactive question timeout record unavailable',
      });
      return;
    }
    const remainingMs = Math.max(0, Date.parse(record.expiresAt) - Date.now());
    const configuredSeconds = Number(store.getSetting(QUESTION_WAIT_SETTING));
    const configuredWaitMs = this.actionTimeoutMs ?? (
      Number.isInteger(configuredSeconds) && configuredSeconds > 0
        ? configuredSeconds * 1000
        : undefined
    );
    const expiresBeforeFallback = configuredWaitMs === undefined || configuredWaitMs >= remainingMs;
    const waitMs = expiresBeforeFallback ? remainingMs : configuredWaitMs;
    const timer = setTimeout(() => {
      if (expiresBeforeFallback) {
        this.expire(adapter, address, questionRequestId).catch((error) => {
          console.error('[question-broker] Failed to expire question:', error);
        });
        return;
      }
      this.fallback(adapter, address, questionRequestId, ['sent']).catch((error) => {
        console.error('[question-broker] Failed to enter fallback after timeout:', error);
      });
    }, waitMs);
    timer.unref?.();
    this.timers.set(questionRequestId, timer);
  }

  private clearTimer(questionRequestId: string): void {
    const timer = this.timers.get(questionRequestId);
    if (timer) clearTimeout(timer);
    this.timers.delete(questionRequestId);
  }

  private async expire(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
    questionRequestId: string,
    notify = true,
    noticeText = expiryText(),
    resolutionMessage = 'Interactive question expired',
  ): Promise<LifecycleTransition> {
    const { store, permissions } = getBridgeContext();
    const record = store.getPendingQuestion?.(questionRequestId);
    if (!record) {
      const resolved = permissions.resolvePendingQuestion?.(questionRequestId, {
        behavior: 'deny',
        message: resolutionMessage,
      }) ?? false;
      return { transitioned: false, resolved };
    }
    if (!ACTIVE_QUESTION_STATES.includes(record.state)) {
      const resolved = permissions.resolvePendingQuestion?.(questionRequestId, {
        behavior: 'deny',
        message: resolutionMessage,
      }) ?? false;
      return { transitioned: false, resolved };
    }
    const transitioned = store.transitionPendingQuestion?.(questionRequestId, [record.state], {
      state: 'expired',
    }) ?? false;
    if (!transitioned) {
      const resolved = permissions.resolvePendingQuestion?.(questionRequestId, {
        behavior: 'deny',
        message: 'Interactive question expiry state changed',
      }) ?? false;
      return { transitioned: false, resolved };
    }
    this.clearTimer(questionRequestId);
    let resolved = false;
    try {
      if (notify) {
        await this.sendExpiryNotice(adapter, address, record, noticeText);
      }
    } finally {
      resolved = permissions.resolvePendingQuestion?.(questionRequestId, {
        behavior: 'deny',
        message: resolutionMessage,
      }) ?? false;
    }
    return { transitioned: true, resolved };
  }

  private async fallback(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
    questionRequestId: string,
    expectedStates: PendingQuestionState[],
  ): Promise<LifecycleTransition> {
    const { store, permissions } = getBridgeContext();
    const record = store.getPendingQuestion?.(questionRequestId);
    if (!record) {
      const resolved = permissions.resolvePendingQuestion?.(questionRequestId, {
        behavior: 'deny',
        message: 'Interactive question fallback record unavailable',
      }) ?? false;
      return { transitioned: false, resolved };
    }
    const transitioned = store.transitionPendingQuestion?.(questionRequestId, expectedStates, {
      state: 'fallback-pending',
    }) ?? false;
    if (!transitioned) {
      const resolved = permissions.resolvePendingQuestion?.(questionRequestId, {
        behavior: 'deny',
        message: 'Interactive question fallback state changed',
      }) ?? false;
      return { transitioned: false, resolved };
    }
    this.clearTimer(questionRequestId);
    const consolidated = await this.expireCompetingFallbacks(adapter, address, questionRequestId);
    let delivered = false;
    let resolved = false;
    try {
      const result = await deliver(adapter, {
        address,
        text: fallbackText(record.questions, requiresMention(address)),
        parseMode: 'plain',
        replyToMessageId: record.replyToMessageId,
      }, { sessionId: record.sessionId });
      delivered = result.ok;
      if (delivered && consolidated > 0) {
        await this.sendConsolidationNotice(adapter, address, record);
      }
      if (!result.ok) {
        console.warn(`[question-broker] Fallback prompt delivery failed: ${result.error || 'unknown error'}`);
        store.transitionPendingQuestion?.(questionRequestId, ['fallback-pending'], { state: 'expired' });
      }
    } catch (error) {
      console.warn('[question-broker] Fallback prompt delivery failed:', error instanceof Error ? error.message : error);
      store.transitionPendingQuestion?.(questionRequestId, ['fallback-pending'], { state: 'expired' });
    } finally {
      resolved = permissions.resolvePendingQuestion?.(questionRequestId, {
        behavior: 'deny',
        message: delivered
          ? 'Interactive question moved to text fallback'
          : 'Interactive question fallback delivery failed',
      }) ?? false;
    }
    return { transitioned: true, resolved };
  }

  private async expireCompetingFallbacks(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
    keepQuestionRequestId: string,
  ): Promise<number> {
    const { store } = getBridgeContext();
    const competing = store.listPendingQuestions?.().filter(
      (record) => record.channelType === adapter.channelType
        && record.chatId === address.chatId
        && record.questionRequestId !== keepQuestionRequestId
        && record.state === 'fallback-pending',
    ) ?? [];
    let consolidated = 0;
    for (const record of competing) {
      const outcome = await this.expire(adapter, address, record.questionRequestId, false);
      if (outcome.transitioned) consolidated += 1;
    }
    return consolidated;
  }

  private async sendConsolidationNotice(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
    record: PendingQuestionRecord,
  ): Promise<void> {
    try {
      const result = await deliver(adapter, {
        address,
        text: 'An earlier pending question was closed. Use only the latest question prompt.',
        parseMode: 'plain',
        replyToMessageId: record.replyToMessageId,
      }, { sessionId: record.sessionId });
      if (!result.ok) {
        console.warn(`[question-broker] Consolidation notice delivery failed: ${result.error || 'unknown error'}`);
      }
    } catch (error) {
      console.warn('[question-broker] Consolidation notice delivery failed:', error instanceof Error ? error.message : error);
    }
  }

  private async sendExpiryNotice(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
    record: PendingQuestionRecord,
    text = expiryText(),
  ): Promise<void> {
    try {
      const result = await deliver(adapter, {
        address,
        text,
        parseMode: 'plain',
        replyToMessageId: record.replyToMessageId,
      }, { sessionId: record.sessionId });
      if (!result.ok) {
        console.warn(`[question-broker] Expiry notice delivery failed: ${result.error || 'unknown error'}`);
      }
    } catch (error) {
      console.warn('[question-broker] Expiry notice delivery failed:', error instanceof Error ? error.message : error);
    }
  }
}

function sortQuestionsByAge(records: PendingQuestionRecord[], context: string): void {
  const timestamps = new Map<string, number>();
  for (const record of records) {
    const parsed = Date.parse(record.createdAt);
    if (Number.isNaN(parsed)) {
      // Deliberately fail closed: an unverifiable record must not displace a
      // valid, visibly ordered prompt and consume the user's next message.
      console.warn(`[question-broker] Invalid createdAt during ${context}; treating ${record.questionRequestId} as oldest`);
      timestamps.set(record.questionRequestId, Number.NEGATIVE_INFINITY);
    } else {
      timestamps.set(record.questionRequestId, parsed);
    }
  }
  records.sort((left, right) => {
    const createdDelta = timestamps.get(left.questionRequestId)! - timestamps.get(right.questionRequestId)!;
    return createdDelta || left.questionRequestId.localeCompare(right.questionRequestId);
  });
}

export const questionBroker = new QuestionBroker();

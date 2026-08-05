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

const DEFAULT_ACTION_TIMEOUT_MS = 5 * 60_000;
const DEFAULT_EXPIRY_MS = 24 * 60 * 60_000;

export interface QuestionCallbackResult {
  handled: boolean;
  accepted: boolean;
  error?: string;
  resumePrompt?: string;
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

function fallbackText(questions: AskQuestion[]): string {
  const lines = ['Question card unavailable or expired. Reply in text to continue:'];
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
  private readonly actionTimeoutMs: number;
  private readonly expiryMs: number;
  private readonly generate: () => string;

  constructor(options: {
    actionTimeoutMs?: number;
    expiryMs?: number;
    generation?: () => string;
  } = {}) {
    this.actionTimeoutMs = options.actionTimeoutMs ?? DEFAULT_ACTION_TIMEOUT_MS;
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
    if (!store.savePendingQuestion || !store.transitionPendingQuestion) {
      throw new Error('Host does not implement pending-question persistence');
    }
    store.savePendingQuestion(record);
    await this.sendCardOrFallback(adapter, address, record);
  }

  async restorePendingQuestions(adapter: BaseChannelAdapter): Promise<void> {
    const { store } = getBridgeContext();
    if (!store.listPendingQuestions || !store.transitionPendingQuestion) return;

    for (const record of store.listPendingQuestions()) {
      if (record.channelType !== adapter.channelType) continue;
      if (!['pending-send', 'sent', 'fallback-pending'].includes(record.state)) continue;
      if (Date.parse(record.expiresAt) <= Date.now()) {
        store.transitionPendingQuestion(record.questionRequestId, [record.state], { state: 'expired' });
        continue;
      }
      const address: ChannelAddress = { channelType: record.channelType, chatId: record.chatId };
      if (record.state === 'fallback-pending') {
        await deliver(adapter, {
          address,
          text: fallbackText(record.questions),
          parseMode: 'plain',
          replyToMessageId: record.replyToMessageId,
        }, { sessionId: record.sessionId });
        continue;
      }
      const nextGeneration = this.generate();
      if (!store.transitionPendingQuestion(record.questionRequestId, [record.state], {
        state: 'pending-send',
        generation: nextGeneration,
        messageId: undefined,
      })) continue;
      const next = store.getPendingQuestion?.(record.questionRequestId);
      if (next) await this.sendCardOrFallback(adapter, address, next);
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
      ...(resolvedLive ? {} : { resumePrompt: resumePrompt(record, answers) }),
    };
  }

  handleFallbackAnswer(chatId: string, text: string): QuestionCallbackResult {
    const { store } = getBridgeContext();
    const candidates = store.listPendingQuestions?.().filter(
      (record) => record.chatId === chatId && record.state === 'fallback-pending',
    ) ?? [];
    const active = candidates.filter((record) => {
      if (Date.parse(record.expiresAt) > Date.now()) return true;
      store.transitionPendingQuestion?.(record.questionRequestId, ['fallback-pending'], {
        state: 'expired',
      });
      return false;
    });
    if (active.length !== 1) return { handled: false, accepted: false };
    const record = active[0];
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
    return { handled: true, accepted: true, resumePrompt: resumePrompt(record, answers) };
  }

  dispose(): void {
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
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
      text: redactLiterals(fallbackText(record.questions), secrets),
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
    if (transitioned) this.scheduleTimeout(adapter, address, record.questionRequestId);
  }

  private scheduleTimeout(adapter: BaseChannelAdapter, address: ChannelAddress, questionRequestId: string): void {
    this.clearTimer(questionRequestId);
    const timer = setTimeout(() => {
      this.fallback(adapter, address, questionRequestId, ['sent']).catch((error) => {
        console.error('[question-broker] Failed to enter fallback after timeout:', error);
      });
    }, this.actionTimeoutMs);
    timer.unref?.();
    this.timers.set(questionRequestId, timer);
  }

  private clearTimer(questionRequestId: string): void {
    const timer = this.timers.get(questionRequestId);
    if (timer) clearTimeout(timer);
    this.timers.delete(questionRequestId);
  }

  private async fallback(
    adapter: BaseChannelAdapter,
    address: ChannelAddress,
    questionRequestId: string,
    expectedStates: PendingQuestionState[],
  ): Promise<void> {
    const { store, permissions } = getBridgeContext();
    const record = store.getPendingQuestion?.(questionRequestId);
    if (!record) return;
    const transitioned = store.transitionPendingQuestion?.(questionRequestId, expectedStates, {
      state: 'fallback-pending',
    }) ?? false;
    if (!transitioned) return;
    this.clearTimer(questionRequestId);
    permissions.resolvePendingQuestion?.(questionRequestId, {
      behavior: 'deny',
      message: 'Interactive question moved to text fallback',
    });
    await deliver(adapter, {
      address,
      text: fallbackText(record.questions),
      parseMode: 'plain',
      replyToMessageId: record.replyToMessageId,
    }, { sessionId: record.sessionId });
  }
}

export const questionBroker = new QuestionBroker();

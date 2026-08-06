import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { FeishuAdapter } from '../../lib/bridge/adapters/feishu-adapter.js';

describe('Feishu CardKit streaming cards', () => {
  it('uses the SDK v1 CardKit surface for create, stream, close, and final update', async () => {
    const calls: Array<{ method: string; payload: any }> = [];
    const record = (method: string, result: unknown = { code: 0, data: {} }) =>
      async (payload: unknown) => {
        calls.push({ method, payload });
        return result;
      };
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      cardkit: {
        v1: {
          card: {
            create: record('card.create', { code: 0, data: { card_id: 'card-1' } }),
            settings: record('card.settings'),
            update: record('card.update'),
          },
          cardElement: {
            content: record('cardElement.content'),
          },
        },
      },
      im: {
        message: {
          reply: record('message.reply', { code: 0, data: { message_id: 'message-1' } }),
        },
      },
    };

    assert.equal(await adapter._doCreateStreamingCard('chat-1', 'incoming-1'), true);
    adapter.updateCardContent('chat-1', 'streamed text');
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(await adapter.finalizeCard('chat-1', 'completed', 'final text'), true);

    assert.deepEqual(calls.map((call) => call.method), [
      'card.create',
      'message.reply',
      'cardElement.content',
      'card.settings',
      'card.update',
    ]);
    assert.deepEqual(calls[2].payload.path, {
      card_id: 'card-1',
      element_id: 'streaming_content',
    });
    assert.equal(calls[2].payload.data.content.includes('streamed text'), true);
    assert.deepEqual(JSON.parse(calls[3].payload.data.settings), { streaming_mode: false });
    assert.deepEqual(calls[4].payload.data.card.type, 'card_json');
    assert.equal(typeof calls[4].payload.data.card.data, 'string');
  });

  it('logs one cause and suppresses repeated card attempts after creation fails', async () => {
    let createCalls = 0;
    const warnings: string[] = [];
    const originalWarn = console.warn;
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      cardkit: { v1: { card: { create: async () => {
        createCalls += 1;
        throw new Error('card create canary');
      } } } },
    };
    console.warn = (...args: unknown[]) => warnings.push(args.map(String).join(' '));

    try {
      assert.equal(await adapter._doCreateStreamingCard('chat-failure', 'incoming-failure'), false);
      adapter.onStreamText('chat-failure', 'one');
      adapter.onStreamText('chat-failure', 'two');
      adapter.onStreamText('chat-failure', 'three');
      await new Promise((resolve) => setImmediate(resolve));
    } finally {
      console.warn = originalWarn;
    }

    assert.equal(createCalls, 1);
    assert.equal(warnings.length, 1);
    assert.match(warnings[0], /plain-message fallback: card create canary/);
  });

  it('recovers from one streaming update failure and still finalizes the card', async () => {
    const calls: string[] = [];
    const warnings: string[] = [];
    const originalWarn = console.warn;
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      cardkit: { v1: {
        card: {
          create: async () => ({ data: { card_id: 'card-transient' } }),
          settings: async () => { calls.push('settings'); return { code: 0 }; },
          update: async () => { calls.push('update'); return { code: 0 }; },
        },
        cardElement: { content: async () => {
          calls.push('content');
          throw new Error('transient content failure');
        } },
      } },
      im: { message: { reply: async () => ({ data: { message_id: 'message-transient' } }) } },
    };
    console.warn = (...args: unknown[]) => warnings.push(args.map(String).join(' '));
    try {
      assert.equal(await adapter._doCreateStreamingCard('chat-transient', 'incoming'), true);
      adapter.updateCardContent('chat-transient', 'partial text');
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(adapter.hasActiveCard('chat-transient'), true);
      assert.equal(await adapter.finalizeCard('chat-transient', 'completed', 'final text'), true);
    } finally {
      console.warn = originalWarn;
    }
    assert.deepEqual(calls, ['content', 'settings', 'update']);
    assert.equal(warnings.filter((line) => line.includes('transient content failure')).length, 1);
  });

  it('ignores a late failed update from an old turn instead of deleting the next card', async () => {
    let rejectOld!: (error: Error) => void;
    const oldUpdate = new Promise<unknown>((_resolve, reject) => { rejectOld = reject; });
    let createCount = 0;
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      cardkit: { v1: {
        card: { create: async () => ({ data: { card_id: `card-${++createCount}` } }) },
        cardElement: { content: () => oldUpdate },
      } },
      im: { message: { reply: async () => ({ data: { message_id: `message-${createCount}` } }) } },
    };

    assert.equal(await adapter._doCreateStreamingCard('chat-reused', 'incoming-1'), true);
    adapter.updateCardContent('chat-reused', 'old turn');
    adapter.cleanupCard('chat-reused');
    assert.equal(await adapter._doCreateStreamingCard('chat-reused', 'incoming-2'), true);
    rejectOld(new Error('late old-turn failure'));
    await new Promise((resolve) => setImmediate(resolve));

    assert.equal(adapter.hasActiveCard('chat-reused'), true);
    assert.equal(adapter.activeCards.get('chat-reused').cardId, 'card-2');
  });

  it('requests a plain fallback when streaming mode could not be closed', async () => {
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      cardkit: { v1: {
        card: {
          create: async () => ({ data: { card_id: 'card-visible' } }),
          settings: async () => { throw new Error('finalize settings failure'); },
          update: async () => ({ code: 0 }),
        },
        cardElement: { content: async () => ({ code: 0 }) },
      } },
      im: { message: { reply: async () => ({ data: { message_id: 'message-visible' } }) } },
    };

    assert.equal(await adapter._doCreateStreamingCard('chat-visible', 'incoming'), true);
    adapter.updateCardContent('chat-visible', 'complete answer');
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(
      await adapter.finalizeCard('chat-visible', 'completed', 'complete answer'),
      false,
    );
  });

  it('reports visible content only after streaming mode closed and final formatting failed', async () => {
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      cardkit: { v1: {
        card: {
          create: async () => ({ data: { card_id: 'card-visible-closed' } }),
          settings: async () => ({ code: 0 }),
          update: async () => { throw new Error('final card formatting failure'); },
        },
        cardElement: { content: async () => ({ code: 0 }) },
      } },
      im: { message: { reply: async () => ({ data: { message_id: 'message-visible-closed' } }) } },
    };

    assert.equal(await adapter._doCreateStreamingCard('chat-visible-closed', 'incoming'), true);
    adapter.updateCardContent('chat-visible-closed', 'complete answer');
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(
      await adapter.finalizeCard('chat-visible-closed', 'completed', 'complete answer'),
      'content-visible',
    );
  });

});

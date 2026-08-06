import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { FeishuAdapter } from '../../lib/bridge/adapters/feishu-adapter.js';
import { initBridgeContext } from '../../lib/bridge/context.js';
import type { BridgeStore } from '../../lib/bridge/host.js';

function init(requireMention: boolean): void {
  delete (globalThis as Record<string, unknown>).__bridge_context__;
  initBridgeContext({
    store: {
      getSetting: (key: string) => key === 'bridge_feishu_require_mention' ? String(requireMention) : null,
    } as BridgeStore,
    llm: { streamChat: () => new ReadableStream() },
    permissions: { resolvePendingPermission: () => false },
    lifecycle: {},
  });
}

describe('Feishu permission cards', () => {
  it('uses the proven schema 2.0 markdown and column_set approval structure', async () => {
    init(true);
    let sentPayload: any;
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      im: {
        message: {
          create: async (payload: unknown) => {
            sentPayload = payload;
            return { code: 0, data: { message_id: 'permission-message-1' } };
          },
        },
      },
    };

    const result = await adapter.send({
      address: { channelType: 'feishu', chatId: 'chat-1', userId: 'user-1', isGroup: true },
      text: '<b>Permission Required</b>\n\nTool: <code>Bash</code>\n<pre>{\n  "command": "touch 2.txt"\n}</pre>\n\nChoose an action:',
      parseMode: 'HTML',
      inlineButtons: [[
        { text: 'Allow', callbackData: 'perm:allow:permission-1' },
        { text: 'Deny', callbackData: 'perm:deny:permission-1' },
      ]],
    });

    assert.equal(result.ok, true);
    const card = JSON.parse(sentPayload.data.content);
    assert.equal(card.schema, '2.0');
    assert.equal(card.header.icon.token, 'lock-chat_filled');
    assert.equal(card.header.padding, '12px 12px 12px 12px');
    assert.equal(card.elements, undefined);
    assert.equal(card.body.elements[0].tag, 'markdown');
    assert.match(card.body.elements[0].content, /Tool: `Bash`/);
    assert.equal(card.body.elements[0].content.includes('touch 2.txt'), true);
    const columns = card.body.elements.find((element: any) => element.tag === 'column_set').columns;
    assert.deepEqual(
      columns.map((column: any) => column.elements[0].value.callback_data),
      ['perm:allow:permission-1', 'perm:allow_session:permission-1', 'perm:deny:permission-1'],
    );
    assert.equal(card.body.elements.some((element: any) => element.tag === 'action'), false);
    assert.match(card.body.elements.at(-1).content, /mention the bot.*`1` Allow/i);
  });

  it('does not tell a 1:1 chat to mention the bot', async () => {
    init(true);
    let sentPayload: any;
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      im: { message: { create: async (payload: unknown) => {
        sentPayload = payload;
        return { code: 0, data: { message_id: 'permission-message-direct' } };
      } } },
    };

    const result = await adapter.send({
      address: { channelType: 'feishu', chatId: 'chat-direct', userId: 'user-1', isGroup: false },
      text: '<b>Permission Required</b>\n\nTool: <code>Bash</code>\n<pre>{"command":"pwd"}</pre>\n\nChoose an action:',
      parseMode: 'HTML',
      inlineButtons: [[{ text: 'Allow', callbackData: 'perm:allow:permission-direct' }]],
    });

    assert.equal(result.ok, true);
    const footer = JSON.parse(sentPayload.data.content).body.elements.at(-1).content as string;
    assert.match(footer, /^Or reply:/);
    assert.doesNotMatch(footer, /mention the bot/i);
  });

  it('keeps rendering aligned with the real inbound mention filter', async () => {
    init(true);
    let sentPayload: any;
    const adapter = new FeishuAdapter() as any;
    adapter.botIds.add('bot-open-id');
    adapter.restClient = {
      im: { message: { create: async (payload: unknown) => {
        sentPayload = payload;
        return { code: 0, data: { message_id: 'permission-message-policy' } };
      } } },
    };
    await adapter.processIncomingEvent({
      sender: { sender_type: 'user', sender_id: { open_id: 'user-1' } },
      message: {
        message_id: 'unmentioned-group-message',
        chat_id: 'chat-policy',
        chat_type: 'group',
        message_type: 'text',
        content: JSON.stringify({ text: '3' }),
        mentions: [],
      },
    });
    assert.equal(adapter.queue.length, 0, 'the real inbound filter must reject an unmentioned group reply');

    await adapter.send({
      address: { channelType: 'feishu', chatId: 'chat-policy', userId: 'user-1', isGroup: true },
      text: '<b>Permission Required</b>',
      parseMode: 'HTML',
      inlineButtons: [[{ text: 'Deny', callbackData: 'perm:deny:permission-policy' }]],
    });

    const footer = JSON.parse(sentPayload.data.content).body.elements.at(-1).content as string;
    assert.match(footer, /mention the bot/i);
  });

  it('keeps tool input from closing the approval markdown code fence', async () => {
    init(false);
    let sentPayload: any;
    const adapter = new FeishuAdapter() as any;
    adapter.restClient = {
      im: { message: { create: async (payload: unknown) => {
        sentPayload = payload;
        return { code: 0, data: { message_id: 'permission-message-2' } };
      } } },
    };

    const result = await adapter.send({
      address: { channelType: 'feishu', chatId: 'chat-2', userId: 'user-1' },
      text: '<b>Permission Required</b>\n\nTool: <code>Bash</code>\n<pre>{"command":"printf ``` **FORGED APPROVAL**"}</pre>\n\nChoose an action:',
      parseMode: 'HTML',
      inlineButtons: [[{ text: 'Allow', callbackData: 'perm:allow:permission-2' }]],
    });

    assert.equal(result.ok, true);
    const content = JSON.parse(sentPayload.data.content).body.elements[0].content as string;
    assert.match(content, /^\*\*Permission Required\*\*[\s\S]*````\n/);
    assert.match(content, /\{"command":"printf ``` \*\*FORGED APPROVAL\*\*"\}/);
    assert.match(content, /\n````\n\nChoose an action:$/);
    assert.doesNotMatch(content, /\u200B/);
    const footer = JSON.parse(sentPayload.data.content).body.elements.at(-1).content as string;
    assert.match(footer, /^Or reply:/);
  });
});

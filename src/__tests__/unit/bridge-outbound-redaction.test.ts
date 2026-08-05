import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { LiteralStreamRedactor, redactLiterals } from '../../lib/bridge/security/outbound-redaction';
import { initBridgeContext } from '../../lib/bridge/context';
import { consumeStream } from '../../lib/bridge/conversation-engine';
import type { BridgeStore } from '../../lib/bridge/host';

function sse(type: string, data: string): string {
  return `data: ${JSON.stringify({ type, data })}\n\n`;
}

describe('outbound secret literal redaction', () => {
  it('redacts every known literal from final replies, errors, and card text', () => {
    const secrets = ['feishu-secret-canary', 'telegram-token-canary'];
    for (const text of [
      'final feishu-secret-canary reply',
      'error: telegram-token-canary',
      '**card** feishu-secret-canary',
    ]) {
      const redacted = redactLiterals(text, secrets);
      assert.doesNotMatch(redacted, /(?:feishu-secret|telegram-token)-canary/);
      assert.match(redacted, /\[REDACTED\]/);
    }
  });

  it('holds a possible secret prefix and redacts a literal split across stream chunks', () => {
    const redactor = new LiteralStreamRedactor(['cross-boundary-secret']);
    assert.equal(redactor.push('safe cross-boundary-'), 'safe ');
    assert.equal(redactor.push('secret tail'), 'safe [REDACTED] tail');
    assert.equal(redactor.finish(), 'safe [REDACTED] tail');
  });

  it('preserves ordinary text that only briefly shares a secret prefix', () => {
    const redactor = new LiteralStreamRedactor(['secret-value']);
    assert.equal(redactor.push('ordinary sec'), 'ordinary ');
    assert.equal(redactor.push('tion'), 'ordinary section');
    assert.equal(redactor.finish(), 'ordinary section');
  });

  it('never exposes a cross-event secret prefix to streaming card callbacks', async () => {
    const saved: string[] = [];
    const store = {
      getSetting: (key: string) => key === 'bridge_feishu_app_secret' ? 'cross-boundary-secret' : null,
      addMessage: (_session: string, _role: string, content: string) => saved.push(content),
      updateSdkSessionId() {},
      updateSessionModel() {},
      syncSdkTasks() {},
    } as unknown as BridgeStore;
    initBridgeContext({
      store,
      llm: { streamChat: () => new ReadableStream() },
      permissions: { resolvePendingPermission: () => false },
      lifecycle: {},
    });
    const partials: string[] = [];
    const stream = new ReadableStream<string>({
      start(controller) {
        controller.enqueue(sse('text', 'safe cross-boundary-'));
        controller.enqueue(sse('text', 'secret tail'));
        controller.close();
      },
    });

    const result = await consumeStream(stream, 'session', undefined, (text) => partials.push(text));
    assert.deepEqual(partials, ['safe ', 'safe [REDACTED] tail']);
    assert.equal(result.responseText, 'safe [REDACTED] tail');
    assert.doesNotMatch(JSON.stringify(saved), /cross-boundary-secret/);
  });
});

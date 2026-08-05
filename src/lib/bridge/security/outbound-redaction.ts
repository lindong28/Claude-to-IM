import type { BridgeStore } from '../host.js';

const SECRET_SETTING_KEYS = [
  'telegram_bot_token',
  'bridge_discord_bot_token',
  'bridge_feishu_app_secret',
  'bridge_qq_app_secret',
] as const;

export function knownOutboundSecrets(store: Pick<BridgeStore, 'getSetting'>): string[] {
  return [...new Set(
    SECRET_SETTING_KEYS
      .map((key) => store.getSetting(key) || '')
      .filter(Boolean),
  )];
}

export function redactLiterals(text: string, secrets: readonly string[]): string {
  let redacted = text;
  for (const secret of secrets) {
    if (secret) redacted = redacted.split(secret).join('[REDACTED]');
  }
  return redacted;
}

/**
 * Stateful literal redactor for streamed text. A suffix that could still grow
 * into a configured secret is withheld until the next chunk disambiguates it.
 */
export class LiteralStreamRedactor {
  private pending = '';
  private output = '';
  private readonly secrets: string[];

  constructor(secrets: readonly string[]) {
    this.secrets = [...new Set(secrets.filter(Boolean))].sort((a, b) => b.length - a.length);
  }

  push(chunk: string): string {
    this.pending += chunk;
    this.drain(false);
    return this.output;
  }

  finish(): string {
    this.drain(true);
    return this.output;
  }

  private drain(final: boolean): void {
    let index = 0;
    while (index < this.pending.length) {
      const matched = this.secrets.find((secret) => this.pending.startsWith(secret, index));
      if (matched) {
        this.output += '[REDACTED]';
        index += matched.length;
        continue;
      }

      if (!final) {
        const suffix = this.pending.slice(index);
        if (this.secrets.some((secret) => secret.startsWith(suffix))) break;
      }

      this.output += this.pending[index];
      index += 1;
    }
    this.pending = this.pending.slice(index);
  }
}

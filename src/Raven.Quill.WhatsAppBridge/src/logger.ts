import { pino, type Logger } from "pino";

// Phone numbers are PII: log lines carry only the last four digits of a JID.
export function redactJid(jid: string): string {
    const user = jid.split("@")[0] ?? "";
    const digits = user.split(":")[0] ?? "";
    return digits.length <= 4 ? `...@${jid.split("@")[1] ?? ""}` : `...${digits.slice(-4)}`;
}

export function createLogger(level: string): Logger {
    return pino({ level, base: undefined });
}

export type { Logger };

import { pino, stdSerializers, type Logger } from "pino";

export function redactJid(jid: string): string {
    const user = jid.split("@")[0] ?? "";
    const digits = user.split(":")[0] ?? "";
    return digits.length <= 4 ? `...@${jid.split("@")[1] ?? ""}` : `...${digits.slice(-4)}`;
}

export function createLogger(level: string): Logger {
    return pino({
        level,
        base: undefined,
        serializers: {
            err: stdSerializers.err,
            error: stdSerializers.err,
            ackErr: stdSerializers.err,
        },
    });
}

export type { Logger };

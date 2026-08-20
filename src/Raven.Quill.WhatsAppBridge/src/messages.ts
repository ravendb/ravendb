export interface ClassifiedMessage {
    sender: string;
    messageId: string;
    kind: "text" | "unsupported";
    text: string | null;
    timestamp: number;
}

interface RawKey {
    remoteJid?: string | null;
    remoteJidAlt?: string | null;
    fromMe?: boolean | null;
    id?: string | null;
}

interface RawMessage {
    key?: RawKey | null;
    message?: Record<string, unknown> | null;
    messageTimestamp?: number | { toNumber(): number } | null;
}

const PHONE_JID_SUFFIX = "@s.whatsapp.net";
const LID_JID_SUFFIX = "@lid";

const WRAPPER_KEYS = ["ephemeralMessage", "viewOnceMessage", "viewOnceMessageV2", "documentWithCaptionMessage"];

// Envelope/protocol payloads that are not user content at all.
const IGNORED_KEYS = new Set([
    "protocolMessage",
    "reactionMessage",
    "pollUpdateMessage",
    "senderKeyDistributionMessage",
    "messageContextInfo",
]);

export function classifyMessage(raw: RawMessage): ClassifiedMessage | null {
    const key = raw.key;
    if (!key || key.fromMe)
        return null;

    const jid = key.remoteJid;
    if (!jid || (!jid.endsWith(PHONE_JID_SUFFIX) && !jid.endsWith(LID_JID_SUFFIX)))
        return null;

    const alt = key.remoteJidAlt;
    const sender = jid.endsWith(LID_JID_SUFFIX) && alt?.endsWith(PHONE_JID_SUFFIX) ? alt : jid;

    let content = raw.message;
    if (!content)
        return null;

    for (const wrapper of WRAPPER_KEYS) {
        const inner = content[wrapper] as { message?: Record<string, unknown> | null } | undefined;
        if (inner?.message)
            content = inner.message;
    }

    const contentKeys = Object.keys(content).filter((k) => content![k] != null);
    if (contentKeys.length === 0 || contentKeys.every((k) => IGNORED_KEYS.has(k)))
        return null;

    const extended = content.extendedTextMessage as { text?: string | null } | undefined;
    const text = (content.conversation as string | undefined) ?? extended?.text ?? null;

    const timestampRaw = raw.messageTimestamp;
    const timestamp =
        typeof timestampRaw === "number" ? timestampRaw : (timestampRaw?.toNumber() ?? 0);

    return {
        sender,
        messageId: key.id ?? "",
        kind: text && text.trim().length > 0 ? "text" : "unsupported",
        text: text && text.trim().length > 0 ? text : null,
        timestamp,
    };
}

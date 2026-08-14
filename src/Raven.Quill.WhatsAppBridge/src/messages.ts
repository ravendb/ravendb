// Pure classification of a raw Baileys message into what the Quill web app receives.
// Kept free of Baileys imports so tests drive it with plain objects.

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

// Wrappers that carry the real content one level deeper.
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
    // Direct chats only: groups (@g.us), broadcasts, newsletters and status updates are dropped.
    // LID-addressed chats are direct too; WhatsApp addresses them by @lid rather than by number.
    if (!jid || (!jid.endsWith(PHONE_JID_SUFFIX) && !jid.endsWith(LID_JID_SUFFIX)))
        return null;

    // Downstream identity (conversation id, PhoneNumber bindings) needs the phone-number JID,
    // so prefer the alternate key WhatsApp pairs with a @lid address. A @lid that arrives
    // without one is resolved by the caller, which owns the LID->PN mapping.
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

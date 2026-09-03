import { tryParseJson } from "@/utils";

/** The chat endpoint's NDJSON wire protocol. Fixed by the server - see `EmbedEndpoints.StreamEmbedChatAsync`. */
type ServerEvent =
    | { type: "chunk"; text?: string }
    | { type: "done"; answer?: { reply?: string } | null }
    | { type: "error"; message?: string };

/** Drives which message the UI shows; `expired` and `limit` are terminal for the session. */
export type ChatErrorKind = "expired" | "limit" | "failed";

export type ChatEvent =
    | { type: "chunk"; text: string }
    | { type: "done"; reply: string | null }
    | { type: "error"; kind: ChatErrorKind; message: string };

const LINK_INACTIVE_MESSAGE = "This conversation link is no longer active.";
const LIMIT_MESSAGE = "This conversation has reached its usage limit.";
const RATE_LIMITED_MESSAGE = "Too many requests right now. Please try again in a moment.";
const GENERIC_MESSAGE = "Something went wrong. Please try again.";

/** The server answers 429 from two places: a link whose invocation budget is spent (a JSON body carrying
 *  `code: "invocation_limit"`) and a per-IP throttle (an empty body). Only the first is terminal — a
 *  throttled visitor just has to wait, so anything else stays a retryable failure. */
async function carriesInvocationLimitCode(response: Response): Promise<boolean> {
    try {
        const body = (await response.json()) as { code?: unknown } | null;
        return body?.code === "invocation_limit";
    } catch {
        return false;
    }
}

async function statusFailure(response: Response): Promise<ChatEvent | null> {
    if (response.status === 404 || response.status === 410)
        return { type: "error", kind: "expired", message: LINK_INACTIVE_MESSAGE };
    if (response.status === 429)
        return (await carriesInvocationLimitCode(response))
            ? { type: "error", kind: "limit", message: LIMIT_MESSAGE }
            : { type: "error", kind: "failed", message: RATE_LIMITED_MESSAGE };
    return null;
}

async function* readLines(body: ReadableStream<Uint8Array>): AsyncGenerator<string> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    try {
        for (;;) {
            const { value, done } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            let newline = buffer.indexOf("\n");
            while (newline >= 0) {
                const line = buffer.slice(0, newline).trim();
                buffer = buffer.slice(newline + 1);
                if (line.length > 0) yield line;
                newline = buffer.indexOf("\n");
            }
        }
    } finally {
        reader.releaseLock();
    }
}

function toChatEvent(line: string): ChatEvent | null {
    const parsed = tryParseJson<ServerEvent>(line);
    if (parsed === null) return null;

    switch (parsed.type) {
        case "chunk":
            return typeof parsed.text === "string" ? { type: "chunk", text: parsed.text } : null;
        case "done":
            return { type: "done", reply: parsed.answer?.reply ?? null };
        case "error":
            return { type: "error", kind: "failed", message: parsed.message ?? GENERIC_MESSAGE };
        default:
            return null;
    }
}

/** Streams one turn. Aborting the signal ends the generator without yielding an error - a user-pressed
 *  stop is not a failure, and the server keeps the invocation it already consumed. */
export async function* streamChat(chatUrl: string, prompt: string, signal: AbortSignal): AsyncGenerator<ChatEvent> {
    let response: Response;
    try {
        response = await fetch(chatUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ prompt }),
            signal,
        });
    } catch (error) {
        if (signal.aborted) return;
        throw error;
    }

    const failure = await statusFailure(response);
    if (failure !== null) {
        yield failure;
        return;
    }

    if (response.ok === false || response.body === null) {
        yield { type: "error", kind: "failed", message: `${GENERIC_MESSAGE} (HTTP ${response.status})` };
        return;
    }

    try {
        for await (const line of readLines(response.body)) {
            const event = toChatEvent(line);
            if (event !== null) yield event;
        }
    } catch (error) {
        if (signal.aborted) return;
        throw error;
    }
}

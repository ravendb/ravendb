import { useRef, useState } from "react";
import { streamChat, type ChatErrorKind } from "@/transport";
import type { ChatRole, HistoryTurn } from "@/widget-config";

export type ChatMessage = {
    id: string;
    role: ChatRole;
    content: string;
};

const GENERIC_ERROR = "Something went wrong. Please try again.";

function toMessages(history: HistoryTurn[]): ChatMessage[] {
    return history.map((turn, index) => ({ id: `h${index}`, role: turn.role, content: turn.content }));
}

export type ChatController = {
    messages: ChatMessage[];
    streamingId: string | null;
    errorMessage: string | null;
    /** Which failure the message describes, so a live embed can tell its host page apart from a retry. */
    errorKind: ChatErrorKind | null;
    /** An expired link or an exhausted invocation budget can't recover, so the composer stays locked. */
    isBlocked: boolean;
    send: (prompt: string) => void;
    stop: () => void;
};

/** `chatUrl` is null in preview mode, which renders a canned transcript and never talks to the server. */
export function useChat(chatUrl: string | null, history: HistoryTurn[]): ChatController {
    const [messages, setMessages] = useState<ChatMessage[]>(() => toMessages(history));
    const [streamingId, setStreamingId] = useState<string | null>(null);
    const [error, setError] = useState<{ kind: ChatErrorKind; message: string } | null>(null);
    const [isBlocked, setIsBlocked] = useState(false);

    const abortRef = useRef<AbortController | null>(null);
    const nextIdRef = useRef(0);

    const appendTo = (id: string, text: string) =>
        setMessages((current) =>
            current.map((message) => (message.id === id ? { ...message, content: message.content + text } : message)),
        );

    const fillIfEmpty = (id: string, reply: string) =>
        setMessages((current) =>
            current.map((message) =>
                message.id === id && message.content.length === 0 ? { ...message, content: reply } : message,
            ),
        );

    const run = async (assistantId: string, prompt: string, signal: AbortSignal) => {
        try {
            for await (const event of streamChat(chatUrl!, prompt, signal)) {
                switch (event.type) {
                    case "chunk":
                        appendTo(assistantId, event.text);
                        break;
                    case "done":
                        // A non-streaming agent sends its whole answer only on the final frame.
                        if (event.reply !== null) fillIfEmpty(assistantId, event.reply);
                        break;
                    case "error":
                        setError({ kind: event.kind, message: event.message });
                        if (event.kind !== "failed") setIsBlocked(true);
                        break;
                }
            }
        } catch {
            setError({ kind: "failed", message: GENERIC_ERROR });
        } finally {
            setStreamingId(null);
            abortRef.current = null;
        }
    };

    const send = (prompt: string) => {
        if (chatUrl === null || streamingId !== null || isBlocked) return;

        const userId = `m${nextIdRef.current++}`;
        const assistantId = `m${nextIdRef.current++}`;
        setMessages((current) => [
            ...current,
            { id: userId, role: "user", content: prompt },
            { id: assistantId, role: "assistant", content: "" },
        ]);
        setError(null);
        setStreamingId(assistantId);

        const controller = new AbortController();
        abortRef.current = controller;
        void run(assistantId, prompt, controller.signal);
    };

    const stop = () => abortRef.current?.abort();

    return {
        messages,
        streamingId,
        errorMessage: error?.message ?? null,
        errorKind: error?.kind ?? null,
        isBlocked,
        send,
        stop,
    };
}

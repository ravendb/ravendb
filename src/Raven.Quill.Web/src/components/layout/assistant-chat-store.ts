import type { QueryClient } from "@tanstack/react-query";
import { create } from "zustand";
import { api } from "@/api/api";
import {
    describeAssistantError,
    isAssistantConsentRequired,
    isQuillSessionExpired,
    type AssistantRelevantLink,
} from "@/api/custom-services/assistant-service";
import { markSessionExpired } from "@/lib/query-client";

export type AssistantMessage = {
    id: string;
    role: "user" | "assistant" | "error";
    text: string;
    relevantLinks?: AssistantRelevantLink[];
};

let messageIdCounter = 0;

function nextMessageId() {
    messageIdCounter += 1;
    return `assistant-message-${messageIdCounter}`;
}

// Kept out of the store: aborting is a transport concern, and nothing renders from it.
let streamAbortController: AbortController | null = null;

function markConsentRequired(queryClient: QueryClient) {
    queryClient.setQueryData(api.queries.assistant.consent().queryKey, { status: "ConsentRequired" });
}

type AssistantChatState = {
    messages: AssistantMessage[];
    /** Ties the next turn to the same upstream conversation; null starts a fresh one. */
    conversationId: string | null;
    isStreaming: boolean;
    sendPrompt: (prompt: string, queryClient: QueryClient) => Promise<void>;
    stopStreaming: () => void;
    clearMessages: () => void;
};

export const useAssistantChatStore = create<AssistantChatState>((set, get) => ({
    messages: [],
    conversationId: null,
    isStreaming: false,
    sendPrompt: async (prompt, queryClient) => {
        if (get().isStreaming) {
            return;
        }

        const promptId = nextMessageId();
        const answerId = nextMessageId();
        set((state) => ({
            messages: [
                ...state.messages,
                { id: promptId, role: "user", text: prompt },
                { id: answerId, role: "assistant", text: "" },
            ],
        }));

        // A cleared conversation drops the answer bubble, and then there is nothing to update.
        const updateAnswer = (changes: Partial<AssistantMessage>) =>
            set((state) => ({
                messages: state.messages.map((message) =>
                    message.id === answerId ? { ...message, ...changes } : message,
                ),
            }));

        const dropAnswerIfEmpty = () =>
            set((state) => ({
                messages: state.messages.filter((message) => message.id !== answerId || message.text !== ""),
            }));

        const abortController = new AbortController();
        streamAbortController = abortController;
        set({ isStreaming: true });

        try {
            for await (const event of api.services.assistantChat.stream(
                { message: prompt, conversationId: get().conversationId },
                abortController.signal,
            )) {
                if (event.type === "chunk") {
                    updateAnswer({ text: event.answer });
                } else if (event.type === "done") {
                    const { ConversationId, Response } = event.result;
                    // An answer that came through empty would strand the "Thinking…" placeholder.
                    updateAnswer(
                        Response?.Answer
                            ? { text: Response.Answer, relevantLinks: Response.RelevantLinks ?? [] }
                            : { role: "error", text: "The AI assistant returned no answer." },
                    );
                    set({ conversationId: ConversationId ?? null });
                } else {
                    if (event.status === "ConsentRequired") {
                        markConsentRequired(queryClient);
                    }

                    updateAnswer({ role: "error", text: event.message });
                }
            }
        } catch (error) {
            // Stopped by the operator (or by clearing the conversation) — a partial answer stands, but
            // an answer that never started would strand the "Thinking…" placeholder.
            if (abortController.signal.aborted) {
                dropAnswerIfEmpty();
                return;
            }

            if (isQuillSessionExpired(error)) {
                updateAnswer({
                    role: "error",
                    text: "Your session expired before the assistant could answer. Please send the message again.",
                });
                markSessionExpired(queryClient);
                return;
            }

            if (isAssistantConsentRequired(error)) {
                markConsentRequired(queryClient);
            }

            updateAnswer({ role: "error", text: describeAssistantError(error) });
        } finally {
            if (streamAbortController === abortController) {
                streamAbortController = null;
                set({ isStreaming: false });
            }
        }
    },
    stopStreaming: () => {
        streamAbortController?.abort();
        streamAbortController = null;
        set({ isStreaming: false });
    },
    clearMessages: () => {
        get().stopStreaming();
        set({ messages: [], conversationId: null });
    },
}));

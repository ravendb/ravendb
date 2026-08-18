import { create } from "zustand";

export type AssistantMessage = {
    id: string;
    role: "user" | "assistant";
    text: string;
};

let messageIdCounter = 0;

export function nextAssistantMessageId() {
    messageIdCounter += 1;
    return `assistant-message-${messageIdCounter}`;
}

type AssistantChatState = {
    messageIds: string[];
    messagesById: Record<string, AssistantMessage>;
    appendMessages: (messages: AssistantMessage[]) => void;
    updateMessageText: (id: string, text: string) => void;
    clearMessages: () => void;
};

// Messages are normalized (id list + by-id map) so components can subscribe per message:
// a streaming update via updateMessageText re-renders only that bubble, and the transcript
// list itself only re-renders when messages are added or removed.
export const useAssistantChatStore = create<AssistantChatState>((set) => ({
    messageIds: [],
    messagesById: {},
    appendMessages: (messages) =>
        set((state) => ({
            messageIds: [...state.messageIds, ...messages.map((message) => message.id)],
            messagesById: {
                ...state.messagesById,
                ...Object.fromEntries(messages.map((message) => [message.id, message])),
            },
        })),
    updateMessageText: (id, text) =>
        set((state) => {
            const message = state.messagesById[id];
            if (!message) {
                return state;
            }
            return { messagesById: { ...state.messagesById, [id]: { ...message, text } } };
        }),
    clearMessages: () => set({ messageIds: [], messagesById: {} }),
}));

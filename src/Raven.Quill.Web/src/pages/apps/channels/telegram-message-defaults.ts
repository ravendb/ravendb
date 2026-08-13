import { z } from "zod";
import type { TelegramChannelMessages } from "@/api/generated/server-api";

type MessageKey = keyof TelegramChannelMessages;

// Mirrors ResolvedTelegramMessages.Defaults on the server; shown as placeholders in the edit form.
const TELEGRAM_MESSAGE_DEFAULTS = {
    greeting: {
        label: "Greeting (/start)",
        defaultText: "Hi! Ask me anything and I'll answer. Send /clear anytime to start a fresh conversation.",
    },
    conversationCleared: {
        label: "Conversation cleared (/clear)",
        defaultText: "Conversation cleared. The next message starts a fresh one.",
    },
    usernameMissing: {
        label: "Telegram username missing",
        defaultText:
            "This assistant needs your Telegram username. Set one in Telegram Settings and send your message again.",
    },
    phoneNumberRequest: {
        label: "Phone number request",
        defaultText:
            "This assistant needs your phone number. Tap the button below to share it, then send your message again.",
    },
    sharePhoneNumberButton: {
        label: "Share-phone button label",
        defaultText: "Share phone number",
    },
    ownContactRequired: {
        label: "Someone else's contact shared",
        defaultText: "That looks like someone else's contact. Tap the button below to share your own number.",
    },
    phoneNumberReceived: {
        label: "Phone number received",
        defaultText: "Thanks, got your phone number. Now send your message again.",
    },
    notConfigured: {
        label: "Agent not fully configured",
        defaultText: "This assistant is not fully configured yet. Please contact whoever set up this bot.",
    },
    overloaded: {
        label: "Chat overloaded",
        defaultText:
            "I'm still working through your earlier messages, so that one didn't make it. Please resend it once I've replied.",
    },
    somethingWentWrong: {
        label: "Something went wrong",
        defaultText: "Sorry - something went wrong handling that message. Please try again.",
    },
    groupChatRefusal: {
        label: "Group chat refusal",
        defaultText: "I only work in one-on-one chats. Message me directly to start a conversation.",
    },
} satisfies Record<MessageKey, { label: string; defaultText: string }>;

export const TELEGRAM_MESSAGE_FIELDS = (Object.keys(TELEGRAM_MESSAGE_DEFAULTS) as MessageKey[]).map((name) => ({
    name,
    ...TELEGRAM_MESSAGE_DEFAULTS[name],
}));

const messageOverrideSchema = z.string().trim().max(4096, "Keep it under 4096 characters");

export const telegramMessagesSchema = z.object(
    Object.fromEntries(TELEGRAM_MESSAGE_FIELDS.map((field) => [field.name, messageOverrideSchema])) as Record<
        MessageKey,
        typeof messageOverrideSchema
    >,
);

export function toMessagesFormValues(messages: TelegramChannelMessages | null | undefined) {
    return Object.fromEntries(
        TELEGRAM_MESSAGE_FIELDS.map((field) => [field.name, messages?.[field.name] ?? ""]),
    ) as Record<MessageKey, string>;
}

export function toMessagesDto(values: Record<MessageKey, string>): TelegramChannelMessages {
    return Object.fromEntries(
        TELEGRAM_MESSAGE_FIELDS.map((field) => [field.name, values[field.name].trim() || null]),
    ) as TelegramChannelMessages;
}

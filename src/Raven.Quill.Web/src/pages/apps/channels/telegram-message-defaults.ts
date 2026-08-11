import type { TelegramChannelMessages } from "@/api/generated/server-api";

// Mirrors ResolvedTelegramMessages.Defaults on the server; shown as placeholders in the edit form.
export const TELEGRAM_MESSAGE_FIELDS: {
    name: keyof TelegramChannelMessages;
    label: string;
    defaultText: string;
}[] = [
    {
        name: "greeting",
        label: "Greeting (/start)",
        defaultText: "Hi! Ask me anything and I'll answer. Send /clear anytime to start a fresh conversation.",
    },
    {
        name: "conversationCleared",
        label: "Conversation cleared (/clear)",
        defaultText: "Conversation cleared. The next message starts a fresh one.",
    },
    {
        name: "usernameMissing",
        label: "Telegram username missing",
        defaultText:
            "This assistant needs your Telegram username. Set one in Telegram Settings and send your message again.",
    },
    {
        name: "phoneNumberRequest",
        label: "Phone number request",
        defaultText:
            "This assistant needs your phone number. Tap the button below to share it, then send your message again.",
    },
    {
        name: "sharePhoneNumberButton",
        label: "Share-phone button label",
        defaultText: "Share phone number",
    },
    {
        name: "ownContactRequired",
        label: "Someone else's contact shared",
        defaultText: "That looks like someone else's contact. Tap the button below to share your own number.",
    },
    {
        name: "phoneNumberReceived",
        label: "Phone number received",
        defaultText: "Thanks, got your phone number. Now send your message again.",
    },
    {
        name: "notConfigured",
        label: "Agent not fully configured",
        defaultText: "This assistant is not fully configured yet. Please contact whoever set up this bot.",
    },
    {
        name: "overloaded",
        label: "Chat overloaded",
        defaultText:
            "I'm still working through your earlier messages, so that one didn't make it. Please resend it once I've replied.",
    },
    {
        name: "somethingWentWrong",
        label: "Something went wrong",
        defaultText: "Sorry - something went wrong handling that message. Please try again.",
    },
    {
        name: "groupChatRefusal",
        label: "Group chat refusal",
        defaultText: "I only work in one-on-one chats. Message me directly to start a conversation.",
    },
];

export function toMessagesFormValues(messages: TelegramChannelMessages | null | undefined) {
    return Object.fromEntries(
        TELEGRAM_MESSAGE_FIELDS.map((field) => [field.name, messages?.[field.name] ?? ""]),
    ) as Record<keyof TelegramChannelMessages, string>;
}

export function toMessagesDto(values: Record<keyof TelegramChannelMessages, string>): TelegramChannelMessages {
    return Object.fromEntries(
        TELEGRAM_MESSAGE_FIELDS.map((field) => [field.name, values[field.name].trim() || null]),
    ) as TelegramChannelMessages;
}

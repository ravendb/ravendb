import type { TelegramParameterSource } from "@/api/generated/server-api";

export const TELEGRAM_PARAMETER_SOURCES: { value: TelegramParameterSource; label: string; hint?: string }[] = [
    { value: "Constant", label: "Constant value" },
    { value: "UserId", label: "Telegram user id" },
    {
        value: "Username",
        label: "Telegram username",
        hint: "Users without a username set in Telegram will be asked to add one before the assistant answers.",
    },
    {
        value: "PhoneNumber",
        label: "Telegram phone number",
        hint: "Users will be asked to share their phone number with the bot before the assistant answers. Telegram shows them a share-contact confirmation.",
    },
];

export function telegramParameterSourceHint(source: TelegramParameterSource | undefined) {
    return TELEGRAM_PARAMETER_SOURCES.find((candidate) => candidate.value === source)?.hint;
}

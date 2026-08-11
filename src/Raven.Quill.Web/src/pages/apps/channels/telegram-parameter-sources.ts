import type { TelegramParameterSource } from "@/api/generated/server-api";

export const TELEGRAM_PARAMETER_SOURCES: { value: TelegramParameterSource; label: string }[] = [
    { value: "Constant", label: "Constant value" },
    { value: "UserId", label: "Telegram user id" },
    { value: "Username", label: "Telegram username" },
    { value: "PhoneNumber", label: "Telegram phone number" },
];

export function telegramParameterSourceLabel(source: TelegramParameterSource | undefined) {
    return TELEGRAM_PARAMETER_SOURCES.find((candidate) => candidate.value === source)?.label ?? (source || "—");
}

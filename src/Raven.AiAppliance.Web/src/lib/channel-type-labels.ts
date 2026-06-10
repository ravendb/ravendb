import type { ChannelType } from "@/api/generated/server-api";

export const CHANNEL_TYPE_LABELS: Record<NonNullable<ChannelType>, string> = {
    IFrame: "iFrame",
    Telegram: "Telegram",
    WhatsApp: "WhatsApp",
};

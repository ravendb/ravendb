import type { ChannelType } from "@/api/generated/server-api";

export const CHANNEL_TYPE_LABELS: Record<NonNullable<ChannelType>, string> = {
    IFrame: "Embedded chat",
    Telegram: "Telegram",
    WhatsApp: "WhatsApp",
    Slack: "Slack",
    Discord: "Discord",
};

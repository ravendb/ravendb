import type { ChannelParameterSource } from "@/api/generated/server-api";

export const SLACK_SOURCE_VALUES = ["Constant", "UserId", "Email"] as const satisfies readonly ChannelParameterSource[];

export const SLACK_PARAMETER_SOURCES: { value: (typeof SLACK_SOURCE_VALUES)[number]; label: string; hint?: string }[] =
    [
        { value: "Constant", label: "Constant value" },
        {
            value: "UserId",
            label: "Sender Slack user ID",
            hint: "Bound per message to the Slack user id (U...) the message came from.",
        },
        {
            value: "Email",
            label: "Sender email",
            hint: "Read from the sender's Slack profile. Needs the users:read and users:read.email scopes — apps installed before those were added must be reinstalled to the workspace. Senders with no email on their profile cannot use the bot.",
        },
    ];

export function slackParameterSourceLabel(source: ChannelParameterSource | undefined) {
    return SLACK_PARAMETER_SOURCES.find((candidate) => candidate.value === source)?.label ?? (source || "—");
}

export function slackParameterSourceHint(source: ChannelParameterSource | undefined) {
    return SLACK_PARAMETER_SOURCES.find((candidate) => candidate.value === source)?.hint;
}

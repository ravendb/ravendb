import type { ChannelParameterSource } from "@/api/generated/server-api";

export const SLACK_PARAMETER_SOURCES: { value: ChannelParameterSource; label: string; hint?: string }[] = [
    { value: "Constant", label: "Constant value" },
    {
        value: "UserId",
        label: "Sender Slack user ID",
        hint: "Bound per message to the Slack user id (U...) the message came from.",
    },
];

export function slackParameterSourceLabel(source: ChannelParameterSource | undefined) {
    return SLACK_PARAMETER_SOURCES.find((candidate) => candidate.value === source)?.label ?? (source || "—");
}

export function slackParameterSourceHint(source: ChannelParameterSource | undefined) {
    return SLACK_PARAMETER_SOURCES.find((candidate) => candidate.value === source)?.hint;
}

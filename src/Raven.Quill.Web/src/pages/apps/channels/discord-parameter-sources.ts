import type { ChannelParameterSource } from "@/api/generated/server-api";

export const DISCORD_SOURCE_VALUES = [
    "Constant",
    "UserId",
    "Username",
] as const satisfies readonly ChannelParameterSource[];

export const DISCORD_PARAMETER_SOURCES: {
    value: (typeof DISCORD_SOURCE_VALUES)[number];
    label: string;
    hint?: string;
}[] = [
    { value: "Constant", label: "Constant value" },
    {
        value: "UserId",
        label: "Sender Discord user ID",
        hint: "Bound per message to the numeric Discord user id the direct message came from.",
    },
    {
        value: "Username",
        label: "Sender Discord username",
        hint: "The sender's unique Discord handle, read straight from the message. Discord bots cannot read a sender's email address, so an email parameter has to be bound to a constant.",
    },
];

export function discordParameterSourceLabel(source: ChannelParameterSource | undefined) {
    return DISCORD_PARAMETER_SOURCES.find((candidate) => candidate.value === source)?.label ?? (source || "—");
}

export function discordParameterSourceHint(source: ChannelParameterSource | undefined) {
    return DISCORD_PARAMETER_SOURCES.find((candidate) => candidate.value === source)?.hint;
}

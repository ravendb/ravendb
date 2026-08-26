import type { AgentSummaryResponse, ChannelSummaryResponse } from "@/api/generated/server-api";
import { ChannelBindingsTab } from "@/pages/apps/channels/channel-bindings-tab";
import {
    DISCORD_PARAMETER_SOURCES,
    DISCORD_SOURCE_VALUES,
    discordParameterSourceHint,
} from "@/pages/apps/channels/discord-parameter-sources";

export function DiscordChannelBindings({
    slug,
    channel,
    agent,
}: {
    slug: string;
    channel: ChannelSummaryResponse;
    agent: AgentSummaryResponse | undefined;
}) {
    return (
        <ChannelBindingsTab
            slug={slug}
            channel={channel}
            agent={agent}
            bindings={channel.discord?.parameterBindings}
            sourceValues={DISCORD_SOURCE_VALUES}
            sources={DISCORD_PARAMETER_SOURCES}
            sourceHint={discordParameterSourceHint}
            buildUpdateRequest={(parameterBindings) => ({
                displayName: null,
                allowedOrigins: null,
                enabled: null,
                discord: { botToken: null, parameterBindings },
            })}
        />
    );
}

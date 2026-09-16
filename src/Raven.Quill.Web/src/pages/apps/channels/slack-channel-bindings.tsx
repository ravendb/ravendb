import type { AgentSummaryResponse, ChannelSummaryResponse } from "@/api/generated/server-api";
import { ChannelBindingsTab } from "@/pages/apps/channels/channel-bindings-tab";
import {
    SLACK_PARAMETER_SOURCES,
    SLACK_SOURCE_VALUES,
    slackParameterSourceHint,
} from "@/pages/apps/channels/slack-parameter-sources";

export function SlackChannelBindings({
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
            bindings={channel.slack?.parameterBindings}
            sourceValues={SLACK_SOURCE_VALUES}
            sources={SLACK_PARAMETER_SOURCES}
            sourceHint={slackParameterSourceHint}
            buildUpdateRequest={(parameterBindings) => ({
                displayName: null,
                allowedOrigins: null,
                enabled: null,
                slack: { botToken: null, signingSecret: null, parameterBindings },
            })}
        />
    );
}

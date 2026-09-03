import type { AgentSummaryResponse, ChannelSummaryResponse } from "@/api/generated/server-api";
import { ChannelBindingsTab } from "@/pages/apps/channels/channel-bindings-tab";
import {
    TELEGRAM_PARAMETER_SOURCES,
    TELEGRAM_SOURCE_VALUES,
    telegramParameterSourceHint,
} from "@/pages/apps/channels/telegram-parameter-sources";

export function TelegramChannelBindings({
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
            bindings={channel.telegram?.parameterBindings}
            sourceValues={TELEGRAM_SOURCE_VALUES}
            sources={TELEGRAM_PARAMETER_SOURCES}
            sourceHint={telegramParameterSourceHint}
            buildUpdateRequest={(parameterBindings) => ({
                displayName: null,
                allowedOrigins: null,
                enabled: null,
                telegram: { botToken: null, parameterBindings },
            })}
        />
    );
}

import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { DiscordChannelHealthResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { CopyableCode } from "@/components/data/copyable-code";
import { Alert } from "@/components/shadcn/ui/alert";
import { Badge } from "@/components/shadcn/ui/badge";
import { Text } from "@/components/typography";
import { formatDateTime, formatRelativeTime } from "@/lib/utils";
import { discordInstallUrl } from "@/pages/apps/channels/discord-app-setup";

export function DiscordStatusPanel({ slug, channelId }: { slug: string; channelId: string }) {
    const healthQuery = useQuery(api.queries.discord.health(slug));
    const health = healthQuery.data?.find((row) => row.channelId === channelId);

    return (
        <ApiState
            isLoading={healthQuery.isPending}
            isError={healthQuery.isError}
            errorTitle="Could not load the Discord connection status"
            onRetry={() => void healthQuery.refetch()}
            loadingLabel="Loading Discord connection status..."
        >
            {health && (
                <div className="space-y-4">
                    <ol className="list-decimal space-y-3 ps-5 text-sm">
                        <li>
                            Invite the bot to a server your users are already in — on Discord a person can only DM a bot
                            they share a server with:
                            <CopyableCode code={discordInstallUrl(health.applicationId)} copyLabel="Copy invite link" />
                        </li>
                        <li>
                            Open a direct message with <span className="font-medium">{health.botUsername}</span> and
                            send it a message.
                        </li>
                    </ol>
                    <DiscordHealthStrip health={health} />
                </div>
            )}
        </ApiState>
    );
}

export function DiscordHealthStrip({ health }: { health: DiscordChannelHealthResponse }) {
    const hasRecentSendError =
        health.lastSendError != null &&
        health.lastSendErrorAt != null &&
        (health.lastInboundAt == null ||
            new Date(health.lastSendErrorAt).getTime() > new Date(health.lastInboundAt).getTime());

    return (
        <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2 text-sm">
                <DiscordTokenBadge tokenValid={health.tokenValid} tokenError={health.tokenError} />
                <DiscordGatewayBadge health={health} />
                {health.lastInboundAt ? (
                    <Text as="span" variant="caption" title={formatDateTime(health.lastInboundAt)}>
                        Last message {formatRelativeTime(health.lastInboundAt)}
                    </Text>
                ) : (
                    <Text as="span" variant="caption">
                        Waiting for the first message...
                    </Text>
                )}
            </div>
            {!health.gatewayConnected && health.lastGatewayError && (
                <Alert variant="destructive">{health.lastGatewayError}</Alert>
            )}
            {hasRecentSendError && (
                <Alert variant="destructive">Last reply could not be delivered: {health.lastSendError}</Alert>
            )}
        </div>
    );
}

function DiscordGatewayBadge({ health }: { health: DiscordChannelHealthResponse }) {
    if (health.gatewayConnected) {
        return (
            <Badge
                variant="success"
                title={health.lastConnectedAt ? formatDateTime(health.lastConnectedAt) : undefined}
            >
                Gateway connected
            </Badge>
        );
    }

    if (!health.enabled) {
        return <Badge variant="secondary">Paused</Badge>;
    }

    if (!health.lastGatewayError) {
        return <Badge variant="secondary">Connecting...</Badge>;
    }

    return (
        <Badge variant="destructive" title={health.lastGatewayError}>
            Gateway disconnected
        </Badge>
    );
}

export function DiscordTokenBadge({
    tokenValid,
    tokenError,
}: {
    tokenValid: boolean | null | undefined;
    tokenError: string | null | undefined;
}) {
    if (tokenValid === true) {
        return <Badge variant="success">Token valid</Badge>;
    }
    if (tokenValid === false) {
        return (
            <Badge variant="destructive" title={tokenError ?? undefined}>
                Token rejected
            </Badge>
        );
    }
    return <Badge variant="secondary">Token status unknown</Badge>;
}

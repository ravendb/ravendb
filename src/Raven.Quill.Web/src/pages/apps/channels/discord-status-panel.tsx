import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { DiscordChannelHealthResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { CopyableCode } from "@/components/data/copyable-code";
import { NumberedSteps, type NumberedStep } from "@/components/data/numbered-steps";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Badge } from "@/components/shadcn/ui/badge";
import { Text } from "@/components/typography";
import { DiscordIcon } from "@/pages/apps/channels/channel-brand-icons";
import { discordInstallUrl } from "@/pages/apps/channels/discord-app-setup";
import { formatDateTime, formatRelativeTime } from "@/lib/utils";

function useDiscordHealth(slug: string, channelId: string) {
    const healthQuery = useQuery(api.queries.discord.health(slug));
    return { healthQuery, health: healthQuery.data?.find((row) => row.channelId === channelId) };
}

// Combined connection + install steps for the create sheet's success step, where both belong in one
// scrolling column. The Connect tab instead composes DiscordConnectionCard and DiscordSetupSteps into
// their own SectionCards, mirroring how Slack splits Connection from its event subscription.
export function DiscordStatusPanel({ slug, channelId }: { slug: string; channelId: string }) {
    return (
        <div className="space-y-4">
            <DiscordConnectionCard slug={slug} channelId={channelId} />
            <DiscordSetupSteps slug={slug} channelId={channelId} />
        </div>
    );
}

// Identity + live health for the connection: which bot it is wired to, whether the token and gateway
// are healthy, and when it last saw traffic. Mirrors SlackConnectionCard so the two channels read the same.
export function DiscordConnectionCard({ slug, channelId }: { slug: string; channelId: string }) {
    const { healthQuery, health } = useDiscordHealth(slug, channelId);

    return (
        <ApiState
            isLoading={healthQuery.isPending}
            isError={healthQuery.isError}
            errorTitle="Could not load the Discord connection status"
            onRetry={() => void healthQuery.refetch()}
            loadingLabel="Loading Discord connection status..."
        >
            {health && <DiscordConnectionCardBody health={health} />}
        </ApiState>
    );
}

function DiscordConnectionCardBody({ health }: { health: DiscordChannelHealthResponse }) {
    const hasRecentSendError =
        health.lastSendError != null &&
        health.lastSendErrorAt != null &&
        (health.lastInboundAt == null ||
            new Date(health.lastSendErrorAt).getTime() > new Date(health.lastInboundAt).getTime());

    return (
        <div className="space-y-3">
            <div className="flex flex-wrap items-center justify-between gap-3 rounded-md border bg-background p-3">
                <div className="flex min-w-0 items-center gap-3">
                    <DiscordIcon className="size-5 shrink-0 text-muted-foreground" aria-hidden={true} />
                    <div className="min-w-0">
                        <Text variant="label" className="truncate">
                            {health.botUsername}
                        </Text>
                        <Text variant="caption">Discord bot</Text>
                    </div>
                </div>
                <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-sm">
                    <div className="flex items-center gap-2">
                        <DiscordTokenBadge tokenValid={health.tokenValid} tokenError={health.tokenError} />
                        <DiscordGatewayBadge health={health} />
                    </div>
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
            </div>

            {!health.gatewayConnected && health.lastGatewayError && (
                <Alert variant="destructive">{health.lastGatewayError}</Alert>
            )}
            {hasRecentSendError && (
                <Alert variant="destructive">Last reply could not be delivered: {health.lastSendError}</Alert>
            )}
            {health.tokenValid === false && (
                <Alert variant="destructive">
                    <AlertDescription>
                        Discord rejected this bot token. Reset it on the app&apos;s Bot page, then open{" "}
                        <span className="font-medium">Edit &rarr; Rotate bot token</span> to paste the new one.
                    </AlertDescription>
                </Alert>
            )}
        </div>
    );
}

// The one-time install walkthrough: invite the bot to a shared server, then DM it. Carries its own
// loading/error state (off the same deduped health query) so the Connect tab's bordered "Invite the
// bot" card never renders empty while health is still loading.
export function DiscordSetupSteps({ slug, channelId }: { slug: string; channelId: string }) {
    const { healthQuery, health } = useDiscordHealth(slug, channelId);

    return (
        <ApiState
            isLoading={healthQuery.isPending}
            isError={healthQuery.isError}
            errorTitle="Could not load the Discord setup steps"
            onRetry={() => void healthQuery.refetch()}
            loadingLabel="Loading setup steps..."
        >
            {health && <DiscordSetupStepsBody health={health} />}
        </ApiState>
    );
}

function DiscordSetupStepsBody({ health }: { health: DiscordChannelHealthResponse }) {
    const steps: NumberedStep[] = [
        {
            title: "Invite the bot to a server",
            content: (
                <div className="space-y-2">
                    <Text variant="muted">
                        On Discord a person can only DM a bot they already share a server with, so invite it to a server
                        your users are in:
                    </Text>
                    <CopyableCode code={discordInstallUrl(health.applicationId)} copyLabel="Copy invite link" />
                </div>
            ),
        },
        {
            title: "Open a direct message",
            content: (
                <Text variant="muted">
                    Open a direct message with <span className="font-medium">{health.botUsername}</span> and send it a
                    message.
                </Text>
            ),
        },
    ];

    return <NumberedSteps steps={steps} />;
}

function DiscordGatewayBadge({ health }: { health: DiscordChannelHealthResponse }) {
    // A rejected token blocks the gateway entirely, so don't also claim it's "Connecting...".
    // The token badge already surfaces the blocking problem, so the gateway state is just noise here.
    if (health.tokenValid === false) {
        return null;
    }

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

import { useQuery } from "@tanstack/react-query";
import { ExternalLink } from "lucide-react";
import { api } from "@/api/api";
import type { SlackChannelHealthResponse, SlackSummaryResponse } from "@/api/generated/server-api";
import { NumberedSteps, type NumberedStep } from "@/components/data/numbered-steps";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Badge } from "@/components/shadcn/ui/badge";
import { Text } from "@/components/typography";
import { Timestamp, TimestampTooltip } from "@/components/data/timestamp";
import { SlackIcon } from "@/pages/apps/channels/channel-brand-icons";

const SLACK_APPS_URL = "https://api.slack.com/apps";

function useSlackHealth(slug: string, channelId: string) {
    return useQuery(api.queries.slack.health(slug)).data?.find((row) => row.channelId === channelId);
}

export function SlackStatusPanel({ slug, channelId }: { slug: string; channelId: string }) {
    return (
        <div className="space-y-4">
            <SlackConnectionCard slug={slug} channelId={channelId} />
            <SlackSetupSteps />
        </div>
    );
}

export function SlackConnectionCard({
    slug,
    channelId,
    slack,
}: {
    slug: string;
    channelId: string;
    slack?: SlackSummaryResponse | null;
}) {
    const health = useSlackHealth(slug, channelId);

    const teamName = slack?.teamName ?? health?.teamName ?? null;
    const botUserId = slack?.botUserId ?? health?.botUserId ?? null;

    const hasRecentSendError =
        health?.lastSendError != null &&
        health.lastSendErrorAt != null &&
        (health.lastInboundAt == null ||
            new Date(health.lastSendErrorAt).getTime() > new Date(health.lastInboundAt).getTime());

    return (
        <div className="space-y-3">
            <div className="flex flex-wrap items-center justify-between gap-3 rounded-md border bg-background p-3">
                <div className="flex min-w-0 items-center gap-3">
                    <SlackIcon className="size-5 shrink-0 text-muted-foreground" aria-hidden={true} />
                    <div className="min-w-0">
                        <Text variant="label" className="truncate">
                            {teamName ?? "Slack workspace"}
                        </Text>
                        <Text variant="caption" className="truncate font-mono">
                            {botUserId ? `Bot ${botUserId}` : "Bot identity pending…"}
                        </Text>
                    </div>
                </div>
                <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-sm">
                    <div className="flex items-center gap-2">
                        <SlackTokenBadge tokenValid={health?.tokenValid} tokenError={health?.tokenError} />
                        {health && <SlackSocketBadge health={health} />}
                    </div>
                    {health?.lastInboundAt ? (
                        <Text as="span" variant="caption">
                            Last message <Timestamp value={health.lastInboundAt} textVariant="inherit" />
                        </Text>
                    ) : (
                        <Text as="span" variant="caption">
                            Waiting for the first message...
                        </Text>
                    )}
                </div>
            </div>

            {health && !health.socketConnected && health.lastSocketError && (
                <Alert variant="destructive">{health.lastSocketError}</Alert>
            )}

            {hasRecentSendError && (
                <Alert variant="destructive">
                    <AlertDescription>
                        The bot couldn&apos;t deliver a reply
                        {health?.lastSendErrorAt ? (
                            <>
                                {" at "}
                                <Timestamp value={health.lastSendErrorAt} textVariant="inherit" />
                            </>
                        ) : null}
                        : {health?.lastSendError}
                    </AlertDescription>
                </Alert>
            )}

            {health?.tokenValid === false && (
                <Alert variant="destructive">
                    <AlertDescription>
                        Slack rejected this bot token. Copy the xoxb- token from the app&apos;s OAuth &amp; Permissions
                        page, then open <span className="font-medium">Edit &rarr; Rotate credentials</span> to paste the
                        new one.
                    </AlertDescription>
                </Alert>
            )}
        </div>
    );
}

export function SlackSetupSteps() {
    const steps: NumberedStep[] = [
        {
            title: "Turn on Socket Mode",
            content: (
                <Text variant="muted">
                    In your{" "}
                    <a
                        href={SLACK_APPS_URL}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-0.5 underline underline-offset-2 hover:text-foreground"
                    >
                        Slack app settings
                        <ExternalLink className="size-3" aria-hidden="true" />
                    </a>
                    , open <span className="font-medium">Socket Mode</span> and enable it. Apps created from the Quill
                    manifest already have it on.
                </Text>
            ),
        },
        {
            title: "Subscribe to bot events",
            content: (
                <Text variant="muted">
                    Under <span className="font-medium">Event Subscriptions</span>, switch{" "}
                    <span className="font-medium">Enable Events</span> on, add{" "}
                    <span className="font-medium">message.im</span> under bot events and save. Apps created from the
                    Quill manifest start with this in place.
                </Text>
            ),
        },
        {
            title: "Test it",
            content: <Text variant="muted">Open a DM with the bot in Slack and send it a message.</Text>,
        },
    ];

    return (
        <div className="space-y-4">
            <NumberedSteps steps={steps} />
            <Text variant="caption">
                The appliance connects out to Slack over a WebSocket, so it needs no public URL. An app created before
                users:read and users:read.email were added to the manifest must be reinstalled to the workspace before a
                parameter can bind to the sender&apos;s email.
            </Text>
        </div>
    );
}

function SlackSocketBadge({ health }: { health: SlackChannelHealthResponse }) {
    if (health.tokenValid === false) {
        return null;
    }

    if (health.socketConnected) {
        const badge = <Badge variant="success">Socket connected</Badge>;
        if (!health.lastConnectedAt) {
            return badge;
        }

        return (
            <TimestampTooltip value={health.lastConnectedAt} prefix="Connected on">
                {badge}
            </TimestampTooltip>
        );
    }

    if (!health.enabled) {
        return <Badge variant="secondary">Paused</Badge>;
    }

    if (!health.lastSocketError) {
        return <Badge variant="secondary">Connecting...</Badge>;
    }

    return (
        <Badge variant="destructive" title={health.lastSocketError}>
            Socket disconnected
        </Badge>
    );
}

export function SlackTokenBadge({
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

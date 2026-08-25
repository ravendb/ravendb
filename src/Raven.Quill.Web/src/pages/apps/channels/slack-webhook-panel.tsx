import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { CopyableCode } from "@/components/data/copyable-code";
import { Alert } from "@/components/shadcn/ui/alert";
import { Badge } from "@/components/shadcn/ui/badge";
import { Text } from "@/components/typography";
import { formatDateTime, formatRelativeTime } from "@/lib/utils";

export function SlackWebhookPanel({ slug, channelId }: { slug: string; channelId: string }) {
    const infoQuery = useQuery(api.queries.slack.webhookInfo(slug, channelId));

    return (
        <ApiState
            isLoading={infoQuery.isPending}
            isError={infoQuery.isError}
            errorTitle="Could not load the event subscription configuration"
            onRetry={() => void infoQuery.refetch()}
            loadingLabel="Loading event subscription configuration..."
        >
            {infoQuery.data && (
                <div className="space-y-4">
                    <ol className="list-decimal space-y-3 ps-5 text-sm">
                        <li>
                            In your{" "}
                            <a href="https://api.slack.com/apps" target="_blank" rel="noreferrer" className="underline">
                                Slack app settings
                            </a>
                            , open <span className="font-medium">Event Subscriptions</span> and turn events on.
                        </li>
                        <li>
                            Paste the request URL — Slack verifies it immediately:
                            <CopyableCode code={infoQuery.data.requestUrl} copyLabel="Copy request URL" />
                        </li>
                        <li>
                            Under <span className="font-medium">Subscribe to bot events</span>, add{" "}
                            <span className="font-medium">message.im</span>, then save.
                        </li>
                        <li>Open a DM with the bot in Slack and send it a message.</li>
                    </ol>
                    <Text variant="caption">
                        The appliance must be reachable from the internet on this URL. Apps created from the current
                        Quill manifest already carry the im:history scope, so adding the event needs no reinstall. An
                        app created before users:read and users:read.email were added to the manifest must be
                        reinstalled to the workspace before a parameter can bind to the sender&apos;s email.
                    </Text>
                    <SlackHealthStrip slug={slug} channelId={channelId} />
                </div>
            )}
        </ApiState>
    );
}

export function SlackHealthStrip({ slug, channelId }: { slug: string; channelId: string }) {
    const healthQuery = useQuery(api.queries.slack.health(slug));
    const health = healthQuery.data?.find((row) => row.channelId === channelId);
    if (!health) {
        return null;
    }

    const hasRecentSignatureFailure =
        health.lastSignatureFailureAt != null &&
        (health.lastInboundAt == null ||
            new Date(health.lastSignatureFailureAt).getTime() > new Date(health.lastInboundAt).getTime());

    return (
        <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2 text-sm">
                <SlackTokenBadge tokenValid={health.tokenValid} tokenError={health.tokenError} />
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
            {hasRecentSignatureFailure && (
                <Alert variant="destructive">
                    A recent delivery failed signature verification — the signing secret configured here likely differs
                    from the Slack app's. Rotate the signing secret on this channel to match.
                </Alert>
            )}
        </div>
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

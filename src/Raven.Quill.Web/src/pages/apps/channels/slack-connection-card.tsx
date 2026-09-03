import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { SlackSummaryResponse } from "@/api/generated/server-api";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Badge } from "@/components/shadcn/ui/badge";
import { Text } from "@/components/typography";
import { Timestamp } from "@/components/data/timestamp";
import { SlackIcon } from "@/pages/apps/channels/channel-brand-icons";

// The connection identity + live health for a Slack channel: which workspace and bot it is wired to,
// whether the token still works, when the last message arrived, and any recent delivery failure. Shown
// on the Connect tab and again on the create sheet's success step so both confirm the same thing.
export function SlackConnectionCard({
    slug,
    channelId,
    slack,
}: {
    slug: string;
    channelId: string;
    slack?: SlackSummaryResponse | null;
}) {
    const healthQuery = useQuery(api.queries.slack.health(slug));
    const health = healthQuery.data?.find((row) => row.channelId === channelId);

    // The channel record carries workspace/bot identity synchronously; the health poll is the fallback
    // right after creation, before the channel list has refetched.
    const teamName = slack?.teamName ?? health?.teamName ?? null;
    const botUserId = slack?.botUserId ?? health?.botUserId ?? null;

    const hasRecentSignatureFailure =
        health?.lastSignatureFailureAt != null &&
        (health.lastInboundAt == null ||
            new Date(health.lastSignatureFailureAt).getTime() > new Date(health.lastInboundAt).getTime());

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
                <div className="flex flex-wrap items-center gap-2 text-sm">
                    <SlackTokenBadge tokenValid={health?.tokenValid} tokenError={health?.tokenError} />
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

            {hasRecentSignatureFailure && health?.lastSignatureFailureAt && (
                <Alert variant="destructive">
                    <AlertDescription>
                        A delivery failed signature verification at{" "}
                        <Timestamp value={health.lastSignatureFailureAt} textVariant="inherit" /> — the signing secret
                        configured here likely differs from the Slack app&apos;s. Rotate the signing secret on this
                        channel to match.
                    </AlertDescription>
                </Alert>
            )}

            {health?.lastSendError && (
                <Alert variant="destructive">
                    <AlertDescription>
                        The bot couldn&apos;t deliver a reply
                        {health.lastSendErrorAt ? (
                            <>
                                {" at "}
                                <Timestamp value={health.lastSendErrorAt} textVariant="inherit" />
                            </>
                        ) : null}
                        : {health.lastSendError}
                    </AlertDescription>
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

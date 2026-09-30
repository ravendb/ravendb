import { useQuery } from "@tanstack/react-query";
import { ExternalLink } from "lucide-react";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { CopyableCode } from "@/components/data/copyable-code";
import { NumberedSteps } from "@/components/data/numbered-steps";
import { Text } from "@/components/typography";

// The admin-facing "wire Slack up to this bot" walkthrough: the per-channel request URL (a bearer
// secret, so it is shown here and nowhere else) and the event-subscription steps. Connection identity
// and live health live in SlackConnectionCard, shown alongside this.
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
                    <NumberedSteps
                        steps={[
                            {
                                title: "Open Event Subscriptions",
                                content: (
                                    <Text variant="muted">
                                        In your{" "}
                                        <a
                                            href="https://api.slack.com/apps"
                                            target="_blank"
                                            rel="noreferrer"
                                            className="inline-flex items-center gap-0.5 underline underline-offset-2 hover:text-foreground"
                                        >
                                            Slack app settings
                                            <ExternalLink className="size-3" aria-hidden="true" />
                                        </a>
                                        , open <span className="font-medium">Event Subscriptions</span> and turn events
                                        on.
                                    </Text>
                                ),
                            },
                            {
                                title: "Paste the request URL",
                                content: (
                                    <div className="space-y-2">
                                        <Text variant="muted">Slack verifies it immediately.</Text>
                                        <CopyableCode code={infoQuery.data.requestUrl} copyLabel="Copy request URL" />
                                    </div>
                                ),
                            },
                            {
                                title: "Subscribe to bot events",
                                content: (
                                    <Text variant="muted">
                                        Under <span className="font-medium">Subscribe to bot events</span>, add{" "}
                                        <span className="font-medium">message.im</span>, then save.
                                    </Text>
                                ),
                            },
                            {
                                title: "Test it",
                                content: (
                                    <Text variant="muted">Open a DM with the bot in Slack and send it a message.</Text>
                                ),
                            },
                        ]}
                    />
                    <Text variant="caption">
                        The appliance must be reachable from the internet on this URL. Apps created from the current
                        Quill manifest already carry the im:history scope, so adding the event needs no reinstall. An
                        app created before users:read and users:read.email were added to the manifest must be
                        reinstalled to the workspace before a parameter can bind to the sender&apos;s email.
                    </Text>
                </div>
            )}
        </ApiState>
    );
}

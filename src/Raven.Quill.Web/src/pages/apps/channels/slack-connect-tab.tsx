import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { SlackConnectionCard } from "@/pages/apps/channels/slack-connection-card";
import { SlackWebhookPanel } from "@/pages/apps/channels/slack-webhook-panel";
import { SectionCard } from "@/pages/apps/section-card";

// "How Slack reaches this bot" — the Slack analogue of the Telegram Connect tab. The connection card
// confirms which workspace/bot is wired up and how it's doing; the event-subscription card is the admin
// wiring.
export function SlackConnectTab({ slug, channel }: { slug: string; channel: ChannelSummaryResponse }) {
    return (
        <div className="grid gap-5">
            {!channel.enabled && (
                <Alert>This channel is paused, so the bot isn’t answering right now. Resume it to go live.</Alert>
            )}

            <SectionCard
                title="Connection"
                description="The Slack workspace and bot this channel is wired to, and how the connection is doing."
                isRaised
            >
                <div className="mt-4">
                    <SlackConnectionCard slug={slug} channelId={channel.channelId} slack={channel.slack} />
                </div>
            </SectionCard>

            <SectionCard
                title="Slack event subscription"
                description="Where Slack delivers this bot's direct messages."
                isRaised
            >
                <div className="mt-4">
                    <SlackWebhookPanel slug={slug} channelId={channel.channelId} />
                </div>
            </SectionCard>
        </div>
    );
}

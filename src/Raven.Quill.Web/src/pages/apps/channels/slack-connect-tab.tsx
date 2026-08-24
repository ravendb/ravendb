import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { SlackWebhookPanel } from "@/pages/apps/channels/slack-webhook-panel";
import { SectionCard } from "@/pages/apps/section-card";

// "How Slack reaches this bot" — the Slack analogue of the Telegram Connect tab. The request URL is
// fetched per channel; the token in it is a bearer secret, so it is shown here and nowhere else.
export function SlackConnectTab({ slug, channel }: { slug: string; channel: ChannelSummaryResponse }) {
    return (
        <div className="grid gap-5">
            {!channel.enabled && (
                <Alert>This channel is paused, so the bot isn’t answering right now. Resume it to go live.</Alert>
            )}

            <SectionCard
                title="Slack event subscription"
                description="Where Slack delivers this bot's direct messages, and how the connection is doing."
                isRaised
            >
                <div className="mt-4">
                    <SlackWebhookPanel slug={slug} channelId={channel.channelId} />
                </div>
            </SectionCard>
        </div>
    );
}

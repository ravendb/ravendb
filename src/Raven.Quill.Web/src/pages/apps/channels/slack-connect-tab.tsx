import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { SlackConnectionCard, SlackSetupSteps } from "@/pages/apps/channels/slack-connection-card";
import { SectionCard } from "@/pages/apps/section-card";

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
                title="Slack app setup"
                description="Socket Mode and the bot event subscription that let Slack hand this bot its direct messages."
                isRaised
            >
                <div className="mt-4">
                    <SlackSetupSteps />
                </div>
            </SectionCard>
        </div>
    );
}

import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { DiscordConnectionCard, DiscordSetupSteps } from "@/pages/apps/channels/discord-status-panel";
import { SectionCard } from "@/pages/apps/section-card";

// "How this bot reaches Discord" — the Discord analogue of the Slack Connect tab. The connection card
// confirms which bot is wired up and how it's doing; the install card is the one-time bot setup.
export function DiscordConnectTab({ slug, channel }: { slug: string; channel: ChannelSummaryResponse }) {
    return (
        <div className="grid gap-5">
            {!channel.enabled && (
                <Alert>This channel is paused, so the bot isn’t answering right now. Resume it to go live.</Alert>
            )}

            <SectionCard
                title="Connection"
                description="The Discord bot this channel is wired to, and how the connection is doing."
                isRaised
            >
                <div className="mt-4">
                    <DiscordConnectionCard slug={slug} channelId={channel.channelId} />
                </div>
            </SectionCard>

            <SectionCard
                title="Invite the bot"
                description="Add the bot to a server your users are in, then open a direct message to start chatting."
                isRaised
            >
                <div className="mt-4">
                    <DiscordSetupSteps slug={slug} channelId={channel.channelId} />
                </div>
            </SectionCard>
        </div>
    );
}

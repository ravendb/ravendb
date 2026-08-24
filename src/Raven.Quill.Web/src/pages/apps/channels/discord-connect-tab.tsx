import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { DiscordStatusPanel } from "@/pages/apps/channels/discord-status-panel";
import { SectionCard } from "@/pages/apps/section-card";

export function DiscordConnectTab({ slug, channel }: { slug: string; channel: ChannelSummaryResponse }) {
    return (
        <div className="grid gap-5">
            {!channel.enabled && (
                <Alert>This channel is paused, so the bot isn’t answering right now. Resume it to go live.</Alert>
            )}

            <SectionCard
                title="Discord bot connection"
                description="How this bot reaches Discord, and how the connection is doing."
                isRaised
            >
                <div className="mt-4">
                    <DiscordStatusPanel slug={slug} channelId={channel.channelId} />
                </div>
            </SectionCard>
        </div>
    );
}

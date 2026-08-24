import { ExternalLink, Send } from "lucide-react";
import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { InlineCode } from "@/components/data/inline-code";
import { NumberedSteps } from "@/components/data/numbered-steps";
import { SectionCard } from "@/pages/apps/section-card";

// Read-only "how to reach this bot" view — the Telegram analogue of the web widget's Embed tab.
// Everything here is either data already on the channel (the bot username) or documentation of the
// bot's existing /start and /clear commands. No secrets, nothing editable.
export function TelegramConnectTab({ channel }: { channel: ChannelSummaryResponse }) {
    const botUsername = channel.telegram?.botUsername;

    if (!botUsername) {
        return <Alert>This Telegram channel isn’t connected to a bot yet.</Alert>;
    }

    const botLink = `https://t.me/${botUsername}`;

    return (
        <div className="grid gap-5">
            {!channel.enabled && (
                <Alert>This channel is paused, so the bot isn’t answering right now. Resume it to go live.</Alert>
            )}

            <SectionCard title="How it works" description="What someone does to chat with this bot." isRaised>
                <div className="mt-4">
                    <NumberedSteps
                        steps={[
                            {
                                title: "Open the bot in Telegram",
                                content: (
                                    <>
                                        <p className="max-w-prose text-sm text-muted-foreground">
                                            Open the bot in Telegram, or search for its username.
                                        </p>
                                        <div className="mt-3 flex max-w-prose items-center justify-between gap-3 rounded-md border bg-background p-3">
                                            <div className="min-w-0">
                                                <p className="font-mono text-sm">@{botUsername}</p>
                                                <p className="truncate text-xs text-muted-foreground">{botLink}</p>
                                            </div>
                                            <Button asChild size="sm" variant="outline" className="shrink-0">
                                                <a href={botLink} target="_blank" rel="noreferrer">
                                                    <Send aria-hidden="true" />
                                                    Open in Telegram
                                                    <ExternalLink aria-hidden="true" />
                                                </a>
                                            </Button>
                                        </div>
                                    </>
                                ),
                            },
                            {
                                title: "Start the conversation",
                                content: (
                                    <p className="max-w-prose text-sm text-muted-foreground">
                                        Press <strong>Start</strong> (sends <InlineCode>/start</InlineCode>) to greet
                                        the bot and begin a conversation.
                                    </p>
                                ),
                            },
                            {
                                title: "Reset anytime",
                                content: (
                                    <p className="max-w-prose text-sm text-muted-foreground">
                                        Send <InlineCode>/clear</InlineCode> anytime to wipe the current conversation
                                        and start fresh.
                                    </p>
                                ),
                            },
                        ]}
                    />
                </div>
            </SectionCard>
        </div>
    );
}

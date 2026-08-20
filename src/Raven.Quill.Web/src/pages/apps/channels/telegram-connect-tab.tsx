import type { ReactNode } from "react";
import { ExternalLink, Send } from "lucide-react";
import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { InlineCode } from "@/components/data/inline-code";

// Read-only "how to reach this bot" view — the Telegram analogue of the web widget's Embed tab.
// Everything here is either data already on the channel (the bot username) or documentation of the
// bot's existing /start and /clear commands. Styled to match the web widget's "Embed on your own
// site" walkthrough. No secrets, nothing editable.
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

            <section className="rounded-md border bg-card p-4">
                <h2 className="text-lg font-semibold tracking-tight">How it works</h2>
                <p className="mt-0.5 text-sm text-muted-foreground">What someone does to chat with this bot.</p>

                <ol className="mt-4 grid gap-0">
                    <Step step={1} title="Open the bot in Telegram">
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
                    </Step>

                    <Step step={2} title="Start the conversation">
                        <p className="max-w-prose text-sm text-muted-foreground">
                            Press <strong>Start</strong> (sends <InlineCode>/start</InlineCode>) to greet the bot and
                            begin a conversation.
                        </p>
                    </Step>

                    <Step step={3} title="Reset anytime" isLast>
                        <p className="max-w-prose text-sm text-muted-foreground">
                            Send <InlineCode>/clear</InlineCode> anytime to wipe the current conversation and start
                            fresh.
                        </p>
                    </Step>
                </ol>
            </section>
        </div>
    );
}

// One numbered step, mirroring the embed walkthrough's stepper: a badge and a connector rail in the
// left column, the step's title and content in the right. The rail is drawn on every step but the last
// so the steps read as a single sequence.
function Step({
    step,
    title,
    isLast = false,
    children,
}: {
    step: number;
    title: string;
    isLast?: boolean;
    children: ReactNode;
}) {
    return (
        <li className="grid grid-cols-[1.5rem_1fr] gap-x-3">
            <div className="flex flex-col items-center">
                <span className="flex size-6 items-center justify-center rounded-full border border-border bg-muted text-xs font-medium text-muted-foreground">
                    {step}
                </span>
                {!isLast && <span aria-hidden="true" className="mt-1 w-px flex-1 bg-border" />}
            </div>
            <div className={isLast ? "" : "pb-5"}>
                <h3 className="mb-1.5 text-base font-medium">{title}</h3>
                {children}
            </div>
        </li>
    );
}

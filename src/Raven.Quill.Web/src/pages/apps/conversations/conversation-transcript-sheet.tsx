import { useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Streamdown } from "streamdown";
import { api } from "@/api/api";
import type { AiConversationMessage } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import {
    Sheet,
    SheetContent,
    SheetDescription,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/shadcn/ui/sheet";
import { cn, formatDateTime } from "@/lib/utils";
import { ConversationToolCall } from "@/pages/apps/conversations/conversation-tool-call";

type ConversationTranscriptSheetProps = {
    slug: string;
    conversationId: string;
    agentName: string;
    channelName: string;
    trigger: ReactNode;
};

export function ConversationTranscriptSheet({
    slug,
    conversationId,
    agentName,
    channelName,
    trigger,
}: ConversationTranscriptSheetProps) {
    const [isOpen, setIsOpen] = useState(false);
    // Only fetch the full thread once the sheet opens.
    const conversationQuery = useQuery({
        ...api.queries.stats.conversation(slug, conversationId),
        enabled: isOpen,
    });

    const transcript = conversationQuery.data?.transcript ?? [];
    const allTurns = transcript.length > 0 ? transcript : (conversationQuery.data?.lastExchange ?? []);
    const turns = allTurns.filter(isDisplayableTurn);

    return (
        <Sheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>{trigger}</SheetTrigger>
            <SheetContent className="w-full sm:max-w-lg data-[side=right]:sm:max-w-lg">
                <SheetHeader className="border-b">
                    <SheetTitle>{agentName}</SheetTitle>
                    <SheetDescription>{channelName}</SheetDescription>
                </SheetHeader>
                <div className="min-h-0 flex-1 space-y-4 overflow-auto px-4 pb-4">
                    <ApiState
                        isLoading={conversationQuery.isPending}
                        isError={conversationQuery.isError}
                        errorTitle="Could not load conversation"
                        onRetry={() => void conversationQuery.refetch()}
                        loadingLabel="Loading conversation..."
                    >
                        {conversationQuery.data &&
                            (turns.length === 0 ? (
                                <p className="text-sm text-muted-foreground">No messages in this conversation.</p>
                            ) : (
                                turns.map((turn, index) => <TranscriptTurn key={index} turn={turn} />)
                            ))}
                    </ApiState>
                </div>
            </SheetContent>
        </Sheet>
    );
}

// The transcript also carries bookkeeping entries (system prompt, summaries, internal turns);
// operators only need the visible exchange plus any tools the agent ran along the way.
function isDisplayableTurn(turn: AiConversationMessage): boolean {
    const hasToolCalls = (turn.toolCalls?.length ?? 0) > 0;
    const hasVisibleContent = (turn.role === "user" || turn.role === "assistant") && Boolean(turn.content?.trim());
    const isParametersMessage = turn.role === "user" && turn.content?.startsWith("AI Agent Parameters:");
    return (hasVisibleContent || hasToolCalls) && !isParametersMessage;
}

function TranscriptTurn({ turn }: { turn: AiConversationMessage }) {
    const isUser = turn.role === "user";
    const content = turn.content?.trim();
    const showContent = Boolean(content) && (isUser || turn.role === "assistant");

    return (
        <div className={cn("flex flex-col gap-1", isUser ? "items-end" : "items-start")}>
            {turn.toolCalls?.map((toolCall, index) => (
                <ConversationToolCall key={toolCall.id || index} toolCall={toolCall} />
            ))}
            {showContent &&
                (isUser ? (
                    <div className="max-w-[85%] rounded-lg bg-primary px-3 py-2 text-sm whitespace-pre-wrap text-primary-foreground">
                        {content}
                    </div>
                ) : (
                    <div className="max-w-[85%] rounded-lg bg-muted px-3 py-2 text-sm">
                        <Streamdown>{content}</Streamdown>
                    </div>
                ))}
            {turn.timestamp && <span className="text-xs text-muted-foreground">{formatDateTime(turn.timestamp)}</span>}
        </div>
    );
}

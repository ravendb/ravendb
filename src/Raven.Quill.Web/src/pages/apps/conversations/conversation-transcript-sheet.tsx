import { useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Streamdown } from "streamdown";
import { api } from "@/api/api";
import type { AiConversationMessage, ConversationParam } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { CardListSkeleton } from "@/components/data/loading-skeletons";
import {
    Sheet,
    SheetContent,
    SheetDescription,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/shadcn/ui/sheet";
import { cn, formatDateTime } from "@/lib/utils";
import { ConversationParams } from "@/pages/apps/conversations/conversation-params";
import { ConversationSystemPrompt } from "@/pages/apps/conversations/conversation-system-prompt";
import { ConversationToolCall } from "@/pages/apps/conversations/conversation-tool-call";
import { TranscriptDisclosureState } from "@/pages/apps/conversations/transcript-disclosure";

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
    // State, not a ref: React attaches a host ref only after its children's layout effects have run,
    // so with cached data the virtualizer would mount alongside this element and never see it.
    const [scrollElement, setScrollElement] = useState<HTMLDivElement | null>(null);
    // Only fetch the full thread once the sheet opens.
    const conversationQuery = useQuery({
        ...api.queries.stats.conversation(slug, conversationId),
        enabled: isOpen,
    });

    const transcript = conversationQuery.data?.transcript ?? [];
    const allTurns = transcript.length > 0 ? transcript : (conversationQuery.data?.lastExchange ?? []);
    const turns = allTurns.filter(isDisplayableTurn);
    const params = conversationQuery.data?.params ?? [];
    const rows: TranscriptRow[] = [
        ...(params.length > 0 ? [{ kind: "params", params } satisfies TranscriptRow] : []),
        ...turns.map((turn) => ({ kind: "turn", turn }) satisfies TranscriptRow),
    ];

    return (
        <Sheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>{trigger}</SheetTrigger>
            <SheetContent className="w-full sm:max-w-lg data-[side=right]:sm:max-w-lg">
                <SheetHeader className="border-b">
                    <SheetTitle>{agentName}</SheetTitle>
                    <SheetDescription>{channelName}</SheetDescription>
                </SheetHeader>
                {/* The virtualizer measures rows against this element, so the rows must be its only content. */}
                <div ref={setScrollElement} className="min-h-0 flex-1 overflow-auto px-4 pb-4">
                    <ApiState
                        isLoading={conversationQuery.isPending}
                        isError={conversationQuery.isError}
                        errorTitle="Could not load conversation"
                        onRetry={() => void conversationQuery.refetch()}
                        loadingLabel="Loading conversation..."
                        skeleton={<CardListSkeleton count={4} />}
                    >
                        {conversationQuery.data &&
                            (turns.length === 0 ? (
                                <p className="text-sm text-muted-foreground">No messages in this conversation.</p>
                            ) : (
                                <TranscriptDisclosureState>
                                    <TranscriptRows scrollElement={scrollElement} rows={rows} />
                                </TranscriptDisclosureState>
                            ))}
                    </ApiState>
                </div>
            </SheetContent>
        </Sheet>
    );
}

// The transcript can span an entire conversation, so only the visible rows are rendered. Rows differ
// wildly in height (a one-line prompt vs. an expanded tool call), so the estimate is only used until
// a row mounts and the virtualizer measures it.
const ESTIMATED_ROW_HEIGHT_IN_PX = 60;
const ROW_GAP_IN_PX = 16;
const OVERSCAN = 12;

type TranscriptRow = { kind: "params"; params: ConversationParam[] } | { kind: "turn"; turn: AiConversationMessage };

function TranscriptRows({ scrollElement, rows }: { scrollElement: HTMLDivElement | null; rows: TranscriptRow[] }) {
    // The virtualizer returns fresh functions on every render, so memoizing this component would
    // freeze the visible window on its first value. The row contents stay compiled.
    "use no memo";

    // eslint-disable-next-line react-hooks/incompatible-library -- handled by "use no memo" above
    const virtualizer = useVirtualizer({
        count: rows.length,
        estimateSize: () => ESTIMATED_ROW_HEIGHT_IN_PX,
        getScrollElement: () => scrollElement,
        gap: ROW_GAP_IN_PX,
        overscan: OVERSCAN,
    });

    return (
        <div className="relative w-full" style={{ height: virtualizer.getTotalSize() }}>
            {virtualizer.getVirtualItems().map((virtualRow) => (
                <div
                    key={virtualRow.key}
                    data-index={virtualRow.index}
                    ref={(node) => virtualizer.measureElement(node)}
                    // Positioned via top instead of translateY: Chromium never shrinks
                    // scrollable overflow contributed by transformed children, so rows
                    // that transiently render lower leave a permanent phantom scrollbar.
                    className="absolute inset-x-0"
                    style={{ top: virtualRow.start }}
                >
                    <TranscriptRowContent row={rows[virtualRow.index]} rowKey={`row-${virtualRow.index}`} />
                </div>
            ))}
        </div>
    );
}

// The transcript also carries bookkeeping entries (summaries, internal turns, the parameters block);
// operators only need the prompt, the visible exchange and any tools the agent ran along the way.
function isDisplayableTurn(turn: AiConversationMessage): boolean {
    const hasToolCalls = (turn.toolCalls?.length ?? 0) > 0;
    const hasVisibleContent =
        (turn.role === "system" || turn.role === "user" || turn.role === "assistant") && Boolean(turn.content?.trim());
    const isParametersMessage = turn.role === "user" && turn.content?.startsWith("AI Agent Parameters:");
    return (hasVisibleContent || hasToolCalls) && !isParametersMessage;
}

function TranscriptRowContent({ row, rowKey }: { row: TranscriptRow; rowKey: string }) {
    if (row.kind === "params") {
        return <ConversationParams disclosureKey={rowKey} params={row.params} />;
    }
    return <TranscriptTurn turn={row.turn} turnKey={rowKey} />;
}

function TranscriptTurn({ turn, turnKey }: { turn: AiConversationMessage; turnKey: string }) {
    const isUser = turn.role === "user";
    const content = turn.content?.trim();
    const showBubble = Boolean(content) && (isUser || turn.role === "assistant");

    return (
        <div className={cn("flex flex-col gap-1", isUser ? "items-end" : "items-start")}>
            {turn.role === "system" && content && (
                <ConversationSystemPrompt disclosureKey={turnKey} content={content} />
            )}
            {turn.toolCalls?.map((toolCall, index) => (
                <ConversationToolCall
                    key={toolCall.id || index}
                    disclosureKey={`${turnKey}-tool-${toolCall.id || index}`}
                    toolCall={toolCall}
                />
            ))}
            {showBubble &&
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

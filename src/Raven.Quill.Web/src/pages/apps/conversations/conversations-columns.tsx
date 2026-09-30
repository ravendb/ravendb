/* eslint-disable react-refresh/only-export-components */

import type { ColumnDef } from "@tanstack/react-table";
import { MessageSquareText } from "lucide-react";
import type { ConversationDto } from "@/api/generated/server-api";
import { Parameters } from "@/components/data/parameters";
import { Button } from "@/components/shadcn/ui/button";
import { Timestamp } from "@/components/data/timestamp";
import { agentAvatarColor } from "@/lib/palette";
import { ConversationStateDot } from "@/pages/apps/conversations/conversation-state";
import { ConversationTranscriptSheet } from "@/pages/apps/conversations/conversation-transcript-sheet";

export function createConversationColumns(slug: string): ColumnDef<ConversationDto>[] {
    return [
        {
            accessorKey: "agentName",
            header: "Agent",
            size: 180,
            cell: ({ row }) => <AgentCell conversation={row.original} />,
        },
        {
            accessorKey: "channelName",
            header: "Channel",
            size: 160,
            cell: ({ getValue }) => <span className="font-medium">{getValue<string>()}</span>,
        },
        {
            id: "parameters",
            header: "Parameters",
            size: 260,
            cell: ({ row }) => (
                <Parameters params={row.original.params.map((param) => ({ name: param.key, value: param.value }))} />
            ),
        },
        {
            id: "lastExchange",
            header: "Last exchange",
            size: 480,
            cell: ({ row }) => <LastExchangeCell conversation={row.original} />,
        },
        {
            accessorKey: "lastActivityAt",
            header: "Last activity",
            size: 200,
            cell: ({ row }) => (
                <span className="flex items-center gap-2">
                    <ConversationStateDot state={row.original.state} />
                    <Timestamp value={row.original.lastActivityAt} />
                </span>
            ),
        },
        {
            id: "actions",
            header: "",
            size: 52,
            enableResizing: false,
            cell: ({ row }) => (
                <ConversationTranscriptSheet
                    slug={slug}
                    conversationId={row.original.id}
                    agentName={row.original.agentName}
                    channelName={row.original.channelName}
                    trigger={
                        <Button
                            variant="ghost"
                            size="icon-sm"
                            aria-label={`View conversation ${row.original.id}`}
                            title="View transcript"
                        >
                            <MessageSquareText className="size-4" aria-hidden="true" />
                        </Button>
                    }
                />
            ),
        },
    ];
}

function AgentCell({ conversation }: { conversation: ConversationDto }) {
    return (
        <span className="flex items-center gap-2 font-medium">
            <span
                className="flex size-6 shrink-0 items-center justify-center rounded-full text-[10px] font-semibold text-white"
                style={{ backgroundColor: agentAvatarColor(conversation.agentName) }}
                aria-hidden="true"
            >
                {conversation.agentInitials}
            </span>
            <span className="truncate">{conversation.agentName}</span>
        </span>
    );
}

function LastExchangeCell({ conversation }: { conversation: ConversationDto }) {
    const turns = [...conversation.lastExchange].sort((left, right) =>
        (left.timestamp ?? "").localeCompare(right.timestamp ?? ""),
    );

    if (turns.length === 0) {
        return <span className="text-muted-foreground">—</span>;
    }

    return (
        <span className="flex max-w-full min-w-0 flex-col gap-1">
            {turns.map((turn, index) => {
                const isAgent = turn.role === "assistant";

                return (
                    <span key={`${turn.timestamp ?? "undated"}-${index}`} className="flex min-w-0 items-center gap-2">
                        <span
                            className="h-3 w-0.5 shrink-0 rounded-full bg-muted-foreground"
                            style={isAgent ? { backgroundColor: agentAvatarColor(conversation.agentName) } : undefined}
                            aria-hidden="true"
                        />
                        <span className="min-w-0 truncate font-medium" title={turn.content ?? undefined}>
                            <span className="sr-only">{isAgent ? "agent" : (turn.role ?? "message")}: </span>
                            {turn.content}
                        </span>
                    </span>
                );
            })}
        </span>
    );
}

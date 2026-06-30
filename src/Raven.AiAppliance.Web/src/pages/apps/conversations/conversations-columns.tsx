/* eslint-disable react-refresh/only-export-components */

import type { ColumnDef } from "@tanstack/react-table";
import { MessageSquareText } from "lucide-react";
import type { ConversationDto } from "@/api/generated/server-api";
import { Parameters } from "@/components/data/parameters";
import { Button } from "@/components/shadcn/ui/button";
import { formatDateTime, formatRelativeTime } from "@/lib/utils";
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
            accessorKey: "lastActivityAt",
            header: "Last activity",
            size: 140,
            cell: ({ row }) => (
                <span className="flex items-center gap-2 whitespace-nowrap text-muted-foreground">
                    <ConversationStateDot state={row.original.state} />
                    <span title={formatDateTime(row.original.lastActivityAt)}>
                        {formatRelativeTime(row.original.lastActivityAt)}
                    </span>
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

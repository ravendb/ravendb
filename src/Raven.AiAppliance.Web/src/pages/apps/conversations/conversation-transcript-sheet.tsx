import { useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
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

    const turns = conversationQuery.data?.transcript ?? conversationQuery.data?.lastExchange ?? [];

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
                                turns.map((turn, index) => (
                                    <div
                                        key={index}
                                        className={cn(
                                            "flex flex-col gap-1",
                                            turn.role === "user" ? "items-end" : "items-start",
                                        )}
                                    >
                                        <div
                                            className={cn(
                                                "max-w-[85%] rounded-lg px-3 py-2 text-sm",
                                                turn.role === "user"
                                                    ? "bg-primary text-primary-foreground"
                                                    : "bg-muted",
                                            )}
                                        >
                                            {turn.text}
                                        </div>
                                        {turn.at && (
                                            <span className="text-xs text-muted-foreground">
                                                {formatDateTime(turn.at)}
                                            </span>
                                        )}
                                    </div>
                                ))
                            ))}
                    </ApiState>
                </div>
            </SheetContent>
        </Sheet>
    );
}

import { useState, type ReactNode } from "react";
import { Text } from "@/components/typography";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { CdcError } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { ErrorDetails } from "@/components/data/error-details";
import { CardListSkeleton } from "@/components/data/loading-skeletons";
import { Badge } from "@/components/shadcn/ui/badge";
import {
    Sheet,
    SheetContent,
    SheetDescription,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/shadcn/ui/sheet";
import { formatCompact } from "@/lib/format";
import { formatDateTime } from "@/lib/utils";

export function CdcErrorsSheet({ slug, trigger }: { slug: string; trigger: ReactNode }) {
    const [isOpen, setIsOpen] = useState(false);

    const errorsQuery = useQuery({
        ...api.queries.apps.cdcErrors(slug),
        enabled: isOpen,
    });

    return (
        <Sheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>{trigger}</SheetTrigger>
            <SheetContent className="w-full sm:max-w-lg data-[side=right]:sm:max-w-lg">
                <SheetHeader className="border-b">
                    <SheetTitle>App errors</SheetTitle>
                    <SheetDescription>Errors reported while syncing data changes for this app.</SheetDescription>
                </SheetHeader>
                <div className="min-h-0 flex-1 space-y-3 overflow-auto px-4 pb-4">
                    <ApiState
                        isLoading={errorsQuery.isPending}
                        isError={errorsQuery.isError}
                        errorTitle="Could not load errors"
                        onRetry={() => void errorsQuery.refetch()}
                        loadingLabel="Loading errors..."
                        skeleton={<CardListSkeleton />}
                    >
                        {errorsQuery.data &&
                            (errorsQuery.data.length === 0 ? (
                                <Text variant="muted">No app errors.</Text>
                            ) : (
                                errorsQuery.data.map((error, index) => <CdcErrorCard key={index} error={error} />)
                            ))}
                    </ApiState>
                </div>
            </SheetContent>
        </Sheet>
    );
}

function CdcErrorCard({ error }: { error: CdcError }) {
    const lineBreakIndex = error.error.indexOf("\n");
    const shortMessage = lineBreakIndex === -1 ? null : error.error.slice(0, lineBreakIndex);

    return (
        <div className="space-y-2 rounded-lg border p-3">
            <div className="flex items-center justify-between gap-2">
                <Text as="span" variant="label">
                    {error.taskName}
                </Text>
                <Text as="span" variant="caption">
                    {formatDateTime(error.createdAt)}
                </Text>
            </div>
            <Badge variant="secondary">{error.step}</Badge>
            {shortMessage && <p className="text-sm break-words whitespace-pre-wrap text-destructive">{shortMessage}</p>}
            {error.error && <ErrorDetails details={error.error} />}
            {error.documentId && (
                <Text variant="caption">
                    Document: <span className="font-mono text-foreground">{error.documentId}</span>
                </Text>
            )}
            {error.affectedDocumentsCount !== null && (
                <Text variant="caption">
                    Affected documents:{" "}
                    <span className="font-mono text-foreground tabular-nums">
                        {formatCompact(error.affectedDocumentsCount)}
                    </span>
                </Text>
            )}
        </div>
    );
}

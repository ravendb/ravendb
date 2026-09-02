import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AiConnectionStringDeleteConflictResponse } from "@/api/generated/server-api";
import { isApiError } from "@/api/http-client";
import { invalidateAiConnectionStringQueries } from "@/lib/query-invalidation";
import { AiConnectionStringUsageList } from "@/components/ai-connection-string/ai-connection-string-usage";
import { DestructiveConfirmDialog } from "@/components/shadcn/ui/destructive-confirm-dialog";

type DeleteAiConnectionStringDialogProps = {
    name: string;
    trigger: ReactNode;
};

// The server refuses (409) to delete a connection string an agent or an AI task still
// references, returning those usages so we can list them.
function getDeleteConflict(error: unknown): AiConnectionStringDeleteConflictResponse | null {
    if (
        isApiError<AiConnectionStringDeleteConflictResponse>(error) &&
        error.status === 409 &&
        error.details != null &&
        Array.isArray(error.details.usedBy)
    ) {
        return error.details;
    }
    return null;
}

function DeleteErrorMessage({ error }: { error: unknown }) {
    const conflict = getDeleteConflict(error);

    if (conflict) {
        return (
            <>
                <p>This connection string is still in use. Remove what uses it first:</p>
                <AiConnectionStringUsageList usedBy={conflict.usedBy} className="mt-1 pl-5" />
            </>
        );
    }

    return error instanceof Error ? error.message : "Could not delete connection string.";
}

export function DeleteAiConnectionStringDialog({ name, trigger }: DeleteAiConnectionStringDialogProps) {
    const [isOpen, setIsOpen] = useState(false);
    const queryClient = useQueryClient();

    const deleteMutation = useMutation({
        mutationFn: () => api.services.aiConnectionStrings.delete(name),
        onSuccess: async () => {
            await Promise.all([
                invalidateAiConnectionStringQueries(queryClient),
                // Drop the deleted string's cached detail so a stale edit sheet can't resurrect it.
                queryClient.invalidateQueries({
                    queryKey: api.queries.aiConnectionStrings.detail(name).queryKey,
                }),
            ]);
            toast.success(`Connection string “${name}” deleted`);
            setIsOpen(false);
        },
    });

    return (
        <DestructiveConfirmDialog
            trigger={trigger}
            title="Delete connection string?"
            description={`“${name}” will be permanently removed. This can’t be undone.`}
            confirmLabel="Delete"
            isOpen={isOpen}
            onOpenChange={(open) => {
                setIsOpen(open);
                if (!open) {
                    deleteMutation.reset();
                }
            }}
            onConfirm={() => deleteMutation.mutate()}
            isPending={deleteMutation.isPending}
            error={deleteMutation.isError ? <DeleteErrorMessage error={deleteMutation.error} /> : undefined}
        />
    );
}

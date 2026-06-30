import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AiConnectionStringDeleteConflictResponse } from "@/api/generated/server-api";
import { isApiError } from "@/api/http-client";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import {
    Dialog,
    DialogClose,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/shadcn/ui/dialog";

type DeleteAiConnectionStringDialogProps = {
    slug: string;
    name: string;
    trigger: ReactNode;
};

// The server refuses (409) to delete a connection string still referenced by an
// agent, returning the offending agent identifiers so we can list them.
function getDeleteConflict(error: unknown): AiConnectionStringDeleteConflictResponse | null {
    if (
        isApiError<AiConnectionStringDeleteConflictResponse>(error) &&
        error.status === 409 &&
        error.details != null &&
        Array.isArray(error.details.referencingAgentIds)
    ) {
        return error.details;
    }
    return null;
}

export function DeleteAiConnectionStringDialog({ slug, name, trigger }: DeleteAiConnectionStringDialogProps) {
    const [isOpen, setIsOpen] = useState(false);
    const queryClient = useQueryClient();

    const deleteMutation = useMutation({
        mutationFn: () => api.services.aiConnectionStrings.delete(slug, name),
        onSuccess: async () => {
            await Promise.all([
                queryClient.invalidateQueries({ queryKey: api.queries.aiConnectionStrings.list(slug).queryKey }),
                // Drop the deleted string's cached detail so a stale edit sheet can't resurrect it.
                queryClient.invalidateQueries({
                    queryKey: api.queries.aiConnectionStrings.detail(slug, name).queryKey,
                }),
            ]);
            toast.success(`Connection string “${name}” deleted`);
            setIsOpen(false);
        },
    });

    const conflict = getDeleteConflict(deleteMutation.error);

    return (
        <Dialog
            open={isOpen}
            onOpenChange={(open) => {
                setIsOpen(open);
                if (!open) {
                    deleteMutation.reset();
                }
            }}
        >
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent>
                <DialogHeader>
                    <DialogTitle>Delete connection string?</DialogTitle>
                    <DialogDescription>“{name}” will be permanently removed. This can’t be undone.</DialogDescription>
                </DialogHeader>

                {deleteMutation.isError &&
                    (conflict ? (
                        <Alert variant="destructive">
                            <p>
                                This connection string is still used by{" "}
                                {conflict.referencingAgentIds.length === 1 ? "an agent" : "agents"}. Remove{" "}
                                {conflict.referencingAgentIds.length === 1 ? "it" : "them"} first:
                            </p>
                            <ul className="mt-1 list-disc pl-5">
                                {conflict.referencingAgentIds.map((agentId) => (
                                    <li key={agentId} className="font-mono text-xs break-all">
                                        {agentId}
                                    </li>
                                ))}
                            </ul>
                        </Alert>
                    ) : (
                        <Alert variant="destructive">
                            {deleteMutation.error instanceof Error
                                ? deleteMutation.error.message
                                : "Could not delete connection string."}
                        </Alert>
                    ))}

                <DialogFooter>
                    <DialogClose asChild>
                        <Button type="button" variant="outline">
                            Cancel
                        </Button>
                    </DialogClose>
                    <Button
                        type="button"
                        variant="destructive"
                        disabled={deleteMutation.isPending}
                        onClick={() => deleteMutation.mutate()}
                    >
                        {deleteMutation.isPending && <Spinner />}
                        Delete
                    </Button>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
}

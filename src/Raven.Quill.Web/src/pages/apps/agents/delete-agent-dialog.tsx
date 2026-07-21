import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AgentSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { invalidateAgentQueries } from "@/lib/query-invalidation";
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

type DeleteAgentDialogProps = {
    slug: string;
    agent: AgentSummaryResponse;
    trigger: ReactNode;
};

export function DeleteAgentDialog({ slug, agent, trigger }: DeleteAgentDialogProps) {
    const [isOpen, setIsOpen] = useState(false);
    const queryClient = useQueryClient();

    const deleteMutation = useMutation({
        mutationFn: () => api.services.agents.delete(slug, agent.agentId),
        onSuccess: async () => {
            await Promise.all([
                invalidateAgentQueries(queryClient, slug),
                // Drop the deleted agent's cached configuration so a stale edit page can't resurrect it.
                queryClient.invalidateQueries({
                    queryKey: api.queries.agents.detail(slug, agent.agentId).queryKey,
                }),
            ]);
            toast.success(`Agent “${agent.name}” deleted`);
            setIsOpen(false);
        },
    });

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
                    <DialogTitle>Delete agent?</DialogTitle>
                    <DialogDescription>
                        “{agent.name}” will be permanently removed. This can’t be undone.
                    </DialogDescription>
                </DialogHeader>

                {deleteMutation.isError && (
                    <Alert variant="destructive">
                        {deleteMutation.error instanceof Error
                            ? deleteMutation.error.message
                            : "Could not delete agent."}
                    </Alert>
                )}

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

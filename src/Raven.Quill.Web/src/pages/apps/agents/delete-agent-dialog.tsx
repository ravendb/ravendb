import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AgentSummaryResponse } from "@/api/generated/server-api";
import { DestructiveConfirmDialog } from "@/components/shadcn/ui/destructive-confirm-dialog";
import { invalidateAgentQueries } from "@/lib/query-invalidation";

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
        <DestructiveConfirmDialog
            trigger={trigger}
            title="Delete agent?"
            description={`“${agent.name}” will be permanently removed. This can’t be undone.`}
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
            error={
                deleteMutation.isError
                    ? deleteMutation.error instanceof Error
                        ? deleteMutation.error.message
                        : "Could not delete agent."
                    : undefined
            }
        />
    );
}

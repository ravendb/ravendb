import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { RotateCw } from "lucide-react";
import { toast } from "sonner";
import { api } from "@/api/api";
import { isApiError } from "@/api/http-client";
import { Button } from "@/components/shadcn/ui/button";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { Spinner } from "@/components/shadcn/ui/spinner";

// Always visible: a stalled sync often records no error at all, so gating the button on the error
// list would hide it exactly when it is needed. Errors only raise its emphasis.
export function RestartSyncButton({ slug }: { slug: string }) {
    const [isConfirmOpen, setIsConfirmOpen] = useState(false);
    const queryClient = useQueryClient();
    const errorsQuery = useQuery(api.queries.apps.cdcErrors(slug));
    const hasSyncErrors = (errorsQuery.data?.length ?? 0) > 0;

    const restartMutation = useMutation({
        mutationFn: () => api.services.apps.cdcRestart(slug),
        onSuccess: async () => {
            await Promise.all([
                queryClient.invalidateQueries({ queryKey: api.queries.stats.dashboardApp(slug).queryKey }),
                queryClient.invalidateQueries({ queryKey: api.queries.apps.cdcErrors(slug).queryKey }),
            ]);
            toast.success("Sync restarted");
        },
        onError: (error) => {
            toast.error(resolveRestartError(error));
        },
    });

    return (
        <>
            <Button
                variant={hasSyncErrors ? "default" : "outline"}
                size="sm"
                onClick={() => setIsConfirmOpen(true)}
                disabled={restartMutation.isPending}
            >
                {restartMutation.isPending ? <Spinner /> : <RotateCw aria-hidden="true" />}
                Restart sync
            </Button>

            <ConfirmDialog
                open={isConfirmOpen}
                onOpenChange={setIsConfirmOpen}
                title="Restart sync?"
                description="Syncing stops and starts again from the last position it saved. Nothing already synced is lost, but new changes can take a moment to arrive while the source database reconnects."
                confirmLabel="Restart"
                onConfirm={() => restartMutation.mutate()}
            />
        </>
    );
}

function resolveRestartError(error: unknown) {
    if (isApiError(error) && error.status === 409) {
        return "Sync is turned off for this data source, so there is nothing to restart.";
    }

    return error instanceof Error ? error.message : "Could not restart the sync.";
}

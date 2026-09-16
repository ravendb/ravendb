import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import { isApiError } from "@/api/http-client";

export function useRestartSync(slug: string) {
    const [isConfirmOpen, setIsConfirmOpen] = useState(false);
    const queryClient = useQueryClient();
    const errorsQuery = useQuery(api.queries.apps.cdcErrors(slug));

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

    return {
        hasSyncErrors: (errorsQuery.data?.length ?? 0) > 0,
        isRestarting: restartMutation.isPending,
        confirm: () => setIsConfirmOpen(true),
        dialogProps: {
            open: isConfirmOpen,
            onOpenChange: setIsConfirmOpen,
            onConfirm: () => restartMutation.mutate(),
        },
    };
}

function resolveRestartError(error: unknown) {
    if (isApiError(error) && error.status === 409) {
        return "Sync is turned off for this data source, so there is nothing to restart.";
    }

    return error instanceof Error ? error.message : "Could not restart the sync.";
}

import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useLocation, useNavigate } from "react-router";
import { toast } from "sonner";
import { api } from "@/api/api";
import { DestructiveConfirmDialog } from "@/components/shadcn/ui/destructive-confirm-dialog";
import { appRoutes } from "@/lib/app-routes";
import { invalidateAppQueries } from "@/lib/query-invalidation";

type DeleteAppDialogProps = {
    slug: string;
    appName: string;
    /** Omit when the dialog is driven by `open`/`onOpenChange` (e.g. from a dropdown menu item). */
    trigger?: ReactNode;
    open?: boolean;
    onOpenChange?: (open: boolean) => void;
};

export function DeleteAppDialog({ slug, appName, trigger, open, onOpenChange }: DeleteAppDialogProps) {
    const [uncontrolledOpen, setUncontrolledOpen] = useState(false);
    const isOpen = open ?? uncontrolledOpen;
    const setIsOpen = onOpenChange ?? setUncontrolledOpen;
    const queryClient = useQueryClient();
    const navigate = useNavigate();
    const location = useLocation();

    const deleteMutation = useMutation({
        mutationFn: () => api.services.apps.delete(slug),
        onSuccess: async () => {
            // When inside the deleted app, leave its routes before touching the cache,
            // so RequireApp doesn't refetch the deleted app and flash its 404 state.
            if (location.pathname.startsWith(appRoutes.app(slug))) {
                void navigate(appRoutes.dashboard());
            }
            queryClient.removeQueries({ queryKey: api.queries.apps.detail(slug).queryKey });
            await invalidateAppQueries(queryClient);
            toast.success(`App “${appName}” deleted`);
            setIsOpen(false);
        },
    });

    return (
        <DestructiveConfirmDialog
            trigger={trigger}
            title="Delete app?"
            description={`“${appName}” and all of its agents, channels, and conversations will be permanently removed. This can’t be undone.`}
            confirmLabel="Delete"
            // Deleting an app cascades into every agent, channel and conversation it owns,
            // and the dashboard button is the only way to trigger it, so make the operator
            // retype the name rather than guarding a cascade behind a single click.
            confirmationText={appName}
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
                        : "Could not delete app."
                    : undefined
            }
        />
    );
}

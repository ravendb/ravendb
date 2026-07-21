import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useLocation, useNavigate } from "react-router";
import { toast } from "sonner";
import { api } from "@/api/api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { appRoutes } from "@/lib/app-routes";
import { invalidateAppQueries } from "@/lib/query-invalidation";
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

type DeleteAppDialogProps = {
    slug: string;
    appName: string;
    trigger: ReactNode;
};

export function DeleteAppDialog({ slug, appName, trigger }: DeleteAppDialogProps) {
    const [isOpen, setIsOpen] = useState(false);
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
                    <DialogTitle>Delete app?</DialogTitle>
                    <DialogDescription>
                        "{appName}" and all of its agents, channels, and conversations will be permanently removed. This
                        can't be undone.
                    </DialogDescription>
                </DialogHeader>

                {deleteMutation.isError && (
                    <Alert variant="destructive">
                        {deleteMutation.error instanceof Error ? deleteMutation.error.message : "Could not delete app."}
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

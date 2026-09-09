import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { DestructiveConfirmDialog } from "@/components/shadcn/ui/destructive-confirm-dialog";
import { invalidateChannelQueries } from "@/lib/query-invalidation";

type DeleteChannelDialogProps = {
    slug: string;
    channel: ChannelSummaryResponse;
    /** Omit when the dialog is driven by `open`/`onOpenChange` (e.g. from a dropdown menu item). */
    trigger?: ReactNode;
    open?: boolean;
    onOpenChange?: (open: boolean) => void;
    /** Called after a successful delete, e.g. to navigate away from the deleted channel's own page. */
    onDeleted?: () => void;
};

export function DeleteChannelDialog({
    slug,
    channel,
    trigger,
    open,
    onOpenChange,
    onDeleted,
}: DeleteChannelDialogProps) {
    const [uncontrolledOpen, setUncontrolledOpen] = useState(false);
    const isOpen = open ?? uncontrolledOpen;
    const setIsOpen = onOpenChange ?? setUncontrolledOpen;
    const queryClient = useQueryClient();

    const deleteMutation = useMutation({
        mutationFn: () => api.services.channels.delete(slug, channel.channelId),
        onSuccess: async () => {
            await Promise.all([
                invalidateChannelQueries(queryClient, slug, channel.type),
                // Deleting a channel orphans its embed links, so drop them from the active-links view.
                queryClient.invalidateQueries({ queryKey: api.queries.embedLinks.list(slug).queryKey }),
            ]);
            toast.success(`Channel “${channel.displayName}” deleted`);
            setIsOpen(false);
            onDeleted?.();
        },
    });

    return (
        <DestructiveConfirmDialog
            trigger={trigger}
            title="Delete channel?"
            description={`“${channel.displayName}” will be permanently removed and widgets embedded with it will stop working. This can’t be undone.`}
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
                        : "Could not delete channel."
                    : undefined
            }
        />
    );
}

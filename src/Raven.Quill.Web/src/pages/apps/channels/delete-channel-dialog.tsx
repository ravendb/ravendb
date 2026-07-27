import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
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

type DeleteChannelDialogProps = {
    slug: string;
    channel: ChannelSummaryResponse;
    trigger: ReactNode;
};

export function DeleteChannelDialog({ slug, channel, trigger }: DeleteChannelDialogProps) {
    const [isOpen, setIsOpen] = useState(false);
    const queryClient = useQueryClient();

    const deleteMutation = useMutation({
        mutationFn: () => api.services.channels.delete(slug, channel.channelId),
        onSuccess: async () => {
            await Promise.all([
                invalidateChannelQueries(queryClient, slug),
                // Deleting a channel orphans its embed links, so drop them from the active-links view.
                queryClient.invalidateQueries({ queryKey: api.queries.embedLinks.list(slug).queryKey }),
            ]);
            toast.success(`Channel “${channel.displayName}” deleted`);
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
                    <DialogTitle>Delete channel?</DialogTitle>
                    <DialogDescription>
                        “{channel.displayName}” will be permanently removed and widgets embedded with it will stop
                        working. This can’t be undone.
                    </DialogDescription>
                </DialogHeader>

                {deleteMutation.isError && (
                    <Alert variant="destructive">
                        {deleteMutation.error instanceof Error
                            ? deleteMutation.error.message
                            : "Could not delete channel."}
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

import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
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

type RevokeEmbedLinkDialogProps = {
    slug: string;
    token: string;
    trigger: ReactNode;
};

export function RevokeEmbedLinkDialog({ slug, token, trigger }: RevokeEmbedLinkDialogProps) {
    const [isOpen, setIsOpen] = useState(false);
    const queryClient = useQueryClient();

    const revokeMutation = useMutation({
        mutationFn: () => api.services.embedLinks.revoke(slug, token),
        onSuccess: async () => {
            await queryClient.invalidateQueries({ queryKey: api.queries.embedLinks.list(slug).queryKey });
            toast.success("Embed link revoked");
            setIsOpen(false);
        },
    });

    return (
        <Dialog
            open={isOpen}
            onOpenChange={(open) => {
                setIsOpen(open);
                if (!open) {
                    revokeMutation.reset();
                }
            }}
        >
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent>
                <DialogHeader>
                    <DialogTitle>Revoke this link?</DialogTitle>
                    <DialogDescription>
                        The embedded widget using this link will stop working immediately — it can’t be undone. The
                        channel and any other links keep working.
                    </DialogDescription>
                </DialogHeader>

                {revokeMutation.isError && (
                    <Alert variant="destructive">
                        {revokeMutation.error instanceof Error
                            ? revokeMutation.error.message
                            : "Could not revoke link."}
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
                        disabled={revokeMutation.isPending}
                        onClick={() => revokeMutation.mutate()}
                    >
                        {revokeMutation.isPending && <Spinner />}
                        Revoke link
                    </Button>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
}

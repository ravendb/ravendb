import { useState, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/api";
import { DestructiveConfirmDialog } from "@/components/shadcn/ui/destructive-confirm-dialog";

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
        <DestructiveConfirmDialog
            trigger={trigger}
            title="Revoke this link?"
            description="The embedded widget using this link will stop working immediately — it can’t be undone. The channel and any other links keep working."
            confirmLabel="Revoke link"
            isOpen={isOpen}
            onOpenChange={(open) => {
                setIsOpen(open);
                if (!open) {
                    revokeMutation.reset();
                }
            }}
            onConfirm={() => revokeMutation.mutate()}
            isPending={revokeMutation.isPending}
            error={
                revokeMutation.isError
                    ? revokeMutation.error instanceof Error
                        ? revokeMutation.error.message
                        : "Could not revoke link."
                    : undefined
            }
        />
    );
}

import { useState, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { EmbedLinkSummaryResponse } from "@/api/generated/server-api";
import { Button } from "@/components/shadcn/ui/button";
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
import { EmbedLinkPreview } from "@/pages/apps/channels/embed-link-preview";
import { buildEmbedUrl } from "@/pages/apps/channels/embed-link-utils";

type PreviewEmbedLinkDialogProps = {
    slug: string;
    link: EmbedLinkSummaryResponse;
    trigger: ReactNode;
};

export function PreviewEmbedLinkDialog({ slug, link, trigger }: PreviewEmbedLinkDialogProps) {
    const [isOpen, setIsOpen] = useState(false);
    const queryClient = useQueryClient();

    return (
        <Dialog
            open={isOpen}
            onOpenChange={(open) => {
                setIsOpen(open);
                if (!open) {
                    queryClient.invalidateQueries({ queryKey: api.queries.embedLinks.list(slug).queryKey });
                }
            }}
        >
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-lg">
                <DialogHeader>
                    <DialogTitle>Embed link</DialogTitle>
                    <DialogDescription>
                        Copy this link or snippet to embed the agent, or check the live preview below.
                    </DialogDescription>
                </DialogHeader>

                <EmbedLinkPreview
                    url={buildEmbedUrl(link.token)}
                    expiresAt={link.expiresAt}
                    maxInvocations={link.maxInvocations}
                />

                <DialogFooter>
                    <DialogClose asChild>
                        <Button type="button" variant="outline">
                            Done
                        </Button>
                    </DialogClose>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
}

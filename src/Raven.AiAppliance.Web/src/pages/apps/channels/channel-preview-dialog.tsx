import type { ReactNode } from "react";
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/shadcn/ui/dialog";

type ChannelPreviewDialogProps = {
    widgetId: string;
    displayName: string;
    trigger: ReactNode;
};

// Frames the live /embed/{widgetId} page — the exact page customers iframe on
// their sites. The relative URL is same-origin in production (the appliance
// serves both) and proxied to the backend in dev (vite.config.ts).
export function ChannelPreviewDialog({ widgetId, displayName, trigger }: ChannelPreviewDialogProps) {
    const embedUrl = `/embed/${widgetId}`;

    return (
        <Dialog>
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent className="sm:max-w-md">
                <DialogHeader>
                    <DialogTitle>Widget preview</DialogTitle>
                    <DialogDescription>
                        How “{displayName}” looks when embedded on a site.{" "}
                        <a href={embedUrl} target="_blank" rel="noreferrer">
                            Open in a new tab
                        </a>
                    </DialogDescription>
                </DialogHeader>
                {/* The embed page styles itself light-only, so the backdrop stays white in dark mode too. */}
                <iframe
                    src={embedUrl}
                    title={`${displayName} widget preview`}
                    className="h-[min(60vh,600px)] w-full rounded-lg border bg-white"
                />
            </DialogContent>
        </Dialog>
    );
}

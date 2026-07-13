import { useMutation, useQueryClient, type QueryKey } from "@tanstack/react-query";
import { toast } from "sonner";
import type { WebWidgetStyleUpdate } from "@/pages/apps/channels/web-widget-style-editor";

// Shared save wiring for the web-widget style editors (per-channel and app-default): run the PUT,
// invalidate the affected queries so the editors re-read the saved style, and toast on success. A null
// style clears a channel's choice so it follows the app default (see WebWidgetStyleEditor).
export function useWebWidgetStyleSave(options: {
    save: (update: WebWidgetStyleUpdate) => Promise<unknown>;
    invalidateKeys: QueryKey[];
    successMessage: string;
}) {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: (update: WebWidgetStyleUpdate) => options.save(update),
        onSuccess: async () => {
            await Promise.all(options.invalidateKeys.map((queryKey) => queryClient.invalidateQueries({ queryKey })));
            toast.success(options.successMessage);
        },
    });
}

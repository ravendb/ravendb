import { useMutation, useQueryClient, type QueryKey } from "@tanstack/react-query";
import { toast } from "sonner";

// Shared save wiring for the web-widget style editors (per-channel and app-default): run the PUT,
// invalidate the source query so the editor re-reads the saved CSS, and toast on success. An empty
// string clears the stored CSS so the record falls back to its default (see WebWidgetStyleEditor).
export function useWebWidgetStyleSave(options: {
    save: (css: string | null) => Promise<unknown>;
    invalidateKey: QueryKey;
    successMessage: string;
}) {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: (css: string) => options.save(css || null),
        onSuccess: async () => {
            await queryClient.invalidateQueries({ queryKey: options.invalidateKey });
            toast.success(options.successMessage);
        },
    });
}

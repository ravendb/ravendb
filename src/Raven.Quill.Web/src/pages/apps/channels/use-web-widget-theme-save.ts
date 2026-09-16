import { useMutation, useQueryClient, type QueryKey } from "@tanstack/react-query";
import { toast } from "sonner";
import type { WidgetTheme } from "@/api/generated/server-api";

// Shared save wiring for the web-widget theme editors (per-channel and app-default): run the PUT, invalidate
// the affected queries so the editors re-read the saved theme, and toast on success. A null theme clears a
// channel's choice so it follows the app default (see WebWidgetThemeEditor).
export function useWebWidgetThemeSave(options: {
    save: (theme: WidgetTheme | null) => Promise<unknown>;
    invalidateKeys: QueryKey[];
    successMessage: string;
}) {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: (theme: WidgetTheme | null) => options.save(theme),
        onSuccess: async () => {
            await Promise.all(options.invalidateKeys.map((queryKey) => queryClient.invalidateQueries({ queryKey })));
            toast.success(options.successMessage);
        },
    });
}

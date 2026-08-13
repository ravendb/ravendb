import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "web-widget";

// Prefix matching every web widget's theme in an app. `theme` is built from it so the two can't drift,
// letting a default-theme save invalidate all widgets at once (see below).
const themesKey = (slug: string) => [baseKey, "theme", slug];

export function createWebWidgetQueries(api: ServerApi["iframe"]) {
    return {
        themesKey,
        theme: (slug: string, channelId: string) =>
            queryOptions({
                queryKey: [...themesKey(slug), channelId],
                queryFn: () => api.getTheme(slug, channelId),
            }),
        defaultTheme: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "default-theme", slug],
                queryFn: () => api.getDefaultTheme(slug),
            }),
    };
}

export type WebWidgetQueries = ReturnType<typeof createWebWidgetQueries>;

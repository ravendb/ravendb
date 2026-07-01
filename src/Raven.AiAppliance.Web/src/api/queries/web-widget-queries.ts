import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "web-widget";

export function createWebWidgetQueries(api: ServerApi["iframe"]) {
    return {
        customization: (slug: string, widgetId: string) =>
            queryOptions({
                queryKey: [baseKey, "customization", slug, widgetId],
                queryFn: () => api.getCustomization(slug, widgetId),
            }),
        defaultCustomization: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "default-customization", slug],
                queryFn: () => api.getDefaultCustomization(slug),
            }),
        preview: (slug: string, title?: string) =>
            queryOptions({
                queryKey: [baseKey, "preview", slug, title ?? ""],
                queryFn: () => api.preview(slug, title ? { title } : undefined),
            }),
        styleGuide: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "style-guide", slug],
                queryFn: () => api.getStyleGuide(slug),
            }),
    };
}

export type WebWidgetQueries = ReturnType<typeof createWebWidgetQueries>;

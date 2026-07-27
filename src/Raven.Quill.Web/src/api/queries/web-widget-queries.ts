import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "web-widget";

// Prefix matching every web widget's customization in an app. `customization` is built from it so
// the two can't drift, letting a default-styles save invalidate all channels at once (see below).
const customizationsKey = (slug: string) => [baseKey, "customization", slug];

export function createWebWidgetQueries(api: ServerApi["iframe"]) {
    return {
        customizationsKey,
        customization: (slug: string, channelId: string) =>
            queryOptions({
                queryKey: [...customizationsKey(slug), channelId],
                queryFn: () => api.getCustomization(slug, channelId),
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

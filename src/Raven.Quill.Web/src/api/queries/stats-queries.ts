import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";
import { datePeriodToSearchParams, type DatePeriod } from "@/lib/date-period";

const baseKey = "stats";

export function createStatsQueries(api: ServerApi["stats"]) {
    return {
        dashboardApps: () =>
            queryOptions({
                queryKey: [baseKey, "dashboardApps"],
                queryFn: () => api.dashboardApps(),
            }),
        dashboardApp: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "dashboardApp", slug],
                queryFn: () => api.dashboardApp(slug),
            }),
        usage: (period: DatePeriod, app?: string) =>
            queryOptions({
                queryKey: [baseKey, "usage", period.year, period.month, period.day, app ?? null],
                queryFn: () => api.usage({ ...datePeriodToSearchParams(period), app }),
            }),
        tokensByApp: () =>
            queryOptions({
                queryKey: [baseKey, "tokensByApp"],
                queryFn: () => api.tokensByApp(),
            }),
        overview: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "overview", slug],
                queryFn: () => api.overview(slug),
            }),
        appUsage: (slug: string, period: DatePeriod) =>
            queryOptions({
                queryKey: [baseKey, "appUsage", slug, period.year, period.month, period.day],
                queryFn: () => api.appUsage(slug, datePeriodToSearchParams(period)),
            }),
        collections: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "collections", slug],
                queryFn: () => api.collections(slug),
            }),
        conversations: (slug: string, period: DatePeriod) =>
            queryOptions({
                queryKey: [baseKey, "conversations", slug, period.year, period.month, period.day],
                queryFn: () => api.conversations(slug, datePeriodToSearchParams(period)),
            }),
        conversation: (slug: string, conversationId: string) =>
            queryOptions({
                queryKey: [baseKey, "conversation", slug, conversationId],
                queryFn: () => api.conversation(slug, conversationId),
            }),
        conversationStats: (slug: string, period: DatePeriod) =>
            queryOptions({
                queryKey: [baseKey, "conversationStats", slug, period.year, period.month, period.day],
                queryFn: () => api.conversationStats(slug, datePeriodToSearchParams(period)),
            }),
        channels: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "channels", slug],
                queryFn: () => api.channels(slug),
            }),
    };
}

export type StatsQueries = ReturnType<typeof createStatsQueries>;

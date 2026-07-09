import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "settings";

export function createSettingsQueries(api: ServerApi["settings"]) {
    return {
        license: () =>
            queryOptions({
                queryKey: [baseKey, "license"],
                queryFn: () => api.license(),
            }),
        usage: (year?: number, month?: number) =>
            queryOptions({
                queryKey: [baseKey, "usage", year ?? null, month ?? null],
                queryFn: () => api.usage(year && month ? { year: String(year), month: String(month) } : undefined),
            }),
    };
}

export type SettingsQueries = ReturnType<typeof createSettingsQueries>;

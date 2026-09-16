import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";
import { datePeriodToSearchParams, type DatePeriod } from "@/lib/date-period";

const baseKey = "settings";

export function createSettingsQueries(api: ServerApi["settings"]) {
    return {
        license: () =>
            queryOptions({
                queryKey: [baseKey, "license"],
                queryFn: () => api.license(),
            }),
        usage: (period: DatePeriod) =>
            queryOptions({
                queryKey: [baseKey, "usage", period.year, period.month, period.day],
                queryFn: () => api.usage(datePeriodToSearchParams(period)),
            }),
    };
}

export type SettingsQueries = ReturnType<typeof createSettingsQueries>;

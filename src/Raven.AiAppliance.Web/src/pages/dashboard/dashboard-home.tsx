import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { parseISO, subDays } from "date-fns";
import { Clock } from "lucide-react";
import { api } from "@/api/api";
import type { LicenseResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { WindowTabs, type WindowKey } from "@/components/data/window-tabs";
import { Badge } from "@/components/shadcn/ui/badge";
import { DashboardAppsTable } from "@/pages/dashboard/dashboard-apps-table";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";

// Rolling-window length in days per key, to sum the day-bucketed monthly writes.
const WINDOW_DAYS: Record<WindowKey, number> = {
    last24h: 1,
    last7d: 7,
    last30d: 30,
};

export function DashboardHome() {
    const [windowKey, setWindowKey] = useState<WindowKey>("last7d");

    const dashboardQuery = useQuery(api.queries.stats.dashboard());
    const appsQuery = useQuery(api.queries.stats.dashboardApps());
    const licenseQuery = useQuery(api.queries.settings.license());

    const windowData = dashboardQuery.data?.[windowKey];

    // Writes are only available as monthly day-buckets, but a rolling window can reach into
    // the previous calendar month — so fetch that month too when the cutoff predates the
    // current one, then sum the days inside the window (otherwise the total drops the days
    // before the 1st and under-counts).
    const now = new Date();
    const todayUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    const cutoff = subDays(todayUtc, WINDOW_DAYS[windowKey] - 1);
    const monthStartUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
    const needsPreviousMonth = cutoff < monthStartUtc;
    const previousMonth = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1));

    const currentMonthWritesQuery = useQuery(api.queries.settings.usage());
    const previousMonthWritesQuery = useQuery({
        ...api.queries.settings.usage(previousMonth.getUTCFullYear(), previousMonth.getUTCMonth() + 1),
        enabled: needsPreviousMonth,
    });

    const isWritesLoading =
        currentMonthWritesQuery.isPending || (needsPreviousMonth && previousMonthWritesQuery.isPending);
    const writesDays = [...(previousMonthWritesQuery.data?.days ?? []), ...(currentMonthWritesQuery.data?.days ?? [])]
        .filter((day) => {
            const date = parseISO(`${day.date}T00:00:00Z`);
            return date >= cutoff && date <= todayUtc;
        })
        .sort((a, b) => a.date.localeCompare(b.date));
    const writesValue = isWritesLoading ? undefined : writesDays.reduce((sum, day) => sum + day.writes, 0);

    const cards: DashboardStatCard[] = [
        {
            label: "Conversations",
            value: windowData?.conversations,
            isLoading: dashboardQuery.isPending,
        },
        {
            label: "Messages",
            value: windowData?.messages,
            isLoading: dashboardQuery.isPending,
        },
        {
            label: "Tokens",
            value: windowData?.tokens,
            isLoading: dashboardQuery.isPending,
        },
        {
            label: "Writes",
            value: writesValue,
            isLoading: isWritesLoading,
            series: isWritesLoading ? undefined : writesDays.map((day) => day.writes),
        },
    ];

    return (
        <div className="space-y-6">
            <header className="flex items-center justify-between gap-3">
                <h1 className="text-2xl font-semibold tracking-tight">My apps</h1>
                <TrialPill license={licenseQuery.data} />
            </header>

            <div className="space-y-4">
                <div className="flex justify-end">
                    <WindowTabs value={windowKey} onChange={setWindowKey} />
                </div>
                <DashboardStatCards cards={cards} />
            </div>

            <ApiState
                isLoading={appsQuery.isPending}
                isError={appsQuery.isError}
                errorTitle="Could not load apps"
                onRetry={() => appsQuery.refetch()}
                loadingLabel="Loading apps…"
            >
                {appsQuery.data && <DashboardAppsTable apps={appsQuery.data} />}
            </ApiState>
        </div>
    );
}

function TrialPill({ license }: { license: LicenseResponse | undefined }) {
    if (!license || license.tier !== "Trial" || license.daysLeft <= 0) {
        return null;
    }

    return (
        <Badge variant="warning" className="gap-1.5">
            <Clock aria-hidden="true" />
            {license.daysLeft} {license.daysLeft === 1 ? "day" : "days"} left in trial
        </Badge>
    );
}

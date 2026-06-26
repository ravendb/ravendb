import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { parseISO, subDays } from "date-fns";
import { Clock } from "lucide-react";
import { api } from "@/api/api";
import type { LicenseResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { Tabs, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { DashboardAppsTable } from "@/pages/dashboard/dashboard-apps-table";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";

type WindowKey = "last24h" | "last7d" | "last30d";

const WINDOW_OPTIONS: { value: WindowKey; label: string; days: number }[] = [
    { value: "last24h", label: "Last 24 hours", days: 1 },
    { value: "last7d", label: "Last 7 days", days: 7 },
    { value: "last30d", label: "Last month", days: 30 },
];

export function DashboardHome() {
    const [windowKey, setWindowKey] = useState<WindowKey>("last7d");

    const dashboardQuery = useQuery(api.queries.stats.dashboard());
    const monthlyWritesQuery = useQuery(api.queries.settings.usage());
    const appsQuery = useQuery(api.queries.stats.dashboardApps());
    const licenseQuery = useQuery(api.queries.settings.license());

    const windowData = dashboardQuery.data?.[windowKey];
    const monthlyWrites = monthlyWritesQuery.data;

    const windowDays = WINDOW_OPTIONS.find((option) => option.value === windowKey)?.days ?? 0;
    const now = new Date();
    const todayUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    const cutoff = subDays(todayUtc, windowDays - 1);
    const writesDays = monthlyWrites?.days.filter((day) => {
        const date = parseISO(`${day.date}T00:00:00Z`);
        return date >= cutoff && date <= todayUtc;
    });
    const writesValue = writesDays?.reduce((sum, day) => sum + day.writes, 0);

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
            isLoading: monthlyWritesQuery.isPending,
            series: writesDays?.map((day) => day.writes),
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
                    <Tabs value={windowKey} onValueChange={(value) => setWindowKey(value as WindowKey)}>
                        <TabsList>
                            {WINDOW_OPTIONS.map((option) => (
                                <TabsTrigger key={option.value} value={option.value}>
                                    {option.label}
                                </TabsTrigger>
                            ))}
                        </TabsList>
                    </Tabs>
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

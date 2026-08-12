import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { getDefaultDatePeriod } from "@/lib/date-period";
import { DashboardAppsTable } from "@/pages/dashboard/dashboard-apps-table";
import { StatCardsSection } from "@/pages/dashboard/dashboard-stat-cards";
import { buildUsageStatCards } from "@/pages/dashboard/usage-stat-cards";

export function DashboardHome() {
    const [period, setPeriod] = useState(getDefaultDatePeriod);

    const usageQuery = useQuery(api.queries.stats.usage(period));
    const appsQuery = useQuery(api.queries.stats.dashboardApps());

    const cards = buildUsageStatCards(usageQuery.data, usageQuery.isPending);

    return (
        <div className="space-y-6">
            <header className="flex items-center justify-between gap-3">
                <h1 className="text-2xl font-semibold tracking-tight">My apps</h1>
            </header>

            {appsQuery.data && appsQuery.data.length > 0 && (
                // The period also drives the apps table below, so the picker stays at page level.
                <div className="space-y-4">
                    <div className="flex justify-end">
                        <DatePeriodPicker value={period} onChange={setPeriod} />
                    </div>
                    <StatCardsSection cards={cards} />
                </div>
            )}

            <ApiState
                isLoading={appsQuery.isPending}
                isError={appsQuery.isError}
                errorTitle="Could not load apps"
                onRetry={() => appsQuery.refetch()}
                loadingLabel="Loading apps…"
            >
                {appsQuery.data && (
                    <DashboardAppsTable
                        apps={appsQuery.data}
                        period={period}
                        writesByApp={usageQuery.data?.writesByApp}
                    />
                )}
            </ApiState>
        </div>
    );
}

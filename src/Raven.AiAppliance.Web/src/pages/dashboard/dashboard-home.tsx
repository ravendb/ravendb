import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { USAGE_WINDOW_BY_KEY } from "@/components/data/usage-window";
import { WindowTabs, type WindowKey } from "@/components/data/window-tabs";
import { DashboardAppsTable } from "@/pages/dashboard/dashboard-apps-table";
import { DashboardStatCards } from "@/pages/dashboard/dashboard-stat-cards";
import { buildUsageStatCards } from "@/pages/dashboard/usage-stat-cards";

export function DashboardHome() {
    const [windowKey, setWindowKey] = useState<WindowKey>("last7d");

    const usageQuery = useQuery(api.queries.stats.usage(USAGE_WINDOW_BY_KEY[windowKey]));
    const appsQuery = useQuery(api.queries.stats.dashboardApps());

    const cards = buildUsageStatCards(usageQuery.data, usageQuery.isPending);

    return (
        <div className="space-y-6">
            <header className="flex items-center justify-between gap-3">
                <h1 className="text-2xl font-semibold tracking-tight">My apps</h1>
            </header>

            {appsQuery.data && appsQuery.data.length > 0 && (
                <div className="space-y-4">
                    <div className="flex justify-end">
                        <WindowTabs value={windowKey} onChange={setWindowKey} />
                    </div>
                    <DashboardStatCards cards={cards} />
                </div>
            )}

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

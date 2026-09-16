import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { getDefaultDatePeriod } from "@/lib/date-period";
import { useSetupStartDate } from "@/lib/use-start-date";
import { DashboardAppsTable, DashboardAppsTableSkeleton } from "@/pages/dashboard/dashboard-apps-table";
import { StatCardsSection } from "@/pages/dashboard/dashboard-stat-cards";
import { buildUsageStatCards } from "@/pages/dashboard/usage-stat-cards";
import { Heading } from "@/components/typography";

export function DashboardHome() {
    const [period, setPeriod] = useState(getDefaultDatePeriod);
    const setupStartDate = useSetupStartDate();

    const usageQuery = useQuery(api.queries.stats.usage(period));
    const appsQuery = useQuery(api.queries.stats.dashboardApps());

    const cards = buildUsageStatCards(usageQuery.data, usageQuery.isPending);
    const hasApps = Boolean(appsQuery.data && appsQuery.data.length > 0);

    return (
        <div className="space-y-6">
            <header className="flex items-center justify-between gap-3">
                <Heading as="h1" variant="page">
                    My apps
                </Heading>
                {hasApps && <DatePeriodPicker value={period} earliest={setupStartDate} onChange={setPeriod} />}
            </header>

            {hasApps && <StatCardsSection cards={cards} />}

            <ApiState
                isLoading={appsQuery.isPending}
                isError={appsQuery.isError}
                errorTitle="Could not load apps"
                onRetry={() => appsQuery.refetch()}
                loadingLabel="Loading apps…"
                skeleton={<DashboardAppsTableSkeleton period={period} />}
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

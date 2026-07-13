import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { AppResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { PagePanel } from "@/components/data/page-panel";
import { CdcPerformanceSection } from "@/pages/apps/cdc-performance-section";
import { CollectionsSection } from "@/pages/apps/collections-section";
import { formatDateTime } from "@/lib/utils";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard } from "@/pages/apps/section-card";

export function AppDataSource() {
    const { slug = "" } = useParams();
    const appQuery = useQuery(api.queries.apps.detail(slug));

    return (
        <PagePanel>
            <div className="space-y-8">
                <SectionCard title="Connection">
                    <ApiState
                        isLoading={appQuery.isPending}
                        onRetry={appQuery.refetch}
                        isError={appQuery.isError}
                        errorTitle="Could not load data source"
                    >
                        {appQuery.data && <ConnectionCards app={appQuery.data} />}
                    </ApiState>
                </SectionCard>
                <CdcPerformanceSection slug={slug} />
                <CollectionsSection slug={slug} />
            </div>
        </PagePanel>
    );
}

function ConnectionCards({ app }: { app: AppResponse }) {
    const cards: DashboardStatCard[] = [
        { label: "Application", value: undefined, valueLabel: app.name, isLoading: false },
        { label: "Source database", value: undefined, valueLabel: app.database, isLoading: false },
        { label: "Created at", value: undefined, valueLabel: formatDateTime(app.createdAt), isLoading: false },
    ];

    return <DashboardStatCards cards={cards} />;
}

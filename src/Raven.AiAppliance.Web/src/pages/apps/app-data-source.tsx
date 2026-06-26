import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { DetailList } from "@/components/data/detail-list";
import { PagePanel } from "@/components/data/page-panel";
import { CdcPerformanceSection } from "@/pages/apps/cdc-performance-section";
import { CollectionsSection } from "@/pages/apps/collections-section";
import { formatDateTime } from "@/lib/utils";

export function AppDataSource() {
    const { slug = "" } = useParams();
    const appQuery = useQuery(api.queries.apps.detail(slug));

    return (
        <PagePanel>
            <div className="space-y-8">
                <section className="space-y-4">
                    <h2 className="text-sm font-semibold">Connection</h2>
                    <ApiState
                        isLoading={appQuery.isPending}
                        onRetry={appQuery.refetch}
                        isError={appQuery.isError}
                        errorTitle="Could not load data source"
                    >
                        {appQuery.data && (
                            <DetailList
                                items={[
                                    { label: "Application", value: appQuery.data.name },
                                    { label: "Source database", value: appQuery.data.database },
                                    { label: "Created at", value: formatDateTime(appQuery.data.createdAt) },
                                ]}
                            />
                        )}
                    </ApiState>
                </section>
                <CdcPerformanceSection slug={slug} />
                <CollectionsSection slug={slug} />
            </div>
        </PagePanel>
    );
}

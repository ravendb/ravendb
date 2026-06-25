import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { DetailList } from "@/components/data/detail-list";
import { PagePanel } from "@/components/data/page-panel";
import { RawDataPreview } from "@/components/data/raw-data-preview";

export function AppDataSource() {
    const { slug = "" } = useParams();
    const appQuery = useQuery(api.queries.apps.detail(slug));
    const collectionsQuery = useQuery(api.queries.stats.collections(slug));
    const cdcPerformanceQuery = useQuery(api.queries.apps.cdcPerformance(slug));

    return (
        <PagePanel>
            <div className="space-y-6">
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
                                { label: "Created at", value: appQuery.data.createdAt },
                            ]}
                        />
                    )}
                </ApiState>
                <RawDataPreview title="stats.collections" query={collectionsQuery} />
                <RawDataPreview title="apps.cdcPerformance" query={cdcPerformanceQuery} />
            </div>
        </PagePanel>
    );
}

import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { DetailList } from "@/components/data/detail-list";
import { PagePanel } from "@/components/data/page-panel";

export function AppDataSource() {
    const { slug = "" } = useParams();
    const appQuery = useQuery(api.queries.apps.detail(slug));

    return (
        <PagePanel>
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
        </PagePanel>
    );
}

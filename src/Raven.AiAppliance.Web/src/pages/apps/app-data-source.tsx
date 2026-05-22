import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { DetailList } from "@/components/data/detail-list";
import { PagePanel } from "@/components/data/page-panel";

export function AppDataSource() {
    const { appId = "" } = useParams();
    const appQuery = useQuery(api.queries.apps.detail(appId));

    return (
        <PagePanel>
            <ApiState
                isLoading={appQuery.isPending}
                isError={appQuery.isError}
                errorTitle="Could not load data source"
                onRetry={() => void appQuery.refetch()}
            >
                {appQuery.data && (
                    <DetailList
                        items={[
                            { label: "Source database", value: appQuery.data.database },
                            { label: "Application", value: appQuery.data.name },
                            { label: "App id", value: appQuery.data.id },
                        ]}
                    />
                )}
            </ApiState>
        </PagePanel>
    );
}

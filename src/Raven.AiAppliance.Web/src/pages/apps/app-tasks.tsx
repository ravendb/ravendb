import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { DetailList } from "@/components/data/detail-list";
import { PagePanel } from "@/components/data/page-panel";

export function AppTasks() {
    const { slug = "" } = useParams();
    const appQuery = useQuery(api.queries.apps.detail(slug));

    return (
        <PagePanel>
            <ApiState
                isLoading={appQuery.isPending}
                isError={appQuery.isError}
                errorTitle="Could not load tasks"
                onRetry={() => void appQuery.refetch()}
            >
                {appQuery.data && (
                    <DetailList
                        items={[
                            { label: "Task type", value: "CDC sink" },
                            { label: "Task name", value: appQuery.data.cdcTaskName },
                            { label: "Database", value: appQuery.data.database },
                        ]}
                    />
                )}
            </ApiState>
        </PagePanel>
    );
}

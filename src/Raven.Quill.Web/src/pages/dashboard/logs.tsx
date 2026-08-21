import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { LogSettingsForm } from "./logs/log-settings-form";

export function DashboardLogs() {
    const configurationQuery = useQuery(api.queries.settings.logConfiguration());

    return (
        <div className="space-y-6">
            <div className="mb-2 space-y-1">
                <h1 className="text-2xl font-semibold tracking-tight">Logs</h1>
                <p className="text-sm text-muted-foreground">Where the appliance writes, and how much of it.</p>
            </div>

            <ApiState
                isLoading={configurationQuery.isPending}
                isError={configurationQuery.isError}
                errorTitle="Could not load log settings"
                onRetry={() => configurationQuery.refetch()}
                loadingLabel="Loading log settings…"
            >
                {configurationQuery.data && <LogSettingsForm configuration={configurationQuery.data} />}
            </ApiState>
        </div>
    );
}

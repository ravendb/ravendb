import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { AuditLogCard } from "./logs/audit-log-card";
import { LogConfigurationForm } from "./logs/log-configuration-form";

export function DashboardLogs() {
    const configurationQuery = useQuery(api.queries.settings.logConfiguration());

    return (
        <div className="space-y-6">
            <div className="space-y-1">
                <h1 className="text-2xl font-semibold tracking-tight">Logs</h1>
                <p className="text-sm text-muted-foreground">Log levels and the log file for the running appliance.</p>
            </div>

            <ApiState
                isLoading={configurationQuery.isPending}
                isError={configurationQuery.isError}
                errorTitle="Could not load log settings"
                onRetry={() => configurationQuery.refetch()}
                loadingLabel="Loading log settings…"
            >
                {configurationQuery.data && (
                    <>
                        <LogConfigurationForm configuration={configurationQuery.data} />
                        <AuditLogCard configuration={configurationQuery.data} />
                    </>
                )}
            </ApiState>
        </div>
    );
}

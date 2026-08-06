import { useQuery } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";
import { Link, useParams } from "react-router";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { appRoutes } from "@/lib/app-routes";
import { EditAgentForm } from "@/pages/apps/agents/edit-agent-form";

export function AppAgentEdit() {
    const { slug = "", agentId = "" } = useParams();

    const agentQuery = useQuery(api.queries.agents.detail(slug, agentId));
    const connectionStringsQuery = useQuery(api.queries.apps.aiConnectionStringsList(slug));

    const onRetry = async () => {
        if (agentQuery.isError) {
            await agentQuery.refetch();
        }
        if (connectionStringsQuery.isError) {
            await connectionStringsQuery.refetch();
        }
    };

    return (
        <div className="flex h-full min-h-0 flex-col gap-5">
            <Link
                to={appRoutes.app(slug, "agents")}
                className="inline-flex w-fit items-center gap-1.5 text-sm text-muted-foreground transition-colors hover:text-foreground"
            >
                <ArrowLeft className="size-3.5" aria-hidden="true" />
                Agents
            </Link>

            <ApiState
                isLoading={agentQuery.isPending || connectionStringsQuery.isPending}
                isError={agentQuery.isError || connectionStringsQuery.isError}
                errorTitle="Could not load agent"
                onRetry={onRetry}
                loadingLabel="Loading agent..."
            >
                {agentQuery.data && connectionStringsQuery.data && (
                    <EditAgentForm
                        slug={slug}
                        agentId={agentId}
                        config={agentQuery.data.configuration}
                        actionBindings={agentQuery.data.actionBindings}
                        connectionStrings={connectionStringsQuery.data}
                    />
                )}
            </ApiState>
        </div>
    );
}

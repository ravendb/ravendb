import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Cable, Cpu, Trash2 } from "lucide-react";
import { useNavigate, useParams } from "react-router";
import { api } from "@/api/api";
import type { AgentSummaryResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { DetailHeader, DetailHeaderMenu, DetailHeaderMetaItem } from "@/components/data/detail-header";
import { FormFieldsSkeleton } from "@/components/data/loading-skeletons";
import { EnabledStatus } from "@/components/data/status-indicator";
import { DropdownMenuItem } from "@/components/shadcn/ui/dropdown-menu";
import { appRoutes } from "@/lib/app-routes";
import { DeleteAgentDialog } from "@/pages/apps/agents/delete-agent-dialog";
import { EditAgentForm } from "@/pages/apps/agents/edit-agent-form";

export function AppAgentEdit() {
    const { slug = "", agentId = "" } = useParams();

    const agentQuery = useQuery(api.queries.agents.detail(slug, agentId));
    const agentsQuery = useQuery(api.queries.agents.list(slug));
    const channelsQuery = useQuery(api.queries.channels.list(slug));
    const connectionStringsQuery = useQuery(api.queries.apps.aiConnectionStringsList(slug));

    // The summary list carries the display fields (name, model, on/off) the detail response omits.
    const summary = agentsQuery.data?.find((candidate) => candidate.agentId === agentId);
    const config = agentQuery.data?.configuration;

    const name = summary?.name ?? config?.name ?? "Agent";
    const disabled = summary?.disabled ?? config?.disabled;
    const model = summary?.model ?? null;

    // Channels reference the agent (not the reverse), so the connection is resolved by matching them.
    const connectedChannels = channelsQuery.data?.filter((candidate) => candidate.agentId === agentId) ?? [];
    const connectedChannelsLabel = connectedChannels.map((candidate) => candidate.displayName).join(", ");

    const onRetry = async () => {
        if (agentQuery.isError) {
            await agentQuery.refetch();
        }
        if (connectionStringsQuery.isError) {
            await connectionStringsQuery.refetch();
        }
    };

    return (
        <div className="flex h-full min-h-0 flex-col">
            <DetailHeader
                title={name}
                status={disabled != null ? <EnabledStatus isEnabled={!disabled} /> : undefined}
                backTo={{ to: appRoutes.app(slug, "agents"), label: "Agents" }}
                meta={
                    <>
                        <DetailHeaderMetaItem icon={Cpu} tooltip="Model">
                            {model ?? "—"}
                        </DetailHeaderMetaItem>
                        {connectedChannels.length > 0 && (
                            <DetailHeaderMetaItem icon={Cable} tooltip="Channels">
                                {connectedChannelsLabel}
                            </DetailHeaderMetaItem>
                        )}
                    </>
                }
                actions={summary && <AgentActions slug={slug} agent={summary} />}
            />

            <ApiState
                isLoading={agentQuery.isPending || connectionStringsQuery.isPending}
                isError={agentQuery.isError || connectionStringsQuery.isError}
                errorTitle="Could not load agent"
                onRetry={onRetry}
                loadingLabel="Loading agent..."
                skeleton={<FormFieldsSkeleton count={5} />}
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

function AgentActions({ slug, agent }: { slug: string; agent: AgentSummaryResponse }) {
    const navigate = useNavigate();
    const [isDeleteOpen, setIsDeleteOpen] = useState(false);

    return (
        <>
            <DetailHeaderMenu>
                <DropdownMenuItem variant="destructive" onSelect={() => setIsDeleteOpen(true)}>
                    <Trash2 aria-hidden="true" />
                    Delete
                </DropdownMenuItem>
            </DetailHeaderMenu>

            <DeleteAgentDialog
                slug={slug}
                agent={agent}
                open={isDeleteOpen}
                onOpenChange={setIsDeleteOpen}
                onDeleted={() => navigate(appRoutes.app(slug, "agents"))}
            />
        </>
    );
}

import { useQuery } from "@tanstack/react-query";
import { useParams } from "react-router";
import { MessageSquarePlus } from "lucide-react";
import { api } from "@/api/api";
import { Alert } from "@/components/shadcn/ui/alert";
import { ApiState } from "@/components/data/api-state";
import { StatusIndicator } from "@/components/data/status-indicator";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { CHANNEL_TYPE_LABELS } from "@/lib/channel-type-labels";
import { AddChannelMenu } from "@/pages/apps/channels/add-channel-menu";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";

// The wizard's final, optional step. Reached only after the agent is provisioned; lets the
// operator attach channels to the new agent or finish and do it later.
export function ChannelsStep() {
    const { slug = "" } = useParams();
    const createdAgent = useCapabilityWizardStore((state) => state.createdAgent);
    const channelsQuery = useQuery({
        ...api.queries.channels.list(slug),
        enabled: Boolean(createdAgent),
    });

    // The flow only advances here after provisioning succeeds, so createdAgent is set; guard
    // defensively in case the step is rendered out of order.
    if (!createdAgent) {
        return <Alert>Create the agent first to add channels.</Alert>;
    }

    const channels = (channelsQuery.data ?? []).filter((channel) => channel.agentId === createdAgent.agentId);

    return (
        <div className="max-w-3xl">
            <ApiState
                isLoading={channelsQuery.isPending}
                isError={channelsQuery.isError}
                errorTitle="Could not load channels"
                onRetry={() => void channelsQuery.refetch()}
                loadingLabel="Loading channels..."
            >
                {channels.length === 0 ? (
                    // The first-run case for a brand-new agent: lead with the action so connecting a
                    // channel — not the "Finish" skip below — reads as the obvious next step.
                    <div className="flex flex-col items-center gap-4 rounded-lg border border-dashed px-6 py-12 text-center">
                        <span className="flex size-11 items-center justify-center rounded-full bg-muted text-muted-foreground">
                            <MessageSquarePlus className="size-5" aria-hidden="true" />
                        </span>
                        <div className="grid gap-1">
                            <p className="text-sm font-medium">No channels connected yet</p>
                            <p className="text-sm text-muted-foreground">
                                Connect one so people can start chatting with “{createdAgent.name}”.
                            </p>
                        </div>
                        <AddChannelMenu slug={slug} agent={createdAgent} />
                    </div>
                ) : (
                    <SectionCard
                        title={`Channels for “${createdAgent.name}”`}
                        action={<AddChannelMenu slug={slug} agent={createdAgent} />}
                    >
                        <SectionTable
                            headers={["Channel name", "Status", "Type"]}
                            isEmpty={false}
                            emptyMessage="No channels yet."
                        >
                            {channels.map((channel) => (
                                <TableRow key={channel.widgetId}>
                                    <TableCell className="font-medium">{channel.displayName}</TableCell>
                                    <TableCell>
                                        <StatusIndicator
                                            tone={channel.enabled ? "positive" : "muted"}
                                            label={channel.enabled ? "Connected" : "Disabled"}
                                        />
                                    </TableCell>
                                    <TableCell>{channel.type ? CHANNEL_TYPE_LABELS[channel.type] : "—"}</TableCell>
                                </TableRow>
                            ))}
                        </SectionTable>
                    </SectionCard>
                )}
            </ApiState>
        </div>
    );
}

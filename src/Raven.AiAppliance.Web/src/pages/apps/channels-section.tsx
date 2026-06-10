import { useQuery } from "@tanstack/react-query";
import { Eye } from "lucide-react";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { StatusIndicator } from "@/components/data/status-indicator";
import { Button } from "@/components/shadcn/ui/button";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { CHANNEL_TYPE_LABELS } from "@/lib/channel-type-labels";
import { AddChannelMenu } from "@/pages/apps/channels/add-channel-menu";
import { ChannelPreviewDialog } from "@/pages/apps/channels/channel-preview-dialog";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

export function ChannelsSection({ slug }: { slug: string }) {
    const agentsQuery = useQuery(api.queries.agents.list(slug));
    const channelsQuery = useQuery(api.queries.channels.list(slug));

    const onRetry = async () => {
        if (channelsQuery.isError) {
            await channelsQuery.refetch();
        }
        if (agentsQuery.isError) {
            await agentsQuery.refetch();
        }
    };

    return (
        <SectionCard title="Channels" action={<AddChannelMenu slug={slug} />}>
            <ApiState
                isLoading={channelsQuery.isPending || agentsQuery.isPending}
                isError={channelsQuery.isError || agentsQuery.isError}
                errorTitle="Could not load channels"
                onRetry={onRetry}
                loadingLabel="Loading channels..."
            >
                {channelsQuery.data && (
                    <SectionTable
                        headers={["", "Channel name", "Agent name", "Status", "Type", "Created", "Widget ID"]}
                        isEmpty={channelsQuery.data.length === 0}
                        emptyMessage="No channels yet."
                    >
                        {channelsQuery.data.map((channel) => {
                            const agent = agentsQuery.data?.find((x) => x.agentId === channel.agentId);
                            return (
                                <TableRow key={channel.widgetId}>
                                    <TableCell className="text-right">
                                        {channel.type === "IFrame" && (
                                            <ChannelPreviewDialog
                                                widgetId={channel.widgetId}
                                                displayName={channel.displayName}
                                                parameterNames={agent?.parameters ?? []}
                                                trigger={
                                                    <Button variant="ghost" size="icon-sm" disabled={!channel.enabled}>
                                                        <Eye className="size-3.5" />
                                                    </Button>
                                                }
                                            />
                                        )}
                                    </TableCell>
                                    <TableCell className="font-medium">{channel.displayName}</TableCell>
                                    <TableCell className="font-medium">{agent?.name}</TableCell>
                                    <TableCell>
                                        <StatusIndicator
                                            tone={channel.enabled ? "positive" : "muted"}
                                            label={channel.enabled ? "Connected" : "Disabled"}
                                        />
                                    </TableCell>
                                    <TableCell>{channel.type ? CHANNEL_TYPE_LABELS[channel.type] : "—"}</TableCell>
                                    <TableCell className="text-muted-foreground">
                                        {formatDate(channel.createdAt)}
                                    </TableCell>
                                    <TableCell className="text-muted-foreground">{channel.widgetId ?? "—"}</TableCell>
                                </TableRow>
                            );
                        })}
                    </SectionTable>
                )}
            </ApiState>
        </SectionCard>
    );
}

function formatDate(value: string) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

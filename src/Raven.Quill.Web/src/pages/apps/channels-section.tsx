import { useQuery } from "@tanstack/react-query";
import { Eye, Link2, Pencil, Trash2 } from "lucide-react";
import { Link } from "react-router";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { StatusIndicator } from "@/components/data/status-indicator";
import { Button } from "@/components/shadcn/ui/button";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { appRoutes } from "@/lib/app-routes";
import { CHANNEL_TYPE_LABELS } from "@/lib/channel-type-labels";
import { formatDateTime } from "@/lib/utils";
import { AddChannelMenu } from "@/pages/apps/channels/add-channel-menu";
import { GenerateEmbedLinkDialog } from "@/pages/apps/channels/generate-embed-link-dialog";
import { DeleteChannelDialog } from "@/pages/apps/channels/delete-channel-dialog";
import { EditChannelSheet } from "@/pages/apps/channels/edit-channel-sheet";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

export function ChannelsSection({ slug }: { slug: string }) {
    const agentsQuery = useQuery(api.queries.agents.list(slug));
    const channelsQuery = useQuery(api.queries.channels.list(slug));
    // Active-link counts are supplementary — kept out of the ApiState gate so a
    // links hiccup never blocks the channels table.
    const embedLinksQuery = useQuery(api.queries.embedLinks.list(slug));

    const activeLinkCounts = new Map<string, number>();
    for (const link of embedLinksQuery.data ?? []) {
        activeLinkCounts.set(link.widgetId, (activeLinkCounts.get(link.widgetId) ?? 0) + 1);
    }

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
                        headers={["Channel name", "Agent name", "Status", "Type", "Active links", "Created", ""]}
                        isEmpty={channelsQuery.data.length === 0}
                        emptyMessage="No channels yet."
                    >
                        {channelsQuery.data.map((channel) => {
                            const agent = agentsQuery.data?.find((x) => x.agentId === channel.agentId);
                            return (
                                <TableRow key={channel.widgetId}>
                                    <TableCell className="font-medium">
                                        <Link
                                            to={appRoutes.app(slug, `channels/${channel.widgetId}`)}
                                            className="hover:underline"
                                            title="Open details"
                                        >
                                            {channel.displayName}
                                        </Link>
                                    </TableCell>
                                    <TableCell className="font-medium">{agent?.name}</TableCell>
                                    <TableCell>
                                        <StatusIndicator
                                            tone={channel.enabled ? "positive" : "muted"}
                                            label={channel.enabled ? "Connected" : "Disabled"}
                                        />
                                    </TableCell>
                                    <TableCell>{channel.type ? CHANNEL_TYPE_LABELS[channel.type] : "—"}</TableCell>
                                    <TableCell className="text-muted-foreground tabular-nums">
                                        {channel.type !== "IFrame" ? (
                                            "—"
                                        ) : embedLinksQuery.isPending ? (
                                            <Skeleton className="h-4 w-6" />
                                        ) : (
                                            (activeLinkCounts.get(channel.widgetId) ?? 0).toLocaleString()
                                        )}
                                    </TableCell>
                                    <TableCell className="text-muted-foreground">
                                        {formatDateTime(channel.createdAt)}
                                    </TableCell>
                                    <TableCell className="text-right">
                                        <div className="flex items-center justify-end gap-1">
                                            <Link
                                                to={appRoutes.app(slug, `channels/${channel.widgetId}`)}
                                                title="Open details"
                                                className="mx-1"
                                            >
                                                <Eye className="size-3.5" aria-hidden="true" />
                                            </Link>
                                            {channel.type === "IFrame" && (
                                                <GenerateEmbedLinkDialog
                                                    slug={slug}
                                                    agentId={channel.agentId}
                                                    displayName={channel.displayName}
                                                    parameterNames={agent?.parameters ?? []}
                                                    trigger={
                                                        <Button
                                                            variant="ghost"
                                                            size="icon-sm"
                                                            aria-label={`Generate embed link for ${channel.displayName}`}
                                                            disabled={!channel.enabled}
                                                            title="Generate link"
                                                        >
                                                            <Link2 className="size-3.5" aria-hidden="true" />
                                                        </Button>
                                                    }
                                                />
                                            )}
                                            <EditChannelSheet
                                                slug={slug}
                                                channel={channel}
                                                trigger={
                                                    <Button
                                                        variant="ghost"
                                                        size="icon-sm"
                                                        aria-label={`Edit ${channel.displayName}`}
                                                        title="Edit channel"
                                                    >
                                                        <Pencil className="size-3.5" aria-hidden="true" />
                                                    </Button>
                                                }
                                            />
                                            <DeleteChannelDialog
                                                slug={slug}
                                                channel={channel}
                                                trigger={
                                                    <Button
                                                        variant="ghost"
                                                        size="icon-sm"
                                                        aria-label={`Delete ${channel.displayName}`}
                                                        title="Delete channel"
                                                    >
                                                        <Trash2 className="size-3.5" aria-hidden="true" />
                                                    </Button>
                                                }
                                            />
                                        </div>
                                    </TableCell>
                                </TableRow>
                            );
                        })}
                    </SectionTable>
                )}
            </ApiState>
        </SectionCard>
    );
}

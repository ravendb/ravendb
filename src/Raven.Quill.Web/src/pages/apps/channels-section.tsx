import { useQuery } from "@tanstack/react-query";
import { Eye, Link2, Pencil, Trash2 } from "lucide-react";
import { Link } from "react-router";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { EnabledStatus } from "@/components/data/status-indicator";
import { Button } from "@/components/shadcn/ui/button";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { appRoutes } from "@/lib/app-routes";
import { CHANNEL_TYPE_LABELS } from "@/lib/channel-type-labels";
import { formatDateTime } from "@/lib/utils";
import { AddChannelMenu } from "@/pages/apps/channels/add-channel-menu";
import type { FixedAgent } from "@/pages/apps/channels/web-widget-channel-form";
import { GenerateEmbedLinkDialog } from "@/pages/apps/channels/generate-embed-link-dialog";
import { DeleteChannelDialog } from "@/pages/apps/channels/delete-channel-dialog";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

// When `agent` is set the section is scoped to that single agent: only its channels are listed,
// the agent name column is dropped, and new channels are routed to it (e.g. the capability wizard).
export function ChannelsSection({ slug, agent: fixedAgent }: { slug: string; agent?: FixedAgent }) {
    const agentsQuery = useQuery(api.queries.agents.list(slug));
    const channelsQuery = useQuery(api.queries.channels.list(slug));
    // Active-link counts are supplementary — kept out of the ApiState gate so a
    // links hiccup never blocks the channels table.
    const embedLinksQuery = useQuery(api.queries.embedLinks.list(slug));

    const activeLinkCounts = new Map<string, number>();
    for (const link of embedLinksQuery.data ?? []) {
        activeLinkCounts.set(link.channelId, (activeLinkCounts.get(link.channelId) ?? 0) + 1);
    }

    const onRetry = async () => {
        if (channelsQuery.isError) {
            await channelsQuery.refetch();
        }
        if (agentsQuery.isError) {
            await agentsQuery.refetch();
        }
    };

    const channels = fixedAgent
        ? (channelsQuery.data ?? []).filter((channel) => channel.agentId === fixedAgent.agentId)
        : channelsQuery.data;

    return (
        <SectionCard
            title={fixedAgent ? `Channels for “${fixedAgent.name}”` : "Channels"}
            action={<AddChannelMenu slug={slug} agent={fixedAgent} />}
        >
            <ApiState
                isLoading={channelsQuery.isPending || agentsQuery.isPending}
                isError={channelsQuery.isError || agentsQuery.isError}
                errorTitle="Could not load channels"
                onRetry={onRetry}
                loadingLabel="Loading channels..."
            >
                {channels && (
                    <SectionTable
                        headers={[
                            "Channel name",
                            ...(fixedAgent ? [] : ["Agent name"]),
                            "Status",
                            "Type",
                            "Active links",
                            "Created",
                            "",
                        ]}
                        isEmpty={channels.length === 0}
                        emptyMessage="No channels yet."
                    >
                        {channels.map((channel) => {
                            const agent = agentsQuery.data?.find((x) => x.agentId === channel.agentId);
                            return (
                                <TableRow key={channel.channelId}>
                                    <TableCell className="font-medium">
                                        <Link
                                            to={appRoutes.app(slug, `channels/${channel.channelId}`)}
                                            className="hover:underline"
                                            title="Open details"
                                        >
                                            {channel.displayName}
                                        </Link>
                                        {channel.telegram?.botUsername && (
                                            <div className="text-xs font-normal text-muted-foreground">
                                                @{channel.telegram.botUsername}
                                            </div>
                                        )}
                                    </TableCell>
                                    {!fixedAgent && <TableCell className="font-medium">{agent?.name}</TableCell>}
                                    <TableCell>
                                        <EnabledStatus isEnabled={channel.enabled} />
                                    </TableCell>
                                    <TableCell>{channel.type ? CHANNEL_TYPE_LABELS[channel.type] : "—"}</TableCell>
                                    <TableCell className="text-muted-foreground tabular-nums">
                                        {channel.type !== "IFrame" ? (
                                            "—"
                                        ) : embedLinksQuery.isPending ? (
                                            <Skeleton className="h-4 w-6" />
                                        ) : (
                                            (activeLinkCounts.get(channel.channelId) ?? 0).toLocaleString()
                                        )}
                                    </TableCell>
                                    <TableCell className="text-muted-foreground">
                                        {formatDateTime(channel.createdAt)}
                                    </TableCell>
                                    <TableCell className="text-right">
                                        <div className="flex items-center justify-end gap-1">
                                            {!fixedAgent && (
                                                <Link
                                                    to={appRoutes.app(slug, `channels/${channel.channelId}`)}
                                                    title="Open details"
                                                    className="mx-1"
                                                >
                                                    <Eye className="size-3.5" aria-hidden="true" />
                                                </Link>
                                            )}
                                            {channel.type === "IFrame" && (
                                                <GenerateEmbedLinkDialog
                                                    slug={slug}
                                                    channelId={channel.channelId}
                                                    agentId={agent?.agentId}
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
                                            <Button
                                                asChild
                                                variant="ghost"
                                                size="icon-sm"
                                                aria-label={`Edit ${channel.displayName}`}
                                                title="Edit channel"
                                            >
                                                <Link to={appRoutes.app(slug, `channels/${channel.channelId}?edit=1`)}>
                                                    <Pencil className="size-3.5" aria-hidden="true" />
                                                </Link>
                                            </Button>
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

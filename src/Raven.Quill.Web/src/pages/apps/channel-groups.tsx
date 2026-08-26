import { useQuery } from "@tanstack/react-query";
import { Cable, CodeXml, Link2, MessageCircle, Palette, Pencil, Send, Trash2 } from "lucide-react";
import type { ComponentType, ReactNode, SVGProps } from "react";
import { Link } from "react-router";
import { api } from "@/api/api";
import type { AgentSummaryResponse, ChannelSummaryResponse, ChannelType } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { CardListSkeleton } from "@/components/data/loading-skeletons";
import { EnabledStatus } from "@/components/data/status-indicator";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardAction, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { Heading, Text } from "@/components/typography";
import { appRoutes } from "@/lib/app-routes";
import { cn, formatDateTime, formatRelativeTime } from "@/lib/utils";
import { DiscordIcon, SlackIcon } from "@/pages/apps/channels/channel-brand-icons";
import { DeleteChannelDialog } from "@/pages/apps/channels/delete-channel-dialog";
import { GenerateEmbedLinkDialog } from "@/pages/apps/channels/generate-embed-link-dialog";

type ChannelGroupConfig = {
    type: NonNullable<ChannelType>;
    label: string;
    icon: ComponentType<SVGProps<SVGSVGElement>>;
};

// Order and icons mirror the "Add channel" menu so the page reads the same way channels are created.
const CHANNEL_GROUPS: ChannelGroupConfig[] = [
    { type: "IFrame", label: "Web widgets", icon: CodeXml },
    { type: "Telegram", label: "Telegram bots", icon: Send },
    { type: "WhatsApp", label: "WhatsApp", icon: MessageCircle },
    { type: "Slack", label: "Slack", icon: SlackIcon },
    { type: "Discord", label: "Discord", icon: DiscordIcon },
];

export function ChannelGroups({ slug }: { slug: string }) {
    const agentsQuery = useQuery(api.queries.agents.list(slug));
    const channelsQuery = useQuery(api.queries.channels.list(slug));
    // Active-link counts are supplementary — kept out of the ApiState gate so a
    // links hiccup never blocks the channel cards.
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

    const channels = channelsQuery.data ?? [];
    const knownTypes = new Set<NonNullable<ChannelType>>(CHANNEL_GROUPS.map((group) => group.type));
    const groups = [
        ...CHANNEL_GROUPS.map((group) => ({
            label: group.label,
            icon: group.icon,
            channels: channels.filter((channel) => channel.type === group.type),
        })),
        // Catch-all so an unknown or untyped channel is never silently dropped.
        {
            label: "Other",
            icon: Cable,
            channels: channels.filter((channel) => channel.type == null || !knownTypes.has(channel.type)),
        },
    ].filter((group) => group.channels.length > 0);

    return (
        <ApiState
            isLoading={channelsQuery.isPending || agentsQuery.isPending}
            isError={channelsQuery.isError || agentsQuery.isError}
            errorTitle="Could not load channels"
            onRetry={onRetry}
            loadingLabel="Loading channels..."
            skeleton={<CardListSkeleton count={2} />}
        >
            {channelsQuery.data &&
                (channels.length === 0 ? (
                    <Text as="div" variant="muted" className="rounded-lg border border-dashed p-10 text-center">
                        No channels yet. Add one to start reaching users.
                    </Text>
                ) : (
                    <div className="space-y-6">
                        {groups.map((group) => (
                            <section key={group.label} className="min-w-0">
                                <div className="mb-3 flex items-center gap-2">
                                    <group.icon className="size-4 text-muted-foreground" aria-hidden="true" />
                                    <Heading variant="label">{group.label}</Heading>
                                    <Badge variant="secondary" className="tabular-nums">
                                        {group.channels.length}
                                    </Badge>
                                    {group.label === "Web widgets" && (
                                        <Button asChild variant="outline">
                                            <Link to={appRoutes.app(slug, "web-widget/default-customize")}>
                                                <Palette aria-hidden="true" />
                                                Customize default appearance
                                            </Link>
                                        </Button>
                                    )}
                                </div>
                                <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
                                    {group.channels.map((channel) => (
                                        <ChannelCard
                                            key={channel.channelId}
                                            slug={slug}
                                            channel={channel}
                                            agent={agentsQuery.data?.find((x) => x.agentId === channel.agentId)}
                                            activeLinkCount={activeLinkCounts.get(channel.channelId) ?? 0}
                                            isLinkCountLoading={embedLinksQuery.isPending}
                                        />
                                    ))}
                                </div>
                            </section>
                        ))}
                    </div>
                ))}
        </ApiState>
    );
}

function ChannelCard({
    slug,
    channel,
    agent,
    activeLinkCount,
    isLinkCountLoading,
}: {
    slug: string;
    channel: ChannelSummaryResponse;
    agent: AgentSummaryResponse | undefined;
    activeLinkCount: number;
    isLinkCountLoading: boolean;
}) {
    const isIFrame = channel.type === "IFrame";

    return (
        // The title's stretched ::after overlay turns the whole card into the "open details" link;
        // interactive children (the footer actions) sit above it with `relative z-10`.
        <Card
            size="sm"
            className="relative gap-3 transition-[background-color,box-shadow] hover:bg-muted/40 hover:ring-foreground/25 has-[a:focus-visible]:ring-2 has-[a:focus-visible]:ring-ring"
        >
            <CardHeader>
                <CardTitle variant="item" className="min-w-0 truncate">
                    <Link
                        to={appRoutes.app(slug, `channels/${channel.channelId}`)}
                        className="after:absolute after:inset-0 after:content-[''] hover:underline"
                        title="Open details"
                    >
                        {channel.displayName}
                    </Link>
                    {channel.telegram?.botUsername && (
                        <Text as="div" variant="caption" className="truncate font-normal">
                            @{channel.telegram.botUsername}
                        </Text>
                    )}
                    {channel.slack?.teamName && (
                        <Text as="div" variant="caption" className="truncate font-normal">
                            {channel.slack.teamName}
                        </Text>
                    )}
                    {channel.discord?.botUsername && (
                        <Text as="div" variant="caption" className="truncate font-normal">
                            {channel.discord.botUsername}
                        </Text>
                    )}
                </CardTitle>
                <CardAction>
                    <EnabledStatus isEnabled={channel.enabled} />
                </CardAction>
            </CardHeader>

            <CardContent className="space-y-3">
                {agent ? (
                    <AgentChip name={agent.name} seed={agent.agentId} />
                ) : (
                    <Text as="span" variant="caption">
                        Unassigned
                    </Text>
                )}

                <div className={cn("grid gap-2", isIFrame ? "grid-cols-2" : "grid-cols-1")}>
                    {isIFrame && (
                        <StatBox
                            label="Active links"
                            value={
                                isLinkCountLoading ? <Skeleton className="h-4 w-6" /> : activeLinkCount.toLocaleString()
                            }
                        />
                    )}
                    <StatBox
                        label="Added"
                        value={
                            <span title={formatDateTime(channel.createdAt)}>
                                {formatRelativeTime(channel.createdAt)}
                            </span>
                        }
                    />
                </div>
            </CardContent>

            <CardFooter className="justify-end gap-1">
                {isIFrame && (
                    <GenerateEmbedLinkDialog
                        slug={slug}
                        channelId={channel.channelId}
                        displayName={channel.displayName}
                        parameters={agent?.parameters ?? []}
                        trigger={
                            <Button
                                variant="ghost"
                                size="icon-sm"
                                className="relative z-10"
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
                    className="relative z-10"
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
                            className="relative z-10"
                            aria-label={`Delete ${channel.displayName}`}
                            title="Delete channel"
                        >
                            <Trash2 className="size-3.5" aria-hidden="true" />
                        </Button>
                    }
                />
            </CardFooter>
        </Card>
    );
}

function StatBox({ label, value }: { label: string; value: ReactNode }) {
    return (
        <div className="rounded-md border bg-muted/30 px-2.5 py-1.5">
            <div className="text-[11px] text-muted-foreground">{label}</div>
            <Text as="div" variant="label" className="tabular-nums">
                {value}
            </Text>
        </div>
    );
}

function AgentChip({ name, seed }: { name: string; seed: string }) {
    return (
        <span className="inline-flex max-w-full items-center gap-1.5 rounded-full border bg-muted/40 py-0.5 ps-0.5 pe-2.5 text-xs font-medium">
            <span
                className="flex size-5 shrink-0 items-center justify-center rounded-full text-[9px] font-semibold text-white"
                style={{ backgroundColor: getAgentColor(seed) }}
                aria-hidden="true"
            >
                {getAgentInitials(name)}
            </span>
            <span className="truncate">{name}</span>
        </span>
    );
}

// White-on-color avatar swatches, matching `agentAvatarColor` in lib/palette.ts: one lightness for
// legible initials, cool hues only so a swatch is never mistaken for the primary or an error.
const AGENT_COLORS = ["#558111", "#158561", "#158186", "#147ba9", "#3f69d3", "#735acc", "#964bb4", "#ae4090"];

function getAgentColor(seed: string): string {
    let hash = 0;
    for (let index = 0; index < seed.length; index += 1) {
        hash = (hash * 31 + seed.charCodeAt(index)) | 0;
    }
    return AGENT_COLORS[Math.abs(hash) % AGENT_COLORS.length];
}

function getAgentInitials(name: string): string {
    const words = name.trim().split(/\s+/).filter(Boolean);
    if (words.length === 0) {
        return "?";
    }
    if (words.length === 1) {
        return words[0].slice(0, 2).toUpperCase();
    }
    return (words[0][0] + words[words.length - 1][0]).toUpperCase();
}

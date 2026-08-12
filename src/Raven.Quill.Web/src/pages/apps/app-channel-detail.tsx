import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, Palette } from "lucide-react";
import { Link, useParams } from "react-router";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { EnabledStatus } from "@/components/data/status-indicator";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { appRoutes } from "@/lib/app-routes";
import { CHANNEL_TYPE_LABELS } from "@/lib/channel-type-labels";
import { ChannelActiveLinks } from "@/pages/apps/channels/channel-active-links";
import { EmbedLinkApiDocs } from "@/pages/apps/channels/embed-link-api-docs";
import { GenerateEmbedLinkDialog } from "@/pages/apps/channels/generate-embed-link-dialog";
import { TelegramChannelBindings } from "@/pages/apps/channels/telegram-channel-bindings";
import { SectionCard } from "@/pages/apps/section-card";

export function AppChannelDetail() {
    const { slug = "", channelId = "" } = useParams();
    const channelsQuery = useQuery(api.queries.channels.list(slug));
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    const channel = channelsQuery.data?.find((candidate) => candidate.channelId === channelId);
    const agent = agentsQuery.data?.find((candidate) => candidate.agentId === channel?.agentId);

    const onRetry = async () => {
        if (channelsQuery.isError) {
            await channelsQuery.refetch();
        }
        if (agentsQuery.isError) {
            await agentsQuery.refetch();
        }
    };

    const isIFrame = channel?.type === "IFrame";

    return (
        <div className="grid gap-5">
            <Link
                to={appRoutes.app(slug, "channels")}
                className="inline-flex w-fit items-center gap-1.5 text-sm text-muted-foreground transition-colors hover:text-foreground"
            >
                <ArrowLeft className="size-3.5" aria-hidden="true" />
                Channels
            </Link>

            <ApiState
                isLoading={channelsQuery.isPending || agentsQuery.isPending}
                isError={channelsQuery.isError || agentsQuery.isError}
                errorTitle="Could not load channel"
                onRetry={onRetry}
                loadingLabel="Loading channel..."
            >
                {channelsQuery.data &&
                    (channel ? (
                        <div className="grid gap-5">
                            <div className="flex items-start justify-between gap-4">
                                <div className="grid gap-1">
                                    <div className="flex items-center gap-3">
                                        <h2 className="text-lg font-semibold">{channel.displayName}</h2>
                                        <EnabledStatus isEnabled={channel.enabled} />
                                    </div>
                                    <p className="text-sm text-muted-foreground">
                                        {channel.type ? CHANNEL_TYPE_LABELS[channel.type] : "—"}
                                        {agent?.name ? ` · ${agent.name}` : ""}
                                        {channel.telegram?.botUsername ? ` · @${channel.telegram.botUsername}` : ""}
                                        <span className="font-mono"> · {channel.channelId}</span>
                                    </p>
                                </div>
                                {isIFrame && (
                                    <Button asChild variant="outline" size="sm" className="shrink-0">
                                        <Link
                                            to={appRoutes.app(
                                                slug,
                                                `web-widget/${encodeURIComponent(channel.channelId)}/customize`,
                                            )}
                                        >
                                            <Palette aria-hidden="true" />
                                            Customize appearance
                                        </Link>
                                    </Button>
                                )}
                            </div>

                            {isIFrame ? (
                                <>
                                    <EmbedLinkApiDocs
                                        slug={slug}
                                        channelId={channel.channelId}
                                        parameterNames={agent?.parameters ?? []}
                                    />
                                    <SectionCard
                                        title="Active links"
                                        action={
                                            <GenerateEmbedLinkDialog
                                                slug={slug}
                                                channelId={channel.channelId}
                                                displayName={channel.displayName}
                                                parameterNames={agent?.parameters ?? []}
                                                trigger={
                                                    <Button size="sm" variant="outline" disabled={!channel.enabled}>
                                                        Generate link
                                                    </Button>
                                                }
                                            />
                                        }
                                    >
                                        <ChannelActiveLinks slug={slug} channelId={channel.channelId} />
                                    </SectionCard>
                                </>
                            ) : channel.type === "Telegram" ? (
                                <TelegramChannelBindings channel={channel} />
                            ) : (
                                <Alert>Embed links apply to web widget channels only.</Alert>
                            )}
                        </div>
                    ) : (
                        <Alert variant="destructive">No channel “{channelId}” in this app.</Alert>
                    ))}
            </ApiState>
        </div>
    );
}

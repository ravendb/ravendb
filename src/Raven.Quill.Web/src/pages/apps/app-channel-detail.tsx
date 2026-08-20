import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
    Bot,
    CodeXml,
    Fingerprint,
    Globe,
    MessageCircle,
    Pause,
    Pencil,
    Play,
    Send,
    Trash2,
    type LucideIcon,
} from "lucide-react";
import { useNavigate, useParams, useSearchParams } from "react-router";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { AgentSummaryResponse, ChannelSummaryResponse, ChannelType } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { DetailHeader, DetailHeaderMenu, DetailHeaderMetaItem } from "@/components/data/detail-header";
import { EnabledStatus } from "@/components/data/status-indicator";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { DropdownMenuItem } from "@/components/shadcn/ui/dropdown-menu";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { UnsavedChangesConfirm } from "@/components/form/unsaved-changes/unsaved-changes-confirm";
import {
    selectHasUnsavedChanges,
    useUnsavedChangesStore,
} from "@/components/form/unsaved-changes/unsaved-changes-store";
import { appRoutes } from "@/lib/app-routes";
import { CHANNEL_TYPE_LABELS } from "@/lib/channel-type-labels";
import { cn } from "@/lib/utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import { getChannelTabs, resolveActiveTab, type ChannelTabDef } from "@/pages/apps/channels/channel-detail-tabs";
import { DeleteChannelDialog } from "@/pages/apps/channels/delete-channel-dialog";
import { EditChannelSheet } from "@/pages/apps/channels/edit-channel-sheet";

const CHANNEL_TYPE_ICONS: Record<NonNullable<ChannelType>, LucideIcon> = {
    IFrame: CodeXml,
    Telegram: Send,
    WhatsApp: MessageCircle,
};

// Roomier hit area than the default trigger padding. The active underline is a single sliding
// indicator (see ChannelTabsList), so the per-tab `after` bar the line variant would draw is hidden.
const CHANNEL_TAB_CLASS = "h-auto rounded-none px-3 py-2.5 after:hidden";

export function AppChannelDetail() {
    const { slug = "", channelId = "" } = useParams();
    const [searchParams] = useSearchParams();
    const channelsQuery = useQuery(api.queries.channels.list(slug));
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    const channel = channelsQuery.data?.find((candidate) => candidate.channelId === channelId);
    const agent = agentsQuery.data?.find((candidate) => candidate.agentId === channel?.agentId);

    const tabs = channel ? getChannelTabs(channel) : [];

    const [requestedTab, setRequestedTab] = useState<string | null>(() => searchParams.get("tab"));
    // Resolve against this channel's tab set so a stale link (or a tab owned by another channel type)
    // can't select a missing tab.
    const activeTab = resolveActiveTab(tabs, requestedTab);

    // Tabs switch via local state, not navigation, so the app-level route guard never sees it. Confirm
    // before leaving a tab whose form is mid-edit, and hold the requested switch until then.
    const hasUnsavedChanges = useUnsavedChangesStore(selectHasUnsavedChanges);
    const [pendingTab, setPendingTab] = useState<string | null>(null);

    const onTabChange = (next: string) => {
        if (next !== activeTab && hasUnsavedChanges) {
            setPendingTab(next);
            return;
        }
        setRequestedTab(next);
    };
    const currentLayout = tabs.find((tab) => tab.key === activeTab)?.layout ?? "padded";
    // "bare" tabs (e.g. the web widget's Customize) bring their own sticky top bar/preview and want content
    // flush under the header. "fill" tabs (the editable Telegram tabs) keep a fixed header while their body
    // scrolls. "padded" tabs get the default breathing padding.
    const isBare = currentLayout === "bare";
    const isFill = currentLayout === "fill";

    const onRetry = async () => {
        if (channelsQuery.isError) {
            await channelsQuery.refetch();
        }
        if (agentsQuery.isError) {
            await agentsQuery.refetch();
        }
    };

    return (
        <Tabs
            value={activeTab}
            onValueChange={onTabChange}
            className={cn("h-full min-h-0", isBare ? "gap-5" : "gap-0")}
        >
            <DetailHeader
                title={channel?.displayName ?? "Channel"}
                status={channel && <EnabledStatus isEnabled={channel.enabled} />}
                backTo={{ to: appRoutes.app(slug, "channels"), label: "Channels" }}
                meta={channel && <ChannelMeta channel={channel} agent={agent} />}
                actions={channel && <ChannelActions slug={slug} channel={channel} />}
                tabs={channel && tabs.length > 0 && <ChannelTabsList tabs={tabs} activeTab={activeTab} />}
            />

            {/* -mx-2/px-2 keeps card borders/shadows off the scroller's clip edge (overflow-y-auto clips
                x too). On padded tabs, py-5 breathes at rest but scrolls away so content sits flush under
                the header; bare tabs keep no padding for their own sticky top bar. "fill" tabs own their
                own scroller (with a fixed header), so this just becomes their flex column. */}
            <div
                className={cn(
                    "min-h-0 flex-1",
                    isFill ? "flex flex-col" : "-mx-2 overflow-y-auto px-2",
                    !isBare && !isFill && "py-5",
                )}
            >
                <ApiState
                    isLoading={channelsQuery.isPending || agentsQuery.isPending}
                    isError={channelsQuery.isError || agentsQuery.isError}
                    errorTitle="Could not load channel"
                    onRetry={onRetry}
                    loadingLabel="Loading channel..."
                >
                    {channelsQuery.data &&
                        (!channel ? (
                            <Alert variant="destructive">No channel “{channelId}” in this app.</Alert>
                        ) : tabs.length === 0 ? (
                            <Alert>This channel type has no settings to configure yet.</Alert>
                        ) : (
                            tabs.map((tab) => (
                                <TabsContent
                                    key={tab.key}
                                    value={tab.key}
                                    className={cn(tab.layout === "fill" && "flex min-h-0 flex-1 flex-col")}
                                >
                                    {tab.render({ slug, channel, agent })}
                                </TabsContent>
                            ))
                        ))}
                </ApiState>
            </div>

            <UnsavedChangesConfirm
                open={pendingTab !== null}
                onOpenChange={(isOpen) => {
                    if (!isOpen) {
                        setPendingTab(null);
                    }
                }}
                onConfirm={() => {
                    // Switching unmounts the dirty tab, which drops its edits and clears its guard entry.
                    setRequestedTab(pendingTab);
                    setPendingTab(null);
                }}
            />
        </Tabs>
    );
}

// Renders the tab strip with a single underline that slides to the active tab, instead of a static
// per-tab underline. The indicator is measured from the active trigger and moved with a CSS
// transition, so no animation library is needed.
function ChannelTabsList({ tabs, activeTab }: { tabs: ChannelTabDef[]; activeTab: string }) {
    const listRef = useRef<HTMLDivElement>(null);
    const [indicator, setIndicator] = useState<{ left: number; width: number } | null>(null);

    useLayoutEffect(() => {
        const list = listRef.current;
        if (!list) {
            return;
        }
        const measure = () => {
            const active = list.querySelector<HTMLElement>('[data-state="active"]');
            if (active) {
                setIndicator({ left: active.offsetLeft, width: active.offsetWidth });
            }
        };
        measure();
        // Re-measure when the strip reflows (viewport resize, tab set changes).
        const observer = new ResizeObserver(measure);
        observer.observe(list);
        return () => observer.disconnect();
    }, [activeTab, tabs]);

    return (
        <div ref={listRef} className="relative -mb-px">
            <TabsList
                variant="line"
                className="gap-2 rounded-none border-0 bg-transparent p-0 group-data-[orientation=horizontal]/tabs:h-auto"
            >
                {tabs.map((tab) => {
                    const Icon = tab.icon;
                    return (
                        <TabsTrigger key={tab.key} value={tab.key} className={CHANNEL_TAB_CLASS}>
                            <Icon aria-hidden="true" />
                            {tab.label}
                        </TabsTrigger>
                    );
                })}
            </TabsList>
            <span
                aria-hidden="true"
                className="pointer-events-none absolute bottom-0 h-0.5 rounded-full bg-foreground transition-[left,width] duration-200 ease-out"
                style={indicator ? { left: indicator.left, width: indicator.width } : { width: 0, opacity: 0 }}
            />
        </div>
    );
}

function ChannelMeta({ channel, agent }: { channel: ChannelSummaryResponse; agent: AgentSummaryResponse | undefined }) {
    const TypeIcon = channel.type ? CHANNEL_TYPE_ICONS[channel.type] : undefined;
    const allowedOrigins = channel.allowedOrigins ?? [];

    return (
        <>
            <DetailHeaderMetaItem icon={TypeIcon} tooltip="Type">
                {channel.type ? CHANNEL_TYPE_LABELS[channel.type] : "—"}
            </DetailHeaderMetaItem>
            {agent?.name && (
                <DetailHeaderMetaItem icon={Bot} tooltip="Agent">
                    {agent.name}
                </DetailHeaderMetaItem>
            )}
            {allowedOrigins.length > 0 && (
                <DetailHeaderMetaItem icon={Globe} tooltip="Allowed origins">
                    {allowedOrigins.join(", ")}
                </DetailHeaderMetaItem>
            )}
            <DetailHeaderMetaItem icon={Fingerprint} mono tooltip="Channel ID">
                {channel.channelId}
            </DetailHeaderMetaItem>
        </>
    );
}

function ChannelActions({ slug, channel }: { slug: string; channel: ChannelSummaryResponse }) {
    const navigate = useNavigate();
    const queryClient = useQueryClient();
    const [searchParams, setSearchParams] = useSearchParams();
    // Opened straight into edit when arriving from the channels list pencil (`?edit=1`).
    const [isEditOpen, setIsEditOpen] = useState(() => searchParams.get("edit") === "1");
    const [isDeleteOpen, setIsDeleteOpen] = useState(false);

    // Consume the flag so refreshing or reopening the sheet later doesn't re-trigger it.
    useEffect(() => {
        if (searchParams.has("edit")) {
            const next = new URLSearchParams(searchParams);
            next.delete("edit");
            setSearchParams(next, { replace: true });
        }
    }, [searchParams, setSearchParams]);

    const toggleMutation = useMutation({
        // Partial update: only the enabled flag changes, the rest is left untouched on the server.
        mutationFn: () =>
            api.services.channels.update(slug, channel.channelId, {
                displayName: null,
                allowedOrigins: null,
                enabled: !channel.enabled,
            }),
        onSuccess: async () => {
            await invalidateChannelQueries(queryClient, slug);
            toast.success(channel.enabled ? "Channel paused" : "Channel resumed");
        },
        onError: (error) => {
            toast.error(error instanceof Error ? error.message : "Could not update the channel.");
        },
    });

    return (
        <>
            <Button
                variant="outline"
                size="sm"
                onClick={() => toggleMutation.mutate()}
                disabled={toggleMutation.isPending}
            >
                {toggleMutation.isPending ? (
                    <Spinner />
                ) : channel.enabled ? (
                    <Pause aria-hidden="true" />
                ) : (
                    <Play aria-hidden="true" />
                )}
                {channel.enabled ? "Pause" : "Resume"}
            </Button>

            <DetailHeaderMenu>
                <DropdownMenuItem onSelect={() => setIsEditOpen(true)}>
                    <Pencil aria-hidden="true" />
                    Edit
                </DropdownMenuItem>
                <DropdownMenuItem variant="destructive" onSelect={() => setIsDeleteOpen(true)}>
                    <Trash2 aria-hidden="true" />
                    Delete
                </DropdownMenuItem>
            </DetailHeaderMenu>

            <EditChannelSheet slug={slug} channel={channel} open={isEditOpen} onOpenChange={setIsEditOpen} />
            <DeleteChannelDialog
                slug={slug}
                channel={channel}
                open={isDeleteOpen}
                onOpenChange={setIsDeleteOpen}
                onDeleted={() => navigate(appRoutes.app(slug, "channels"))}
            />
        </>
    );
}

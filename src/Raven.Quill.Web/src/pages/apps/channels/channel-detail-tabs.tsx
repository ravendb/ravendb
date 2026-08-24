import type { ReactNode } from "react";
import { CodeXml, Link2, MessageSquareText, Palette, Plug, Variable, type LucideIcon } from "lucide-react";
import type { AgentSummaryResponse, ChannelSummaryResponse, ChannelType } from "@/api/generated/server-api";
import { Button } from "@/components/shadcn/ui/button";
import { ChannelActiveLinks } from "@/pages/apps/channels/channel-active-links";
import { EmbedLinkApiDocs } from "@/pages/apps/channels/embed-link-api-docs";
import { GenerateEmbedLinkDialog } from "@/pages/apps/channels/generate-embed-link-dialog";
import { TelegramChannelBindings } from "@/pages/apps/channels/telegram-channel-bindings";
import { TelegramConnectTab } from "@/pages/apps/channels/telegram-connect-tab";
import { TelegramMessagesTab } from "@/pages/apps/channels/telegram-messages-tab";
import { WebWidgetAppearanceTab } from "@/pages/apps/channels/web-widget-appearance-tab";
import { SectionCard } from "@/pages/apps/section-card";

export type ChannelTabContext = {
    slug: string;
    channel: ChannelSummaryResponse;
    agent: AgentSummaryResponse | undefined;
};

export type ChannelTabDef = {
    key: string;
    label: string;
    icon: LucideIcon;
    // "padded": default breathing padding. "bare": brings its own sticky top bar / preview and wants
    // content flush (the web widget's Customize tab). "fill": a fixed header that stays put while the body
    // scrolls beneath it (the editable Telegram tabs).
    layout: "padded" | "bare" | "fill";
    render: (ctx: ChannelTabContext) => ReactNode;
};

const IFRAME_TABS: ChannelTabDef[] = [
    {
        key: "embed",
        label: "Embed",
        icon: CodeXml,
        layout: "padded",
        render: ({ slug, channel, agent }) => (
            <EmbedLinkApiDocs slug={slug} channelId={channel.channelId} parameterNames={agent?.parameters ?? []} />
        ),
    },
    {
        key: "active-links",
        label: "Active links",
        icon: Link2,
        layout: "padded",
        render: ({ slug, channel, agent }) => (
            <SectionCard
                title="Active links"
                action={
                    <GenerateEmbedLinkDialog
                        slug={slug}
                        channelId={channel.channelId}
                        agentId={agent?.agentId}
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
        ),
    },
    {
        key: "customize",
        label: "Customize appearance",
        icon: Palette,
        layout: "bare",
        render: ({ slug, channel }) => <WebWidgetAppearanceTab slug={slug} channelId={channel.channelId} />,
    },
];

const TELEGRAM_TABS: ChannelTabDef[] = [
    {
        key: "connect",
        label: "Connect",
        icon: Plug,
        layout: "padded",
        render: ({ channel }) => <TelegramConnectTab channel={channel} />,
    },
    {
        key: "parameters",
        label: "Parameters",
        icon: Variable,
        layout: "fill",
        render: ({ slug, channel, agent }) => <TelegramChannelBindings slug={slug} channel={channel} agent={agent} />,
    },
    {
        key: "messages",
        label: "Bot messages",
        icon: MessageSquareText,
        layout: "fill",
        render: ({ slug, channel }) => <TelegramMessagesTab slug={slug} channel={channel} />,
    },
];

const TABS_BY_TYPE: Record<NonNullable<ChannelType>, ChannelTabDef[]> = {
    IFrame: IFRAME_TABS,
    Telegram: TELEGRAM_TABS,
    WhatsApp: [],
};

// The tabs a channel's detail view shows, chosen by its type. New channel types declare their tabs
// here instead of adding inline `type === "..."` branches to the detail page.
export function getChannelTabs(channel: ChannelSummaryResponse): ChannelTabDef[] {
    return channel.type ? TABS_BY_TYPE[channel.type] : [];
}

// Resolve which tab is active: the requested key if this channel has it, otherwise the first tab, so a
// stale `?tab=` link (or a tab that only exists for another channel type) can't select a missing tab.
export function resolveActiveTab(tabs: ChannelTabDef[], requested: string | null): string {
    return tabs.find((tab) => tab.key === requested)?.key ?? tabs[0]?.key ?? "";
}

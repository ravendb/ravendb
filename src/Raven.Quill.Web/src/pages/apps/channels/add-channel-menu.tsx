import { useState, type ComponentType, type SVGProps } from "react";
import { CodeXml, MessageCircle, Plus, Send } from "lucide-react";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
} from "@/components/shadcn/ui/dropdown-menu";
import { SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/shadcn/ui/sheet";
import { GuardedSheet } from "@/components/form/unsaved-changes/guarded-overlays";
import { DiscordIcon, SlackIcon } from "@/pages/apps/channels/channel-brand-icons";
import { DiscordChannelForm } from "@/pages/apps/channels/discord-channel-form";
import { SlackChannelForm } from "@/pages/apps/channels/slack-channel-form";
import { TelegramChannelForm } from "@/pages/apps/channels/telegram-channel-form";
import { WebWidgetChannelForm, type FixedAgent } from "@/pages/apps/channels/web-widget-channel-form";
import { Text } from "@/components/typography";

type ChannelOptionId = "web-widget" | "telegram" | "slack" | "discord";

type ChannelOption = {
    label: string;
    description: string;
    icon: ComponentType<SVGProps<SVGSVGElement>>;
} & ({ id: ChannelOptionId; enabled: true } | { id: string; enabled: false });

// The web widget, Telegram, Slack, and Discord are backed by the channels API today; the rest are previewed as disabled.
const CHANNEL_OPTIONS: ChannelOption[] = [
    {
        id: "web-widget",
        label: "Web widget",
        description: "Embed a chat widget on your site",
        icon: CodeXml,
        enabled: true,
    },
    {
        id: "telegram",
        label: "Telegram bot",
        description: "Connect a bot via @BotFather",
        icon: Send,
        enabled: true,
    },
    {
        id: "whatsapp-personal",
        label: "WhatsApp Personal",
        description: "Link a phone for QA & testing",
        icon: MessageCircle,
        enabled: false,
    },
    {
        id: "whatsapp-business",
        label: "WhatsApp Business",
        description: "Connect via Meta Cloud API",
        icon: MessageCircle,
        enabled: false,
    },
    {
        id: "slack",
        label: "Slack",
        description: "Connect a Slack app for DMs",
        icon: SlackIcon,
        enabled: true,
    },
    {
        id: "discord",
        label: "Discord",
        description: "Connect a Discord bot for DMs",
        icon: DiscordIcon,
        enabled: true,
    },
];

export function AddChannelMenu({
    slug,
    agent,
    label = "Add channel",
    variant = "outline",
}: {
    slug: string;
    agent?: FixedAgent;
    label?: string;
    variant?: "default" | "outline";
}) {
    const [openOption, setOpenOption] = useState<ChannelOptionId | null>(null);

    return (
        <>
            <DropdownMenu>
                <DropdownMenuTrigger asChild>
                    <Button size="sm" variant={variant}>
                        <Plus className="size-3.5" aria-hidden="true" />
                        {label}
                    </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-72">
                    {CHANNEL_OPTIONS.map((option) => (
                        <DropdownMenuItem
                            key={option.id}
                            className="items-start gap-2.5 py-2"
                            disabled={!option.enabled}
                            onSelect={option.enabled ? () => setOpenOption(option.id) : undefined}
                        >
                            <option.icon className="mt-0.5 size-4 text-muted-foreground" aria-hidden="true" />
                            <div className="flex flex-col gap-0.5">
                                <span className="flex items-center gap-2">
                                    <span className="leading-none font-medium">{option.label}</span>
                                    {!option.enabled && (
                                        <Badge variant="secondary" className="text-muted-foreground">
                                            Coming soon
                                        </Badge>
                                    )}
                                </span>
                                <Text as="span" variant="caption">
                                    {option.description}
                                </Text>
                            </div>
                        </DropdownMenuItem>
                    ))}
                </DropdownMenuContent>
            </DropdownMenu>

            <GuardedSheet
                open={openOption !== null}
                onOpenChange={(open) => {
                    if (!open) {
                        setOpenOption(null);
                    }
                }}
            >
                <SheetContent className="w-full gap-0 sm:max-w-lg data-[side=right]:sm:max-w-lg">
                    {openOption === "telegram" ? (
                        <>
                            <SheetHeader className="border-b">
                                <SheetTitle>New Telegram bot channel</SheetTitle>
                                <SheetDescription>
                                    {agent
                                        ? `Connect a Telegram bot, routed to “${agent.name}”.`
                                        : "Connect a Telegram bot and route it to an agent."}
                                </SheetDescription>
                            </SheetHeader>
                            <TelegramChannelForm slug={slug} agent={agent} onCreated={() => setOpenOption(null)} />
                        </>
                    ) : openOption === "slack" ? (
                        <>
                            <SheetHeader className="border-b">
                                <SheetTitle>New Slack channel</SheetTitle>
                                <SheetDescription>
                                    {agent
                                        ? `Connect a Slack app for direct messages, routed to “${agent.name}”.`
                                        : "Connect a Slack app and answer its direct messages with an agent."}
                                </SheetDescription>
                            </SheetHeader>
                            <SlackChannelForm slug={slug} agent={agent} onDone={() => setOpenOption(null)} />
                        </>
                    ) : openOption === "discord" ? (
                        <>
                            <SheetHeader className="border-b">
                                <SheetTitle>New Discord channel</SheetTitle>
                                <SheetDescription>
                                    {agent
                                        ? `Connect a Discord bot for direct messages, routed to “${agent.name}”.`
                                        : "Connect a Discord bot and answer its direct messages with an agent."}
                                </SheetDescription>
                            </SheetHeader>
                            <DiscordChannelForm slug={slug} agent={agent} onDone={() => setOpenOption(null)} />
                        </>
                    ) : (
                        <>
                            <SheetHeader className="border-b">
                                <SheetTitle>New web widget channel</SheetTitle>
                                <SheetDescription>
                                    {agent
                                        ? `Embed a chat widget on your site, routed to “${agent.name}”.`
                                        : "Embed a chat widget on your site and route it to an agent."}
                                </SheetDescription>
                            </SheetHeader>
                            <WebWidgetChannelForm slug={slug} agent={agent} onCreated={() => setOpenOption(null)} />
                        </>
                    )}
                </SheetContent>
            </GuardedSheet>
        </>
    );
}

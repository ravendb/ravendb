import { useState } from "react";
import { CodeXml, MessageCircle, Plus, Send, type LucideIcon } from "lucide-react";
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
import { TelegramChannelForm } from "@/pages/apps/channels/telegram-channel-form";
import { WebWidgetChannelForm, type FixedAgent } from "@/pages/apps/channels/web-widget-channel-form";
import { WhatsAppPersonalChannelForm } from "@/pages/apps/channels/whatsapp-personal-channel-form";

type ChannelOptionId = "web-widget" | "telegram" | "whatsapp-personal";

type ChannelOption = {
    label: string;
    description: string;
    icon: LucideIcon;
} & ({ id: ChannelOptionId; enabled: true } | { id: string; enabled: false });

// The web widget, Telegram and WhatsApp Personal are backed by the channels API today;
// WhatsApp Business is previewed as disabled.
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
        enabled: true,
    },
    {
        id: "whatsapp-business",
        label: "WhatsApp Business",
        description: "Connect via Meta Cloud API",
        icon: MessageCircle,
        enabled: false,
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
                                <span className="text-xs text-muted-foreground">{option.description}</span>
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
                    ) : openOption === "whatsapp-personal" ? (
                        <>
                            <SheetHeader className="border-b">
                                <SheetTitle>New WhatsApp Personal channel</SheetTitle>
                                <SheetDescription>
                                    {agent
                                        ? `Link a phone by scanning a QR code, routed to “${agent.name}”.`
                                        : "Link a personal WhatsApp phone by scanning a QR code."}
                                </SheetDescription>
                            </SheetHeader>
                            <WhatsAppPersonalChannelForm slug={slug} agent={agent} onDone={() => setOpenOption(null)} />
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

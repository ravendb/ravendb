import { useQuery } from "@tanstack/react-query";
import { ChevronDown, CircleHelp } from "lucide-react";
import { api } from "@/api/api";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from "@/components/shadcn/ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { getHelpLinks } from "@/lib/help-links";
import { cn } from "@/lib/utils";

/**
 * Help center links with two layouts, mirroring the ThemeSwitcher split:
 *
 * - `inline` (expanded sidebar): a collapsible group that pushes the links into
 *   the sidebar itself.
 *
 * - `dropdown` (collapsed rail): a single icon button opening a menu, since a
 *   narrow rail has no room to indent a nested list.
 *
 * Every item leaves the product, so the rows carry no per-item external-link
 * glyph - it would repeat the same signal on every line.
 */
export function HelpMenu({ variant = "inline" }: { variant?: "inline" | "dropdown" }) {
    // Shares the query key with the license and dashboard views, so this is
    // usually a cache hit rather than an extra request.
    const licenseQuery = useQuery(api.queries.settings.license());
    const helpLinks = getHelpLinks(licenseQuery.data?.response.id);

    if (variant === "dropdown") {
        return (
            <DropdownMenu>
                <Tooltip>
                    <TooltipTrigger asChild>
                        <DropdownMenuTrigger
                            aria-label="Help center"
                            className={cn(
                                "flex size-8 items-center justify-center rounded-md text-sidebar-foreground/85 transition-colors",
                                "hover:bg-sidebar-foreground/8 hover:text-sidebar-foreground",
                                "focus-visible:ring-2 focus-visible:ring-ring/60 focus-visible:outline-none",
                            )}
                        >
                            <CircleHelp className="size-4" aria-hidden="true" />
                        </DropdownMenuTrigger>
                    </TooltipTrigger>
                    <TooltipContent side="right">Help center</TooltipContent>
                </Tooltip>
                <DropdownMenuContent side="right" align="end" className="w-48">
                    <DropdownMenuLabel>Help center</DropdownMenuLabel>
                    <DropdownMenuSeparator />
                    {helpLinks.map(({ label, href, icon: Icon }) => (
                        <DropdownMenuItem key={label} asChild>
                            <a href={href} target="_blank" rel="noreferrer">
                                <Icon aria-hidden="true" />
                                <span>{label}</span>
                            </a>
                        </DropdownMenuItem>
                    ))}
                </DropdownMenuContent>
            </DropdownMenu>
        );
    }

    return (
        <Collapsible>
            <CollapsibleTrigger
                className={cn(
                    "group flex h-8 w-full items-center gap-2 rounded-md px-2 text-sm transition-colors",
                    "text-sidebar-foreground/85 hover:bg-sidebar-foreground/8 hover:text-sidebar-foreground",
                    "focus-visible:ring-2 focus-visible:ring-ring/60 focus-visible:outline-none",
                )}
            >
                <CircleHelp className="size-4 shrink-0" aria-hidden="true" />
                <span className="truncate">Help center</span>
                <ChevronDown
                    className="ml-auto size-4 shrink-0 transition-transform duration-200 group-data-[state=open]:rotate-180"
                    aria-hidden="true"
                />
            </CollapsibleTrigger>
            <CollapsibleContent
                className={cn(
                    "overflow-hidden",
                    "data-[state=closed]:animate-collapsible-up data-[state=open]:animate-collapsible-down",
                )}
            >
                <div className="flex flex-col gap-0.5 pt-0.5">
                    {helpLinks.map(({ label, href, icon: Icon }) => (
                        <a
                            key={label}
                            href={href}
                            target="_blank"
                            rel="noreferrer"
                            className={cn(
                                "flex h-8 items-center gap-2 rounded-md pr-2 pl-8 text-sm transition-colors",
                                "text-sidebar-foreground/85 hover:bg-sidebar-foreground/8 hover:text-sidebar-foreground",
                            )}
                        >
                            <Icon className="size-4 shrink-0" aria-hidden="true" />
                            <span className="truncate">{label}</span>
                        </a>
                    ))}
                </div>
            </CollapsibleContent>
        </Collapsible>
    );
}

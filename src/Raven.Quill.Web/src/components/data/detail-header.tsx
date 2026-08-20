import type { ComponentType, ReactNode } from "react";
import { ArrowLeft, EllipsisVertical } from "lucide-react";
import { Link } from "react-router";

import { Button } from "@/components/shadcn/ui/button";
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from "@/components/shadcn/ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { cn } from "@/lib/utils";

type BackTo = { to: string; label: string };

// The fixed header shared by the data-source, agent, and channel detail views. It pins to the top of
// the scrolling panel and exposes slots so each view fills only the parts it needs: identity (title +
// status, optional back arrow), a sub-info line, action controls, and optional navigation tabs.
export function DetailHeader({
    title,
    status,
    backTo,
    meta,
    actions,
    tabs,
}: {
    title: ReactNode;
    status?: ReactNode;
    backTo?: BackTo;
    meta?: ReactNode;
    actions?: ReactNode;
    tabs?: ReactNode;
}) {
    return (
        // Bleed all the way to the panel's inner border (cancelling both the scroll gutter and the main
        // panel padding) so each band's bottom border closes against the edges. Padding lives on the
        // bands, not here, so the borders stay full-bleed while their content stays aligned with the body.
        // The scroll container clips horizontal overflow, so this never adds a sideways scrollbar.
        <div className="sticky top-0 z-10 -mx-4 grid bg-surface1 lg:-mx-5 dark:bg-surface2">
            <div className="flex items-start justify-between gap-4 border-b px-4 pb-4 lg:px-5">
                <div className="flex min-w-0 items-start gap-2">
                    {backTo && (
                        <Button
                            asChild
                            variant="ghost"
                            size="icon-sm"
                            className="mt-0.5 -ml-1 shrink-0 text-muted-foreground"
                            aria-label={`Back to ${backTo.label}`}
                        >
                            <Link to={backTo.to}>
                                <ArrowLeft aria-hidden="true" />
                            </Link>
                        </Button>
                    )}
                    {/* Title and meta share this column so the sub-info stays aligned with the title,
                        never indenting under the back arrow. */}
                    <div className="grid min-w-0 gap-1.5">
                        <div className="flex items-center gap-3">
                            <h1 className="truncate text-2xl font-semibold tracking-tight">{title}</h1>
                            {status}
                        </div>
                        {meta && (
                            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-muted-foreground">
                                <TooltipProvider>{meta}</TooltipProvider>
                            </div>
                        )}
                    </div>
                </div>
                {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
            </div>

            {/* The tabs sit in their own band with its own bottom border, so the navigation reads as a
                distinct strip separated from the identity above it. */}
            {tabs && <div className="border-b px-4 lg:px-5">{tabs}</div>}
        </div>
    );
}

// One entry in the sub-info line: a muted icon followed by its value. `mono` renders the value in a
// monospace font (used for ids and connection targets). `tooltip` labels what the value is on hover,
// since the value alone (a model id, a hex channel id, an engine name) is not self-explanatory.
export function DetailHeaderMetaItem({
    icon: Icon,
    mono,
    tooltip,
    children,
}: {
    icon?: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
    mono?: boolean;
    tooltip?: ReactNode;
    children: ReactNode;
}) {
    const item = (
        <span className="inline-flex min-w-0 items-center gap-1.5">
            {Icon && <Icon className="size-3.5 shrink-0 text-muted-foreground" aria-hidden={true} />}
            <span className={cn("truncate", mono && "font-mono")}>{children}</span>
        </span>
    );

    if (!tooltip) {
        return item;
    }

    return (
        <Tooltip>
            <TooltipTrigger asChild>{item}</TooltipTrigger>
            <TooltipContent>{tooltip}</TooltipContent>
        </Tooltip>
    );
}

// The three-dots "more options" trigger + menu. Callers pass the menu items as children.
export function DetailHeaderMenu({ children }: { children: ReactNode }) {
    return (
        <DropdownMenu>
            <DropdownMenuTrigger asChild>
                <Button variant="outline" size="icon-sm" aria-label="More options">
                    <EllipsisVertical aria-hidden="true" />
                </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="min-w-52">
                {children}
            </DropdownMenuContent>
        </DropdownMenu>
    );
}

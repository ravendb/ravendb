import { NavLink, useMatch } from "react-router";
import type { ReactNode } from "react";
import { PanelLeftClose, PanelLeftOpen } from "lucide-react";
import { appNavigationSections, dashboardNavigationSections, navigationItems, type NavigationItem } from "@/routes";
import { HelpMenu } from "@/components/layout/help-menu";
import { ThemeSwitcher } from "@/components/layout/theme-switcher";
import { Badge } from "@/components/shadcn/ui/badge";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { appRoutes } from "@/lib/app-routes";
import { cn } from "@/lib/utils";

type AppSidebarProps = {
    slug?: string;
    hasActiveApp: boolean;
    isCollapsed: boolean;
    isToggleVisible: boolean;
    onToggleCollapsed: () => void;
};

export function AppSidebar({ slug, hasActiveApp, isCollapsed, isToggleVisible, onToggleCollapsed }: AppSidebarProps) {
    return (
        <TooltipProvider>
            <div className="flex h-full flex-col justify-between overflow-hidden text-sidebar-foreground">
                <nav
                    className={cn(
                        "flex flex-1 flex-col overflow-x-hidden overflow-y-auto py-2",
                        // Collapsed: one flat centered stack of size-8 buttons, identical
                        // to the bottom block below. Expanded: the per-group layout.
                        isCollapsed && "no-scrollbar items-center gap-1",
                    )}
                    aria-label="Apps"
                >
                    <SidebarGroup isCollapsed={isCollapsed}>
                        {navigationItems.map((item) => (
                            <SidebarNavLink key={item.label} item={item} isCollapsed={isCollapsed} />
                        ))}
                    </SidebarGroup>

                    {!hasActiveApp &&
                        dashboardNavigationSections.map((section) => (
                            <SidebarGroup key={section.label} label={section.label} isCollapsed={isCollapsed}>
                                {section.items.map((item) => (
                                    <SidebarNavLink key={item.label} item={item} isCollapsed={isCollapsed} />
                                ))}
                            </SidebarGroup>
                        ))}

                    {hasActiveApp &&
                        appNavigationSections.map((section) => (
                            <SidebarGroup key={section.label} label={section.label} isCollapsed={isCollapsed}>
                                {section.items.map((item) => (
                                    <SidebarNavLink
                                        key={item.label}
                                        item={{ ...item, to: getAppNavigationUrl(slug, item.to) }}
                                        isCollapsed={isCollapsed}
                                    />
                                ))}
                            </SidebarGroup>
                        ))}
                </nav>
                <div className={cn("flex flex-col gap-0.5 py-2", isCollapsed ? "items-center" : "px-3")}>
                    <HelpMenu variant={isCollapsed ? "dropdown" : "inline"} />
                    <div
                        className={cn(
                            "flex",
                            isCollapsed ? "flex-col items-center gap-1" : "items-center justify-between",
                        )}
                    >
                        {isToggleVisible && <CollapseButton isCollapsed={isCollapsed} onToggle={onToggleCollapsed} />}
                        <ThemeSwitcher variant={isCollapsed ? "dropdown" : "inline"} />
                    </div>
                </div>
            </div>
        </TooltipProvider>
    );
}

function getAppNavigationUrl(appSlug: string | undefined, path: string) {
    if (!appSlug) {
        return appRoutes.dashboard();
    }

    return appRoutes.app(appSlug, path);
}

function SidebarGroup({ label, isCollapsed, children }: { label?: string; isCollapsed: boolean; children: ReactNode }) {
    // Collapsed: dissolve the group wrapper entirely so links join the single
    // centered stack (no padding, no extra box). This is what makes the nav
    // icons share the exact alignment of the collapse/theme icons.
    if (isCollapsed) {
        return <>{children}</>;
    }

    return (
        <div className="flex flex-col px-3 pt-3 first:pt-0">
            {label && (
                <div className="flex h-7 items-center px-2 text-xs text-sidebar-foreground/70">
                    <span className="truncate">{label}</span>
                </div>
            )}
            <div className="flex flex-col gap-0.5">{children}</div>
        </div>
    );
}

function SidebarNavLink({ item, isCollapsed }: { item: NavigationItem; isCollapsed: boolean }) {
    // Resolve active state ourselves so the NavLink can take a plain string
    // className. A function className would be stringified (not invoked) when
    // the NavLink is passed through a Radix `asChild` Slot in the collapsed
    // tooltip. Mirror NavLink semantics: an `end` link matches exactly;
    // otherwise it also matches descendant routes (`to/*`).
    const descendantPath = item.to === "/" ? "/*" : `${item.to}/*`;
    const isExactMatch = Boolean(useMatch({ path: item.to, end: true }));
    const isDescendantMatch = Boolean(useMatch({ path: descendantPath, end: true }));
    const isActive = item.isEnd ? isExactMatch : isExactMatch || isDescendantMatch;

    if (item.isComingSoon) {
        const disabledItem = (
            <span
                aria-disabled="true"
                className={cn(
                    "flex h-8 cursor-default items-center gap-2 rounded-md text-sm text-sidebar-foreground/50",
                    isCollapsed ? "size-8 justify-center px-0" : "px-2",
                )}
            >
                <item.icon className="size-4 shrink-0" aria-hidden="true" />
                {!isCollapsed && (
                    <>
                        <span className="truncate">{item.label}</span>
                        <Badge variant="secondary" className="ml-auto text-muted-foreground">
                            Coming soon
                        </Badge>
                    </>
                )}
            </span>
        );

        if (!isCollapsed) {
            return disabledItem;
        }

        return (
            <Tooltip>
                <TooltipTrigger asChild>{disabledItem}</TooltipTrigger>
                <TooltipContent side="right">{item.label} (coming soon)</TooltipContent>
            </Tooltip>
        );
    }

    const link = (
        <NavLink
            to={item.to}
            end={item.isEnd}
            aria-label={isCollapsed ? item.label : undefined}
            className={cn(
                "flex h-8 items-center gap-2 rounded-md text-sm transition-colors",
                isCollapsed ? "size-8 justify-center px-0" : "px-2",
                isActive
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground/85 hover:bg-sidebar-foreground/8 hover:text-sidebar-foreground",
            )}
        >
            <item.icon className="size-4 shrink-0" aria-hidden="true" />
            {!isCollapsed && <span className="truncate">{item.label}</span>}
        </NavLink>
    );

    if (!isCollapsed) {
        return link;
    }

    return (
        <Tooltip>
            <TooltipTrigger asChild>{link}</TooltipTrigger>
            <TooltipContent side="right">{item.label}</TooltipContent>
        </Tooltip>
    );
}

function CollapseButton({ isCollapsed, onToggle }: { isCollapsed: boolean; onToggle: () => void }) {
    const button = (
        <button
            type="button"
            onClick={onToggle}
            aria-label={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
            aria-pressed={isCollapsed}
            title={isCollapsed ? undefined : "Collapse sidebar"}
            className="flex size-8 items-center justify-center rounded-md text-sidebar-foreground/70 hover:bg-sidebar-foreground/8 hover:text-sidebar-foreground"
        >
            {isCollapsed ? (
                <PanelLeftOpen className="size-4" aria-hidden="true" />
            ) : (
                <PanelLeftClose className="size-4" aria-hidden="true" />
            )}
        </button>
    );

    if (!isCollapsed) {
        return button;
    }

    return (
        <Tooltip>
            <TooltipTrigger asChild>{button}</TooltipTrigger>
            <TooltipContent side="right">Expand sidebar</TooltipContent>
        </Tooltip>
    );
}

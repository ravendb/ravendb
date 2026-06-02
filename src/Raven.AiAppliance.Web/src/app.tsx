import { Link, NavLink, Outlet, useMatches, useParams } from "react-router";
import { useState } from "react";
import type { ComponentType, ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { CircleHelp, Moon, PanelLeftClose, PanelLeftOpen, Sparkles, Sun, Users } from "lucide-react";
import { appNavigationSections, isAppRouteHandle, navigationItems } from "@/routes";
import { api } from "@/api/api";
import { Button } from "@/components/shadcn/ui/button";
import { useTheme } from "@/components/shadcn/theme-provider";
import { appRoutes } from "@/lib/app-routes";
import { useMediaQuery } from "@/lib/use-media-query";
import { cn } from "@/lib/utils";

const compactSidebarMediaQuery = "(max-width: 63.999rem)";

function App() {
    const { slug } = useParams();
    const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
    const isCompactSidebarViewport = useMediaQuery(compactSidebarMediaQuery);
    const activeRoute = [...useMatches()]
        .reverse()
        .map((match) => match.handle)
        .find(isAppRouteHandle);
    const hasActiveApp = Boolean(slug || activeRoute?.appScoped);
    const isSidebarHidden = Boolean(activeRoute?.isSidebarHidden);
    const isPageTitleHidden = Boolean(activeRoute?.isPageTitleHidden);
    const isBareLayout = Boolean(activeRoute?.isBareLayout);
    const activeAppQuery = useQuery({
        ...api.queries.apps.detail(slug ?? ""),
        enabled: Boolean(slug),
    });
    const activeAppLabel = activeAppQuery.data?.name ?? slug;
    const breadcrumbLabel = hasActiveApp ? activeAppLabel : activeRoute?.breadcrumb;
    const isSidebarEffectivelyCollapsed = isCompactSidebarViewport || isSidebarCollapsed;

    return (
        <div
            className={cn(
                "app-shell bg-background text-foreground",
                isSidebarEffectivelyCollapsed && "app-shell--collapsed",
                isSidebarHidden && "app-shell--no-sidebar",
            )}
        >
            <header className="app-shell__header border-b bg-background px-3 py-2">
                <div className="flex min-w-0 items-center gap-3">
                    <Link
                        to={appRoutes.dashboard()}
                        className="flex size-8 items-center justify-center rounded-full bg-sidebar-accent text-xs font-medium text-sidebar-accent-foreground"
                        aria-label="RavenDB home"
                    >
                        R
                    </Link>
                    <Link to={appRoutes.dashboard()} className="text-sm font-semibold text-sidebar-foreground">
                        ravendb
                    </Link>
                    {breadcrumbLabel && (
                        <>
                            <span className="text-sidebar-foreground/40">/</span>
                            <Link
                                to={hasActiveApp && slug ? appRoutes.app(slug) : "."}
                                className="truncate text-sm font-semibold text-sidebar-foreground"
                            >
                                {breadcrumbLabel}
                            </Link>
                        </>
                    )}
                </div>

                <nav
                    className="ml-4 flex shrink-0 items-center gap-4 text-xs font-semibold"
                    aria-label="Top navigation"
                >
                    <Link to={appRoutes.dashboard()} className="text-foreground hover:text-muted-foreground">
                        Dashboard
                    </Link>
                    <a
                        href="https://docs.ravendb.net/"
                        target="_blank"
                        rel="noreferrer"
                        className="text-foreground hover:text-muted-foreground"
                    >
                        Docs
                    </a>
                    <ThemeSwitch />
                    <Link to="/ai" className="text-foreground hover:text-muted-foreground" aria-label="AI assistant">
                        <Sparkles className="size-4" aria-hidden="true" />
                    </Link>
                </nav>
            </header>

            {!isSidebarHidden && (
                <aside className="app-shell__sidebar border-r border-sidebar-border bg-sidebar">
                    {!isCompactSidebarViewport && (
                        <Button
                            type="button"
                            variant="ghost"
                            size="icon-sm"
                            className="w-full px-1"
                            onClick={() => setIsSidebarCollapsed((value) => !value)}
                            aria-label={isSidebarEffectivelyCollapsed ? "Expand navigation" : "Collapse navigation"}
                            title={isSidebarEffectivelyCollapsed ? "Expand navigation" : "Collapse navigation"}
                        >
                            {isSidebarEffectivelyCollapsed ? (
                                <PanelLeftOpen className="size-4" aria-hidden="true" />
                            ) : (
                                <PanelLeftClose className="size-4" aria-hidden="true" />
                            )}
                        </Button>
                    )}
                    <nav className="flex-1 space-y-5 px-3 py-2" aria-label="Apps">
                        <SidebarSection>
                            {navigationItems.map((item) => (
                                <SidebarLink key={item.to} item={item} isCollapsed={isSidebarEffectivelyCollapsed} />
                            ))}
                        </SidebarSection>

                        {hasActiveApp &&
                            appNavigationSections.map((section) => (
                                <SidebarSection
                                    key={section.label}
                                    label={section.label}
                                    isCollapsed={isSidebarEffectivelyCollapsed}
                                >
                                    {section.items.map((item) => (
                                        <SidebarLink
                                            key={item.to}
                                            item={{
                                                ...item,
                                                to: getAppNavigationUrl(slug, item.to),
                                            }}
                                            isCollapsed={isSidebarEffectivelyCollapsed}
                                        />
                                    ))}
                                </SidebarSection>
                            ))}
                    </nav>
                    <div className="space-y-2 p-3">
                        <SidebarAction
                            to="/community"
                            icon={Users}
                            label="Community"
                            isCollapsed={isSidebarEffectivelyCollapsed}
                        />
                        <SidebarAction
                            to="/help"
                            icon={CircleHelp}
                            label="Help"
                            isCollapsed={isSidebarEffectivelyCollapsed}
                        />
                    </div>
                </aside>
            )}

            <main
                className={cn(
                    "app-shell__main",
                    isBareLayout ? "gap-0 p-0" : "gap-3 px-4 py-5 lg:px-5",
                    isPageTitleHidden && "grid-rows-[minmax(0,1fr)]",
                )}
            >
                {!isPageTitleHidden && (
                    <h1 className="text-xl font-semibold tracking-normal">{activeRoute?.title ?? "My apps"}</h1>
                )}
                <div className="min-h-0 overflow-auto">
                    <Outlet />
                </div>
            </main>
        </div>
    );
}

function SidebarSection({
    label,
    isCollapsed = false,
    children,
}: {
    label?: string;
    isCollapsed?: boolean;
    children: ReactNode;
}) {
    return (
        <div className="space-y-1">
            {label && (
                <p className={cn("px-2 pb-1 text-xs font-medium text-muted-foreground", isCollapsed && "sr-only")}>
                    {label}
                </p>
            )}
            {children}
        </div>
    );
}

function ThemeSwitch() {
    const { theme, setTheme } = useTheme();
    const isDark = theme === "dark";

    return (
        <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            onClick={() => setTheme(isDark ? "light" : "dark")}
            aria-label={isDark ? "Switch to light theme" : "Switch to dark theme"}
            title={isDark ? "Switch to light theme" : "Switch to dark theme"}
        >
            {isDark ? <Sun className="size-4" aria-hidden="true" /> : <Moon className="size-4" aria-hidden="true" />}
        </Button>
    );
}

function getAppNavigationUrl(appSlug: string | undefined, path: string) {
    if (!appSlug) {
        return appRoutes.dashboard();
    }

    return appRoutes.app(appSlug, path);
}

type SidebarLinkProps = {
    item: {
        label: string;
        to: string;
        icon: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
    };
    isCollapsed: boolean;
};

function SidebarLink({ item, isCollapsed }: SidebarLinkProps) {
    const isExactRoute = item.to === "/" || /^\/apps\/[^/]+$/.test(item.to);

    return (
        <NavLink
            to={item.to}
            end={isExactRoute}
            className={({ isActive }) =>
                cn(
                    "flex h-8 items-center gap-2 rounded-md px-2 text-sm font-medium text-sidebar-foreground transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                    isCollapsed && "justify-center",
                    isActive && "bg-sidebar-accent text-sidebar-accent-foreground",
                )
            }
            aria-label={isCollapsed ? item.label : undefined}
            title={isCollapsed ? item.label : undefined}
        >
            <item.icon className="size-4" aria-hidden />
            <span className={cn("truncate", isCollapsed && "sr-only")}>{item.label}</span>
        </NavLink>
    );
}

type SidebarActionProps = {
    to: string;
    icon: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
    label: string;
    isCollapsed: boolean;
};

function SidebarAction({ to, icon: Icon, label, isCollapsed }: SidebarActionProps) {
    return (
        <Link
            to={to}
            className={cn(
                "flex h-8 items-center gap-2 rounded-md px-2 text-sm text-sidebar-foreground transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                isCollapsed && "justify-center",
            )}
            aria-label={label}
            title={label}
        >
            <Icon className="size-4" aria-hidden />
            <span className={cn(isCollapsed && "sr-only")}>{label}</span>
        </Link>
    );
}

export default App;

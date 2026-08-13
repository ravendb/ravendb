import { Link, Outlet, useMatches, useParams } from "react-router";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { MessageCircle, Sparkles } from "lucide-react";
import { isAppRouteHandle } from "@/routes";
import { api } from "@/api/api";
import { AppBreadcrumbSwitcher } from "@/components/layout/app-breadcrumb-switcher";
import { AppSidebar } from "@/components/layout/app-sidebar";
import { CommandPalette } from "@/components/layout/command-palette";
import { UserMenu } from "@/components/layout/user-menu";
import { appRoutes } from "@/lib/app-routes";
import { useMediaQuery } from "@/lib/use-media-query";
import { cn } from "@/lib/utils";
import { RavenLogo } from "@/components/brand/raven-logo";
import { ContactSheet } from "@/components/layout/contact-sheet";
import { Button } from "@/components/shadcn/ui/button";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";

const compactSidebarMediaQuery = "(max-width: 63.999rem)";
const SIDEBAR_COLLAPSED_STORAGE_KEY = "sidebar-collapsed";

function readStoredSidebarCollapsed() {
    return localStorage.getItem(SIDEBAR_COLLAPSED_STORAGE_KEY) === "true";
}

function App() {
    const { slug } = useParams();
    const isCompactSidebarViewport = useMediaQuery(compactSidebarMediaQuery);
    const activeRoute = [...useMatches()]
        .reverse()
        .map((match) => match.handle)
        .find(isAppRouteHandle);
    const shouldCollapseSidebarForRoute = Boolean(activeRoute?.isSidebarCollapsed);
    const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(
        () => shouldCollapseSidebarForRoute || readStoredSidebarCollapsed(),
    );
    const [wasSidebarCollapsedForRoute, setWasSidebarCollapsedForRoute] = useState(shouldCollapseSidebarForRoute);

    // Routes like wizards start with a collapsed sidebar, but the user can still
    // expand it. Restore the stored preference when leaving such a route.
    if (shouldCollapseSidebarForRoute !== wasSidebarCollapsedForRoute) {
        setWasSidebarCollapsedForRoute(shouldCollapseSidebarForRoute);
        setIsSidebarCollapsed(shouldCollapseSidebarForRoute || readStoredSidebarCollapsed());
    }

    const hasActiveApp = Boolean(slug || activeRoute?.appScoped);
    const isPageTitleHidden = Boolean(activeRoute?.isPageTitleHidden);
    const isBareLayout = Boolean(activeRoute?.isBareLayout);
    const activeAppQuery = useQuery({
        ...api.queries.apps.detail(slug ?? ""),
        enabled: Boolean(slug),
    });
    const activeAppLabel = activeAppQuery.data?.name ?? slug;
    const breadcrumbLabel = hasActiveApp ? activeAppLabel : activeRoute?.breadcrumb;
    const isSidebarEffectivelyCollapsed = isCompactSidebarViewport || isSidebarCollapsed;

    const toggleSidebarCollapsed = () => {
        const isCollapsed = !isSidebarCollapsed;
        setIsSidebarCollapsed(isCollapsed);
        localStorage.setItem(SIDEBAR_COLLAPSED_STORAGE_KEY, String(isCollapsed));
    };

    return (
        <div
            className={cn(
                "app-shell bg-surface2 text-foreground dark:bg-surface1",
                isSidebarEffectivelyCollapsed && "app-shell--collapsed",
            )}
        >
            <header className="app-shell__header relative px-3 py-2">
                <div className="flex min-w-0 items-center gap-2">
                    <Link
                        to={appRoutes.dashboard()}
                        className="flex items-center justify-center rounded-full"
                        aria-label="Quill home"
                    >
                        <RavenLogo className="size-6" />
                    </Link>
                    <Link to={appRoutes.dashboard()} className="text-sm font-semibold text-sidebar-foreground">
                        Quill
                    </Link>
                    {breadcrumbLabel && (
                        <>
                            <span className="text-sidebar-foreground/40">/</span>
                            {hasActiveApp && slug ? (
                                <AppBreadcrumbSwitcher slug={slug} appName={breadcrumbLabel} />
                            ) : (
                                <Link to="." className="truncate text-sm font-semibold text-sidebar-foreground">
                                    {breadcrumbLabel}
                                </Link>
                            )}
                        </>
                    )}
                </div>

                <CommandPalette slug={slug} appName={activeAppLabel} />

                <nav className="ml-4 flex shrink-0 items-center gap-4 text-sm" aria-label="Top navigation">
                    <a
                        href="https://docs.ravendb.net/quill"
                        target="_blank"
                        rel="noreferrer"
                        className="text-muted-foreground transition-colors hover:text-foreground"
                    >
                        Docs
                    </a>
                    <ContactSheet
                        trigger={
                            <Button variant="outline" size="sm">
                                <MessageCircle aria-hidden="true" />
                                Feedback
                            </Button>
                        }
                    />
                    <TooltipProvider>
                        <Tooltip>
                            <TooltipTrigger asChild>
                                <span
                                    aria-disabled="true"
                                    aria-label="AI assistant (coming soon)"
                                    className="cursor-default text-primary/50 [filter:drop-shadow(0_0_6px_var(--brand-400))]"
                                >
                                    <Sparkles className="size-4" aria-hidden="true" />
                                </span>
                            </TooltipTrigger>
                            <TooltipContent>AI assistant (coming soon)</TooltipContent>
                        </Tooltip>
                    </TooltipProvider>
                    <UserMenu />
                </nav>
            </header>

            <aside className="app-shell__sidebar">
                <AppSidebar
                    slug={slug}
                    hasActiveApp={hasActiveApp}
                    isCollapsed={isSidebarEffectivelyCollapsed}
                    isToggleVisible={!isCompactSidebarViewport}
                    onToggleCollapsed={toggleSidebarCollapsed}
                />
            </aside>

            <main
                className={cn(
                    "app-shell__main me-2 mb-2 rounded-lg border bg-surface1 dark:bg-surface2",
                    isBareLayout ? "gap-0 p-0" : "gap-3 px-4 py-5 lg:px-5",
                    isPageTitleHidden && "grid-rows-[minmax(0,1fr)]",
                )}
            >
                {!isPageTitleHidden && (
                    <h1 className="text-2xl font-semibold tracking-tight">{activeRoute?.title ?? "My apps"}</h1>
                )}
                <div className={cn("min-h-0 overflow-auto", !isBareLayout && "-mx-2 px-2")}>
                    <Outlet />
                </div>
            </main>
        </div>
    );
}

export default App;

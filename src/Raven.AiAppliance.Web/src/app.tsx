import { Link, Outlet, useMatches, useParams } from "react-router";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Sparkles } from "lucide-react";
import { isAppRouteHandle } from "@/routes";
import { api } from "@/api/api";
import { AppSidebar } from "@/components/layout/app-sidebar";
import { appRoutes } from "@/lib/app-routes";
import { useMediaQuery } from "@/lib/use-media-query";
import { cn } from "@/lib/utils";

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
                "app-shell bg-background text-foreground",
                isSidebarEffectivelyCollapsed && "app-shell--collapsed",
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
                    <Link to="/ai" className="text-foreground hover:text-muted-foreground" aria-label="AI assistant">
                        <Sparkles className="size-4" aria-hidden="true" />
                    </Link>
                </nav>
            </header>

            <aside className="app-shell__sidebar border-r border-sidebar-border bg-sidebar">
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

export default App;

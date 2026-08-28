import { Link, Outlet, useMatches, useParams } from "react-router";
import { useState, type CSSProperties } from "react";
import { useQuery } from "@tanstack/react-query";
import { MessageCircle, Sparkles } from "lucide-react";
import { isAppRouteHandle } from "@/routes";
import { api } from "@/api/api";
import { AppBreadcrumbSwitcher } from "@/components/layout/app-breadcrumb-switcher";
import { AppSidebar } from "@/components/layout/app-sidebar";
import { CommandPalette } from "@/components/layout/command-palette";
import { UserMenu } from "@/components/layout/user-menu";
import { appRoutes } from "@/lib/app-routes";
import { COMPACT_LAYOUT_MEDIA_QUERY, useMediaQuery } from "@/lib/use-media-query";
import { cn } from "@/lib/utils";
import { readStoredValue, writeStoredValue } from "@/lib/safe-storage";
import QuillMark from "@/components/brand/quill-mark.svg?react";
import { AssistantPanel } from "@/components/layout/assistant-panel";
import { ASSISTANT_PANEL_TITLE_ID, useAssistantPinning, useAssistantStore } from "@/components/layout/assistant-store";
import { FeedbackSheet } from "@/components/layout/feedback-sheet";
import { Heading } from "@/components/typography";
import { Button } from "@/components/shadcn/ui/button";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";

const SIDEBAR_COLLAPSED_STORAGE_KEY = "sidebar-collapsed";

function readStoredSidebarCollapsed() {
    return readStoredValue(SIDEBAR_COLLAPSED_STORAGE_KEY) === "true";
}

function App() {
    const { slug } = useParams();
    const isCompactSidebarViewport = useMediaQuery(COMPACT_LAYOUT_MEDIA_QUERY);
    const activeRoute = [...useMatches()]
        .reverse()
        .map((match) => match.handle)
        .find(isAppRouteHandle);
    const shouldCollapseSidebarForRoute = Boolean(activeRoute?.isSidebarCollapsed);
    const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(
        () => shouldCollapseSidebarForRoute || readStoredSidebarCollapsed(),
    );
    const [wasSidebarCollapsedForRoute, setWasSidebarCollapsedForRoute] = useState(shouldCollapseSidebarForRoute);
    const { isPinned: isAssistantPinned } = useAssistantPinning();
    const isAssistantOpen = useAssistantStore((state) => state.isOpen);
    const isAssistantResizing = useAssistantStore((state) => state.isResizing);
    const assistantWidthPx = useAssistantStore((state) => state.widthPx);
    const assistantHeightPx = useAssistantStore((state) => state.heightPx);
    const setAssistantOpen = useAssistantStore((state) => state.setOpen);

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
        writeStoredValue(SIDEBAR_COLLAPSED_STORAGE_KEY, String(isCollapsed));
    };

    return (
        <div
            className={cn(
                "app-shell bg-surface2 text-foreground dark:bg-surface1",
                isSidebarEffectivelyCollapsed && "app-shell--collapsed",
                isAssistantOpen && isAssistantPinned && "app-shell--assistant-open",
                isAssistantResizing && "app-shell--assistant-resizing",
            )}
            style={
                {
                    "--app-assistant-width": `${assistantWidthPx}px`,
                    "--app-assistant-height": `${assistantHeightPx}px`,
                } as CSSProperties
            }
        >
            <header className="app-shell__header relative px-3 py-2">
                <div className="flex min-w-0 items-center gap-2">
                    <Link
                        to={appRoutes.dashboard()}
                        className="group flex size-8 items-center justify-center rounded-md"
                        title="Home"
                        aria-label="Home"
                    >
                        <QuillMark className="size-6 text-sidebar-foreground/85 transition-all group-hover:scale-110 group-hover:text-primary" />
                    </Link>
                    <span className="text-sidebar-foreground/40">/</span>
                    {/* One segment, always the switcher: it shows where you are — the app, or the
                        route that has no app, like the add-application wizard — and opens the app
                        list. Only a route with no title of its own falls back to the placeholder. */}
                    <AppBreadcrumbSwitcher slug={slug} appName={breadcrumbLabel} />
                </div>

                <CommandPalette slug={slug} appName={activeAppLabel} />

                <nav className="ml-4 flex shrink-0 items-center gap-4 text-sm" aria-label="Top navigation">
                    <FeedbackSheet
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
                                <Button
                                    variant="ghost"
                                    size="icon-sm"
                                    onClick={() => setAssistantOpen(!isAssistantOpen)}
                                    aria-label={isAssistantOpen ? "Close AI assistant" : "Open AI assistant"}
                                    aria-pressed={isAssistantOpen}
                                >
                                    <Sparkles
                                        className="text-primary [filter:drop-shadow(0_0_6px_var(--brand-400))]"
                                        aria-hidden="true"
                                    />
                                </Button>
                            </TooltipTrigger>
                            <TooltipContent>AI assistant</TooltipContent>
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
                    <Heading as="h1" variant="page">
                        {activeRoute?.title ?? "My apps"}
                    </Heading>
                )}
                <div
                    className={cn(
                        "min-h-0",
                        // Bleed the scroll area to the panel's inner border (padding keeps content in place) so
                        // a full-bleed detail header can reach the edges instead of being clipped short. Clip
                        // horizontal overflow so the page never scrolls sideways; wide content self-scrolls.
                        isBareLayout ? "overflow-auto" : "-mx-4 overflow-x-clip overflow-y-auto px-4 lg:-mx-5 lg:px-5",
                    )}
                >
                    <Outlet />
                </div>
            </main>

            {/* Stays mounted while closed so the conversation and draft survive toggling. */}
            <aside
                className={cn(
                    isAssistantPinned
                        ? "app-shell__assistant"
                        : "fixed right-4 bottom-4 z-40 flex h-[min(var(--app-assistant-height),calc(100svh-4rem))] w-[min(var(--app-assistant-width),calc(100vw-1rem))] flex-col",
                    !isAssistantPinned && !isAssistantOpen && "hidden",
                )}
                aria-labelledby={ASSISTANT_PANEL_TITLE_ID}
                inert={!isAssistantOpen}
            >
                <AssistantPanel />
            </aside>
        </div>
    );
}

export default App;

import {
    Bot,
    Cable,
    Database,
    Globe,
    Home,
    KeyRound,
    LayoutGrid,
    LineChart,
    MessagesSquare,
    Network,
    Plug,
    Settings,
    ShieldCheck,
    Sparkles,
    type LucideIcon,
} from "lucide-react";
import type { ReactNode } from "react";
import { createBrowserRouter, Outlet, type RouteObject } from "react-router";
import App from "@/app";
import { UnsavedChangesProvider } from "@/components/form/unsaved-changes/unsaved-changes-provider";
import { RedirectAuthenticated, RequireAuth } from "@/components/auth/auth-routes";
import { AppAgentEdit } from "@/pages/apps/app-agent-edit";
import { AppAgents } from "@/pages/apps/app-agents";
import { AppChannelDetail } from "@/pages/apps/app-channel-detail";
import { AppChannels } from "@/pages/apps/app-channels";
import { AppWebWidgetCustomize } from "@/pages/apps/app-web-widget-customize";
import { AppWebWidgetDefaultCustomize } from "@/pages/apps/app-web-widget-default-customize";
import { AppConversations } from "@/pages/apps/app-conversations";
import { AppDataSource } from "@/pages/apps/app-data-source";
import { AppOverview } from "@/pages/apps/app-overview";
import { AppSettings } from "@/pages/apps/app-settings";
import { AppAnalytics } from "@/pages/apps/app-analytics";
import { Login } from "@/pages/auth/login";
import { DashboardCertificates } from "@/pages/dashboard/certificates";
import { DashboardConnectionStrings } from "@/pages/dashboard/connection-strings";
import { DashboardHome } from "@/pages/dashboard/dashboard-home";
import { DashboardIpConfiguration } from "@/pages/dashboard/ip-configuration";
import { DashboardLicense } from "@/pages/dashboard/license";
import { DashboardUsage } from "@/pages/dashboard/usage";
import { appRoutes as appRouteBuilders, ROUTE_PATTERNS } from "@/lib/app-routes";
import { RequireApp } from "@/pages/apps/require-app";
import { AppScopedNotFoundPage, NotFoundPage } from "@/pages/utility/not-found-page";
import { RouteErrorBoundary } from "@/pages/utility/route-error-boundary";
import { SimpleInfoPage } from "@/pages/utility/simple-info-page";
import { AddAppWizard } from "@/pages/setup/add-app-wizard/add-app-wizard";
import { EditAppWizard } from "@/pages/setup/add-app-wizard/edit-app-wizard";
import { AddCapabilityWizard } from "@/pages/setup/add-capability-wizard/add-capability-wizard";

export type AppRouteHandle = {
    title: string;
    subtitle?: string;
    appScoped?: boolean;
    breadcrumb?: string;
    isPageTitleHidden?: boolean;
    isBareLayout?: boolean;
    isSidebarCollapsed?: boolean;
};

export type NavigationItem = {
    label: string;
    to: string;
    icon: LucideIcon;
    isEnd?: boolean;
    isComingSoon?: boolean;
};

type DashboardNavigationDefinition = {
    label: string;
    icon: LucideIcon;
    section?: "database" | "data-prep" | "settings" | "license-billing";
};

type AppRouteDefinitionBase = {
    title: string;
    subtitle?: string;
    navigation?: DashboardNavigationDefinition;
    isPageTitleHidden?: boolean;
} & (
    | {
          element: ReactNode;
          isComingSoon?: never;
      }
    | {
          // Coming-soon pages appear in navigation (disabled, with a badge) but get no route.
          element?: never;
          isComingSoon: true;
      }
);

type AppRouteDefinition =
    | (AppRouteDefinitionBase & {
          index: true;
          path?: never;
      })
    | (AppRouteDefinitionBase & {
          index?: never;
          path: string;
      });

const dashboardPages: AppRouteDefinition[] = [
    {
        index: true,
        title: "My apps",
        navigation: {
            label: "My apps",
            icon: LayoutGrid,
        },
        isPageTitleHidden: true,
        element: <DashboardHome />,
    },
    {
        path: "usage",
        title: "Usage",
        navigation: {
            label: "Usage",
            icon: LineChart,
            section: "license-billing",
        },
        isPageTitleHidden: true,
        element: <DashboardUsage />,
    },
    {
        path: "license",
        title: "License",
        navigation: {
            label: "License",
            icon: KeyRound,
            section: "license-billing",
        },
        isPageTitleHidden: true,
        element: <DashboardLicense />,
    },
    {
        path: "connection-strings",
        title: "AI connection strings",
        navigation: {
            label: "AI connection strings",
            icon: Plug,
            section: "settings",
        },
        isPageTitleHidden: true,
        element: <DashboardConnectionStrings />,
    },
    {
        path: "certificates",
        title: "Certificates",
        navigation: {
            label: "Certificates",
            icon: ShieldCheck,
            section: "settings",
        },
        isPageTitleHidden: true,
        element: <DashboardCertificates />,
    },
    {
        path: "ip-configuration",
        title: "IP configuration",
        navigation: {
            label: "IP configuration",
            icon: Globe,
            section: "settings",
        },
        isPageTitleHidden: true,
        element: <DashboardIpConfiguration />,
    },
];

const appPages: AppRouteDefinition[] = [
    {
        index: true,
        title: "Overview",
        navigation: {
            label: "Overview",
            icon: Home,
            section: "database",
        },
        element: <AppOverview />,
    },
    {
        path: "data-source",
        title: "Data source",
        navigation: {
            label: "Data source",
            icon: Database,
            section: "database",
        },
        element: <AppDataSource />,
    },
    {
        path: "agents",
        title: "Agents",
        navigation: {
            label: "Agents",
            icon: Bot,
            section: "database",
        },
        isPageTitleHidden: true,
        element: <AppAgents />,
    },
    {
        // Agent edit — the full agent configuration form. No navigation entry:
        // reached by editing an agent from the Agents list.
        path: "agents/:agentId/edit",
        title: "Edit agent",
        element: <AppAgentEdit />,
    },
    {
        path: "conversations",
        title: "Conversations",
        navigation: {
            label: "Conversations",
            icon: MessagesSquare,
            section: "database",
        },
        isPageTitleHidden: true,
        element: <AppConversations />,
    },
    {
        path: "gen-ai",
        title: "GenAI",
        navigation: {
            label: "GenAI",
            icon: Sparkles,
            section: "data-prep",
        },
        isComingSoon: true,
    },
    {
        path: "embeddings",
        title: "Embeddings",
        navigation: {
            label: "Embeddings",
            icon: Network,
            section: "data-prep",
        },
        isComingSoon: true,
    },
    {
        path: "channels",
        title: "Channels",
        navigation: {
            label: "Channels",
            icon: Cable,
            section: "settings",
        },
        isPageTitleHidden: true,
        element: <AppChannels />,
    },
    {
        // Channel detail — active embed links for one channel. No navigation entry:
        // reached by opening a channel from the Channels list.
        path: "channels/:channelId",
        title: "Channel",
        element: <AppChannelDetail />,
    },
    {
        // Per-widget embed styling editor + live preview. Reached from channel detail.
        path: "web-widget/:channelId/customize",
        title: "Web widget appearance",
        element: <AppWebWidgetCustomize />,
    },
    {
        // App-level default web-widget styling. Reached from the Channels list.
        path: "web-widget/default-customize",
        title: "Default web widget appearance",
        element: <AppWebWidgetDefaultCustomize />,
    },
    {
        path: "analytics",
        title: "Analytics",
        navigation: {
            label: "Analytics",
            icon: LineChart,
            section: "settings",
        },
        element: <AppAnalytics />,
    },
    {
        path: "settings",
        title: "Settings",
        navigation: {
            label: "Settings",
            icon: Settings,
            section: "settings",
        },
        element: <AppSettings />,
    },
];

function toDashboardNavigationItem(page: AppRouteDefinition): NavigationItem {
    return {
        ...page.navigation!,
        to: page.index ? appRouteBuilders.dashboard() : `/${page.path}`,
        isEnd: Boolean(page.index),
    };
}

export const navigationItems = dashboardPages.flatMap((page) =>
    page.navigation && !page.navigation.section ? [toDashboardNavigationItem(page)] : [],
) satisfies NavigationItem[];

const dashboardNavigationSectionDefinitions = [
    { section: "license-billing", label: "License & Billing" },
    { section: "settings", label: "Settings" },
] as const;

export const dashboardNavigationSections = dashboardNavigationSectionDefinitions.map(({ section, label }) => ({
    label,
    items: dashboardPages.flatMap((page) =>
        page.navigation?.section === section ? [toDashboardNavigationItem(page)] : [],
    ),
})) satisfies Array<{ label: string; items: NavigationItem[] }>;

// Sub-pages that exist in every app, so the app switcher can stay on them.
// Excludes detail pages whose path contains a resource id from the current app.
export const appSectionPaths = new Set(
    appPages.flatMap((page) => (page.path && page.element && !page.path.includes(":") ? [page.path] : [])),
);

const appNavigationSectionDefinitions = [
    { section: "database", label: "Database" },
    { section: "data-prep", label: "Data prep" },
    { section: "settings", label: "Settings" },
] as const;

export const appNavigationSections = appNavigationSectionDefinitions.map(({ section, label }) => ({
    label,
    items: appPages.flatMap((page) =>
        page.navigation?.section === section
            ? [
                  {
                      ...page.navigation,
                      to: page.index ? "" : (page.path ?? ""),
                      isEnd: Boolean(page.index),
                      isComingSoon: page.isComingSoon,
                  },
              ]
            : [],
    ),
})) satisfies Array<{ label: string; items: NavigationItem[] }>;

const dashboardRoutes: RouteObject[] = dashboardPages.map((page) => toRouteObject(page));

const appRoutes: RouteObject[] = appPages.flatMap((page) => (page.isComingSoon ? [] : [toRouteObject(page, true)]));

function toRouteObject(page: AppRouteDefinition, appScoped = false): RouteObject {
    const handle = {
        title: page.title,
        subtitle: page.subtitle,
        appScoped: appScoped,
        isPageTitleHidden: page.isPageTitleHidden,
    } satisfies AppRouteHandle;

    if (page.index) {
        return {
            index: true,
            element: page.element,
            handle,
        };
    }

    return {
        path: page.path,
        element: page.element,
        handle,
    };
}

const utilityRoutes: RouteObject[] = [
    {
        path: ROUTE_PATTERNS.addApp,
        element: <AddAppWizard />,
        handle: {
            title: "Add new application",
            subtitle: "Application connection wizard",
            breadcrumb: "Add new application",
            isBareLayout: true,
            isPageTitleHidden: true,
            isSidebarCollapsed: true,
        } satisfies AppRouteHandle,
    },
    {
        path: "docs",
        element: <SimpleInfoPage title="Docs" description="Open RavenDB documentation from the top navigation." />,
        handle: {
            title: "Docs",
        } satisfies AppRouteHandle,
    },
];

export function isAppRouteHandle(handle: unknown): handle is AppRouteHandle {
    return typeof handle === "object" && handle != null && "title" in handle && typeof handle.title === "string";
}

export const router = createBrowserRouter([
    {
        errorElement: <RouteErrorBoundary />,
        element: (
            <UnsavedChangesProvider>
                <Outlet />
            </UnsavedChangesProvider>
        ),
        children: [
            {
                path: "/login",
                element: (
                    <RedirectAuthenticated>
                        <Login />
                    </RedirectAuthenticated>
                ),
            },
            {
                path: "/",
                element: (
                    <RequireAuth>
                        <App />
                    </RequireAuth>
                ),
                children: [
                    ...dashboardRoutes,
                    ...utilityRoutes,
                    {
                        path: ROUTE_PATTERNS.app,
                        element: <RequireApp />,
                        children: [
                            ...appRoutes,
                            {
                                path: ROUTE_PATTERNS.addCapability,
                                element: <AddCapabilityWizard />,
                                handle: {
                                    title: "Add AI Capability",
                                    appScoped: true,
                                    isBareLayout: true,
                                    isPageTitleHidden: true,
                                    isSidebarCollapsed: true,
                                } satisfies AppRouteHandle,
                            },
                            {
                                // No navigation entry: reached from the dashboard's apps table.
                                path: ROUTE_PATTERNS.editApp,
                                element: <EditAppWizard />,
                                handle: {
                                    title: "Edit application",
                                    appScoped: true,
                                    isBareLayout: true,
                                    isPageTitleHidden: true,
                                    isSidebarCollapsed: true,
                                } satisfies AppRouteHandle,
                            },
                            {
                                path: "*",
                                element: <AppScopedNotFoundPage />,
                                handle: {
                                    title: "Page not found",
                                    appScoped: true,
                                    isPageTitleHidden: true,
                                } satisfies AppRouteHandle,
                            },
                        ],
                    },
                    {
                        path: "*",
                        element: <NotFoundPage />,
                        handle: {
                            title: "Page not found",
                            isPageTitleHidden: true,
                        } satisfies AppRouteHandle,
                    },
                ],
            },
        ],
    },
]);

import {
    Bot,
    Cable,
    Database,
    Home,
    LayoutGrid,
    LineChart,
    MessagesSquare,
    Network,
    Settings,
    Sparkles,
    type LucideIcon,
} from "lucide-react";
import type { ReactNode } from "react";
import { createBrowserRouter, type RouteObject } from "react-router";
import App from "@/app";
import { RedirectAuthenticated, RequireAuth } from "@/components/auth/auth-routes";
import { AppAgents } from "@/pages/apps/app-agents";
import { AppApiUnavailable } from "@/pages/apps/app-api-unavailable";
import { AppConversations } from "@/pages/apps/app-conversations";
import { AppDataSource } from "@/pages/apps/app-data-source";
import { AppOverview } from "@/pages/apps/app-overview";
import { AppSettings } from "@/pages/apps/app-settings";
import { Login } from "@/pages/auth/login";
import { DashboardHome } from "@/pages/dashboard/dashboard-home";
import { appRoutes as appRouteBuilders, ROUTE_PATTERNS } from "@/lib/app-routes";
import { AiPage } from "@/pages/utility/ai-page";
import { SimpleInfoPage } from "@/pages/utility/simple-info-page";
import { AddAppWizard } from "@/pages/setup/add-app-wizard/add-app-wizard";
import { AddCapabilityWizard } from "@/pages/setup/add-capability-wizard/add-capability-wizard";

export type AppRouteHandle = {
    title: string;
    subtitle?: string;
    appScoped?: boolean;
    breadcrumb?: string;
    isPageTitleHidden?: boolean;
    isSidebarHidden?: boolean;
    isBareLayout?: boolean;
};

export type NavigationItem = {
    label: string;
    to: string;
    icon: LucideIcon;
    isEnd?: boolean;
};

type DashboardNavigationDefinition = {
    label: string;
    icon: LucideIcon;
    section?: "database" | "data-prep" | "settings";
};

type AppRouteDefinitionBase = {
    title: string;
    subtitle?: string;
    navigation?: DashboardNavigationDefinition;
    element: ReactNode;
};

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
        element: <DashboardHome />,
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
        element: <AppAgents />,
    },
    {
        path: "conversations",
        title: "Conversations",
        navigation: {
            label: "Conversations",
            icon: MessagesSquare,
            section: "database",
        },
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
        element: <AppApiUnavailable feature="GenAI" />,
    },
    {
        path: "embeddings",
        title: "Embeddings",
        navigation: {
            label: "Embeddings",
            icon: Network,
            section: "data-prep",
        },
        element: <AppApiUnavailable feature="Embeddings" />,
    },
    {
        path: "channels",
        title: "Channels",
        navigation: {
            label: "Channels",
            icon: Cable,
            section: "settings",
        },
        element: <AppApiUnavailable feature="Channels" />,
    },
    {
        path: "usage",
        title: "Usage",
        navigation: {
            label: "Usage",
            icon: LineChart,
            section: "settings",
        },
        element: <AppApiUnavailable feature="Usage" />,
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

export const navigationItems = dashboardPages.flatMap((page) =>
    page.navigation
        ? [
              {
                  ...page.navigation,
                  to: page.index ? appRouteBuilders.dashboard() : `/${page.path}`,
                  isEnd: Boolean(page.index),
              },
          ]
        : [],
) satisfies NavigationItem[];

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
                  },
              ]
            : [],
    ),
})) satisfies Array<{ label: string; items: NavigationItem[] }>;

const dashboardRoutes: RouteObject[] = dashboardPages.map((page) => toRouteObject(page));

const appRoutes: RouteObject[] = appPages.map((page) => toRouteObject(page, true));

function toRouteObject(page: AppRouteDefinition, appScoped = false): RouteObject {
    const handle = {
        title: page.title,
        subtitle: page.subtitle,
        appScoped: appScoped,
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
            isSidebarHidden: true,
        } satisfies AppRouteHandle,
    },
    {
        path: "docs",
        element: <SimpleInfoPage title="Docs" description="Open RavenDB documentation from the top navigation." />,
        handle: {
            title: "Docs",
        } satisfies AppRouteHandle,
    },
    {
        path: "ai",
        element: <AiPage />,
        handle: {
            title: "AI",
        } satisfies AppRouteHandle,
    },
    {
        path: "community",
        element: <SimpleInfoPage title="Community" description="No community API is exposed by this frontend yet." />,
        handle: {
            title: "Community",
        } satisfies AppRouteHandle,
    },
    {
        path: "help",
        element: <SimpleInfoPage title="Help" description="No help API is exposed by this frontend yet." />,
        handle: {
            title: "Help",
        } satisfies AppRouteHandle,
    },
];

export function isAppRouteHandle(handle: unknown): handle is AppRouteHandle {
    return typeof handle === "object" && handle != null && "title" in handle && typeof handle.title === "string";
}

export const router = createBrowserRouter([
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
                            isSidebarHidden: true,
                        } satisfies AppRouteHandle,
                    },
                ],
            },
        ],
    },
]);

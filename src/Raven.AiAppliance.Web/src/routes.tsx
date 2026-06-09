import {
    BarChart3,
    Cable,
    Database,
    LayoutDashboard,
    MessageSquareText,
    Settings,
    SquareKanban,
    type LucideIcon,
} from "lucide-react";
import type { ReactNode } from "react";
import { createBrowserRouter, type RouteObject } from "react-router";
import App from "@/app";
import { RedirectAuthenticated, RequireAuth } from "@/components/auth/auth-routes";
import { AppApiUnavailable } from "@/pages/apps/app-api-unavailable";
import { AppConversations } from "@/pages/apps/app-conversations";
import { AppDataSource } from "@/pages/apps/app-data-source";
import { AppOverview } from "@/pages/apps/app-overview";
import { AppSettings } from "@/pages/apps/app-settings";
import { AppTasks } from "@/pages/apps/app-tasks";
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
};

type DashboardNavigationDefinition = {
    label: string;
    icon: LucideIcon;
    section?: "database" | "settings";
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
            icon: LayoutDashboard,
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
            icon: LayoutDashboard,
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
        path: "tasks",
        title: "Tasks",
        navigation: {
            label: "Tasks",
            icon: SquareKanban,
            section: "database",
        },
        element: <AppTasks />,
    },
    {
        path: "conversations",
        title: "Conversations",
        navigation: {
            label: "Conversations",
            icon: MessageSquareText,
            section: "database",
        },
        element: <AppConversations />,
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
            icon: BarChart3,
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
              },
          ]
        : [],
) satisfies NavigationItem[];

export const appNavigationSections = [
    {
        label: "Database",
        items: appPages.flatMap((page) =>
            page.navigation?.section === "database"
                ? [
                      {
                          ...page.navigation,
                          to: page.index ? "" : (page.path ?? ""),
                      },
                  ]
                : [],
        ),
    },
    {
        label: "Settings",
        items: appPages.flatMap((page) =>
            page.navigation?.section === "settings"
                ? [
                      {
                          ...page.navigation,
                          to: page.path ?? "",
                      },
                  ]
                : [],
        ),
    },
] satisfies Array<{ label: string; items: NavigationItem[] }>;

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

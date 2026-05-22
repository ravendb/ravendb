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
import { DashboardHome } from "@/pages/dashboard-home";
import { Login } from "@/pages/login";
import { PlaceholderPage } from "@/pages/placeholder-page";
import { SetupConnect } from "@/pages/setup-connect";

export type AppRouteHandle = {
    title: string;
    subtitle?: string;
    appScoped?: boolean;
    breadcrumb?: string;
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
        element: (
            <PlaceholderPage title="Overview" description="App overview placeholder for the selected application." />
        ),
    },
    {
        path: "data-source",
        title: "Data source",
        navigation: {
            label: "Data source",
            icon: Database,
            section: "database",
        },
        element: <PlaceholderPage title="Data source" />,
    },
    {
        path: "tasks",
        title: "Tasks",
        navigation: {
            label: "Tasks",
            icon: SquareKanban,
            section: "database",
        },
        element: <PlaceholderPage title="Tasks" />,
    },
    {
        path: "conversations",
        title: "Conversations",
        navigation: {
            label: "Conversations",
            icon: MessageSquareText,
            section: "database",
        },
        element: <PlaceholderPage title="Conversations" />,
    },
    {
        path: "channels",
        title: "Channels",
        navigation: {
            label: "Channels",
            icon: Cable,
            section: "settings",
        },
        element: <PlaceholderPage title="Channels" />,
    },
    {
        path: "usage",
        title: "Usage",
        navigation: {
            label: "Usage",
            icon: BarChart3,
            section: "settings",
        },
        element: <PlaceholderPage title="Usage" />,
    },
    {
        path: "settings",
        title: "Settings",
        navigation: {
            label: "Settings",
            icon: Settings,
            section: "settings",
        },
        element: <PlaceholderPage title="Settings" description="Settings placeholder for the selected app." />,
    },
];

export const navigationItems = dashboardPages.flatMap((page) =>
    page.navigation
        ? [
              {
                  ...page.navigation,
                  to: page.index ? "/" : `/${page.path}`,
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

const dashboardRoutes: RouteObject[] = dashboardPages.map((page) => ({
    path: page.path,
    index: page.index,
    element: page.element,
    handle: {
        title: page.title,
        subtitle: page.subtitle,
    } satisfies AppRouteHandle,
}));

const appRoutes: RouteObject[] = appPages.map((page) => ({
    path: page.path,
    index: page.index,
    element: page.element,
    handle: {
        title: page.title,
        subtitle: page.subtitle,
        appScoped: true,
    } satisfies AppRouteHandle,
}));

const utilityRoutes: RouteObject[] = [
    {
        path: "setup/connect",
        element: <SetupConnect />,
        handle: {
            title: "Add new application",
            subtitle: "Application connection wizard",
            breadcrumb: "Add new application",
        } satisfies AppRouteHandle,
    },
    {
        path: "docs",
        element: (
            <PlaceholderPage title="Docs" description="Documentation placeholder for the initial navigation shell." />
        ),
        handle: {
            title: "Docs",
        } satisfies AppRouteHandle,
    },
    {
        path: "ai",
        element: (
            <PlaceholderPage title="AI" description="AI assistant placeholder for the initial navigation shell." />
        ),
        handle: {
            title: "AI",
        } satisfies AppRouteHandle,
    },
    {
        path: "community",
        element: (
            <PlaceholderPage title="Community" description="Community placeholder for the initial navigation shell." />
        ),
        handle: {
            title: "Community",
        } satisfies AppRouteHandle,
    },
    {
        path: "help",
        element: <PlaceholderPage title="Help" description="Help placeholder for the initial navigation shell." />,
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
                path: "apps/:appId",
                children: appRoutes,
            },
        ],
    },
]);

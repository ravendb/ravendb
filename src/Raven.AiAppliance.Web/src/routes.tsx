import {
  Bot,
  Cable,
  LayoutDashboard,
  MessageSquareText,
  PlugZap,
  Settings,
  type LucideIcon,
} from "lucide-react";
import type { ReactNode } from "react";
import { createBrowserRouter, type RouteObject } from "react-router";
import App from "@/App";
import { DashboardHome } from "@/pages/DashboardHome";
import { PlaceholderPage } from "@/pages/PlaceholderPage";
import { SetupConnect } from "@/pages/SetupConnect";

export type AppRouteHandle = {
  title: string;
  subtitle?: string;
};

export type NavigationItem = {
  label: string;
  to: string;
  icon: LucideIcon;
};

type DashboardNavigationDefinition = {
  label: string;
  icon: LucideIcon;
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

const dashboardPages = [
  {
    index: true,
    title: "Overview",
    subtitle: "Local appliance",
    navigation: {
      label: "Overview",
      icon: LayoutDashboard,
    },
    element: <DashboardHome />,
  },
  {
    path: "cdc",
    title: "CDC",
    navigation: {
      label: "CDC",
      icon: PlugZap,
    },
    element: <PlaceholderPage title="CDC" />,
  },
  {
    path: "ai-agents",
    title: "AI Agents",
    navigation: {
      label: "AI Agents",
      icon: Bot,
    },
    element: <PlaceholderPage title="AI Agents" />,
  },
  {
    path: "conversations",
    title: "Conversations",
    navigation: {
      label: "Conversations",
      icon: MessageSquareText,
    },
    element: <PlaceholderPage title="Conversations" />,
  },
  {
    path: "channels-adapters",
    title: "Channels & Adapters",
    navigation: {
      label: "Channels & Adapters",
      icon: Cable,
    },
    element: <PlaceholderPage title="Channels & Adapters" />,
  },
  {
    path: "settings",
    title: "Settings",
    navigation: {
      label: "Settings",
      icon: Settings,
    },
    element: (
      <PlaceholderPage
        title="Settings"
        description="Read-only settings placeholder for the initial dashboard shell."
      />
    ),
  },
] satisfies AppRouteDefinition[];

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

const dashboardRoutes: RouteObject[] = dashboardPages.map((page) => ({
  path: page.path,
  index: page.index,
  element: page.element,
  handle: {
    title: page.title,
    subtitle: page.subtitle,
  } satisfies AppRouteHandle,
}));

const setupConnectRoute: RouteObject = {
  path: "/setup/connect",
  element: <SetupConnect />,
  handle: {
    title: "Setup",
    subtitle: "Application connection wizard",
  } satisfies AppRouteHandle,
};

export function isAppRouteHandle(handle: unknown): handle is AppRouteHandle {
  return (
    typeof handle === "object" &&
    handle != null &&
    "title" in handle &&
    typeof handle.title === "string"
  );
}

export const router = createBrowserRouter([
  {
    path: "/",
    Component: App,
    children: dashboardRoutes,
  },
  setupConnectRoute,
]);

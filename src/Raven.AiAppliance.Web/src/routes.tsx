import App from "@/App";
import { DashboardHome } from "@/pages/DashboardHome";
import { PlaceholderPage } from "@/pages/PlaceholderPage";
import { SetupConnect } from "@/pages/SetupConnect";
import { createBrowserRouter } from "react-router";

export const router = createBrowserRouter([
  {
    path: "/",
    Component: App,
    children: [
      {
        index: true,
        Component: DashboardHome,
      },
      {
        path: "overview",
        element: <PlaceholderPage title="Overview" />,
      },
      {
        path: "cdc",
        element: <PlaceholderPage title="CDC" />,
      },
      {
        path: "ai-agents",
        element: <PlaceholderPage title="AI Agents" />,
      },
      {
        path: "conversations",
        element: <PlaceholderPage title="Conversations" />,
      },
      {
        path: "channels-adapters",
        element: <PlaceholderPage title="Channels & Adapters" />,
      },
      {
        path: "settings",
        element: (
          <PlaceholderPage
            title="Settings"
            description="Read-only settings placeholder for the initial dashboard shell."
          />
        ),
      },
    ],
  },
  {
    path: "/setup/connect",
    Component: SetupConnect,
  },
]);

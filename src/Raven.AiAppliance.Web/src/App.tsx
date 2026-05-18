import { NavLink, Outlet } from "react-router";
import {
  Bot,
  Cable,
  LayoutDashboard,
  MessageSquareText,
  PanelLeft,
  PlugZap,
  Settings,
} from "lucide-react";
import { cn } from "@/lib/utils";

const navigationItems = [
  {
    label: "Overview",
    path: "/overview",
    icon: LayoutDashboard,
  },
  {
    label: "CDC",
    path: "/cdc",
    icon: PlugZap,
  },
  {
    label: "AI Agents",
    path: "/ai-agents",
    icon: Bot,
  },
  {
    label: "Conversations",
    path: "/conversations",
    icon: MessageSquareText,
  },
  {
    label: "Channels & Adapters",
    path: "/channels-adapters",
    icon: Cable,
  },
  {
    label: "Settings",
    path: "/settings",
    icon: Settings,
  },
] as const;

function App() {
  return (
    <div className="min-h-svh bg-background text-foreground">
      <aside className="fixed inset-y-0 left-0 hidden w-64 border-r bg-sidebar/80 lg:block">
        <div className="flex h-16 items-center gap-3 border-b px-5">
          <div className="flex size-9 items-center justify-center rounded-md bg-primary text-primary-foreground">
            <PanelLeft className="size-4" aria-hidden="true" />
          </div>
          <div>
            <p className="text-sm font-semibold">RavenDB</p>
            <p className="text-xs text-muted-foreground">AI Appliance</p>
          </div>
        </div>

        <nav className="space-y-1 p-3" aria-label="Dashboard">
          {navigationItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              className={({ isActive }) =>
                cn(
                  "flex h-9 items-center gap-3 rounded-md px-3 text-sm font-medium text-sidebar-foreground transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                  isActive &&
                    "bg-sidebar-accent text-sidebar-accent-foreground shadow-xs",
                )
              }
            >
              <item.icon className="size-4" aria-hidden="true" />
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>

      <div className="lg:pl-64">
        <header className="sticky top-0 z-10 flex h-16 items-center justify-between border-b bg-background/95 px-4 backdrop-blur lg:px-6">
          <div>
            <p className="text-sm font-semibold">AI Appliance Dashboard</p>
            <p className="text-xs text-muted-foreground">Local appliance</p>
          </div>
          <div className="rounded-md border bg-muted px-3 py-1 text-xs font-medium text-muted-foreground">
            Demo skeleton
          </div>
        </header>

        <main className="px-4 py-6 lg:px-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

export default App;

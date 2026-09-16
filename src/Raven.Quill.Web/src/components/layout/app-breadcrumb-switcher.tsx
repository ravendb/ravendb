import { useState } from "react";
import { Text } from "@/components/typography";
import { useLocation, useNavigate } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { ChevronsUpDown, Plus } from "lucide-react";
import { api } from "@/api/api";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuRadioGroup,
    DropdownMenuRadioItem,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from "@/components/shadcn/ui/dropdown-menu";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { appRoutes } from "@/lib/app-routes";
import { cn } from "@/lib/utils";
import { appSectionPaths } from "@/routes";

// Both optional: the switcher is a permanent header control, so it also renders with nothing
// selected — on the dashboard, on a utility route, and for an operator whose first app does not
// exist yet. In that state it is the entry point to creating one.
type AppBreadcrumbSwitcherProps = {
    slug?: string;
    appName?: string;
};

export function AppBreadcrumbSwitcher({ slug, appName }: AppBreadcrumbSwitcherProps) {
    const [isOpen, setIsOpen] = useState(false);
    const navigate = useNavigate();
    const { pathname } = useLocation();
    // The list is only needed once the menu opens; fetch lazily like the command palette.
    const appsQuery = useQuery({ ...api.queries.apps.list(), enabled: isOpen });
    const apps = appsQuery.data ?? [];

    const switchToApp = (nextSlug: string) => {
        if (nextSlug === slug) {
            return;
        }
        if (!slug) {
            navigate(appRoutes.app(nextSlug));
            return;
        }
        const currentAppPrefix = `${appRoutes.app(slug)}/`;
        const sectionPath = pathname.startsWith(currentAppPrefix) ? pathname.slice(currentAppPrefix.length) : "";
        // Stay on the same section in the next app; detail pages reference
        // resources of the current app, so those fall back to the overview.
        navigate(appRoutes.app(nextSlug, appSectionPaths.has(sectionPath) ? sectionPath : undefined));
    };

    return (
        <DropdownMenu open={isOpen} onOpenChange={setIsOpen}>
            <DropdownMenuTrigger
                aria-label="Switch app"
                className={cn(
                    "flex min-w-0 items-center gap-1 rounded-md text-sm transition-colors outline-none hover:text-sidebar-foreground/70 focus-visible:ring-2 focus-visible:ring-ring",
                    appName ? "text-sidebar-foreground" : "text-sidebar-foreground/60",
                )}
            >
                <span className="truncate">{appName ?? "Select app"}</span>
                <ChevronsUpDown className="size-3.5 shrink-0 text-sidebar-foreground/50" aria-hidden="true" />
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="w-64">
                <DropdownMenuLabel>Switch app</DropdownMenuLabel>
                <DropdownMenuSeparator />
                {apps.length > 0 ? (
                    <DropdownMenuRadioGroup value={slug ?? ""}>
                        {apps.map((app) => (
                            <DropdownMenuRadioItem
                                key={app.slug}
                                value={app.slug}
                                onSelect={() => switchToApp(app.slug)}
                            >
                                <span className="truncate">{app.name}</span>
                            </DropdownMenuRadioItem>
                        ))}
                    </DropdownMenuRadioGroup>
                ) : appsQuery.isLoading ? (
                    <Text variant="muted" as="div" className="flex items-center gap-2 px-1.5 py-2">
                        <Spinner />
                        Loading apps…
                    </Text>
                ) : (
                    <Text variant="muted" className="px-1.5 py-2">
                        {appsQuery.isError ? "Couldn't load apps." : "No apps yet."}
                    </Text>
                )}
                <DropdownMenuSeparator />
                <DropdownMenuItem onSelect={() => navigate(appRoutes.addApp())}>
                    <Plus aria-hidden="true" />
                    New app
                </DropdownMenuItem>
            </DropdownMenuContent>
        </DropdownMenu>
    );
}

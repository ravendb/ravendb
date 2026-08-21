import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { AppWindow, BookOpen, Plus, Search } from "lucide-react";
import { api } from "@/api/api";
import { useTheme } from "@/components/shadcn/theme-provider";
import {
    Command,
    CommandDialog,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
} from "@/components/shadcn/ui/command";
import { appNavigationSections, navigationItems } from "@/routes";
import { appRoutes } from "@/lib/app-routes";
import { THEME_OPTIONS } from "@/lib/theme-options";

const IS_MAC = typeof navigator !== "undefined" && navigator.platform.toUpperCase().includes("MAC");
const DOCS_URL = "https://docs.ravendb.net/quill";

type CommandPaletteProps = {
    slug?: string;
    appName?: string;
};

export function CommandPalette({ slug, appName }: CommandPaletteProps) {
    // Read in render, not at module scope: this module is part of the
    // routes -> app -> command-palette import cycle, and the routes exports
    // are still in their temporal dead zone while the cycle initializes.
    const appPageCommands = appNavigationSections.flatMap((section) =>
        section.items.filter((item) => !item.isComingSoon),
    );

    const [isOpen, setIsOpen] = useState(false);
    const navigate = useNavigate();
    const { setTheme } = useTheme();
    const appsQuery = useQuery({ ...api.queries.apps.list(), enabled: isOpen });
    const apps = appsQuery.data ?? [];

    useEffect(() => {
        const handleKeyDown = (event: KeyboardEvent) => {
            if (event.key?.toLowerCase() === "k" && (event.metaKey || event.ctrlKey)) {
                event.preventDefault();
                setIsOpen((wasOpen) => !wasOpen);
            }
        };

        window.addEventListener("keydown", handleKeyDown);
        return () => window.removeEventListener("keydown", handleKeyDown);
    }, []);

    const runCommand = (command: () => void) => {
        setIsOpen(false);
        command();
    };

    return (
        <>
            <button
                type="button"
                onClick={() => setIsOpen(true)}
                className="absolute left-1/2 hidden h-8 w-72 -translate-x-1/2 items-center gap-2 rounded-md border bg-surface1 px-3 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-foreground md:flex dark:bg-surface2"
            >
                <Search className="size-4 shrink-0" aria-hidden="true" />
                <span className="flex-1 truncate text-left">Search or run a command…</span>
                <kbd className="rounded border px-1.5 py-0.5 font-mono text-[10px]">{IS_MAC ? "⌘K" : "Ctrl K"}</kbd>
            </button>

            <CommandDialog open={isOpen} onOpenChange={setIsOpen} className="top-[14vh] sm:max-w-xl">
                <Command>
                    <CommandInput placeholder="Search apps, pages, actions…" />
                    <CommandList className="max-h-96">
                        <CommandEmpty>No results found.</CommandEmpty>
                        {slug && (
                            <CommandGroup heading={appName ?? slug}>
                                {appPageCommands.map((item) => (
                                    <CommandItem
                                        key={item.label}
                                        onSelect={() => runCommand(() => navigate(appRoutes.app(slug, item.to)))}
                                    >
                                        <item.icon aria-hidden="true" />
                                        <span>{item.label}</span>
                                    </CommandItem>
                                ))}
                            </CommandGroup>
                        )}
                        {apps.length > 0 && (
                            <CommandGroup heading="Apps">
                                {apps.map((app) => (
                                    <CommandItem
                                        key={app.slug}
                                        value={`${app.name} ${app.slug}`}
                                        onSelect={() => runCommand(() => navigate(appRoutes.app(app.slug)))}
                                    >
                                        <AppWindow aria-hidden="true" />
                                        <span>{app.name}</span>
                                        <span className="text-xs text-muted-foreground">{app.slug}</span>
                                    </CommandItem>
                                ))}
                            </CommandGroup>
                        )}
                        <CommandGroup heading="Navigation">
                            {navigationItems.map((item) => (
                                <CommandItem key={item.label} onSelect={() => runCommand(() => navigate(item.to))}>
                                    <item.icon aria-hidden="true" />
                                    <span>{item.label}</span>
                                </CommandItem>
                            ))}
                            <CommandItem
                                onSelect={() => runCommand(() => window.open(DOCS_URL, "_blank", "noreferrer"))}
                            >
                                <BookOpen aria-hidden="true" />
                                <span>Documentation</span>
                            </CommandItem>
                        </CommandGroup>
                        <CommandGroup heading="Actions">
                            <CommandItem onSelect={() => runCommand(() => navigate(appRoutes.addApp()))}>
                                <Plus aria-hidden="true" />
                                <span>Add app</span>
                            </CommandItem>
                            {THEME_OPTIONS.map((option) => (
                                <CommandItem
                                    key={option.value}
                                    keywords={["theme"]}
                                    onSelect={() => runCommand(() => setTheme(option.value))}
                                >
                                    <option.icon aria-hidden="true" />
                                    <span>{option.label} theme</span>
                                </CommandItem>
                            ))}
                        </CommandGroup>
                    </CommandList>
                    <div className="-mx-1 -mb-1 flex items-center gap-3 border-t px-3 py-2 text-xs text-muted-foreground">
                        <FooterHint keys="↑↓" label="navigate" />
                        <FooterHint keys="↵" label="select" />
                        <FooterHint keys="esc" label="close" />
                    </div>
                </Command>
            </CommandDialog>
        </>
    );
}

function FooterHint({ keys, label }: { keys: string; label: string }) {
    return (
        <span className="flex items-center gap-1.5">
            <kbd className="rounded border px-1 py-0.5 font-mono text-[10px]">{keys}</kbd>
            {label}
        </span>
    );
}

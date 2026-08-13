import { createContext, useContext, useState, type ReactNode } from "react";
import type { Decorator } from "@storybook/react-vite";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createMemoryRouter, RouterProvider } from "react-router";
import { AuthProvider } from "@/components/auth/auth-provider";
import { UnsavedChangesProvider } from "@/components/form/unsaved-changes/unsaved-changes-provider";
import { ThemeProvider, type Theme } from "@/components/shadcn/theme-provider";
import { Toaster } from "@/components/shadcn/ui/sonner";
import { cn } from "@/lib/utils";

// Mounts a route-aware page at a path so it can read `useParams`/`useSearchParams`,
// e.g. an app-scoped page that needs a `:slug`. Stories that don't set this render
// at "/" under a catch-all route, which exposes no params (the common case).
type RouterParameter = {
    // Location the story starts at, e.g. "/apps/demo". Defaults to "/".
    initialPath?: string;
    // Route pattern the page is mounted under, e.g. "/apps/:slug". Defaults to "*".
    path?: string;
};

// Renders the story inside the app shell's main content area (the same padding,
// rounded card, and full-height grid `App` gives a routed page). Omit it for
// standalone full-screen pages like login, which manage their own layout.
type PageParameter = {
    // Route title shown above the page, mirroring the app header. Omit to hide it.
    title?: string;
    // Bare layout used by full-screen flows like the setup wizards: the content
    // fills the available height with no padding or title.
    bare?: boolean;
};

// Type the `router` and `page` story parameters so their keys autocomplete and are
// checked, the same way `msw` is typed in `default-mocks.ts`.
declare module "storybook/internal/csf" {
    interface Parameters {
        router?: RouterParameter;
        page?: PageParameter;
    }
}

function createStoryQueryClient() {
    return new QueryClient({
        defaultOptions: {
            queries: {
                retry: false,
                refetchOnWindowFocus: false,
                staleTime: Infinity,
            },
            mutations: {
                retry: false,
            },
        },
    });
}

// Mirrors the `<main>` in `app.tsx`: a full-height grid card with the route title in
// an `auto` row and the page content in the remaining space.
function StoryPageLayout({ children, page }: { children: ReactNode; page: PageParameter }) {
    const showTitle = !page.bare && Boolean(page.title);

    return (
        <div className="h-svh bg-surface2 p-2 dark:bg-surface1">
            <main
                className={cn(
                    "app-shell__main h-full rounded-lg border bg-surface1 dark:bg-surface2",
                    page.bare ? "gap-0 p-0" : "gap-3 px-4 py-5 lg:px-5",
                    !showTitle && "grid-rows-[minmax(0,1fr)]",
                )}
            >
                {showTitle && <h1 className="text-2xl font-semibold tracking-tight">{page.title}</h1>}
                <div className="min-h-0 overflow-auto">{children}</div>
            </main>
        </div>
    );
}

// The story flows in through context: baked into the route element, it would be frozen at its first args.
const StoryContentContext = createContext<ReactNode>(null);

function StoryContent() {
    return useContext(StoryContentContext);
}

function StoryProviders({ children, theme, router }: { children: ReactNode; theme: Theme; router?: RouterParameter }) {
    const [queryClient] = useState(createStoryQueryClient);
    // A data router, because the unsaved-changes guard's useBlocker needs one.
    const [storyRouter] = useState(() =>
        createMemoryRouter(
            [
                {
                    path: router?.path ?? "*",
                    element: (
                        <UnsavedChangesProvider>
                            <StoryContent />
                        </UnsavedChangesProvider>
                    ),
                },
            ],
            { initialEntries: [router?.initialPath ?? "/"] },
        ),
    );

    return (
        <QueryClientProvider client={queryClient}>
            <ThemeProvider key={theme} defaultTheme={theme}>
                <AuthProvider>
                    <StoryContentContext.Provider value={children}>
                        <RouterProvider router={storyRouter} />
                    </StoryContentContext.Provider>
                    <Toaster />
                </AuthProvider>
            </ThemeProvider>
        </QueryClientProvider>
    );
}

export const withProviders: Decorator = (Story, context) => {
    const theme = (context.globals.theme as Theme) ?? "light";

    // ThemeProvider reads the persisted theme on mount; keep them in sync so the
    // toolbar toggle re-applies the right class when the provider remounts.
    localStorage.setItem("theme", theme);

    const { page } = context.parameters;
    const story = page ? (
        <StoryPageLayout page={page}>
            <Story />
        </StoryPageLayout>
    ) : (
        <div className="min-h-svh bg-background text-foreground">
            <Story />
        </div>
    );

    return (
        <StoryProviders key={context.id} theme={theme} router={context.parameters.router}>
            {story}
        </StoryProviders>
    );
};

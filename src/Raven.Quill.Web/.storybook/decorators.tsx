import { createContext, useContext, useState, type ReactNode } from "react";
import type { Decorator } from "@storybook/react-vite";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createMemoryRouter, isRouteErrorResponse, RouterProvider, useLocation, useRouteError } from "react-router";
import { AuthProvider } from "@/components/auth/auth-provider";
import { UnsavedChangesProvider } from "@/components/form/unsaved-changes/unsaved-changes-provider";
import { AppShellMain } from "@/components/layout/app-shell-main";
import { ThemeProvider, type Theme } from "@/components/shadcn/theme-provider";
import { Toaster } from "@/components/shadcn/ui/sonner";
import { Text } from "@/components/typography";

// Mounts a route-aware page at a path so it can read `useParams`/`useSearchParams`,
// e.g. an app-scoped page that needs a `:slug`. Stories that don't set this render
// at "/" under a catch-all route, which exposes no params (the common case).
type RouterParameter = {
    // Location the story starts at, e.g. "/apps/demo". Defaults to "/".
    initialPath?: string;
    // Route pattern the page is mounted under, e.g. "/apps/:slug". Defaults to "*".
    path?: string;
};

// Renders the story inside the app shell's main content area, the same `AppShellMain` that `App` gives a
// routed page. Omit it for standalone full-screen pages like login, which manage their own layout.
type PageParameter =
    | {
          // The route's title, which the app shows above the page. Omit it when the route sets `isPageTitleHidden`.
          title?: string;
          bare?: false;
      }
    | {
          // Bare layout used by full-screen flows like the setup wizards: the content
          // fills the available height with no padding or title.
          bare: true;
          title?: never;
      };

// Type the `router` and `page` story parameters so their keys autocomplete and are checked.
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

function StoryPageLayout({ children, page }: { children: ReactNode; page: PageParameter }) {
    return (
        <div className="h-svh bg-surface2 p-2 dark:bg-surface1">
            <AppShellMain title={page.title} isBareLayout={page.bare} className="h-full">
                {children}
            </AppShellMain>
        </div>
    );
}

// The story flows in through context: baked into the route element, it would be frozen at its first args.
const StoryContentContext = createContext<ReactNode>(null);

function StoryContent() {
    return useContext(StoryContentContext);
}

// React Router's default error page would catch a story that throws, so the story and its test would pass.
// Rethrowing hands the error to Storybook, which shows it and fails the test. A route error response is not a
// crash: the story navigated to a route it does not mount, e.g. back to the list after a delete.
function StoryRouteError() {
    const error = useRouteError();
    const { pathname } = useLocation();

    if (!isRouteErrorResponse(error)) {
        throw error;
    }

    return (
        <Text className="p-4">
            The story navigated to {pathname}, which it does not mount ({error.status} {error.statusText}).
        </Text>
    );
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
                    errorElement: <StoryRouteError />,
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

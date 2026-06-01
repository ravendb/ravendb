export const ROUTE_PATTERNS = {
    app: "apps/:appId",
    setupConnect: "setup/connect",
} as const;

export const appRoutes = {
    app: (appId: string, path?: string) => {
        const basePath = `/apps/${encodeURIComponent(appId)}`;
        return path ? `${basePath}/${path}` : basePath;
    },
    dashboard: () => "/",
    setupConnect: () => `/${ROUTE_PATTERNS.setupConnect}`,
} as const;

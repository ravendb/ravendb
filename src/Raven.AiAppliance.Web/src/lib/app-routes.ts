export const ROUTE_PATTERNS = {
    app: "apps/:appId",
    addApp: "app/add",
} as const;

export const appRoutes = {
    app: (appId: string, path?: string) => {
        const basePath = `/apps/${encodeURIComponent(appId)}`;
        return path ? `${basePath}/${path}` : basePath;
    },
    dashboard: () => "/",
    addApp: () => `/${ROUTE_PATTERNS.addApp}`,
} as const;

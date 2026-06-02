export const ROUTE_PATTERNS = {
    app: "apps/:slug",
    addApp: "app/add",
} as const;

export const appRoutes = {
    app: (slug: string, path?: string) => {
        const basePath = `/apps/${encodeURIComponent(slug)}`;
        return path ? `${basePath}/${path}` : basePath;
    },
    dashboard: () => "/",
    addApp: () => `/${ROUTE_PATTERNS.addApp}`,
} as const;

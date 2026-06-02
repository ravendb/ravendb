export const ROUTE_PATTERNS = {
    app: "apps/:slug",
    addApp: "app/add",
    // Relative to the app route (apps/:slug).
    addCapability: "capability/add",
} as const;

export const appRoutes = {
    app: (slug: string, path?: string) => {
        const basePath = `/apps/${encodeURIComponent(slug)}`;
        return path ? `${basePath}/${path}` : basePath;
    },
    dashboard: () => "/",
    addApp: () => `/${ROUTE_PATTERNS.addApp}`,
    addCapability: (slug: string) => `/apps/${encodeURIComponent(slug)}/${ROUTE_PATTERNS.addCapability}`,
} as const;

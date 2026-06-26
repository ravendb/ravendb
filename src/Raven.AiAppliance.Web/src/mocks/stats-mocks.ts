import type {
    ApplianceAppResponse,
    DashboardResponse,
    TokensByAppResponse,
    UsagePoint,
} from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const statsMocks = {
    dashboard: (response: DashboardResponse = sampleDashboard) =>
        apiHttp.get("/api/dashboard", ({ response: res }) => res(200).json(response)),
    dashboardApps: (apps: ApplianceAppResponse[] = sampleDashboardApps) =>
        apiHttp.get("/api/dashboard/apps", ({ response }) => response(200).json(apps)),
    usage: (points: UsagePoint[] = sampleUsage) =>
        apiHttp.get("/api/usage", ({ response }) => response(200).json(points)),
    tokensByApp: (response: TokensByAppResponse = sampleTokensByApp) =>
        apiHttp.get("/api/usage/by-app", ({ response: res }) => res(200).json(response)),
};

export const sampleDashboard: DashboardResponse = {
    apps: 4,
    last24h: { conversations: 1100, messages: 3200, tokens: 890000 },
    last7d: { conversations: 7400, messages: 21800, tokens: 6100000 },
    last30d: { conversations: 28900, messages: 86400, tokens: 24300000 },
};

// 24 hourly points with a gentle wave so the agent-runs/tokens sparklines have shape.
export const sampleUsage: UsagePoint[] = Array.from({ length: 24 }, (_, hour) => {
    const wave = Math.sin((hour / 23) * Math.PI * 1.5);
    const base = 120 + wave * 70;
    return {
        timestamp: `2026-06-25T${String(hour).padStart(2, "0")}:00:00Z`,
        invocations: Math.round(base),
        tokens: Math.round(base * 280),
    };
});

export const sampleDashboardApps: ApplianceAppResponse[] = [
    {
        id: "acme-shop",
        name: "acme-shop",
        slug: "acme-shop",
        status: "running",
        source: { type: "PostgreSQL", connectionString: "" },
        tablesCount: 12,
        documentsCount: 482000,
        capabilitiesCount: 3,
        channelsCount: 3,
        adaptersCount: 0,
        agentsCount: 3,
        writesPerMonth: 18400000,
        channelsLabel: "Web widget, Telegram, WhatsApp",
        statusSubtitle: null,
        createdAt: "2026-05-02T10:00:00Z",
        updatedAt: "2026-06-25T09:00:00Z",
    },
    {
        id: "acme-support",
        name: "acme-support",
        slug: "acme-support",
        status: "warning",
        source: { type: "MySQL", connectionString: "" },
        tablesCount: 6,
        documentsCount: 91000,
        capabilitiesCount: 2,
        channelsCount: 1,
        adaptersCount: 0,
        agentsCount: 2,
        writesPerMonth: 4100000,
        channelsLabel: "iframe",
        statusSubtitle: "Replication lag: 42 min",
        createdAt: "2026-05-14T08:30:00Z",
        updatedAt: "2026-06-25T08:18:00Z",
    },
    {
        id: "acme-warehouse",
        name: "acme-warehouse",
        slug: "acme-warehouse",
        status: "loading",
        source: { type: "SQL Server", connectionString: "" },
        tablesCount: 0,
        documentsCount: 0,
        capabilitiesCount: 0,
        channelsCount: 0,
        adaptersCount: 0,
        agentsCount: 0,
        writesPerMonth: null,
        channelsLabel: null,
        statusSubtitle: "Initial load 62% · ETA 1h 14m",
        createdAt: "2026-06-25T07:40:00Z",
        updatedAt: "2026-06-25T09:02:00Z",
    },
    {
        id: "acme-internal",
        name: "acme-internal",
        slug: "acme-internal",
        status: "failed",
        source: { type: "PostgreSQL", connectionString: "" },
        tablesCount: 4,
        documentsCount: 23000,
        capabilitiesCount: 2,
        channelsCount: 1,
        adaptersCount: 0,
        agentsCount: 2,
        writesPerMonth: 1200000,
        channelsLabel: "Telegram",
        statusSubtitle: "CDC failed · 2h ago",
        createdAt: "2026-04-28T12:00:00Z",
        updatedAt: "2026-06-25T07:05:00Z",
    },
];

export const sampleTokensByApp: TokensByAppResponse = {
    apps: [
        { slug: "acme-shop", tokens: 14200000 },
        { slug: "acme-support", tokens: 3100000 },
        { slug: "acme-internal", tokens: 900000 },
    ],
    refreshedMinutesAgo: 2,
};

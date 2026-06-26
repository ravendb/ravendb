import type {
    ActivityEventDto,
    AgentStatsResponse,
    ApplianceAppResponse,
    AppUsageResponse,
    ChannelStatsResponse,
    ConversationDto,
    ConversationStatsResponse,
    DataCollectionDto,
    DashboardResponse,
    SeriesData,
    SeriesKey,
    TokensByAppResponse,
    UsagePoint,
} from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const statsMocks = {
    dashboard: (response: DashboardResponse = sampleDashboard) =>
        apiHttp.get("/api/dashboard", ({ response: res }) => res(200).json(response)),
    dashboardApps: (apps: ApplianceAppResponse[] = sampleDashboardApps) =>
        apiHttp.get("/api/dashboard/apps", ({ response }) => response(200).json(apps)),
    dashboardApp: (app: ApplianceAppResponse = sampleDashboardApps[0]) =>
        apiHttp.get("/api/dashboard/apps/{slug}", ({ response }) => response(200).json(app)),
    usage: (points: UsagePoint[] = sampleUsage) =>
        apiHttp.get("/api/usage", ({ response }) => response(200).json(points)),
    tokensByApp: (response: TokensByAppResponse = sampleTokensByApp) =>
        apiHttp.get("/api/usage/by-app", ({ response: res }) => res(200).json(response)),
    conversationStats: (response: ConversationStatsResponse = sampleConversationStats) =>
        apiHttp.get("/api/apps/{slug}/conversations/stats", ({ response: res }) => res(200).json(response)),
    activity: (events: ActivityEventDto[] = sampleActivity) =>
        apiHttp.get("/api/apps/{slug}/activity", ({ response }) => response(200).json(events)),
    agents: (response: AgentStatsResponse = sampleAgentStats) =>
        apiHttp.get("/api/apps/{slug}/agents/stats", ({ response: res }) => res(200).json(response)),
    channels: (response: ChannelStatsResponse = sampleChannelStats) =>
        apiHttp.get("/api/apps/{slug}/channels/stats", ({ response: res }) => res(200).json(response)),
    collections: (collections: DataCollectionDto[] = sampleCollections) =>
        apiHttp.get("/api/apps/{slug}/collections", ({ response }) => response(200).json(collections)),
    conversations: (conversations: ConversationDto[] = sampleConversations) =>
        apiHttp.get("/api/apps/{slug}/conversations", ({ response }) => response(200).json(conversations)),
    conversation: (conversations: ConversationDto[] = sampleConversations) =>
        apiHttp.get("/api/apps/{slug}/conversations/{conversationId}", ({ params, response }) => {
            const found = conversations.find((candidate) => candidate.id === params.conversationId);
            if (!found) {
                return response(404).json({ error: `Unknown conversation: ${params.conversationId}` });
            }
            return response(200).json({ ...found, transcript: sampleTranscript });
        }),
    appUsage: (response: AppUsageResponse = sampleAppUsage) =>
        apiHttp.get("/api/apps/{slug}/usage", ({ response: res }) => res(200).json(response)),
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

export const sampleConversationStats: ConversationStatsResponse = {
    last24h: { conversations: 1100, messages: 3200, tokens: 890000 },
    last7d: { conversations: 7400, messages: 21800, tokens: 6100000 },
    last30d: { conversations: 28900, messages: 86400, tokens: 24300000 },
};

export const sampleActivity: ActivityEventDto[] = [
    {
        id: "act-1",
        appId: "acme-shop",
        type: "conversation",
        message: "Sales assistant resolved a conversation in #sales",
        timestamp: "2026-06-25T09:02:00Z",
    },
    {
        id: "act-2",
        appId: "acme-shop",
        type: "cdc",
        message: "CDC batch processed 1,240 documents",
        timestamp: "2026-06-25T08:41:00Z",
    },
    {
        id: "act-3",
        appId: "acme-shop",
        type: "channel",
        message: "Web widget “Website widget” connected",
        timestamp: "2026-06-24T17:15:00Z",
    },
    {
        id: "act-4",
        appId: "acme-shop",
        type: "agent",
        message: "FAQ bot was disabled",
        timestamp: "2026-06-24T11:30:00Z",
    },
];

// Agent ids mirror agents-mocks so the per-agent usage table can resolve names.
export const sampleAgentStats: AgentStatsResponse = {
    configuredAgents: 2,
    last24h: { conversations: 320, messages: 980, tokens: 210000 },
    last7d: { conversations: 2100, messages: 6400, tokens: 1500000 },
    last30d: { conversations: 8800, messages: 26100, tokens: 6300000 },
    agents: [
        { agentId: "agents/sales", conversations: 1800, messages: 5200, tokens: 1200000 },
        { agentId: "agents/faq", conversations: 300, messages: 1200, tokens: 300000 },
    ],
};

export const sampleChannelStats: ChannelStatsResponse = {
    total: 2,
    active: 1,
};

export const sampleCollections: DataCollectionDto[] = [
    { appId: "demo", name: "Products", documentsCount: 482000, fields: ["Name", "Price", "Sku", "Description"] },
    { appId: "demo", name: "Orders", documentsCount: 91000, fields: ["Total", "Status", "CustomerId"] },
    { appId: "demo", name: "Customers", documentsCount: 23000, fields: ["Email", "Name"] },
];

export const sampleConversations: ConversationDto[] = [
    {
        id: "chats/1001",
        appId: "demo",
        channelName: "Website widget",
        agentName: "Sales assistant",
        agentInitials: "SA",
        agentColor: "#6366f1",
        params: [{ key: "customerId", value: "cust-42" }],
        lastExchange: [
            { role: "user", text: "Do you have the wireless mouse in stock?", at: "2026-06-25T09:00:00Z" },
            { role: "assistant", text: "Yes, the Wireless Mouse is in stock for $24.99.", at: "2026-06-25T09:00:04Z" },
        ],
        transcript: null,
        state: "Completed",
        lastActivityAt: "2026-06-25T09:00:04Z",
        startedAt: "2026-06-25T08:59:30Z",
        maxDuration: null,
    },
    {
        id: "chats/1002",
        appId: "demo",
        channelName: "Telegram bot",
        agentName: "FAQ bot",
        agentInitials: "FB",
        agentColor: "#0ea5e9",
        params: [],
        lastExchange: [
            { role: "user", text: "What are your opening hours?", at: "2026-06-24T16:20:00Z" },
            { role: "assistant", text: "We're online 24/7 for chat support.", at: "2026-06-24T16:20:03Z" },
        ],
        transcript: null,
        state: "Active",
        lastActivityAt: "2026-06-24T16:20:03Z",
        startedAt: "2026-06-24T16:19:40Z",
        maxDuration: null,
    },
    {
        id: "chats/1003",
        appId: "demo",
        channelName: "Website widget",
        agentName: "Sales assistant",
        agentInitials: "SA",
        agentColor: "#6366f1",
        params: [{ key: "customerId", value: "cust-7" }],
        lastExchange: [
            { role: "user", text: "Where is my order #4821?", at: "2026-06-24T10:05:00Z" },
            {
                role: "assistant",
                text: "Order #4821 shipped yesterday and arrives Friday.",
                at: "2026-06-24T10:05:06Z",
            },
        ],
        transcript: null,
        state: "Completed",
        lastActivityAt: "2026-06-24T10:05:06Z",
        startedAt: "2026-06-24T10:04:20Z",
        maxDuration: null,
    },
];

// Returned by the conversation-detail mock so the transcript sheet has a full thread.
const sampleTranscript: ConversationDto["lastExchange"] = [
    { role: "user", text: "Hi, do you have the wireless mouse in stock?", at: "2026-06-25T08:59:30Z" },
    { role: "assistant", text: "Let me check that for you.", at: "2026-06-25T08:59:34Z" },
    {
        role: "assistant",
        text: "Yes, the Wireless Mouse is in stock for $24.99. Want me to add it to your cart?",
        at: "2026-06-25T09:00:01Z",
    },
    { role: "user", text: "Yes please.", at: "2026-06-25T09:00:03Z" },
    { role: "assistant", text: "Done — it's in your cart. Anything else?", at: "2026-06-25T09:00:04Z" },
];

const usageSparkline = (base: number) =>
    Array.from({ length: 14 }, (_, index) => Math.round(base * (0.7 + 0.3 * Math.sin((index / 13) * Math.PI * 2))));

// Builds a multi-series breakdown shaped like the backend's SeriesData — one
// { t, <key>: number } row per bucket. The generated `points` type is Record<string, never>,
// so assemble the rows loosely and cast to the contract shape.
const buildSeries = (keys: SeriesKey[], bases: number[]): SeriesData => {
    const seriesValues = bases.map((base) => usageSparkline(base));
    const points = Array.from({ length: 14 }, (_, index) => {
        const row: Record<string, number | string> = { t: `2026-06-${String(index + 12).padStart(2, "0")}` };
        keys.forEach((key, seriesIndex) => {
            row[key.key] = seriesValues[seriesIndex][index];
        });
        return row;
    });
    return { points: points as unknown as SeriesData["points"], keys };
};

export const sampleAppUsage: AppUsageResponse = {
    granularity: "Day",
    metrics: {
        conversations: { value: 7400, delta: 12.5, sparkline: usageSparkline(520) },
        tokens: { value: 6100000, delta: 15, sparkline: usageSparkline(430000) },
        cost: { value: 128.4, delta: -3, sparkline: usageSparkline(9) },
        cdcWrites: { value: 18400000, delta: 3, sparkline: usageSparkline(1300000) },
    },
    tokensByCapability: buildSeries(
        [
            { key: "sales", label: "Sales assistant", color: "#3b82f6" },
            { key: "faq", label: "FAQ bot", color: "#8b5cf6" },
        ],
        [280000, 150000],
    ),
    tokensByModel: buildSeries(
        [
            { key: "claude-opus-4-8", label: "claude-opus-4-8", color: "#10b981" },
            { key: "gpt-4o", label: "gpt-4o", color: "#f59e0b" },
        ],
        [300000, 130000],
    ),
    conversationsByChannel: buildSeries(
        [
            { key: "web", label: "Web widget", color: "#22d3ee" },
            { key: "telegram", label: "Telegram", color: "#a855f7" },
        ],
        [320, 200],
    ),
    cdcWrites: Array.from({ length: 14 }, (_, index) => ({
        t: `2026-06-${String(index + 12).padStart(2, "0")}`,
        writes: Math.round(1300000 * (0.7 + 0.3 * Math.sin((index / 13) * Math.PI * 2))),
    })),
    topTables: [
        { name: "Products", writes: 9200000, lagSeconds: 2, lastWriteAt: "2026-06-25T09:00:00Z" },
        { name: "Orders", writes: 5100000, lagSeconds: 4, lastWriteAt: "2026-06-25T08:58:00Z" },
        { name: "Customers", writes: 2300000, lagSeconds: 1, lastWriteAt: "2026-06-25T08:55:00Z" },
        { name: "Inventory", writes: 1800000, lagSeconds: 9, lastWriteAt: "2026-06-25T08:40:00Z" },
    ],
    topCapabilities: [
        { name: "Sales assistant", invocations: 8100, avgTokens: 540, totalTokens: 4374000, cost: 92.1 },
        { name: "FAQ bot", invocations: 2400, avgTokens: 320, totalTokens: 768000, cost: 18.6 },
        { name: "Order tracker", invocations: 1200, avgTokens: 410, totalTokens: 492000, cost: 17.7 },
    ],
};

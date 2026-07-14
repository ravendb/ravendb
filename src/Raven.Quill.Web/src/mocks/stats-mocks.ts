import type {
    ActivityEventDto,
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
    channels: (response: ChannelStatsResponse = sampleChannelStats) =>
        apiHttp.get("/api/apps/{slug}/channels/stats", ({ response: res }) => res(200).json(response)),
    collections: (collections: DataCollectionDto[] = sampleCollections) =>
        apiHttp.get("/api/apps/{slug}/collections", ({ response }) => response(200).json(collections)),
    conversations: (conversations: ConversationDto[] = sampleConversations) =>
        apiHttp.get("/api/apps/{slug}/conversations", ({ response }) =>
            response(200).json(
                conversations.map((conversation) => ({
                    ...conversation,
                    transcript: null,
                })),
            ),
        ),
    conversation: (conversations: ConversationDto[] = sampleConversations) =>
        apiHttp.get("/api/apps/{slug}/conversations/{conversationId}", ({ params, response }) => {
            const found = conversations.find((candidate) => candidate.id === params.conversationId);
            if (!found) {
                return response(404).json({ error: `Unknown conversation: ${params.conversationId}` });
            }
            const transcript = found.lastExchange.length > 0 ? found.lastExchange : sampleTranscript;
            return response(200).json({ ...found, transcript });
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

// 24 hourly points (the Last24h window) with a gentle wave so the series have shape.
export const sampleUsage: UsagePoint[] = Array.from({ length: 24 }, (_, hour) => {
    const wave = Math.sin((hour / 23) * Math.PI * 1.5);
    const base = 120 + wave * 70;
    return {
        timestamp: `2026-06-25T${String(hour).padStart(2, "0")}:00:00Z`,
        conversations: Math.round(base * 0.4),
        messages: Math.round(base),
        tokens: Math.round(base * 280),
        writes: Math.round(base * 45),
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

export const sampleChannelStats: ChannelStatsResponse = {
    total: 2,
    active: 1,
};

export const sampleCollections: DataCollectionDto[] = [
    { appId: "demo", name: "Products", documentsCount: 482000, fields: ["Name", "Price", "Sku", "Description"] },
    { appId: "demo", name: "Orders", documentsCount: 91000, fields: ["Total", "Status", "CustomerId"] },
    { appId: "demo", name: "Customers", documentsCount: 23000, fields: ["Email", "Name"] },
];

// Timestamps are relative to "now" so the "Last activity" column always reads like a live feed
// ("19m ago", "1h ago") regardless of when the mocks are served.
const minutesAgo = (minutes: number) => new Date(Date.now() - minutes * 60_000).toISOString();

export const sampleConversations: ConversationDto[] = [
    {
        id: "cv_49aBc",
        appId: "demo",
        channelName: "Email support",
        agentName: "Support",
        agentInitials: "S",
        params: [
            { key: "locale", value: "en" },
            { key: "surface", value: "email" },
            { key: "tier", value: "gold" },
        ],
        lastExchange: [
            { role: "user", text: "Not yet. What about audit logs and data residency?", at: minutesAgo(20) },
            {
                role: "agent",
                text: "At 60 seats you'd cross the Gold cap. Enterprise removes it — want me to draft a quote?",
                at: minutesAgo(19),
            },
        ],
        transcript: null,
        state: "Active",
        lastActivityAt: minutesAgo(19),
        startedAt: minutesAgo(34),
        maxDuration: null,
    },
    {
        id: "cv_02zXy",
        appId: "demo",
        channelName: "@DataQueryBot",
        agentName: "Orders",
        agentInitials: "O",
        params: [
            { key: "locale", value: "en" },
            { key: "surface", value: "data" },
            { key: "orderId", value: "ord_881" },
        ],
        lastExchange: [
            { role: "user", text: "Thanks, that worked!", at: minutesAgo(21) },
            {
                role: "agent",
                text: "Glad to hear it. I'll mark this resolved — reach out anytime.",
                at: minutesAgo(20),
            },
        ],
        transcript: null,
        state: "Completed",
        lastActivityAt: minutesAgo(20),
        startedAt: minutesAgo(26),
        maxDuration: null,
    },
    {
        id: "cv_a1xTk",
        appId: "demo",
        channelName: "Telegram bot",
        agentName: "Support",
        agentInitials: "S",
        params: [
            { key: "locale", value: "de" },
            { key: "tier", value: "gold" },
        ],
        lastExchange: [
            { role: "user", text: "Wann läuft mein Trial ab?", at: minutesAgo(23) },
            {
                role: "agent",
                text: "Your trial ends on June 14, 2026 — 16 days from now. I can send a reminder.",
                at: minutesAgo(22),
            },
        ],
        transcript: null,
        state: "Active",
        lastActivityAt: minutesAgo(22),
        startedAt: minutesAgo(35),
        maxDuration: null,
    },
    {
        id: "cv_63yTr",
        appId: "demo",
        channelName: "@BillingAssistant",
        agentName: "Returns",
        agentInitials: "R",
        params: [
            { key: "locale", value: "en" },
            { key: "surface", value: "invoice" },
            { key: "ticket", value: "T-2241" },
        ],
        lastExchange: [
            { role: "user", text: "Hi, just checking in — has there been any update?", at: minutesAgo(24) },
            {
                role: "agent",
                text: "Got it. I'll look into your order status and get back shortly.",
                at: minutesAgo(23),
            },
        ],
        transcript: null,
        state: "Idle",
        lastActivityAt: minutesAgo(23),
        startedAt: minutesAgo(45),
        maxDuration: null,
    },
    {
        id: "cv_b2yUl",
        appId: "demo",
        channelName: "Web widget",
        agentName: "Support",
        agentInitials: "S",
        params: [
            { key: "locale", value: "en" },
            { key: "surface", value: "widget" },
            { key: "customerId", value: "cust_7712" },
        ],
        lastExchange: [
            { role: "user", text: "Can I get a copy of my last invoice?", at: minutesAgo(42) },
            {
                role: "agent",
                text: "Invoice #INV-2026-0489 for $348 (May 1) is attached. Want me to email it too?",
                at: minutesAgo(41),
            },
        ],
        transcript: null,
        state: "Idle",
        lastActivityAt: minutesAgo(41),
        startedAt: minutesAgo(58),
        maxDuration: null,
    },
    {
        id: "cv_91bCd",
        appId: "demo",
        channelName: "Email support",
        agentName: "Support",
        agentInitials: "S",
        params: [
            { key: "locale", value: "fr" },
            { key: "surface", value: "email" },
            { key: "userId", value: "u_8810" },
        ],
        lastExchange: [
            { role: "user", text: "Oui, merci. Et pour les remboursements ?", at: minutesAgo(55) },
            {
                role: "agent",
                text: "Le remboursement sera traité sous 3–5 jours ouvrés sur votre carte.",
                at: minutesAgo(54),
            },
        ],
        transcript: null,
        state: "Active",
        lastActivityAt: minutesAgo(54),
        startedAt: minutesAgo(72),
        maxDuration: null,
    },
    {
        id: "cv_c3zVm",
        appId: "demo",
        channelName: "Web widget",
        agentName: "Support",
        agentInitials: "S",
        params: [
            { key: "locale", value: "en" },
            { key: "surface", value: "widget" },
            { key: "tier", value: "starter" },
        ],
        lastExchange: [
            { role: "user", text: "Do you have a student discount?", at: minutesAgo(57) },
            {
                role: "agent",
                text: "We offer 40% off on the Starter plan for verified students. I can send the link.",
                at: minutesAgo(56),
            },
        ],
        transcript: null,
        state: "Active",
        lastActivityAt: minutesAgo(56),
        startedAt: minutesAgo(70),
        maxDuration: null,
    },
    {
        id: "cv_28ulq",
        appId: "demo",
        channelName: "@DataQueryBot",
        agentName: "Orders",
        agentInitials: "O",
        params: [
            { key: "locale", value: "de" },
            { key: "surface", value: "data" },
            { key: "tier", value: "business" },
        ],
        lastExchange: [
            {
                role: "user",
                text: "Welche Zahlungsmethoden akzeptieren Sie für Geschäftskunden?",
                at: minutesAgo(61),
            },
            {
                role: "agent",
                text: "Wir akzeptieren SEPA-Lastschrift, Kreditkarte und Rechnungskauf.",
                at: minutesAgo(60),
            },
        ],
        transcript: null,
        state: "Idle",
        lastActivityAt: minutesAgo(60),
        startedAt: minutesAgo(95),
        maxDuration: null,
    },
    {
        id: "cv_74oJu",
        appId: "demo",
        channelName: "@AcmeVIPBot",
        agentName: "Returns",
        agentInitials: "R",
        params: [
            { key: "locale", value: "en" },
            { key: "surface", value: "orders" },
            { key: "userId", value: "u_9001" },
        ],
        lastExchange: [],
        transcript: null,
        state: "Active",
        lastActivityAt: minutesAgo(120),
        startedAt: minutesAgo(122),
        maxDuration: null,
    },
    {
        id: "cv_d4aWn",
        appId: "demo",
        channelName: "Telegram bot",
        agentName: "Billing",
        agentInitials: "B",
        params: [
            { key: "locale", value: "en" },
            { key: "customerId", value: "cust_3301" },
        ],
        lastExchange: [
            {
                role: "agent",
                text: "Your refund of $84.00 has been processed and should appear on…",
                at: minutesAgo(131),
            },
            { role: "user", text: "Thank you, that was fast!", at: minutesAgo(130) },
        ],
        transcript: null,
        state: "Completed",
        lastActivityAt: minutesAgo(130),
        startedAt: minutesAgo(150),
        maxDuration: null,
    },
    {
        id: "cv_5kPq2",
        appId: "demo",
        channelName: "Web widget",
        agentName: "Sales",
        agentInitials: "SA",
        params: [
            { key: "locale", value: "en" },
            { key: "surface", value: "widget" },
            { key: "tier", value: "business" },
        ],
        lastExchange: [
            { role: "user", text: "Can you compare Pro vs Enterprise?", at: minutesAgo(181) },
            {
                role: "agent",
                text: "Enterprise adds SSO, audit logs, and unlimited seats. Here's a quick breakdown.",
                at: minutesAgo(180),
            },
        ],
        transcript: null,
        state: "Completed",
        lastActivityAt: minutesAgo(180),
        startedAt: minutesAgo(210),
        maxDuration: null,
    },
    {
        id: "cv_7mNb8",
        appId: "demo",
        channelName: "@BillingAssistant",
        agentName: "Billing",
        agentInitials: "B",
        params: [
            { key: "locale", value: "en" },
            { key: "surface", value: "invoice" },
            { key: "tier", value: "gold" },
        ],
        lastExchange: [
            { role: "user", text: "My card was charged twice.", at: minutesAgo(301) },
            {
                role: "agent",
                text: "I see two charges on June 27 — I've refunded the duplicate. Sorry about that!",
                at: minutesAgo(300),
            },
        ],
        transcript: null,
        state: "Completed",
        lastActivityAt: minutesAgo(300),
        startedAt: minutesAgo(340),
        maxDuration: null,
    },
];

// Returned by the conversation-detail mock so the transcript sheet has a full thread.
const sampleTranscript: ConversationDto["lastExchange"] = [
    { role: "user", text: "Hi, do you have the wireless mouse in stock?", at: "2026-06-25T08:59:30Z" },
    { role: "agent", text: "Let me check that for you.", at: "2026-06-25T08:59:34Z" },
    {
        role: "agent",
        text: "Yes, the Wireless Mouse is in stock for $24.99. Want me to add it to your cart?",
        at: "2026-06-25T09:00:01Z",
    },
    { role: "user", text: "Yes please.", at: "2026-06-25T09:00:03Z" },
    { role: "agent", text: "Done — it's in your cart. Anything else?", at: "2026-06-25T09:00:04Z" },
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
    metrics: {
        conversations: { value: 7400, delta: 12.5, sparkline: usageSparkline(520) },
        tokens: { value: 6100000, delta: 15, sparkline: usageSparkline(430000) },
        cdcWrites: { value: 18400000, delta: 3, sparkline: usageSparkline(1300000) },
    },
    tokensByCapability: buildSeries(
        [
            { key: "sales", label: "Sales assistant" },
            { key: "faq", label: "FAQ bot" },
        ],
        [280000, 150000],
    ),
    tokensByModel: buildSeries(
        [
            { key: "claude-opus-4-8", label: "claude-opus-4-8" },
            { key: "gpt-4o", label: "gpt-4o" },
        ],
        [300000, 130000],
    ),
    conversationsByChannel: buildSeries(
        [
            { key: "web", label: "Web widget" },
            { key: "telegram", label: "Telegram" },
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
        { name: "Sales assistant", invocations: 8100, avgTokens: 540, totalTokens: 4374000 },
        { name: "FAQ bot", invocations: 2400, avgTokens: 320, totalTokens: 768000 },
        { name: "Order tracker", invocations: 1200, avgTokens: 410, totalTokens: 492000 },
    ],
};

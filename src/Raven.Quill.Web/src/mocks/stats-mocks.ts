import type {
    ApplianceAppResponse,
    AppUsageResponse,
    ChannelStatsResponse,
    ConversationDto,
    ConversationStatsResponse,
    DataCollectionDto,
    SeriesData,
    SeriesKey,
    TokensByAppResponse,
    UsagePoint,
    UsageResponse,
} from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const statsMocks = {
    dashboardApps: (apps: ApplianceAppResponse[] = sampleDashboardApps) =>
        apiHttp.get("/api/dashboard/apps", ({ response }) => response(200).json(apps)),
    dashboardApp: (app: ApplianceAppResponse = sampleDashboardApps[0]) =>
        apiHttp.get("/api/dashboard/apps/{slug}", ({ response }) => response(200).json(app)),
    usage: (usage: UsageResponse = sampleUsageResponse) =>
        apiHttp.get("/api/usage", ({ response }) => response(200).json(usage)),
    tokensByApp: (response: TokensByAppResponse = sampleTokensByApp) =>
        apiHttp.get("/api/usage/by-app", ({ response: res }) => res(200).json(response)),
    conversationStats: (response: ConversationStatsResponse = sampleConversationStats) =>
        apiHttp.get("/api/apps/{slug}/conversations/stats", ({ response: res }) => res(200).json(response)),
    channels: (response: ChannelStatsResponse = sampleChannelStats) =>
        apiHttp.get("/api/apps/{slug}/channels/stats", ({ response: res }) => res(200).json(response)),
    collections: (collections: DataCollectionDto[] = sampleCollections) =>
        apiHttp.get("/api/apps/{slug}/collections", ({ response }) => response(200).json(collections)),
    conversations: (conversations: ConversationDto[] = sampleConversations) =>
        apiHttp.get("/api/apps/{slug}/conversations", ({ query, response }) => {
            const start = Number(query.get("start") ?? 0);
            const pageSize = Number(query.get("pageSize") ?? conversations.length);
            return response(200).json({
                conversations: conversations.slice(start, start + pageSize).map((conversation) => ({
                    ...conversation,
                    transcript: [],
                })),
                totalResults: conversations.length,
            });
        }),
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

export const sampleUsageResponse: UsageResponse = {
    points: sampleUsage,
    writesByApp: [
        { slug: "acme-shop", writes: 18400000 },
        { slug: "acme-support", writes: 4100000 },
        { slug: "acme-warehouse", writes: 0 },
        { slug: "acme-internal", writes: 1200000 },
    ],
};

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
        channelsLabel: "Web widget, Telegram, WhatsApp, Slack, Discord",
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
        channelsLabel: "Telegram",
        statusSubtitle: "Sync failed · 2h ago",
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
    conversations: 7400,
    messages: 21800,
    tokens: 6100000,
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
            { role: "user", content: "Not yet. What about audit logs and data residency?", timestamp: minutesAgo(20) },
            {
                role: "assistant",
                content: "At 60 seats you'd cross the Gold cap. Enterprise removes it — want me to draft a quote?",
                timestamp: minutesAgo(19),
            },
        ],
        transcript: [],
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
            { role: "user", content: "Thanks, that worked!", timestamp: minutesAgo(21) },
            {
                role: "assistant",
                content: "Glad to hear it. I'll mark this resolved — reach out anytime.",
                timestamp: minutesAgo(20),
            },
        ],
        transcript: [],
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
            { role: "user", content: "Wann läuft mein Trial ab?", timestamp: minutesAgo(23) },
            {
                role: "assistant",
                content: "Your trial ends on June 14, 2026 — 16 days from now. I can send a reminder.",
                timestamp: minutesAgo(22),
            },
        ],
        transcript: [],
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
            { role: "user", content: "Hi, just checking in — has there been any update?", timestamp: minutesAgo(24) },
            {
                role: "assistant",
                content: "Got it. I'll look into your order status and get back shortly.",
                timestamp: minutesAgo(23),
            },
        ],
        transcript: [],
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
            { role: "user", content: "Can I get a copy of my last invoice?", timestamp: minutesAgo(42) },
            {
                role: "assistant",
                content: "Invoice #INV-2026-0489 for $348 (May 1) is attached. Want me to email it too?",
                timestamp: minutesAgo(41),
            },
        ],
        transcript: [],
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
            { role: "user", content: "Oui, merci. Et pour les remboursements ?", timestamp: minutesAgo(55) },
            {
                role: "assistant",
                content: "Le remboursement sera traité sous 3–5 jours ouvrés sur votre carte.",
                timestamp: minutesAgo(54),
            },
        ],
        transcript: [],
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
            { role: "user", content: "Do you have a student discount?", timestamp: minutesAgo(57) },
            {
                role: "assistant",
                content: "We offer 40% off on the Starter plan for verified students. I can send the link.",
                timestamp: minutesAgo(56),
            },
        ],
        transcript: [],
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
                content: "Welche Zahlungsmethoden akzeptieren Sie für Geschäftskunden?",
                timestamp: minutesAgo(61),
            },
            {
                role: "assistant",
                content: "Wir akzeptieren SEPA-Lastschrift, Kreditkarte und Rechnungskauf.",
                timestamp: minutesAgo(60),
            },
        ],
        transcript: [],
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
        transcript: [],
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
                role: "assistant",
                content: "Your refund of $84.00 has been processed and should appear on…",
                timestamp: minutesAgo(131),
            },
            { role: "user", content: "Thank you, that was fast!", timestamp: minutesAgo(130) },
        ],
        transcript: [],
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
            { role: "user", content: "Can you compare Pro vs Enterprise?", timestamp: minutesAgo(181) },
            {
                role: "assistant",
                content: "Enterprise adds SSO, audit logs, and unlimited seats. Here's a quick breakdown.",
                timestamp: minutesAgo(180),
            },
        ],
        transcript: [],
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
            { role: "user", content: "My card was charged twice.", timestamp: minutesAgo(301) },
            {
                role: "assistant",
                content: "I see two charges on June 27 — I've refunded the duplicate. Sorry about that!",
                timestamp: minutesAgo(300),
            },
        ],
        transcript: [],
        state: "Completed",
        lastActivityAt: minutesAgo(300),
        startedAt: minutesAgo(340),
        maxDuration: null,
    },
];

// The detail EP returns every message, so a real transcript runs far past a screenful. This uneven
// tail follows the scripted exchange below, to give the virtualized list something to virtualize.
const sampleTranscriptTail: ConversationDto["transcript"] = Array.from({ length: 60 }, (_, index) => {
    const at = new Date(Date.parse("2026-06-25T09:01:00Z") + index * 45_000).toISOString();
    return index % 2 === 0
        ? { role: "user" as const, content: `Follow-up ${index / 2 + 1}: can you check that one too?`, timestamp: at }
        : {
              role: "assistant" as const,
              content: `Checked it.${" Here is a little more detail about what I found.".repeat((index % 6) + 1)}`,
              timestamp: at,
          };
});

const sampleTranscript: ConversationDto["transcript"] = [
    {
        role: "system",
        content:
            "You are a helpful store assistant. Answer questions about products, stock, and orders using the available tools.",
        timestamp: "2026-06-25T08:59:20Z",
    },
    { role: "user", content: "Hi, do you have the wireless mouse in stock?", timestamp: "2026-06-25T08:59:30Z" },
    {
        role: "assistant",
        content: "Let me check that for you.",
        timestamp: "2026-06-25T08:59:34Z",
        toolCalls: [
            {
                id: "call_01",
                name: "search-products",
                arguments: JSON.stringify({ searchTerm: "wireless mouse", pageSize: 5 }),
                result: JSON.stringify([
                    { name: "Wireless Mouse", price: 24.99, unitsInStock: 132 },
                    { name: "Wireless Mouse Pro", price: 39.99, unitsInStock: 0 },
                ]),
            },
        ],
    },
    {
        role: "assistant",
        content: "Yes, the Wireless Mouse is in stock for $24.99. Want me to add it to your cart?",
        timestamp: "2026-06-25T09:00:01Z",
    },
    { role: "user", content: "Yes please.", timestamp: "2026-06-25T09:00:03Z" },
    {
        role: "assistant",
        content: "Done — it's in your cart. Anything else?",
        timestamp: "2026-06-25T09:00:04Z",
        toolCalls: [
            {
                id: "call_02",
                name: "add-to-cart",
                arguments: JSON.stringify({ productName: "Wireless Mouse", quantity: 1 }),
                result: JSON.stringify({ status: "added", cartTotal: 24.99 }),
            },
        ],
    },
    ...sampleTranscriptTail,
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
    topCapabilities: [
        { name: "Sales assistant", invocations: 8100, avgTokens: 540, totalTokens: 4374000 },
        { name: "FAQ bot", invocations: 2400, avgTokens: 320, totalTokens: 768000 },
        { name: "Order tracker", invocations: 1200, avgTokens: 410, totalTokens: 492000 },
    ],
};

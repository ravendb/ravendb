import type { AnyHandler } from "msw";
import type { AiHelperStatus, DiscoverResponse } from "@/api/generated/server-api";
import { agentsMocks } from "./agents-mocks";
import { aiConnectionStringsMocks } from "./ai-connection-strings-mocks";
import { aiModelsMocks } from "./ai-models-mocks";
import { appsMocks } from "./apps-mocks";
import { assistantMocks } from "./assistant-mocks";
import { authMocks } from "./auth-mocks";
import { bootstrapMocks } from "./bootstrap-mocks";
import { channelsMocks } from "./channels-mocks";
import { discordMocks } from "./discord-mocks";
import { dnsMocks } from "./dns-mocks";
import { embedLinksMocks } from "./embed-links-mocks";
import { iframeHandlers } from "./iframe-mocks";
import { migrationMocks } from "./migration-mocks";
import { settingsMocks } from "./settings-mocks";
import { setupMocks } from "./setup-mocks";
import { slackMocks } from "./slack-mocks";
import { statsMocks } from "./stats-mocks";

// Happy-path defaults for every server endpoint, keyed by service. Storybook merges
// parameters deeply, so a story can replace a single service and keep the rest, and
// every mock accepts a custom response value:
//
//     parameters: {
//         msw: mockHandlers({
//             bootstrap: [bootstrapMocks.status({ state: "NeedsActivation" })],
//         }),
//     },
export const defaultApiMocks = {
    agents: [agentsMocks.list(), agentsMocks.get(), agentsMocks.edit(), agentsMocks.delete()],
    aiConnectionStrings: [
        aiConnectionStringsMocks.list(),
        aiConnectionStringsMocks.detail(),
        aiConnectionStringsMocks.create(),
        aiConnectionStringsMocks.test(),
        aiConnectionStringsMocks.delete(),
    ],
    aiModels: [aiModelsMocks.list()],
    apps: [
        appsMocks.list(),
        appsMocks.detail(),
        appsMocks.delete(),
        appsMocks.cdcProgress(),
        appsMocks.cdcGet(),
        appsMocks.cdcErrors(),
        appsMocks.cdcRestart(),
        appsMocks.provisionAgent(),
        appsMocks.setupTry(),
        appsMocks.suggestAgent(),
        appsMocks.aiConnectionStringsList(),
    ],
    assistant: [assistantMocks.chat(), assistantMocks.consent(), assistantMocks.giveConsent()],
    auth: [authMocks.status(), authMocks.login(), authMocks.logout()],
    bootstrap: [bootstrapMocks.status()],
    channels: [channelsMocks.list(), channelsMocks.create(), channelsMocks.update(), channelsMocks.delete()],
    slack: [slackMocks.health()],
    discord: [discordMocks.health()],
    dns: [dnsMocks.ipBinding()],
    embedLinks: [embedLinksMocks.list(), embedLinksMocks.mint(), embedLinksMocks.revoke()],
    iframe: iframeHandlers(),
    settings: [
        settingsMocks.feedback(),
        settingsMocks.license(),
        settingsMocks.usage(),
        settingsMocks.certificates(),
        settingsMocks.certificatesGenerate(),
        settingsMocks.certificatesEdit(),
    ],
    stats: [
        statsMocks.dashboardApps(),
        statsMocks.dashboardApp(),
        statsMocks.usage(),
        statsMocks.tokensByApp(),
        statsMocks.conversationStats(),
        statsMocks.channels(),
        statsMocks.collections(),
        statsMocks.conversations(),
        statsMocks.conversation(),
        statsMocks.appUsage(),
    ],
    setup: [
        setupMocks.connect(),
        setupMocks.discover(),
        setupMocks.draftMissing(),
        setupMocks.draftSave(),
        setupMocks.verifyCdc(),
        setupMocks.map(),
        setupMocks.testMapping(),
        setupMocks.provision(),
        migrationMocks.proposalStart(),
        migrationMocks.proposalAsk(),
        migrationMocks.areaStart(),
        migrationMocks.areaAsk(),
        migrationMocks.stop(),
        migrationMocks.planGetMissing(),
        migrationMocks.planDiscard(),
        migrationMocks.planSave(),
        migrationMocks.apply(),
    ],
} satisfies Record<string, AnyHandler[]>;

// Handler overrides are keyed by the service names above. A story (or the preview
// default) can replace any subset and keep the rest.
type MockHandlers = Partial<Record<keyof typeof defaultApiMocks, AnyHandler[]>>;

// Builds the `msw` story parameter so overrides autocomplete and a mistyped service name is a
// compile error instead of a silently ignored handler. msw-storybook-addon already types
// `parameters.msw` loosely, so a stricter module augmentation would conflict with it.
export function mockHandlers(handlers: MockHandlers) {
    return { handlers };
}

// Each override goes first because MSW answers with the first matching handler; the rest of the
// service keeps its defaults.
export function discoverHandlers(discovery: DiscoverResponse): MockHandlers {
    return { setup: [setupMocks.discover(discovery), ...defaultApiMocks.setup] };
}

export function consentHandlers(status: AiHelperStatus): MockHandlers {
    return { assistant: [assistantMocks.consent({ status }), ...defaultApiMocks.assistant] };
}

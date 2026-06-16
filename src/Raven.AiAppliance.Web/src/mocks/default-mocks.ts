import type { RequestHandler } from "msw";
import { agentsMocks } from "./agents-mocks";
import { aiConnectionStringsMocks } from "./ai-connection-strings-mocks";
import { appsMocks } from "./apps-mocks";
import { bootstrapMocks } from "./bootstrap-mocks";
import { channelsMocks } from "./channels-mocks";
import { chatMocks } from "./chat-mocks";
import { embedLinksMocks } from "./embed-links-mocks";
import { setupMocks } from "./setup-mocks";

// Happy-path defaults for every server endpoint, keyed by service. Storybook merges
// parameters deeply, so a story can replace a single service and keep the rest, and
// every mock accepts a custom response value:
//
//     parameters: {
//         msw: {
//             handlers: {
//                 bootstrap: [bootstrapMocks.status({ state: "NeedsActivation" })],
//             },
//         },
//     },
export const defaultApiMocks = {
    agents: [agentsMocks.list()],
    aiConnectionStrings: [
        aiConnectionStringsMocks.list(),
        aiConnectionStringsMocks.detail(),
        aiConnectionStringsMocks.create(),
        aiConnectionStringsMocks.delete(),
    ],
    apps: [
        appsMocks.list(),
        appsMocks.detail(),
        appsMocks.provisionAgent(),
        appsMocks.setupTry(),
        appsMocks.suggestAgent(),
    ],
    bootstrap: [bootstrapMocks.status(), bootstrapMocks.redeemLicense()],
    channels: [channelsMocks.list(), channelsMocks.create(), channelsMocks.update(), channelsMocks.delete()],
    chat: [chatMocks.stream()],
    embedLinks: [embedLinksMocks.list(), embedLinksMocks.mint(), embedLinksMocks.revoke()],
    setup: [
        setupMocks.connect(),
        setupMocks.discover(),
        setupMocks.map(),
        setupMocks.suggestCdc(),
        setupMocks.testMapping(),
        setupMocks.provision(),
    ],
} satisfies Record<string, RequestHandler[]>;

// Handler overrides are keyed by the service names above. A story (or the preview
// default) can replace any subset and keep the rest.
type MockHandlers = Partial<Record<keyof typeof defaultApiMocks, RequestHandler[]>>;

// Type the `msw` story parameter against those keys so overrides autocomplete and a
// mistyped service name is a compile error instead of a silently ignored handler.
declare module "storybook/internal/csf" {
    interface Parameters {
        msw?: {
            handlers: MockHandlers;
        };
    }
}

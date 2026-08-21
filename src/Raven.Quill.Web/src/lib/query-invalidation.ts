import type { QueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import { AI_CONSENT_REQUIRED_MESSAGE } from "@/api/custom-services/assistant-service";
import type { ChannelType } from "@/api/generated/server-api";
import { APP_AI_CONNECTION_STRINGS_KEY } from "@/api/queries/apps-queries";

// The dashboard "My apps" table (stats.dashboardApps) summarizes each app's agent and
// channel counts, so creating or removing an app, agent, or channel makes it stale too —
// not just that entity's own list. Centralizing the fan-out keeps every mutation honest.

// Pass a slug when an existing app changed: its own name, source, and mapping are cached per slug.
export function invalidateAppQueries(queryClient: QueryClient, slug?: string) {
    return Promise.all([
        queryClient.invalidateQueries({ queryKey: api.queries.apps.list().queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.dashboardApps().queryKey }),
        ...(slug
            ? [
                  queryClient.invalidateQueries({ queryKey: api.queries.apps.detail(slug).queryKey }),
                  queryClient.invalidateQueries({ queryKey: api.queries.apps.cdcGet(slug).queryKey }),
                  queryClient.invalidateQueries({ queryKey: api.queries.stats.dashboardApp(slug).queryKey }),
              ]
            : []),
    ]);
}

export function invalidateAgentQueries(queryClient: QueryClient, slug: string) {
    return Promise.all([
        queryClient.invalidateQueries({ queryKey: api.queries.agents.list(slug).queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.dashboardApps().queryKey }),
    ]);
}

export function invalidateAiConnectionStringQueries(queryClient: QueryClient) {
    return Promise.all([
        queryClient.invalidateQueries({ queryKey: api.queries.aiConnectionStrings.list().queryKey }),
        queryClient.invalidateQueries({ queryKey: APP_AI_CONNECTION_STRINGS_KEY }),
    ]);
}

function channelTypeQueryKeys(slug: string, channelType: NonNullable<ChannelType>) {
    switch (channelType) {
        case "Slack":
            return [api.queries.slack.health(slug).queryKey];
        default:
            return [];
    }
}

export function invalidateChannelQueries(queryClient: QueryClient, slug: string, channelType?: ChannelType) {
    return Promise.all([
        queryClient.invalidateQueries({ queryKey: api.queries.channels.list(slug).queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.channels(slug).queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.dashboardApps().queryKey }),
        ...(channelType ? channelTypeQueryKeys(slug, channelType) : []).map((queryKey) =>
            queryClient.invalidateQueries({ queryKey }),
        ),
    ]);
}

export function invalidateConsentBlockedSuggestions(queryClient: QueryClient) {
    return queryClient.invalidateQueries({
        predicate: (query) => isConsentRequiredResult(query.state.data) || isConsentRequiredFailure(query.state.error),
    });
}

function isConsentRequiredResult(data: unknown) {
    return typeof data === "object" && data != null && "isConsentRequired" in data && data.isConsentRequired === true;
}

function isConsentRequiredFailure(error: unknown) {
    return error instanceof Error && error.message === AI_CONSENT_REQUIRED_MESSAGE;
}

import type { QueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
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

export function invalidateChannelQueries(queryClient: QueryClient, slug: string) {
    return Promise.all([
        queryClient.invalidateQueries({ queryKey: api.queries.channels.list(slug).queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.channels(slug).queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.dashboardApps().queryKey }),
    ]);
}

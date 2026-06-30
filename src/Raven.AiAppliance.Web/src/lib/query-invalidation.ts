import type { QueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";

// The dashboard "My apps" table (stats.dashboardApps) summarizes each app's agent and
// channel counts, so creating or removing an app, agent, or channel makes it stale too —
// not just that entity's own list. Centralizing the fan-out keeps every mutation honest.

export function invalidateAppQueries(queryClient: QueryClient) {
    return Promise.all([
        queryClient.invalidateQueries({ queryKey: api.queries.apps.list().queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.dashboardApps().queryKey }),
    ]);
}

export function invalidateAgentQueries(queryClient: QueryClient, slug: string) {
    return Promise.all([
        queryClient.invalidateQueries({ queryKey: api.queries.agents.list(slug).queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.dashboardApps().queryKey }),
    ]);
}

export function invalidateChannelQueries(queryClient: QueryClient, slug: string) {
    return Promise.all([
        queryClient.invalidateQueries({ queryKey: api.queries.channels.list(slug).queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.channels(slug).queryKey }),
        queryClient.invalidateQueries({ queryKey: api.queries.stats.dashboardApps().queryKey }),
    ]);
}

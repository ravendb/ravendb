interface EndpointEntry {
    path: string;
    paramToDisplay?: string;
    isDataSubmission?: boolean;
}

const endpointsWhitelist: EndpointEntry[] = [
    { path: "/databases/{database}/notifications" },
    { path: "/databases/{database}/debug/attachments/hash" },
    { path: "/databases/{database}/data-archival/config" },
    { path: "/databases/{database}/documents-compression/config" },
    { path: "/databases/{database}/expiration/config" },
    { path: "/databases/{database}/indexes" },
    { path: "/databases/{database}/indexes/stats" },
    { path: "/databases/{database}/indexes/staleness" },
    { path: "/databases/{database}/indexes/progress" },
    { path: "/databases/{database}/indexes/errors" },
    { path: "/databases/{database}/indexes/total-time" },
    { path: "/databases/{database}/indexes/performance" },
    { path: "/databases/{database}/debug/io-metrics" },
    { path: "/databases/{database}/debug/perf-metrics" },
    { path: "/databases/{database}/refresh/config" },
    { path: "/databases/{database}/replication/performance" },
    { path: "/databases/{database}/replication/active-connections" },
    { path: "/databases/{database}/replication/debug/outgoing-failures" },
    { path: "/databases/{database}/replication/debug/incoming-last-activity-time" },
    { path: "/databases/{database}/replication/debug/incoming-rejection-info" },
    { path: "/databases/{database}/replication/debug/outgoing-reconnect-queue" },
    { path: "/databases/{database}/replication/conflicts/solver" },
    { path: "/databases/{database}/replication/progress" },
    { path: "/databases/{database}/replication/internal/outgoing/progress" },
    { path: "/databases/{database}/revisions/bin-cleaner/config" },
    { path: "/databases/{database}/revisions/config" },
    { path: "/databases/{database}/revisions/conflicts/config" },
    { path: "/databases/{database}/revisions/bin" },
    { path: "/databases/{database}/stats/essential" },
    { path: "/databases/{database}/metrics" },
    { path: "/databases/{database}/subscriptions/state" },
    { path: "/databases/{database}/debug/subscriptions/resend" },
    { path: "/databases/{database}/subscriptions/connection-details" },
    { path: "/databases/{database}/subscriptions" },
    { path: "/databases/{database}/timeseries/config" },
    { path: "/databases/{database}/debug/documents/huge" },
    { path: "/databases/{database}/debug/documents/scan-corrupted-ids" },
    { path: "/databases/{database}/debug/identities" },
    { path: "/databases/{database}/debug/queries/cache/list" },
    { path: "/databases/{database}/etl/stats" },
    { path: "/databases/{database}/etl/debug/stats" },
    { path: "/databases/{database}/etl/performance" },
    { path: "/databases/{database}/etl/progress" },
    { path: "/databases/{database}/admin/periodic-backup/config" },
    { path: "/databases/{database}/admin/debug/periodic-backup/timers" },
    { path: "/databases/{database}/admin/backup-data-directory" },
    { path: "/databases/{database}/admin/backup/running" },
    { path: "/databases/{database}/admin/debug/cluster/txinfo" },
    { path: "/databases/{database}/admin/configuration/settings" },
    { path: "/databases/{database}/admin/tombstones/state" },
    { path: "/admin/configuration/settings" },
    { path: "/admin/metrics" },
    { path: "/admin/configuration/server-wide/backup" },
    { path: "/admin/configuration/server-wide/tasks" },
    { path: "/admin/server-wide/tasks" },
    { path: "/admin/server-wide/backup-data-directory" },
    { path: "/admin/debug/info/tcp/stats" },
    { path: "/admin/debug/info/tcp/active-connections" },
    { path: "/admin/certificates" },
    { path: "/admin/certificates/replacement/status" },
    { path: "/admin/certificates/letsencrypt/renewal-date" },
    { path: "/admin/certificates/local-state" },
    { path: "/admin/debug/memory/low-mem-log" },
    { path: "/admin/debug/proc/status" },
    { path: "/admin/debug/proc/meminfo" },
    { path: "/admin/debug/memory/stats" },
    { path: "/admin/debug/node/remote-connections" },
    { path: "/admin/debug/node/state-change-history" },
    { path: "/admin/debug/node/ping" },
    { path: "/admin/debug/cpu/stats" },
    { path: "/admin/debug/proc/stats" },
    { path: "/license-server/connectivity" },
    { path: "/certificates/whoami" },
    { path: "/periodic-backup" },
    { path: "/databases" },
    { path: "/databases/{database}/replication/tombstones" },
    { path: "/databases/{database}/collections/last-change-vector" },
    { path: "/cluster/topology" },
    { path: "/admin/configuration/server-wide" },
    { path: "/databases/{database}/healthcheck" },
    { path: "/info/remote-task/topology" },
    { path: "/info/remote-task/tcp" },
    { path: "/admin/cluster/maintenance-stats" },
    { path: "/admin/certificates/mode" },
    { path: "/admin/cluster/observer/decisions" },
    { path: "/databases/{database}/tasks" },
    { path: "/databases/{database}/tasks/pull-replication/hub" },
    { path: "/databases/{database}/info/tcp" },
    { path: "/databases/{database}/tcp" },
    { path: "/info/tcp" },
    { path: "/operations/state" },
    { path: "/periodic-backup/status" },
    { path: "/license/status" },
    { path: "/admin/debug/periodic-backup/timers" },
    { path: "/admin/test/empty-message" },
    { path: "/admin/test/delay" },
    { path: "/admin/test/sized-message" },
    { path: "/license/limits-usage" },
    { path: "/admin/debug/databases/idle" },
    { path: "/databases/{database}/indexes/source" },
    { path: "/databases/{database}/indexes/c-sharp-index-definition" },
    { path: "/databases/{database}/indexes/suggest-index-merge" },
    { path: "/admin/certificates/cluster-domains" },
    { path: "/admin/debug/node/engine-logs" },
    { path: "/admin/debug/script-runners" },
    { path: "/databases/{database}/debug/script-runners" },
    { path: "/admin/stats" },
    { path: "/databases/{database}/docs/size" },
    { path: "/databases/{database}/stats/detailed" },
    { path: "/databases/{database}/collections/stats" },
    { path: "/databases/{database}/collections/stats/detailed" },
    { path: "/databases/{database}/indexes/debug" },
    { path: "/databases/{database}/timeseries/stats" },
    { path: "/databases/{database}/timeseries/debug/segments-summary" },
    { path: "/databases/{database}/debug/sharding/buckets" },
    { path: "/databases/{database}/debug/sharding/bucket" },
    { path: "/databases/{database}/operations" },
    { path: "/databases/{database}/operations/state" },
    { path: "/databases/{database}/debug/attachments/missing" },
    { path: "/databases/{database}/revisions/collections/stats" },
    { path: "/databases/{database}/indexes/status" },
    { path: "/admin/databases" },
    { path: "/databases/{database}/revisions/size" },
    { path: "/databases/{database}/docs/class" },
    { path: "/databases/{database}/debug/attachments/metadata" },
    { path: "/databases/{database}/revisions/count" },
    { path: "/databases/{database}/admin/debug/txinfo" },
    { path: "/debug/sharding/find/bucket" },
    { path: "/databases/{database}/admin/tasks/pull-replication/hub/access" },
    { path: "/topology" },
    { path: "/databases/{database}/task" },
    { path: "/databases/{database}/task" },

    // Additional (not listed in debug endpoints)
    { path: "/databases/{database}/docs", paramToDisplay: "id", isDataSubmission: true },
    { path: "/databases/{database}/collections/docs/ids" },
] as const;

interface EndpointWithRegex extends EndpointEntry {
    regex: RegExp;
}

function getEndpointWithRegex(endpointEntry: EndpointEntry): EndpointWithRegex {
    const regexPattern = _.escapeRegExp(endpointEntry.path).replace(/\\{database\\}/g, "[\\w.-]+");
    const regex = new RegExp(`^${regexPattern}$`);

    return {
        ...endpointEntry,
        regex,
    };
}

const whitelistRegexEndpoints = endpointsWhitelist.map(getEndpointWithRegex);

const dataSubmissionRegexEndpoints = endpointsWhitelist
    .filter((endpoint) => endpoint.isDataSubmission)
    .map(getEndpointWithRegex);

const paramToDisplayRegexEndpoints = endpointsWhitelist
    .filter((endpoint) => endpoint.paramToDisplay)
    .map(getEndpointWithRegex);

export const chatbotConstants = {
    whitelistRegexEndpoints,
    dataSubmissionRegexEndpoints,
    paramToDisplayRegexEndpoints,
};

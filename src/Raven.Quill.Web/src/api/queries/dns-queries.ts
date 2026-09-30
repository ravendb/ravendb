import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "dns";

export function createDnsQueries(api: ServerApi["dns"]) {
    return {
        ipBinding: () =>
            queryOptions({
                queryKey: [baseKey, "ip-binding"],
                queryFn: () => api.ipBinding(),
            }),
    };
}

export type DnsQueries = ReturnType<typeof createDnsQueries>;

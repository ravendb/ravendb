import { queryOptions } from "@tanstack/react-query";
import type { ServerApi, TestMappingRequest } from "@/api/generated/server-api";

const baseKey = "setup";

export function createSetupQueries(api: ServerApi["setup"]) {
    return {
        testMapping: (request: TestMappingRequest) =>
            queryOptions({
                queryKey: [baseKey, "testMapping", request],
                queryFn: () => api.testMapping(request),
            }),
    };
}

export type SetupQueries = ReturnType<typeof createSetupQueries>;

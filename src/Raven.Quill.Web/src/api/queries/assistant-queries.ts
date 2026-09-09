import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "assistant";

export function createAssistantQueries(api: ServerApi["assistant"]) {
    return {
        consent: () =>
            queryOptions({
                queryKey: [baseKey, "consent"],
                queryFn: () => api.consent(),
                staleTime: (query) => (query.state.data?.status === "Success" ? Infinity : 0),
            }),
    };
}

export type AssistantQueries = ReturnType<typeof createAssistantQueries>;

import { queryOptions } from "@tanstack/react-query";
import type { AiModelsRequest, ServerApi } from "@/api/generated/server-api";

const baseKey = "aiModels";

export function createAiModelsQueries(api: ServerApi["aiModels"]) {
    return {
        list: (request: AiModelsRequest) =>
            queryOptions({
                queryKey: [baseKey, "list", request],
                queryFn: () => api.list(request),
                // A wrong API key would fail again on retry; the form just shows no suggestions.
                retry: false,
                // The key embeds the provider credentials, so drop entries (one per settled
                // keystroke) as soon as they go unused instead of retaining them for the
                // default gc window.
                gcTime: 0,
            }),
    };
}

export type AiModelsQueries = ReturnType<typeof createAiModelsQueries>;

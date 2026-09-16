import { API_ENDPOINTS } from "@/api/generated/server-api";
import type { ApiErrorResponse, SuggestCdcRequest, SuggestCdcResponse } from "@/api/generated/server-api";
import type { ApiClient } from "@/api/http-client";

// The AI mapping suggestion runs for a minute or more, and the wizard abandons it as soon as the
// operator changes the inputs it was asked about. The generated client takes no request options, so
// it cannot pass the AbortSignal that cancelling such a call needs.
export function createSetupSuggestionsService(client: ApiClient) {
    return {
        suggestCdc: (request: SuggestCdcRequest, signal?: AbortSignal) =>
            client.post<SuggestCdcResponse, ApiErrorResponse>(API_ENDPOINTS.setup.suggestCdc, request, { signal }),
    };
}

export type SetupSuggestionsService = ReturnType<typeof createSetupSuggestionsService>;

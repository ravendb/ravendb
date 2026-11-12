import { MutationCache, QueryCache, QueryClient } from "@tanstack/react-query";
import { isQuillSessionExpired } from "@/api/http-client";
import { AUTH_STATUS_QUERY_KEY, UNAUTHENTICATED_STATUS } from "@/lib/auth-query";
import { MS_IN } from "@/lib/time";

export function markSessionExpired(client: QueryClient) {
    client.setQueryData(AUTH_STATUS_QUERY_KEY, UNAUTHENTICATED_STATUS);
}

// A 401 with a body is a refusal the AI service sent, such as a missing consent. The session is still there.
function handleUnauthorized(error: unknown) {
    if (isQuillSessionExpired(error)) {
        markSessionExpired(queryClient);
    }
}

export const queryClient = new QueryClient({
    queryCache: new QueryCache({ onError: handleUnauthorized }),
    mutationCache: new MutationCache({ onError: handleUnauthorized }),
    defaultOptions: {
        queries: {
            staleTime: MS_IN.minute,
            refetchOnWindowFocus: false,
            retry: 1,
        },
    },
});

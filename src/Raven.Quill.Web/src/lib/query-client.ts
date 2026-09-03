import { MutationCache, QueryCache, QueryClient } from "@tanstack/react-query";
import { isApiError } from "@/api/http-client";
import { AUTH_STATUS_QUERY_KEY, UNAUTHENTICATED_STATUS } from "@/lib/auth-query";
import { MS_IN } from "@/lib/time";

export function markSessionExpired() {
    queryClient.setQueryData(AUTH_STATUS_QUERY_KEY, UNAUTHENTICATED_STATUS);
}

// Any operator API call that comes back 401 means the session is gone.
function handleUnauthorized(error: unknown) {
    if (isApiError(error) && error.status === 401) {
        markSessionExpired();
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

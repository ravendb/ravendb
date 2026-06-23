import { MutationCache, QueryCache, QueryClient } from "@tanstack/react-query";
import { isApiError } from "@/api/http-client";
import { AUTH_STATUS_QUERY_KEY, UNAUTHENTICATED_STATUS } from "@/lib/auth-query";

// Any operator API call that comes back 401 means the session is gone. Flip the
// cached auth status to unauthenticated so RequireAuth redirects back to /login.
function handleUnauthorized(error: unknown) {
    if (isApiError(error) && error.status === 401) {
        queryClient.setQueryData(AUTH_STATUS_QUERY_KEY, UNAUTHENTICATED_STATUS);
    }
}

export const queryClient = new QueryClient({
    queryCache: new QueryCache({ onError: handleUnauthorized }),
    mutationCache: new MutationCache({ onError: handleUnauthorized }),
    defaultOptions: {
        queries: {
            staleTime: 60_000,
            refetchOnWindowFocus: false,
            retry: 1,
        },
    },
});

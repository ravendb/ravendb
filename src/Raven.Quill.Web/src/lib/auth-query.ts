import type { AuthStatusResponse } from "@/api/generated/server-api";

// Shared cache key + value for the operator auth status, so the auth provider and the
// global 401 handler in the query client read and write the exact same query.
export const AUTH_STATUS_QUERY_KEY = ["auth", "status"] as const;

export const UNAUTHENTICATED_STATUS: AuthStatusResponse = { authenticated: false };

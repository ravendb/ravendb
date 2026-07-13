import type { ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import { AuthContext, type LoginResult } from "@/components/auth/auth-context";
import { AUTH_STATUS_QUERY_KEY, UNAUTHENTICATED_STATUS } from "@/lib/auth-query";

export function AuthProvider({ children }: { children: ReactNode }) {
    const queryClient = useQueryClient();
    const statusQuery = useQuery({
        queryKey: AUTH_STATUS_QUERY_KEY,
        queryFn: () => api.services.auth.status(),
    });
    const isAuthenticated = statusQuery.data?.authenticated === true;

    async function login(apiKey: string): Promise<LoginResult> {
        const status = await api.services.auth.login({ apiKey });

        if (!status.authenticated) {
            queryClient.setQueryData(AUTH_STATUS_QUERY_KEY, status);
            return { authenticated: false };
        }

        const apps = await queryClient.fetchQuery(api.queries.apps.list());
        queryClient.setQueryData(AUTH_STATUS_QUERY_KEY, status);
        return { authenticated: true, hasApps: apps?.length > 0 };
    }

    async function logout() {
        await api.services.auth.logout();
        queryClient.removeQueries({
            predicate: (query) => {
                const root = query.queryKey[0];
                return root !== "auth" && root !== "bootstrap";
            },
        });
        queryClient.setQueryData(AUTH_STATUS_QUERY_KEY, UNAUTHENTICATED_STATUS);
    }

    return (
        <AuthContext.Provider
            value={{
                isAuthenticated,
                isLoading: statusQuery.isPending,
                login,
                logout,
            }}
        >
            {children}
        </AuthContext.Provider>
    );
}

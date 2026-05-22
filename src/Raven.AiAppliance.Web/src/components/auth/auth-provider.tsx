import type { ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { LoginRequest } from "@/api/auth-service";
import { AuthContext } from "@/components/auth/auth-context";

export function AuthProvider({ children }: { children: ReactNode }) {
    const queryClient = useQueryClient();
    const authStatusQuery = api.queries.auth.status();
    const statusQuery = useQuery(authStatusQuery);
    const isAuthenticated = statusQuery.data?.isAuthenticated ?? false;

    async function login(request: LoginRequest) {
        const status = await api.services.auth.login(request);
        queryClient.setQueryData(authStatusQuery.queryKey, status);
        return status.isAuthenticated;
    }

    return (
        <AuthContext.Provider
            value={{
                isAuthenticated,
                isLoading: statusQuery.isPending,
                login,
            }}
        >
            {children}
        </AuthContext.Provider>
    );
}
